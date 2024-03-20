import logging
import sys
from typing import Any

import click
import kombu
from elasticsearch import Elasticsearch
from kombu import Connection, Consumer, Exchange, Queue

from blockchainetl.streaming.streamer_adapter_stub import StreamerAdapterStub
from blockchainetl.streaming.streaming_utils import configure_logging, configure_signals
from ethereumetl.config.envs import envs
from ethereumetl.enumeration.entity_type import ALL_FOR_STREAMING
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.streaming.clickhouse_eth_streamer_adapter import ClickhouseEthStreamerAdapter
from ethereumetl.streaming.eth_streamer_adapter import EthStreamerAdapter
from ethereumetl.streaming.item_exporter_creator import create_item_exporters
from ethereumetl.thread_local_proxy import ThreadLocalProxy


class AmqpStreamerAdapter:
    """
    Consumes items from AMQP broker.
    """

    def __init__(
        self,
        amqp_url: str,
        eth_streamer: StreamerAdapterStub,
        routing_key: str,
        queue_name: str,
        exchange_name: str,
    ):
        self._eth_streamer = eth_streamer
        self._exchange_name = exchange_name
        self._dlx_name = f'{self._exchange_name}_dlx'
        self._queue_name = queue_name
        self._amqp_url = amqp_url
        self._routing_key = routing_key

        self._connection: Connection = ...
        self._consumer: Consumer = ...
        self._dlx: Exchange = ...
        self._dlx_producer: kombu.Producer = ...
        self.closed = True

    def open(self):
        """
        raises: IOError.
        """
        self._eth_streamer.open()

        if not self._connection:
            raise RuntimeError('already opened')

        self._connection = Connection(
            self._amqp_url,
            connect_timeout=10,
            transport_options={'max_retries': 1},
        )
        consume_channel = self._connection.channel()
        publish_channel = self._connection.channel()
        exchange = Exchange(
            self._exchange_name,
            type='direct',
            durable=True,
        )
        self._consumer = Consumer(
            consume_channel,
            queues=Queue(
                self._queue_name,
                exchange=exchange,
                routing_key=self._routing_key,
            ),
            callbacks=[self._process_message],
            accept=['json'],
            auto_declare=True,
            no_ack=False,
            prefetch_count=1,
        )
        self._dlx = Exchange(
            name=self._dlx_name,
            type='direct',
            durable=True,
        )
        self._dlx_producer = self._connection.Producer(
            channel=publish_channel,
            exchange=self._dlx,
            routing_key='dlq',
        )
        assert self._consumer is not ...
        assert self._connection is not ...
        assert self._dlx is not ...
        assert self._dlx_producer is not ...

        self.closed = False

    def close(self):
        self.closed = True
        if self._connection is not None:
            self._consumer.recover(requeue=True)
            self._consumer.close()
            self._eth_streamer.close()
            self._connection.close()
            self._connection = None
            self._consumer = None

    def get_current_block_number(self) -> int:
        return self._eth_streamer.get_current_block_number()

    def _process_message(self, body: list[dict[str, Any]], message: kombu.Message) -> None:
        if self.closed:
            return
        items_type = body[0]['type']
        if items_type is None:
            logging.warning('Received message without item_type: %s', body)
            self._send_to_dlq(body)
            message.ack()
            return
        elif items_type == 'block':
            start_block, end_block = body[0]['number'], body[-1]['number']
        else:
            start_block, end_block = body[0]['block_number'], body[-1]['block_number']
        self._process_item(start_block, end_block, body)
        message.ack()

    def _process_item(self, start_block: int, end_block: int, body: list[dict[str, Any]]) -> None:
        logging.info('Processing items from block %s to %s', start_block, end_block)
        try:
            self._eth_streamer.export_all(start_block, end_block)
        except Exception as e:
            # send to dead letter queue
            logging.error('Failed to process message: %s', e, exc_info=True)
            self._send_to_dlq(body)

    def _send_to_dlq(self, body: list[dict[str, Any]]) -> None:
        self._dlx_producer.publish(
            body,
        )

    def consume(self):
        if self.closed:
            raise RuntimeError('streamer is closed')
        with self._consumer:
            # Consume messages indefinitely
            self._consumer.consume()

            while True:
                try:
                    self._connection.drain_events()
                except (KeyboardInterrupt, SystemExit):
                    self.close()
                    break
                except Exception as e:
                    logging.error('Failed to consume message: %s', e)
                    self._reopen()

    def _reopen(self):
        self.close()
        self.open()


@click.command(context_settings={'help_option_names': ['-h', '--help']})
@click.option(
    '-c',
    '--chain-id',
    default=envs.CHAIN_ID,
    show_default=True,
    type=int,
    help='The chain network to connect to.',
)
@click.option(
    '-p',
    '--provider-uri',
    default=envs.PROVIDER_URL,
    show_default=True,
    type=str,
    help='The URI of the web3 provider e.g. '
    'file://$HOME/Library/Ethereum/geth.ipc or https://mainnet.infura.io',
)
@click.option(
    '-a',
    '--amqp-url',
    default=envs.AMQP_URL,
    show_default=True,
    type=str,
    help='The URI of the AMQP broker e.g. ',
)
@click.option(
    '-e',
    '--entity-types',
    default=envs.ENTITY_TYPES,
    show_default=True,
    type=str,
    help='The entity types to export.',
)
@click.option(
    '--export-from-clickhouse',
    default=envs.EXPORT_FROM_CLICKHOUSE,
    show_default=True,
    type=str,
    help='Export from ClickHouse. Example: clickhouse://default:@localhost:9000/ethereum',
)
@click.option(
    '-o',
    '--output',
    default=envs.OUTPUT,
    show_default=True,
    type=str,
    help='The output file. If not specified stdout is used.',
)
@click.option(
    '-b',
    '--batch-size',
    default=envs.BATCH_SIZE,
    show_default=True,
    type=int,
    help='The number of items to batch in one request.',
)
@click.option(
    '-w',
    '--max-workers',
    default=envs.MAX_WORKERS,
    show_default=True,
    type=int,
    help='The maximum number of workers.',
)
@click.option(
    '-l',
    '--elastic-url',
    default=envs.ELASTIC_URL,
    show_default=True,
    type=str,
    help='The URL of the ElasticSearch instance to export to.',
)
@click.option(
    '-r',
    '--routing-key',
    default=envs.ROUTING_KEY,
    show_default=True,
    type=str,
)
@click.option(
    '-q',
    '--queue-name',
    default=envs.QUEUE_NAME,
    show_default=True,
    type=str,
)
@click.option(
    '--exchange-name',
    default=envs.EXCHANGE_NAME,
    show_default=True,
    type=str,
)
def amqp_stream(
    chain_id,
    provider_uri,
    amqp_url,
    entity_types,
    export_from_clickhouse,
    output,
    batch_size,
    max_workers,
    elastic_url,
    routing_key,
    queue_name=None,
    exchange_name=None,
):
    """Streams data to AMQP broker."""
    sys.setrecursionlimit(3000)
    configure_signals()
    configure_logging(None)
    queue_name = (
        f'{chain_id}_indexation_etl_{queue_name}'
        if queue_name
        else f'{chain_id}_indexation_etl_{entity_types}'
    )
    exchange_name = (
        f'{chain_id}_{exchange_name}' if exchange_name else f'{chain_id}_indexation_etl'
    )
    entity_types = entity_types.split(',')
    if not entity_types:
        raise RuntimeError('Entity types are not specified')
    streamer_adapter: StreamerAdapterStub
    streamer_adapter = EthStreamerAdapter(
        batch_web3_provider=ThreadLocalProxy(
            lambda: get_provider_from_uri(provider_uri, batch=True)
        ),
        item_exporter=create_item_exporters(output, chain_id),
        batch_size=batch_size,
        max_workers=max_workers,
        entity_types=entity_types,
        chain_id=chain_id,
        elastic_client=Elasticsearch(elastic_url) if elastic_url else None,
    )
    if export_from_clickhouse:
        rewrite_entity_types = ALL_FOR_STREAMING

        streamer_adapter = ClickhouseEthStreamerAdapter(
            eth_streamer=streamer_adapter,
            clickhouse_url=export_from_clickhouse,
            chain_id=chain_id,
            rewrite_entity_types=rewrite_entity_types,
        )
    amqp_streamer_adapter = AmqpStreamerAdapter(
        amqp_url=amqp_url,
        eth_streamer=streamer_adapter,
        routing_key=routing_key,
        queue_name=queue_name,
        exchange_name=exchange_name,
    )
    try:
        amqp_streamer_adapter.open()
    except (OSError, AssertionError) as e:
        logging.error('Failed to open: %s', e)
        exit(1)

    try:
        amqp_streamer_adapter.consume()
    except RuntimeError as e:
        logging.error('Failed to consume: %s', e)
    finally:
        amqp_streamer_adapter.close()


# amqp_stream.callback(
#     chain_id=137,
#     provider_uri='http://rpc-gw-stage.dexguru.biz/full/137',
#     amqp_url='amqp://guest:guest@localhost:5672/dex',
#     entity_types='block,transaction,log',
#     export_from_clickhouse='clickhouse+http://testuser3:testplpassword@stage-ch-polygon-01.dexguru.biz/polygon',
#     output='amqp://guest:guest@localhost:5672/dex',
#     batch_size=5,
#     max_workers=10,
#     elastic_url='http://10.0.100.34:9200',
#     routing_key='block',
#     queue_name='verify_all',
#     exchange_name='verifier_etl',
# )
