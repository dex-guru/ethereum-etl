# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
import logging
import random

import click
from blockchainetl.streaming.streaming_utils import configure_signals, configure_logging
from ethereumetl.config.envs import envs
from ethereumetl.enumeration.entity_type import EntityType

from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.streaming.item_exporter_creator import (
    create_item_exporters,
    make_item_type_to_table_mapping,
)
from ethereumetl.thread_local_proxy import ThreadLocalProxy


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-c', '--chain-id', default=envs.CHAIN_ID, show_default=True, type=int, help='Chain ID')
@click.option('-l', '--last-synced-block-provider-uri', default=envs.LAST_SYNCED_BLOCK_PROVIDER, show_default=True, type=str,
            help='examples:'
            ' file://relative/path/to/file.txt'
            ' redis://localhost:6379/0?key=last_synced_block'
            ' postgresql://user:pass@localhost:5432/db?table_name=last_synced_block'
            ' clickhouse://default:@localhost:8123?table_name=last_synced_block'
            ' clickhouse+native://default:@localhost:9000?table_name=last_synced_block'
            ' or any SQLAlchemy supported connection string.'
            ' Query parameters:'
            ' table_name, sync_id - table name and primary key for SQL databases;'
            ' key - key for Redis.'
             )
@click.option('--lag', default=envs.LAG, show_default=True, type=int, help='The number of blocks to lag behind the network.')
@click.option('-p', '--provider-uri', default=envs.PROVIDER_URL, show_default=True, type=str,
              help='The URI of the web3 provider e.g. '
                   'file://$HOME/Library/Ethereum/geth.ipc or https://mainnet.infura.io')
@click.option('-o', '--output', type=str, default=envs.OUTPUT, show_default=True,
              help='Either Google PubSub topic path e.g. projects/your-project/topics/crypto_ethereum; '
                   'or Postgres connection url e.g. postgresql+pg8000://postgres:admin@127.0.0.1:5432/ethereum; '
                   'or Clickhouse connection url e.g. clickhouse://default:@localhost/ethereum; '
                   'or GCS bucket e.g. gs://your-bucket-name; '
                   'or kafka, output name and connection host:port e.g. kafka/127.0.0.1:9092 '
                   'or Kinesis, e.g. kinesis://your-data-stream-name'
                   'If not specified will print to console')
@click.option('-s', '--start-block', default=envs.START_BLOCK, show_default=True, type=int, help='Start block')
@click.option('-e', '--entity-types', default=envs.ENTITY_TYPES, show_default=True, type=str,
              help='The list of entity types to export.')
@click.option('--period-seconds', default=envs.POLLING_PERIOD, show_default=True, type=int, help='How many seconds to sleep between syncs')
@click.option('-b', '--batch-size', default=envs.BATCH_SIZE, show_default=True, type=int, help='How many blocks to batch in single request')
@click.option('-B', '--block-batch-size', default=envs.BLOCK_BATCH_SIZE, show_default=True, type=int, help='How many blocks to batch in single sync round')
@click.option('-w', '--max-workers', default=envs.MAX_WORKERS, show_default=True, type=int, help='The number of workers')
@click.option('--log-file', default=None, show_default=True, type=str, help='Log file')
@click.option('--pid-file', default=None, show_default=True, type=str, help='pid file')
@click.option('--export-from-clickhouse', default=envs.EXPORT_FROM_CLICKHOUSE, show_default=True, type=str,
             help='connection URL to Clickhouse containing data from previous exports, e.g. clickhouse://default:@localhost/ethereum')
def stream(chain_id, last_synced_block_provider_uri, lag, provider_uri, output, start_block, entity_types,
           period_seconds=10, batch_size=2, block_batch_size=10, max_workers=5, log_file=None, pid_file=None,
           export_from_clickhouse=None):
    """Streams all data types to console or Google Pub/Sub."""
    configure_logging(log_file)
    configure_signals()
    entity_types = parse_entity_types(entity_types)

    from ethereumetl.streaming.eth_streamer_adapter import EthStreamerAdapter
    from ethereumetl.streaming.clickhouse_eth_streamer_adapter import ClickhouseEthStreamerAdapter
    from blockchainetl.streaming.streamer import Streamer

    # TODO: Implement fallback mechanism for provider uris instead of picking randomly
    provider_uri = pick_random_provider_uri(provider_uri)
    logging.info('Using ' + provider_uri)

    streamer_adapter = EthStreamerAdapter(
        batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, batch=True)),
        item_exporter=create_item_exporters(output, chain_id),
        batch_size=batch_size,
        max_workers=max_workers,
        entity_types=entity_types
    )
    if export_from_clickhouse:
        streamer_adapter = ClickhouseEthStreamerAdapter(
            eth_streamer_adapter=streamer_adapter,
            clickhouse_url=export_from_clickhouse,
            chain_id=chain_id,
            item_type_to_table_mapping=make_item_type_to_table_mapping(chain_id)
        )

    streamer = Streamer(
        chain_id=chain_id,
        blockchain_streamer_adapter=streamer_adapter,
        last_synced_block_provider_uri=last_synced_block_provider_uri,
        lag=lag,
        start_block=start_block,
        period_seconds=period_seconds,
        block_batch_size=block_batch_size,
        pid_file=pid_file
    )
    streamer.stream()


def parse_entity_types(entity_types):
    entity_types = [c.strip() for c in entity_types.split(',')]

    # validate passed types
    for entity_type in entity_types:
        if entity_type not in EntityType.ALL_FOR_STREAMING:
            raise click.BadOptionUsage(
                '--entity-type', '{} is not an available entity type. Supply a comma separated list of types from {}'
                    .format(entity_type, ','.join(EntityType.ALL_FOR_STREAMING)))

    return entity_types


def pick_random_provider_uri(provider_uri):
    provider_uris = [uri.strip() for uri in provider_uri.split(',')]
    return random.choice(provider_uris)
