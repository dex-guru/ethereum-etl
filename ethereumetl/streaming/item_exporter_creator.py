#  MIT License
#
#  Copyright (c) 2020 Evgeny Medvedev, evge.medvedev@gmail.com
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.
from enum import Enum

from blockchainetl.exporters import BaseItemExporter
from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from blockchainetl.jobs.exporters.elasticsearch_exporter import ElasticsearchItemExporter
from blockchainetl.jobs.exporters.multi_item_exporter import MultiItemExporter
from ethereumetl.enumeration.entity_type import EntityType


def create_item_exporters(outputs, chain_id) -> BaseItemExporter:
    split_outputs = [output.strip() for output in outputs.split(',')] if outputs else ['console']

    item_exporters = [create_item_exporter(output, chain_id) for output in split_outputs]
    return MultiItemExporter(item_exporters)


def make_item_type_to_table_mapping(chain_id: int | None = None) -> dict[EntityType, str]:
    item_type_to_table_mapping = {
        EntityType.BLOCK: 'blocks',
        EntityType.TRANSACTION: 'transactions',
        EntityType.LOG: 'logs',
        EntityType.TOKEN_TRANSFER: 'token_transfers',
        EntityType.TOKEN_BALANCE: 'token_balances',
        EntityType.TRACE: 'traces',
        EntityType.CONTRACT: 'contracts',
        EntityType.TOKEN: 'tokens',
        EntityType.ERROR: 'errors',
        EntityType.GETH_TRACE: 'geth_traces',
        EntityType.INTERNAL_TRANSFER: 'internal_transfers',
        EntityType.NATIVE_BALANCE: 'native_balances',
    }
    if chain_id:
        item_type_to_table_mapping = {
            k: f"{chain_id}_{v}" for k, v in item_type_to_table_mapping.items()
        }
    return item_type_to_table_mapping


def create_item_exporter(output, chain_id) -> BaseItemExporter:
    item_exporter_type = determine_item_exporter_type(output)
    if item_exporter_type == ItemExporterType.PUBSUB:
        from blockchainetl.jobs.exporters.google_pubsub_item_exporter import (
            GooglePubSubItemExporter,
        )

        enable_message_ordering = 'sorted' in output or 'ordered' in output
        item_exporter: BaseItemExporter = GooglePubSubItemExporter(
            item_type_to_topic_mapping={
                EntityType.BLOCK: output + '.blocks',
                EntityType.TRANSACTION: output + '.transactions',
                EntityType.LOG: output + '.logs',
                EntityType.TOKEN_TRANSFER: output + '.token_transfers',
                EntityType.TRACE: output + '.traces',
                EntityType.CONTRACT: output + '.contracts',
                EntityType.TOKEN: output + '.tokens',
            },
            message_attributes=('item_id', 'item_timestamp'),
            batch_max_bytes=1024 * 1024 * 5,
            batch_max_latency=2,
            batch_max_messages=1000,
            enable_message_ordering=enable_message_ordering,
        )
    elif item_exporter_type == ItemExporterType.KINESIS:
        from blockchainetl.jobs.exporters.kinesis_item_exporter import KinesisItemExporter

        item_exporter = KinesisItemExporter(
            stream_name=output[len('kinesis://') :],
        )
    elif item_exporter_type == ItemExporterType.POSTGRES:
        from blockchainetl.jobs.exporters.converters.int_to_decimal_item_converter import (
            IntToDecimalItemConverter,
        )
        from blockchainetl.jobs.exporters.converters.list_field_item_converter import (
            ListFieldItemConverter,
        )
        from blockchainetl.jobs.exporters.converters.unix_timestamp_item_converter import (
            UnixTimestampItemConverter,
        )
        from blockchainetl.jobs.exporters.postgres_item_exporter import PostgresItemExporter
        from blockchainetl.streaming.postgres_utils import create_insert_statement_for_table
        from ethereumetl.streaming.postgres_tables import (
            BLOCKS,
            CONTRACTS,
            LOGS,
            TOKEN_TRANSFERS,
            TOKENS,
            TRACES,
            TRANSACTIONS,
        )

        item_exporter = PostgresItemExporter(
            output,
            item_type_to_insert_stmt_mapping={
                EntityType.BLOCK: create_insert_statement_for_table(BLOCKS),
                EntityType.TRANSACTION: create_insert_statement_for_table(TRANSACTIONS),
                EntityType.LOG: create_insert_statement_for_table(LOGS),
                EntityType.TOKEN_TRANSFER: create_insert_statement_for_table(TOKEN_TRANSFERS),
                EntityType.TRACE: create_insert_statement_for_table(TRACES),
                EntityType.TOKEN: create_insert_statement_for_table(TOKENS),
                EntityType.CONTRACT: create_insert_statement_for_table(CONTRACTS),
            },
            converters=[
                UnixTimestampItemConverter(),
                IntToDecimalItemConverter(),
                ListFieldItemConverter('topics', 'topic', fill=4),
            ],
        )
    elif item_exporter_type == ItemExporterType.GCS:
        from blockchainetl.jobs.exporters.gcs_item_exporter import GcsItemExporter

        bucket, path = get_bucket_and_path_from_gcs_output(output)
        item_exporter = GcsItemExporter(bucket=bucket, path=path)
    elif item_exporter_type == ItemExporterType.CONSOLE:
        item_exporter = ConsoleItemExporter()
    elif item_exporter_type == ItemExporterType.KAFKA:
        from blockchainetl.jobs.exporters.kafka_exporter import KafkaItemExporter

        item_exporter = KafkaItemExporter(
            output,
            item_type_to_topic_mapping={
                EntityType.BLOCK: 'blocks',
                EntityType.TRANSACTION: 'transactions',
                EntityType.LOG: 'logs',
                EntityType.TOKEN_TRANSFER: 'token_transfers',
                EntityType.TRACE: 'traces',
                EntityType.CONTRACT: 'contracts',
                EntityType.TOKEN: 'tokens',
            },
        )
    elif item_exporter_type == ItemExporterType.CLICKHOUSE:
        from blockchainetl.jobs.exporters.clickhouse_exporter import ClickHouseItemExporter

        item_type_to_table_mapping = make_item_type_to_table_mapping(chain_id)
        item_exporter = ClickHouseItemExporter(
            output, item_type_to_table_mapping=item_type_to_table_mapping, chain_id=chain_id
        )
    elif item_exporter_type == ItemExporterType.AMQP:
        from blockchainetl.jobs.exporters.amqp_exporter import AMQPItemExporter

        item_exporter = AMQPItemExporter(amqp_url=output, exchange=f'ethereumetl_{chain_id}')
    elif item_exporter_type == ItemExporterType.ELASTIC:
        output = output.replace('elasticsearch://', 'http://')
        item_exporter = ElasticsearchItemExporter(
            connection_url=output,
            item_type_to_index_mapping={
                EntityType.TOKEN_TRANSFER_PRICED: 'transactions',
            },
            chain_id=chain_id,
        )
    else:
        raise ValueError('Unable to determine item exporter type for output ' + output)

    return item_exporter


def get_bucket_and_path_from_gcs_output(output):
    output = output.replace('gs://', '')
    bucket_and_path = output.split('/', 1)
    bucket = bucket_and_path[0]
    if len(bucket_and_path) > 1:
        path = bucket_and_path[1]
    else:
        path = ''
    return bucket, path


def determine_item_exporter_type(output: str | None) -> 'ItemExporterType':
    if output is None or output == 'console':
        return ItemExporterType.CONSOLE

    if output.startswith('projects'):
        return ItemExporterType.PUBSUB
    if output.startswith('kinesis://'):
        return ItemExporterType.KINESIS
    if output.startswith('kafka'):
        return ItemExporterType.KAFKA
    if output.startswith('postgresql'):
        return ItemExporterType.POSTGRES
    if output.startswith('gs://'):
        return ItemExporterType.GCS
    if output.startswith('clickhouse'):
        return ItemExporterType.CLICKHOUSE
    if output.startswith(('amqp', 'rabbitmq')):
        return ItemExporterType.AMQP
    if (
        output.startswith('elasticsearch')
        or output.startswith('http')
        and output.endswith(':9200')
    ):
        return ItemExporterType.ELASTIC

    return ItemExporterType.UNKNOWN


class ItemExporterType(Enum):
    PUBSUB = 'pubsub'
    KINESIS = 'kinesis'
    POSTGRES = 'postgres'
    GCS = 'gcs'
    CONSOLE = 'console'
    KAFKA = 'kafka'
    CLICKHOUSE = 'clickhouse'
    UNKNOWN = 'unknown'
    AMQP = 'amqp'
    ELASTIC = 'elastic'
