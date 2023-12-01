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

import click
import clickhouse_connect

from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
from blockchainetl.logging_utils import logging_basic_config
from ethereumetl.jobs.export_blocks_job import ExportBlocksJob
from ethereumetl.jobs.exporters.blocks_and_transactions_item_exporter import (
    blocks_and_transactions_item_exporter,
)
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.thread_local_proxy import ThreadLocalProxy
from ethereumetl.utils import check_classic_provider_uri, parse_clickhouse_url

logging_basic_config()


@click.command(context_settings={'help_option_names': ['-h', '--help']})
@click.option(
    '-s',
    '--from-timestamp',
    default=0,
    show_default=True,
    type=int,
    help='Start timestamp.',
)
@click.option(
    '-e',
    '--to-timestamp',
    default=0,
    show_default=True,
    type=int,
    help='End timestamp.',
)
@click.option(
    '-b',
    '--batch-size',
    default=100,
    show_default=True,
    type=int,
    help='The number of blocks to export at a time.',
)
@click.option(
    '-p',
    '--provider-uri',
    default='https://mainnet.infura.io',
    show_default=True,
    type=str,
    help='The URI of the web3 provider e.g. '
    'file://$HOME/Library/Ethereum/geth.ipc or https://mainnet.infura.io',
)
@click.option(
    '-w',
    '--max-workers',
    default=5,
    show_default=True,
    type=int,
    help='The maximum number of workers.',
)
@click.option(
    '--blocks-output',
    required=True,
    type=str,
    help='The output file for blocks. If not provided blocks will not be exported. Use "-" for stdout',
)
@click.option(
    '--blocks-reorged-output',
    required=True,
    type=str,
    help='The output file for reorged blocks. '
    'If not provided transactions will not be exported. Use "-" for stdout',
)
@click.option(
    '-u',
    '--clickhouse-url',
    default='clickhouse://localhost:9000',
    show_default=True,
    type=str,
    help='The ClickHouse url to connect to.',
)
def get_reorged_and_finalized_blocks(
    from_timestamp,
    to_timestamp,
    batch_size,
    provider_uri,
    max_workers,
    blocks_reorged_output,
    blocks_output,
    clickhouse_url,
):
    """Exports blocks."""

    connect_kwargs = parse_clickhouse_url(clickhouse_url)
    clickhouse_client = clickhouse_connect.create_client(
        **connect_kwargs, compress=False, query_limit=0, send_receive_timeout=600
    )
    blocks_from_storage = clickhouse_client.query(
        f"""
        SELECT number, hash
        FROM blocks
        WHERE timestamp >= {from_timestamp} AND timestamp < {to_timestamp}
        ORDER BY number
        """
    ).named_results()

    if not blocks_from_storage:
        logging.info('No blocks found for the given timestamp range')
    blocks_from_storage = [block for block in blocks_from_storage]
    in_memory_exporter = InMemoryItemExporter(['block'])

    job = ExportBlocksJob(
        start_block=blocks_from_storage[0]['number'],
        end_block=blocks_from_storage[-1]['number'],
        batch_size=batch_size,
        batch_web3_provider=ThreadLocalProxy(
            lambda: get_provider_from_uri(provider_uri, batch=True)
        ),
        max_workers=max_workers,
        item_exporter=in_memory_exporter,
        export_blocks=blocks_output is not None,
        export_transactions=False,
    )
    job.run()
    blocks_w3 = in_memory_exporter.get_items('block')

    exporter_for_blocks = blocks_and_transactions_item_exporter(blocks_output)
    exporter_for_reorged_blocks = blocks_and_transactions_item_exporter(blocks_reorged_output)
    exporter_for_blocks.open()
    exporter_for_reorged_blocks.open()
    for block_from_storage, block_w3 in zip(
        blocks_from_storage, sorted(blocks_w3, key=lambda x: x['number'])
    ):
        if block_from_storage['hash'] == block_w3['hash']:
            exporter_for_blocks.export_item(block_w3)
        else:
            exporter_for_reorged_blocks.export_item(block_w3)
    exporter_for_blocks.close()
    exporter_for_reorged_blocks.close()


# export_blocks_by_timestamp.callback(
#     from_timestamp=1701355629,
#     to_timestamp=1701355729,
#     batch_size=10,
#     provider_uri="http://rpc-gw-stage.dexguru.biz/full/1",
#     max_workers=5,
#     blocks_output="blocks.csv",
#     blocks_reorged_output="blocks_reorged.csv",
#     clickhouse_url="http+clickhouse://testuser3:testplpassword@10.0.100.170/ethereum",
# )
