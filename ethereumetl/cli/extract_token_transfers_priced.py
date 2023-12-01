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


import csv
import json
from collections.abc import Iterable

import click
from elasticsearch import Elasticsearch

from blockchainetl.file_utils import smart_open
from blockchainetl.jobs.exporters.converters.int_to_string_item_converter import (
    IntToStringItemConverter,
)
from blockchainetl.logging_utils import logging_basic_config
from ethereumetl.csv_utils import set_max_field_size_limit
from ethereumetl.jobs.exporters.token_transfers_priced_item_exporter import (
    token_transfers_priced_item_exporter,
)
from ethereumetl.jobs.extract_token_transfers_priced import ExtractTokenTransfersPricedJob

logging_basic_config()

set_max_field_size_limit()


@click.command(context_settings={'help_option_names': ['-h', '--help']})
@click.option(
    '-t',
    '--token-transfers',
    type=str,
    required=True,
    help='The CSV/JSON file containing token transfers.',
)
@click.option(
    '--tokens',
    type=str,
    required=True,
    help='The CSV/JSON file containing token information.',
)
@click.option(
    '-b',
    '--batch-size',
    default=100,
    show_default=True,
    type=int,
    help='The number of blocks to filter at a time.',
)
@click.option(
    '-o',
    '--output',
    default='-',
    show_default=True,
    type=str,
    help='The output file. If not specified stdout is used.',
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
    '--values-as-strings',
    default=False,
    show_default=True,
    is_flag=True,
    help='Whether to convert values to strings.',
)
@click.option(
    '-c',
    '--chain-id',
    required=True,
    help='The chain network to connect to.',
)
@click.option(
    '-e',
    '--elastic-url',
    required=True,
    help='The elasticsearch host to connect to.',
)
def extract_token_transfers_priced(
    token_transfers,
    tokens,
    batch_size,
    output,
    max_workers,
    chain_id,
    elastic_url,
    values_as_strings=False,
):
    """Enriches token transfers with USD prices."""
    elastic_client = Elasticsearch(elastic_url)
    with smart_open(token_transfers, 'r') as transfers_file:
        if token_transfers.endswith('.json'):
            transfer_records: Iterable[dict] = (json.loads(line) for line in transfers_file)
        else:
            transfer_records = csv.DictReader(transfers_file)
        converters = [IntToStringItemConverter(keys=['value'])] if values_as_strings else []
        with smart_open(tokens, 'r') as tokens_file:
            if tokens.endswith('.json'):
                token_records: Iterable[dict] = (json.loads(line) for line in tokens_file)
            else:
                token_records = csv.DictReader(tokens_file)
            tokens = [token for token in token_records]
        job = ExtractTokenTransfersPricedJob(
            token_transfers=transfer_records,
            tokens=tokens,
            chain_id=chain_id,
            batch_size=batch_size,
            max_workers=max_workers,
            item_exporter=token_transfers_priced_item_exporter(output, converters=converters),
            elastic_client=elastic_client,
        )
        job.run()
