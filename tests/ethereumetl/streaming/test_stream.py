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

import contextlib
import json
import os
from collections import Counter
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from elasticsearch import Elasticsearch
from retry import retry

import tests.resources
from blockchainetl.jobs.exporters.clickhouse_exporter import ClickHouseItemExporter
from blockchainetl.jobs.exporters.composite_item_exporter import CompositeItemExporter
from blockchainetl.jobs.exporters.elasticsearch_exporter import ElasticsearchItemExporter
from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
from blockchainetl.streaming.streamer import Streamer
from blockchainetl.streaming.streamer_adapter_stub import StreamerAdapterStub
from ethereumetl.domain.token_transfer import TokenStandard
from ethereumetl.enumeration import entity_type
from ethereumetl.enumeration.entity_type import ALL, EntityType
from ethereumetl.providers.rpc import BatchHTTPProvider
from ethereumetl.streaming.clickhouse_eth_streamer_adapter import (
    ClickhouseEthStreamerAdapter,
    VerifyingClickhouseEthStreamerAdapter,
)
from ethereumetl.streaming.eth_item_id_calculator import EthItemIdCalculator
from ethereumetl.streaming.eth_streamer_adapter import EthStreamerAdapter
from ethereumetl.streaming.item_exporter_creator import create_item_exporter
from ethereumetl.thread_local_proxy import ThreadLocalProxy
from tests.ethereumetl.job.helpers import get_web3_provider
from tests.helpers import (
    compare_lines_ignore_order,
    read_file,
    run_slow_tests,
    skip_if_slow_tests_disabled,
)

RESOURCE_GROUP = 'test_stream'


def read_resource(resource_group, file_name):
    return tests.resources.read_resource([RESOURCE_GROUP, resource_group], file_name)


# fmt: off
@pytest.mark.parametrize("chain_id, start_block, end_block, batch_size, resource_group, entity_types, provider_type", [
    (1, 1755634, 1755635, 1, 'blocks_1755634_1755635', entity_type.ALL_FOR_INFURA, 'mock'),
    skip_if_slow_tests_disabled(
        [1, 1755634, 1755635, 1, 'blocks_1755634_1755635', entity_type.ALL_FOR_INFURA, 'infura']),
    (1, 508110, 508110, 1, 'blocks_508110_508110', ['trace', 'contract', 'token'], 'infura'),
    (1, 2112234, 2112234, 1, 'blocks_2112234_2112234', ['trace', 'contract'], 'infura'),
])
# fmt: on
def test_stream(
    tmpdir,
    chain_id,
    start_block,
    end_block,
    batch_size,
    resource_group,
    entity_types,
    provider_type,
):
    with contextlib.suppress(OSError):
        os.remove('last_synced_block.txt')

    blocks_output_file = str(tmpdir.join('actual_blocks.json'))
    transactions_output_file = str(tmpdir.join('actual_transactions.json'))
    logs_output_file = str(tmpdir.join('actual_logs.json'))
    token_transfers_output_file = str(tmpdir.join('actual_token_transfers.json'))
    traces_output_file = str(tmpdir.join('actual_traces.json'))
    contracts_output_file = str(tmpdir.join('actual_contracts.json'))
    tokens_output_file = str(tmpdir.join('actual_tokens.json'))
    token_transfers_priced_output_file = str(tmpdir.join('actual_token_transfers_priced.json'))

    streamer_adapter = EthStreamerAdapter(
        batch_web3_provider=ThreadLocalProxy(
            lambda: get_web3_provider(
                provider_type,
                read_resource_lambda=lambda file: read_resource(resource_group, file),
                write_resource_lambda=lambda file, content: tests.resources.write_resource(
                    [RESOURCE_GROUP, resource_group], file, content
                ),
                batch=True,
            )
        ),
        batch_size=batch_size,
        item_exporter=CompositeItemExporter(
            filename_mapping={
                'block': blocks_output_file,
                'transaction': transactions_output_file,
                'log': logs_output_file,
                'token_transfer': token_transfers_output_file,
                'token_balance': '/dev/null',
                'trace': traces_output_file,
                'contract': contracts_output_file,
                'token': tokens_output_file,
                'token_transfer_priced': token_transfers_priced_output_file,
            }
        ),
        entity_types=entity_types,
    )
    streamer = Streamer(
        chain_id=chain_id,
        blockchain_streamer_adapter=streamer_adapter,
        start_block=start_block,
        end_block=end_block,
        retry_errors=False,
    )
    streamer.stream()

    if 'block' in entity_types:
        print('=====================')
        print(read_file(blocks_output_file))
        compare_lines_ignore_order(
            read_resource(resource_group, 'expected_blocks.json'), read_file(blocks_output_file)
        )

    if 'transaction' in entity_types:
        print('=====================')
        print(read_file(transactions_output_file))
        compare_lines_ignore_order(
            read_resource(resource_group, 'expected_transactions.json'),
            read_file(transactions_output_file),
        )

    if 'log' in entity_types:
        print('=====================')
        print(read_file(logs_output_file))
        compare_lines_ignore_order(
            read_resource(resource_group, 'expected_logs.json'), read_file(logs_output_file)
        )

    if 'token_transfer' in entity_types:
        print('=====================')
        print(read_file(token_transfers_output_file))
        compare_lines_ignore_order(
            read_resource(resource_group, 'expected_token_transfers.json'),
            read_file(token_transfers_output_file),
        )

    if 'trace' in entity_types:
        print('=====================')
        print(read_file(traces_output_file))
        compare_lines_ignore_order(
            read_resource(resource_group, 'expected_traces.json'), read_file(traces_output_file)
        )

    if 'contract' in entity_types:
        print('=====================')
        print(read_file(contracts_output_file))
        compare_lines_ignore_order(
            read_resource(resource_group, 'expected_contracts.json'),
            read_file(contracts_output_file),
        )

    if 'token' in entity_types:
        print('=====================')
        print(read_file(tokens_output_file))
        compare_lines_ignore_order(
            read_resource(resource_group, 'expected_tokens.json'), read_file(tokens_output_file)
        )


# fmt: off
@pytest.mark.parametrize("chain_id, start_block, end_block, batch_size, resource_group, entity_types, provider_type", [
    (1, 1755634, 1755635, 1, 'blocks_1755634_1755635', {*ALL} - {EntityType.RECEIPT}, 'mock'),
])
# fmt: on
def test_stream_clickhouse(
    tmpdir,
    chain_id,
    start_block,
    end_block,
    batch_size,
    resource_group,
    entity_types,
    provider_type,
    clickhouse_migrated_url,
    elastic_url,
):
    ####################################################################
    # first run - get data from blockchain
    ####################################################################

    with contextlib.suppress(OSError):
        os.remove('last_synced_block.txt')

    batch_web3_provider = ThreadLocalProxy(
        lambda: get_web3_provider(
            provider_type,
            read_resource_lambda=lambda file: read_resource(resource_group, file),
            # write_resource_lambda=lambda file, content: tests.resources.write_resource(
            #     [RESOURCE_GROUP, resource_group], file, content
            # ),
            batch=True,
        )
    )

    item_exporter = ClickHouseItemExporter(clickhouse_migrated_url)

    eth_streamer_adapter = EthStreamerAdapter(
        batch_web3_provider=batch_web3_provider,
        batch_size=batch_size,
        item_exporter=item_exporter,
        entity_types=entity_types,
        elastic_client=Elasticsearch(elastic_url, timeout=30),
        chain_id=1,
    )

    ch_eth_streamer_adapter = ClickhouseEthStreamerAdapter(
        eth_streamer=eth_streamer_adapter,
        clickhouse_url=clickhouse_migrated_url,
        chain_id=chain_id,
    )

    streamer = Streamer(
        chain_id=chain_id,
        blockchain_streamer_adapter=ch_eth_streamer_adapter,
        start_block=start_block,
        end_block=end_block,
        retry_errors=False,
    )
    streamer.stream()  # 1st run

    ####################################################################
    # second run - get data from clickhouse
    ####################################################################

    with contextlib.suppress(OSError):
        os.remove('last_synced_block.txt')

    blocks_output_file = str(tmpdir.join('actual_blocks.json'))
    transactions_output_file = str(tmpdir.join('actual_transactions.json'))
    logs_output_file = str(tmpdir.join('actual_logs.json'))
    token_transfers_output_file = str(tmpdir.join('actual_token_transfers.json'))
    token_balances_output_file = str(tmpdir.join('actual_token_balances.json'))
    traces_output_file = str(tmpdir.join('actual_traces.json'))
    contracts_output_file = str(tmpdir.join('actual_contracts.json'))
    tokens_output_file = str(tmpdir.join('actual_tokens.json'))
    geth_traces_output_file = str(tmpdir.join('actual_geth_traces.json'))
    internal_transfers_output_file = str(tmpdir.join('actual_internal_transfers.json'))
    native_balances_output_file = str(tmpdir.join('actual_native_balances.json'))

    item_exporter = CompositeItemExporter(  # type: ignore
        filename_mapping={
            EntityType.BLOCK: blocks_output_file,
            EntityType.TRANSACTION: transactions_output_file,
            EntityType.LOG: logs_output_file,
            EntityType.TOKEN_TRANSFER: token_transfers_output_file,
            EntityType.TOKEN_BALANCE: token_balances_output_file,
            EntityType.TRACE: traces_output_file,
            EntityType.CONTRACT: contracts_output_file,
            EntityType.TOKEN: tokens_output_file,
            EntityType.GETH_TRACE: geth_traces_output_file,
            EntityType.INTERNAL_TRANSFER: internal_transfers_output_file,
            EntityType.NATIVE_BALANCE: native_balances_output_file,
        }
    )

    fake_batch_web3_provider = Mock(spec=BatchHTTPProvider)
    fake_batch_web3_provider.make_batch_request.side_effect = Exception(
        'make_batch_request() should not be called'
    )
    fake_batch_web3_provider.make_request.side_effect = Exception(
        'make_request() should not be called'
    )

    eth_streamer_adapter = EthStreamerAdapter(
        # expect that we don't use web3 provider and get data from clickhouse
        batch_web3_provider=fake_batch_web3_provider,
        batch_size=batch_size,
        item_exporter=item_exporter,
        entity_types=entity_types
        - {EntityType.TOKEN_TRANSFER_PRICED, EntityType.INTERNAL_TRANSFER_PRICED},
        chain_id=1,
    )
    eth_streamer_adapter.get_current_block_number = lambda *_: 12242307  # type: ignore

    ch_eth_streamer_adapter = ClickhouseEthStreamerAdapter(
        eth_streamer=eth_streamer_adapter,
        clickhouse_url=clickhouse_migrated_url,
        chain_id=chain_id,
    )

    streamer = Streamer(
        chain_id=chain_id,
        blockchain_streamer_adapter=ch_eth_streamer_adapter,
        start_block=start_block,
        end_block=end_block,
        retry_errors=False,
    )
    streamer.stream()  # 2nd run

    print('=====================')
    print(read_file(blocks_output_file))

    # This is a bad thing, but this is because the current architecture
    # of the program takes item dicts from clickhouse as is without validation
    # and filtering extra fields.
    actual_blocks = '\n'.join(
        json.dumps({k: v for k, v in json.loads(line).items() if k != 'is_reorged'})
        for line in read_file(blocks_output_file).splitlines()
    )

    compare_lines_ignore_order(
        read_resource(resource_group, 'expected_blocks.json'), actual_blocks
    )

    print('=====================')
    print(read_file(transactions_output_file))
    compare_lines_ignore_order(
        read_resource(resource_group, 'expected_transactions.json'),
        read_file(transactions_output_file),
    )

    print('=====================')
    print(read_file(logs_output_file))
    compare_lines_ignore_order(
        read_resource(resource_group, 'expected_logs.json'), read_file(logs_output_file)
    )

    print('=====================')
    print(read_file(token_transfers_output_file))
    compare_lines_ignore_order(
        read_resource(resource_group, 'expected_token_transfers.json'),
        read_file(token_transfers_output_file),
    )

    print('=====================')

    # trace_index field is not written to Clickhouse, so remove it from the actual output.
    # The expected output file is a legacy file so probably need to fix it in the future.
    traces = '\n'.join(
        json.dumps({field: v for field, v in json.loads(line).items() if field != 'trace_index'})
        for line in read_file(traces_output_file).splitlines()
        if line.strip()
    )
    print(traces)
    compare_lines_ignore_order(read_resource(resource_group, 'expected_traces.json'), traces)

    print('=====================')
    print(read_file(contracts_output_file))
    compare_lines_ignore_order(
        read_resource(resource_group, 'expected_contracts.json'), read_file(contracts_output_file)
    )

    print('=====================')
    print(read_file(tokens_output_file))
    compare_lines_ignore_order(
        read_resource(resource_group, 'expected_tokens.json'), read_file(tokens_output_file)
    )

    print('=====================')
    print(read_file(native_balances_output_file))
    compare_lines_ignore_order(
        read_resource(resource_group, 'expected_native_balances.json'),
        read_file(native_balances_output_file),
    )

    ####################################################################
    # third run - do not rewrite data in clickhouse
    ####################################################################

    os.remove('last_synced_block.txt')

    item_exporter = ClickHouseItemExporter(clickhouse_migrated_url)

    def fake_ch_export_items(items):
        items = [
            item for item in items if item['type'] in item_exporter.item_type_to_table_mapping
        ]
        counter = Counter(item['type'] for item in items)
        assert dict(counter) == {'trace': 7, 'token_balance': 2}

    item_exporter.export_items = fake_ch_export_items  # type: ignore

    eth_streamer_adapter = EthStreamerAdapter(
        # expect that we don't use web3 provider and get data from clickhouse
        batch_web3_provider=fake_batch_web3_provider,
        batch_size=batch_size,
        item_exporter=item_exporter,
        entity_types=entity_types
        - {EntityType.TOKEN_TRANSFER_PRICED, EntityType.INTERNAL_TRANSFER_PRICED},
        chain_id=1,
    )
    eth_streamer_adapter.get_current_block_number = lambda *_: 12242307  # type: ignore

    ch_eth_streamer_adapter = ClickhouseEthStreamerAdapter(
        eth_streamer=eth_streamer_adapter,
        clickhouse_url=clickhouse_migrated_url,
        chain_id=chain_id,
        rewrite_entity_types=(  # <--------------------------------------- checking this
            EntityType.TOKEN_BALANCE,
            EntityType.TRACE,
        ),
    )

    streamer = Streamer(
        chain_id=chain_id,
        blockchain_streamer_adapter=ch_eth_streamer_adapter,
        start_block=start_block,
        end_block=end_block,
        retry_errors=False,
    )
    streamer.stream()  # 3rd run


@pytest.mark.skipif(not run_slow_tests, reason='slow tests not enabled')
@pytest.mark.parametrize(
    'streamer_adapter_cls', [EthStreamerAdapter, ClickhouseEthStreamerAdapter]
)
def test_stream_token_balances(tmp_path: Path, streamer_adapter_cls, clickhouse_url):
    exporter = InMemoryItemExporter(item_types=[EntityType.TOKEN_BALANCE])

    eth_streamer_adapter = EthStreamerAdapter(
        batch_web3_provider=ThreadLocalProxy(lambda: get_web3_provider('infura', batch=True)),
        batch_size=20,
        item_exporter=exporter,
        entity_types=[EntityType.TOKEN_BALANCE],
    )

    if streamer_adapter_cls is EthStreamerAdapter:
        streamer_adapter: StreamerAdapterStub = eth_streamer_adapter
    elif streamer_adapter_cls is ClickhouseEthStreamerAdapter:
        streamer_adapter = ClickhouseEthStreamerAdapter(
            eth_streamer=eth_streamer_adapter,
            clickhouse_url=clickhouse_url,
            chain_id=1,
        )
    else:
        raise NotImplementedError(f'Unknown streamer adapter class: {streamer_adapter_cls}')

    streamer = Streamer(
        chain_id=1,
        blockchain_streamer_adapter=streamer_adapter,
        start_block=17179063,
        end_block=17179063,
        retry_errors=False,
        last_synced_block_provider_uri=f"file://{tmp_path / 'last_synced_block.txt'}",
    )
    streamer.stream()

    token_balance_items = sorted(
        exporter.get_items(EntityType.TOKEN_BALANCE),
        key=lambda x: (x['token_address'], x['holder_address'], x['token_id']),
    )

    first_erc1155_token_balance_item = next(
        item for item in token_balance_items if item['token_standard'] == TokenStandard.ERC1155
    )
    assert first_erc1155_token_balance_item == {
        'block_hash': '0x97ebb349d7ab33966221767701765deb064362405a3a4a878d252465700ed350',
        'block_number': 17179063,
        'type': 'token_balance',
        'token_address': '0xd1988bea35478229ebee68331714b215e3529510',
        'token_standard': TokenStandard.ERC1155,
        'holder_address': '0x3597770531bd28805a688003c5bc0292f4b9bf2c',
        'token_id': 2,
        'value': 1,
        'block_timestamp': 1683103055,
        'item_id': (
            'token_balance'
            '_17179063'
            '_0xd1988bea35478229ebee68331714b215e3529510'
            '_0x3597770531bd28805a688003c5bc0292f4b9bf2c'
            '_2'
        ),
        'item_timestamp': '2023-05-03T08:37:35Z',
    }

    first_erc721_token_balance_item = next(
        item for item in token_balance_items if item['token_standard'] == TokenStandard.ERC721
    )

    assert first_erc721_token_balance_item == {
        'block_hash': '0x97ebb349d7ab33966221767701765deb064362405a3a4a878d252465700ed350',
        'block_number': 17179063,
        'block_timestamp': 1683103055,
        'holder_address': '0x48a90cfb0a65a73f820c3c915acd9399851aa739',
        'item_id': (
            'token_balance'
            '_17179063'
            '_0x104e73df39c6d90e4159dca7f13890b6402a2f1b'
            '_0x48a90cfb0a65a73f820c3c915acd9399851aa739'
            '_122'
        ),
        'item_timestamp': '2023-05-03T08:37:35Z',
        'token_address': '0x104e73df39c6d90e4159dca7f13890b6402a2f1b',
        'token_id': 122,
        'token_standard': TokenStandard.ERC721,
        'type': 'token_balance',
        'value': 1,
    }

    first_erc20_token_balance = next(
        item for item in token_balance_items if item['token_standard'] == TokenStandard.ERC20
    )

    assert first_erc20_token_balance == {
        'block_hash': '0x97ebb349d7ab33966221767701765deb064362405a3a4a878d252465700ed350',
        'block_number': 17179063,
        'block_timestamp': 1683103055,
        'holder_address': '0x994002da75a0003235b0cc34705d235ad90418de',
        'item_id': (
            'token_balance'
            '_17179063'
            '_0x0000000000a39bb272e79075ade125fd351887ac'
            '_0x994002da75a0003235b0cc34705d235ad90418de'
            '_0'
        ),
        'item_timestamp': '2023-05-03T08:37:35Z',
        'token_address': '0x0000000000a39bb272e79075ade125fd351887ac',
        'token_id': 0,
        'token_standard': TokenStandard.ERC20,
        'type': 'token_balance',
        'value': 900000000000000000,
    }

    assert len(token_balance_items) == 429


def test_eth_streamer_with_clickhouse_exporter(tmp_path, clickhouse_migrated_url, elastic_url):
    exporter = ClickHouseItemExporter(clickhouse_migrated_url)

    streamer_adapter = EthStreamerAdapter(
        batch_web3_provider=ThreadLocalProxy(lambda: get_web3_provider('infura', batch=True)),
        batch_size=10,
        item_exporter=exporter,
        entity_types=entity_type.ALL,
        elastic_client=Elasticsearch(elastic_url),
        chain_id=1,
    )
    streamer = Streamer(
        chain_id=1,
        blockchain_streamer_adapter=streamer_adapter,
        start_block=17179063,
        end_block=17179063,
        retry_errors=False,
        last_synced_block_provider_uri=f"file://{tmp_path / 'last_synced_block.txt'}",
    )
    streamer.stream()

    with exporter.create_connection() as clickhouse:

        def assert_table_not_empty(entity_type):
            table_name = exporter.item_type_to_table_mapping[entity_type]
            records = clickhouse.query(f"SELECT * FROM {table_name} LIMIT 1").named_results()
            record = next(records, None)
            assert record, f"Table {table_name} is empty"

        assert_table_not_empty(EntityType.BLOCK)
        assert_table_not_empty(EntityType.TRANSACTION)
        assert_table_not_empty(EntityType.LOG)
        assert_table_not_empty(EntityType.TOKEN_TRANSFER)
        assert_table_not_empty(EntityType.TOKEN_BALANCE)
        assert_table_not_empty(EntityType.GETH_TRACE)
        assert_table_not_empty(EntityType.INTERNAL_TRANSFER)
        assert_table_not_empty(EntityType.NATIVE_BALANCE)


def test_clickhouse_exporter_export_items(tmp_path, clickhouse_migrated_url):
    exporter = ClickHouseItemExporter(clickhouse_migrated_url)
    exporter.open()
    items = [
        {
            'type': 'error',
            'item_id': 'error_123',
            'timestamp': 1686069627,
            'block_number': 123,
            'block_timestamp': 1656329840,
            'kind': 'some_error',
            'data_json': '{"some": "data"}',
            'block_hash': '0x123',
        },
        {
            'type': 'token_balance',
            'block_hash': '0x97ebb349d7ab33966221767701765deb064362405a3a4a878d252465700ed350',
            'block_number': 17179063,
            'block_timestamp': 1683103055,
            'holder_address': '0x994002da75a0003235b0cc34705d235ad90418de',
            'item_id': (
                'token_balance'
                '_17179063'
                '_0x0000000000a39bb272e79075ade125fd351887ac'
                '_0x994002da75a0003235b0cc34705d235ad90418de'
                '_0'
            ),
            'item_timestamp': '2023-05-03T08:37:35Z',
            'token_address': '0x0000000000a39bb272e79075ade125fd351887ac',
            'token_id': 0,
            'token_standard': TokenStandard.ERC20,
            'value': 900000000000000000,
        },
    ]

    try:
        exporter.export_items(items)
    finally:
        exporter.close()

    with exporter.create_connection() as clickhouse:

        def assert_table_not_empty(entity_type):
            table_name = exporter.item_type_to_table_mapping[entity_type]
            records = clickhouse.query(f"SELECT * FROM {table_name} LIMIT 1").named_results()
            record = next(records, None)
            assert record, f"Table {table_name} is empty"

        assert_table_not_empty(EntityType.ERROR)
        assert_table_not_empty(EntityType.TOKEN_BALANCE)


def test_item_id_calculator_id_fields_contains_all_entity_types():
    for type_ in EntityType:
        if type_ == EntityType.RECEIPT:
            # Receipts are not exported currently
            continue

        assert (
            type_ in EthItemIdCalculator.ID_FIELDS
        ), f"missing item_id calculator for entity type {type_!r}"


@pytest.fixture()
def ch_verifier(clickhouse_url):
    exporter = ClickHouseItemExporter(clickhouse_url)

    class NoMethodsShouldBeCalled:
        def __getattr__(self, item):
            raise AssertionError(f"method {item} should not be called")

    eth_streamer_adapter = EthStreamerAdapter(
        batch_web3_provider=NoMethodsShouldBeCalled,
        batch_size=20,
        item_exporter=exporter,
        entity_types=[EntityType.TOKEN_BALANCE],
    )

    ch_streamer_adapter = ClickhouseEthStreamerAdapter(
        eth_streamer=eth_streamer_adapter,
        clickhouse_url=clickhouse_url,
        chain_id=1,
    )

    adapter = VerifyingClickhouseEthStreamerAdapter(
        clickhouse_eth_streamer_adapter=ch_streamer_adapter,
    )
    adapter.open()
    yield adapter
    adapter.close()


@patch.object(ClickhouseEthStreamerAdapter, 'select_distinct')
@patch.object(EthStreamerAdapter, 'export_blocks_and_transactions')
@patch.object(ClickhouseEthStreamerAdapter, 'export_all')
def test_verify_all_with_consistent_data(
    export_all_mock,
    mock_export_blocks_and_transactions,
    mock_select_distinct,
    ch_verifier,
):
    # Configure the mock return values
    mock_select_distinct.side_effect = (
        [
            {'number': 1, 'hash': 'block_hash_1', 'transaction_count': 3},
            {'number': 2, 'hash': 'block_hash_2', 'transaction_count': 2},
            {'number': 3, 'hash': 'block_hash_3', 'transaction_count': 1},
        ],
        [
            {
                'hash': 'txn_hash_1',
                'block_hash': 'block_hash_1',
                'block_number': 1,
                'transaction_index': 0,
                'from_address': 'address_1',
                'to_address': 'address_2',
                'value': 100,
                'gas': 200,
                'gas_price': 20,
                'input': 'input_1',
                'nonce': 1,
            },
            {
                'hash': 'txn_hash_2',
                'block_hash': 'block_hash_1',
                'block_number': 1,
                'transaction_index': 1,
                'from_address': 'address_2',
                'to_address': 'address_3',
                'value': 200,
                'gas': 300,
                'gas_price': 30,
                'input': 'input_2',
                'nonce': 2,
            },
            {
                'hash': 'txn_hash_3',
                'block_hash': 'block_hash_1',
                'block_number': 1,
                'transaction_index': 2,
                'from_address': 'address_3',
                'to_address': 'address_4',
                'value': 300,
                'gas': 400,
                'gas_price': 40,
                'input': 'input_3',
                'nonce': 3,
            },
        ],
    )
    mock_export_blocks_and_transactions.return_value = (
        [
            {'number': 1, 'hash': 'block_hash_1', 'transaction_count': 3},
            {'number': 2, 'hash': 'block_hash_2', 'transaction_count': 2},
            {'number': 3, 'hash': 'block_hash_3', 'transaction_count': 1},
        ],
        [
            {
                'hash': 'txn_hash_1',
                'block_hash': 'block_hash_1',
                'block_number': 1,
                'transaction_index': 0,
                'from_address': 'address_1',
                'to_address': 'address_2',
                'value': 100,
                'gas': 200,
                'gas_price': 20,
                'input': 'input_1',
                'nonce': 1,
            },
            {
                'hash': 'txn_hash_2',
                'block_hash': 'block_hash_1',
                'block_number': 1,
                'transaction_index': 1,
                'from_address': 'address_2',
                'to_address': 'address_3',
                'value': 200,
                'gas': 300,
                'gas_price': 30,
                'input': 'input_2',
                'nonce': 2,
            },
            {
                'hash': 'txn_hash_3',
                'block_hash': 'block_hash_1',
                'block_number': 1,
                'transaction_index': 2,
                'from_address': 'address_3',
                'to_address': 'address_4',
                'value': 300,
                'gas': 400,
                'gas_price': 40,
                'input': 'input_3',
                'nonce': 3,
            },
        ],
    )

    ch_verifier.export_all(start_block=1, end_block=3)

    assert export_all_mock.not_called()


@patch.object(ClickhouseEthStreamerAdapter, 'select_distinct')
@patch.object(EthStreamerAdapter, 'export_blocks_and_transactions')
@patch.object(EthStreamerAdapter, 'export_receipts_and_logs')
@patch.object(EthStreamerAdapter, 'export_all', new_callable=Mock)
def test_verify_all_with_inconsistent_data(
    export_all_mock,
    mock_export_receipts_and_logs,
    mock_export_blocks_and_transactions,
    mock_select_distinct,
    ch_verifier,
):
    fake_ch_client = ch_verifier.ch_streamer.clickhouse = Mock()

    # Configure the mock return values to introduce inconsistencies
    mock_select_distinct.side_effect = (
        [
            {'number': 1, 'hash': 'block_hash_1', 'transaction_count': 3, 'timestamp': 10},
            {'number': 3, 'hash': 'invalid', 'transaction_count': 1, 'timestamp': 30},
        ],
        [
            {
                'hash': 'txn_hash_1',
                'block_hash': 'block_hash_1',
                'block_number': 1,
                'transaction_index': 0,
                'from_address': 'address_2',
                'to_address': 'address_3',
                'value': 200,
                'gas': 300,
                'gas_price': 30,
                'input': 'input_2',
                'nonce': 2,
            },
            {
                'hash': 'txn_hash_2',
                'block_hash': 'block_hash_2',
                'block_number': 2,
                'transaction_index': 1,
                'from_address': 'address_2',
                'to_address': 'address_3',
                'value': 200,
                'gas': 300,
                'gas_price': 30,
                'input': 'input_2',
                'nonce': 2,
            },
            {
                'hash': 'txn_hash_3',
                'block_hash': 'block_hash_3',
                'block_number': 3,
                'transaction_index': 2,
                'from_address': 'address_3',
                'to_address': 'address_4',
                'value': 300,
                'gas': 400,
                'gas_price': 40,
                'input': 'input_3',
                'nonce': 3,
            },
        ],
    )

    mock_blockchain_data = {
        "blocks": [
            {
                'type': 'block',
                'number': 1,
                'hash': 'block_hash_1',
                'transaction_count': 3,
                'timestamp': 10,
            },
            {
                'type': 'block',
                'number': 2,
                'hash': 'block_hash_2',
                'transaction_count': 1,
                'timestamp': 20,
            },
            {
                'type': 'block',
                'number': 3,
                'hash': 'block_hash_3',
                'transaction_count': 2,
                'timestamp': 30,
            },
        ],
        "transactions": [
            {
                'type': 'transaction',
                'hash': 'txn_hash_1',
                'block_hash': 'block_hash_1',
                'block_number': 1,
                'transaction_index': 0,
                'from_address': 'address_2',
                'to_address': 'address_3',
                'value': 200,
                'gas': 300,
                'gas_price': 30,
                'input': 'input_2',
                'nonce': 2,
            },
            {
                'type': 'transaction',
                'hash': 'txn_hash_2',
                'block_hash': 'block_hash_2',
                'block_number': 2,
                'transaction_index': 1,
                'from_address': 'address_2',
                'to_address': 'address_3',
                'value': 200,
                'gas': 300,
                'gas_price': 30,
                'input': 'input_2',
                'nonce': 2,
            },
            {
                'type': 'transaction',
                'hash': 'txn_hash_4',
                'block_hash': 'block_hash_3',
                'block_number': 3,
                'transaction_index': 0,
                'from_address': 'address_3',
                'to_address': 'address_4',
                'value': 300,
                'gas': 400,
                'gas_price': 40,
                'input': 'input_4',
                'nonce': 3,
            },
        ],
        "receipts": [
            {
                'type': 'receipt',
                'transaction_hash': 'txn_hash_1',
                'transaction_index': 0,
                'block_hash': 'block_hash_1',
                'block_number': 1,
                'gas_used': 100,
                'cumulative_gas_used': 100,
                'contract_address': 'contract_address_1',
                'root': 'root_1',
                'status': 1,
                'effective_gas_price': 10,
            },
            {
                'type': 'receipt',
                'transaction_hash': 'txn_hash_2',
                'transaction_index': 1,
                'block_hash': 'block_hash_2',
                'block_number': 2,
                'gas_used': 200,
                'cumulative_gas_used': 200,
                'contract_address': 'contract_address_2',
                'root': 'root_2',
                'status': 1,
                'effective_gas_price': 20,
            },
            {
                'type': 'receipt',
                'transaction_hash': 'txn_hash_4',
                'transaction_index': 0,
                'block_hash': 'block_hash_3',
                'block_number': 3,
                'gas_used': 300,
                'cumulative_gas_used': 300,
                'contract_address': 'contract_address_4',
                'root': 'root_4',
                'status': 1,
                'effective_gas_price': 40,
            },
        ],
        "logs": [
            {
                'type': 'log',
                'transaction_hash': 'txn_hash_1',
                'transaction_index': 0,
                'block_hash': 'block_hash_1',
                'block_number': 1,
                'address': 'address_1',
                'topics': ['topic_1_1', 'topic_1_2'],
                'data': 'data_1',
                'log_index': 0,
            },
            {
                'type': 'log',
                'transaction_hash': 'txn_hash_2',
                'transaction_index': 1,
                'block_hash': 'block_hash_2',
                'block_number': 2,
                'address': 'address_2',
                'topics': ['topic_2_1', 'topic_2_2'],
                'data': 'data_2',
                'log_index': 1,
            },
            {
                'type': 'log',
                'transaction_hash': 'txn_hash_3',
                'transaction_index': 2,
                'block_hash': 'block_hash_3',
                'block_number': 3,
                'address': 'address_3',
                'topics': ['topic_3_1', 'topic_3_2'],
                'data': 'data_3',
                'log_index': 2,
            },
        ],
    }
    mock_export_blocks_and_transactions.return_value = (
        mock_blockchain_data['blocks'],
        mock_blockchain_data['transactions'],
    )
    mock_export_receipts_and_logs.return_value = (
        mock_blockchain_data['receipts'],
        mock_blockchain_data['logs'],
        [],  # errors
    )

    # Call the method under test
    ch_verifier.export_all(start_block=1, end_block=3)

    fake_ch_client.command.assert_any_call(
        "INSERT INTO blocks"
        " SELECT * EXCEPT is_reorged, 1 FROM blocks"
        " WHERE number IN (3,)"
        " AND timestamp IN (30,)"
        " AND hash IN ('invalid',)"
    )
    fake_ch_client.command.assert_any_call(
        "INSERT INTO transactions"
        " SELECT * EXCEPT is_reorged, 1 FROM transactions"
        " WHERE block_number IN (3,)"
        " AND block_timestamp IN (30,)"
        " AND block_hash IN ('invalid',)"
    )

    export_all_mock.assert_called_with(start_block=2, end_block=3)


def test_elastic_export_items(tmp_path, elastic_url):
    exporter: ElasticsearchItemExporter = create_item_exporter(elastic_url, chain_id=1)  # type: ignore
    items = [
        {
            "token_addresses": ["tess_address"],
            "wallets": ["wallet_1", "wallet_2"],
            "direction": {"from_address": "wallet_1", "to_address": "wallet_2"},
            "transaction_address": "test_hash",
            "block_number": 123,
            "id": "test_hash-0",
            "transfer_type": "erc20",
            "timestamp": 123,
            "transaction_type": "transfer",
            "chain_id": 1,
            "symbols": ["TST"],
            "prices_stable": [0],
            "amount_stable": 0.0,
            "amounts": [10.0],
            "type": "token_transfer_priced",
        },
        {
            "token_addresses": ["tess_address"],
            "wallets": ["wallet_1", "wallet_2"],
            "direction": {"from_address": "wallet_1", "to_address": "wallet_2"},
            "transaction_address": "test_hash",
            "block_number": 123,
            "id": "test_hash-1",
            "transfer_type": "erc20",
            "timestamp": 123,
            "transaction_type": "transfer",
            "chain_id": 1,
            "symbols": ["TST"],
            "prices_stable": [0],
            "amount_stable": 0.0,
            "amounts": [10.0],
            "type": "token_transfer_priced",
        },
    ]

    expected_items = [
        {
            'token_addresses': ['tess_address'],
            'wallets': ['wallet_1', 'wallet_2'],
            'direction': {'from_address': 'wallet_1', 'to_address': 'wallet_2'},
            'transaction_address': 'test_hash',
            'block_number': 123,
            'id': 'test_hash-0',
            'timestamp': 123,
            'transaction_type': 'transfer',
            'chain_id': 1,
            'symbols': ['TST'],
            'prices_stable': [0],
            'amount_stable': 0.0,
            'amounts': [10.0],
            'type': 'erc20',
            'doc_type': 'transaction',
        },
        {
            'token_addresses': ['tess_address'],
            'wallets': ['wallet_1', 'wallet_2'],
            'direction': {'from_address': 'wallet_1', 'to_address': 'wallet_2'},
            'transaction_address': 'test_hash',
            'block_number': 123,
            'id': 'test_hash-1',
            'timestamp': 123,
            'transaction_type': 'transfer',
            'chain_id': 1,
            'symbols': ['TST'],
            'prices_stable': [0],
            'amount_stable': 0.0,
            'amounts': [10.0],
            'type': 'erc20',
            'doc_type': 'transaction',
        },
    ]

    @retry(AssertionError, tries=10, delay=0.2)
    def check_indexed_items(elastic):
        index = exporter.item_type_to_index_mapping[EntityType.TOKEN_TRANSFER_PRICED]
        records = elastic.search(
            index=index, body={'query': {'bool': {'filter': {'term': {'block_number': 123}}}}}
        )
        assert records['hits']['total']['value'] == 2
        for hit in records['hits']['hits']:
            assert hit['_source'] in expected_items

    exporter.open()
    try:
        exporter.export_items(items)
        assert exporter.client
        with exporter.client as elastic:
            check_indexed_items(elastic)
    finally:
        exporter.close()
