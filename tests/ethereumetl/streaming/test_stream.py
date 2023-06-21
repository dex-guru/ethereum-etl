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

import os
from collections import Counter
from pathlib import Path

import pytest

import tests.resources
from blockchainetl.jobs.exporters.clickhouse_exporter import ClickHouseItemExporter
from blockchainetl.jobs.exporters.composite_item_exporter import CompositeItemExporter
from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
from blockchainetl.streaming.streamer import Streamer
from ethereumetl.config.envs import envs
from ethereumetl.domain.token_transfer import TokenStandard
from ethereumetl.enumeration import entity_type
from ethereumetl.enumeration.entity_type import EntityType
from ethereumetl.streaming.clickhouse_eth_streamer_adapter import ClickhouseEthStreamerAdapter
from ethereumetl.streaming.eth_item_id_calculator import EthItemIdCalculator
from ethereumetl.streaming.eth_streamer_adapter import EthStreamerAdapter
from ethereumetl.streaming.item_exporter_creator import make_item_type_to_table_mapping
from ethereumetl.thread_local_proxy import ThreadLocalProxy
from tests.ethereumetl.job.helpers import get_web3_provider
from tests.helpers import (
    compare_lines_ignore_order,
    read_file,
    run_slow_tests,
    skip_if_slow_tests_disabled,
)

RESOURCE_GROUP = 'test_stream'
TEST_TABLE_NAME_PREFIX = 'test_stream_clickhouse_etl_99'


def read_resource(resource_group, file_name):
    return tests.resources.read_resource([RESOURCE_GROUP, resource_group], file_name)


# fmt: off
@pytest.mark.parametrize("chain_id, start_block, end_block, batch_size, resource_group, entity_types, provider_type", [
    (1, 1755634, 1755635, 1, 'blocks_1755634_1755635', entity_type.ALL_FOR_INFURA, 'mock'),
    skip_if_slow_tests_disabled([1, 1755634, 1755635, 1, 'blocks_1755634_1755635', entity_type.ALL_FOR_INFURA, 'infura']),
    (1, 508110, 508110, 1, 'blocks_508110_508110', ['trace', 'contract', 'token'], 'mock'),
    (1, 2112234, 2112234, 1, 'blocks_2112234_2112234', ['trace', 'contract', 'token'], 'mock'),
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
    try:
        os.remove('last_synced_block.txt')
    except OSError:
        pass

    blocks_output_file = str(tmpdir.join('actual_blocks.json'))
    transactions_output_file = str(tmpdir.join('actual_transactions.json'))
    logs_output_file = str(tmpdir.join('actual_logs.json'))
    token_transfers_output_file = str(tmpdir.join('actual_token_transfers.json'))
    traces_output_file = str(tmpdir.join('actual_traces.json'))
    contracts_output_file = str(tmpdir.join('actual_contracts.json'))
    tokens_output_file = str(tmpdir.join('actual_tokens.json'))

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


@pytest.fixture
def cleanup():
    assert envs.EXPORT_FROM_CLICKHOUSE, 'EXPORT_FROM_CLICKHOUSE env var must be set'

    clickhouse = ClickhouseEthStreamerAdapter.clickhouse_client_from_url(
        envs.EXPORT_FROM_CLICKHOUSE
    )

    def do_cleanup():
        records = clickhouse.query(f"show tables like '{TEST_TABLE_NAME_PREFIX}%'").named_results()
        for record in records:
            table_name = record['name']
            clickhouse.query(f'drop table if exists {table_name}')

    do_cleanup()
    yield
    do_cleanup()


# fmt: off
@pytest.mark.skipif(not envs.EXPORT_FROM_CLICKHOUSE, reason='EXPORT_FROM_CLICKHOUSE env var not set')
@pytest.mark.parametrize("chain_id, start_block, end_block, batch_size, resource_group, entity_types, provider_type", [
    (1, 1755634, 1755635, 1, 'blocks_1755634_1755635', entity_type.ALL, 'mock'),
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
    cleanup,
):
    assert envs.EXPORT_FROM_CLICKHOUSE, 'EXPORT_FROM_CLICKHOUSE env var must be set'

    ####################################################################
    # first run - get data from blockchain
    ####################################################################

    try:
        os.remove('last_synced_block.txt')
    except OSError:
        pass

    batch_web3_provider = ThreadLocalProxy(
        lambda: get_web3_provider(
            provider_type,
            read_resource_lambda=lambda file: read_resource(resource_group, file),
            batch=True,
        )
    )

    item_type_to_table_mapping = make_item_type_to_table_mapping(chain_id)
    item_type_to_table_mapping = {
        k: f"{TEST_TABLE_NAME_PREFIX}_{v}" for k, v in item_type_to_table_mapping.items()
    }

    item_exporter = ClickHouseItemExporter(envs.EXPORT_FROM_CLICKHOUSE, item_type_to_table_mapping)

    eth_streamer_adapter = EthStreamerAdapter(
        batch_web3_provider=batch_web3_provider,
        batch_size=batch_size,
        item_exporter=item_exporter,
        entity_types=entity_types,
    )

    ch_eth_streamer_adapter = ClickhouseEthStreamerAdapter(
        eth_streamer_adapter=eth_streamer_adapter,
        clickhouse_url=envs.EXPORT_FROM_CLICKHOUSE,
        chain_id=chain_id,
        item_type_to_table_mapping=item_type_to_table_mapping,
    )

    streamer = Streamer(
        chain_id=chain_id,
        blockchain_streamer_adapter=ch_eth_streamer_adapter,
        start_block=start_block,
        end_block=end_block,
        retry_errors=False,
    )
    streamer.stream()

    ####################################################################
    # second run - get data from clickhouse
    ####################################################################

    try:
        os.remove('last_synced_block.txt')
    except OSError:
        pass

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
        }
    )

    eth_streamer_adapter = EthStreamerAdapter(
        # expect that we don't use web3 provider and get data from clickhouse
        batch_web3_provider=None,
        batch_size=batch_size,
        item_exporter=item_exporter,
        entity_types=entity_types,
    )
    eth_streamer_adapter.get_current_block_number = lambda *_: 12242307  # type: ignore

    ch_eth_streamer_adapter = ClickhouseEthStreamerAdapter(
        eth_streamer_adapter=eth_streamer_adapter,
        clickhouse_url=envs.EXPORT_FROM_CLICKHOUSE,
        chain_id=chain_id,
        item_type_to_table_mapping=item_type_to_table_mapping,
    )

    streamer = Streamer(
        chain_id=chain_id,
        blockchain_streamer_adapter=ch_eth_streamer_adapter,
        start_block=start_block,
        end_block=end_block,
        retry_errors=False,
    )
    streamer.stream()

    print('=====================')
    print(read_file(blocks_output_file))
    compare_lines_ignore_order(
        read_resource(resource_group, 'expected_blocks.json'), read_file(blocks_output_file)
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
    print(read_file(traces_output_file))
    compare_lines_ignore_order(
        read_resource(resource_group, 'expected_traces.json'), read_file(traces_output_file)
    )

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

    ####################################################################
    # third run - do not rewrite data in clickhouse
    ####################################################################

    os.remove('last_synced_block.txt')

    item_exporter = ClickHouseItemExporter(envs.EXPORT_FROM_CLICKHOUSE, item_type_to_table_mapping)

    def fake_ch_export_items(items):
        counter = Counter((item['type'] for item in items))
        assert dict(counter) == {'trace': 7, 'token_balance': 2}

    item_exporter.export_items = fake_ch_export_items  # type: ignore

    eth_streamer_adapter = EthStreamerAdapter(
        # expect that we don't use web3 provider and get data from clickhouse
        batch_web3_provider=None,
        batch_size=batch_size,
        item_exporter=item_exporter,
        entity_types=entity_types,
    )
    eth_streamer_adapter.get_current_block_number = lambda *_: 12242307  # type: ignore

    ch_eth_streamer_adapter = ClickhouseEthStreamerAdapter(
        eth_streamer_adapter=eth_streamer_adapter,
        clickhouse_url=envs.EXPORT_FROM_CLICKHOUSE,
        chain_id=chain_id,
        item_type_to_table_mapping=item_type_to_table_mapping,
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
    streamer.stream()


@pytest.mark.skipif(not run_slow_tests, reason='slow tests not enabled')
@pytest.mark.parametrize(
    'streamer_adapter_cls', [EthStreamerAdapter, ClickhouseEthStreamerAdapter]
)
def test_stream_token_balances(tmp_path: Path, streamer_adapter_cls, cleanup):
    exporter = InMemoryItemExporter(item_types=[EntityType.TOKEN_BALANCE])

    eth_streamer_adapter = EthStreamerAdapter(
        batch_web3_provider=ThreadLocalProxy(lambda: get_web3_provider('infura', batch=True)),
        batch_size=20,
        item_exporter=exporter,
        entity_types=[EntityType.TOKEN_BALANCE],
    )

    if streamer_adapter_cls is EthStreamerAdapter:
        streamer_adapter = eth_streamer_adapter
    elif streamer_adapter_cls is ClickhouseEthStreamerAdapter:
        streamer_adapter = ClickhouseEthStreamerAdapter(
            eth_streamer_adapter=eth_streamer_adapter,
            clickhouse_url=envs.EXPORT_FROM_CLICKHOUSE,
            chain_id=1,
            item_type_to_table_mapping=make_item_type_to_table_mapping(chain_id=1),
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


def test_clickhouse_exporter(tmp_path, cleanup):
    item_type_to_table_mapping = make_item_type_to_table_mapping(chain_id=1)
    item_type_to_table_mapping = {
        k: f"{TEST_TABLE_NAME_PREFIX}_{v}" for k, v in item_type_to_table_mapping.items()
    }
    exporter = ClickHouseItemExporter(envs.EXPORT_FROM_CLICKHOUSE, item_type_to_table_mapping)

    streamer_adapter = EthStreamerAdapter(
        batch_web3_provider=ThreadLocalProxy(lambda: get_web3_provider('infura', batch=True)),
        batch_size=100,
        item_exporter=exporter,
        entity_types=entity_type.ALL_FOR_INFURA,
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
            table_name = item_type_to_table_mapping[entity_type]
            records = clickhouse.query(f"SELECT * FROM {table_name} LIMIT 1").named_results()
            record = next(records, None)
            assert record, f"Table {table_name} is empty"

        assert_table_not_empty(EntityType.BLOCK)
        assert_table_not_empty(EntityType.TRANSACTION)
        assert_table_not_empty(EntityType.LOG)
        assert_table_not_empty(EntityType.TOKEN_TRANSFER)
        assert_table_not_empty(EntityType.TOKEN_BALANCE)


def test_clickhouse_exporter_export_items(tmp_path, cleanup):
    item_type_to_table_mapping = make_item_type_to_table_mapping(chain_id=1)
    item_type_to_table_mapping = {
        k: f"{TEST_TABLE_NAME_PREFIX}_{v}" for k, v in item_type_to_table_mapping.items()
    }
    exporter = ClickHouseItemExporter(envs.EXPORT_FROM_CLICKHOUSE, item_type_to_table_mapping)
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
            table_name = item_type_to_table_mapping[entity_type]
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
