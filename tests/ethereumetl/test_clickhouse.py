import json
from collections.abc import Sequence

import pytest
from clickhouse_connect.driver.client import Client

from blockchainetl.jobs.exporters.clickhouse_exporter import ClickHouseItemExporter
from ethereumetl.enumeration.entity_type import EntityType
from ethereumetl.scripts.load_abi_to_event_inventory import load_abis_to_event_inventory
from ethereumetl.streaming.clickhouse_eth_streamer_adapter import ClickhouseEthStreamerAdapter
from ethereumetl.streaming.eth_streamer_adapter import EthStreamerAdapter


@pytest.fixture()
def clickhouse_adapter(clickhouse_url) -> ClickhouseEthStreamerAdapter:
    item_exporter = ClickHouseItemExporter(clickhouse_url)

    eth_streamer_adapter = EthStreamerAdapter(
        batch_web3_provider=lambda _: None,
        batch_size=1,
        item_exporter=item_exporter,
        entity_types=[EntityType.BLOCK],
        chain_id=1,
    )

    ch_eth_streamer_adapter = ClickhouseEthStreamerAdapter(
        eth_streamer=eth_streamer_adapter,
        clickhouse_url=clickhouse_url,
        chain_id=1,
    )
    ch_eth_streamer_adapter.open()
    try:
        yield ch_eth_streamer_adapter
    finally:
        ch_eth_streamer_adapter.close()


def insert_records(clickhouse: Client, table: str, records: Sequence[dict]):
    columns = records[0].keys()
    data = [[r[col] for col in columns] for r in records]
    clickhouse.insert(table, data, columns)


def test_events_mat_view(clickhouse_migrated):
    event_infos = [
        {
            'event_signature_hash': '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62',
            'event_signature': 'TransferSingle(address,address,address,uint256,uint256)',
            'event_topic_count': 4,
            'event_name': 'TransferSingle',
            'namespace': "base",
            'contract_name': 'ERC1155',
            'event_abi_json': json.dumps(
                {
                    'anonymous': False,
                    # fmt: off
                    'inputs': [
                        {'indexed': True, 'internalType': 'address', 'name': 'operator', 'type': 'address'},
                        {'indexed': True, 'internalType': 'address', 'name': 'from', 'type': 'address'},
                        {'indexed': True, 'internalType': 'address', 'name': 'to', 'type': 'address'},
                        {'indexed': False, 'internalType': 'uint256', 'name': 'id', 'type': 'uint256'},
                        {'indexed': False, 'internalType': 'uint256', 'name': 'value', 'type': 'uint256'},
                    ],
                    # fmt: on
                    'name': 'TransferSingle',
                    'type': 'event',
                }
            ),
        },
    ]

    # Add some event infos to be captured my the MaterializedView
    insert_records(clickhouse_migrated, 'event_inventory_src', event_infos)

    # event_inventory_src
    assert list(
        clickhouse_migrated.query('SELECT * FROM event_inventory_src').named_results()
    ) == [
        {
            'event_abi_json': event_infos[0]['event_abi_json'],
            'event_name': event_infos[0]['event_name'],
            'event_signature': event_infos[0]['event_signature'],
            'event_signature_hash': event_infos[0]['event_signature_hash'],
            'event_topic_count': event_infos[0]['event_topic_count'],
            'namespace': event_infos[0]['namespace'],
            'contract_name': event_infos[0]['contract_name'],
        },
    ]

    # event_inventory
    assert list(clickhouse_migrated.query('SELECT * FROM event_inventory').named_results()) == [
        {
            'event_abi_json': event_infos[0]['event_abi_json'],
            'event_name': event_infos[0]['event_name'],
            'event_signature': event_infos[0]['event_signature'],
            'event_signature_hash_and_log_topic_count': (
                '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62',
                4,
            ),
            'namespace': [event_infos[0]['namespace']],
            'contract_name': [event_infos[0]['contract_name']],
        },
    ]


def test_load_abis_to_event_inventory(clickhouse_migrated_url, clickhouse, tmp_path):
    abi = [
        {
            'anonymous': False,
            'inputs': [
                {
                    'indexed': True,
                    'internalType': 'address',
                    'name': 'operator',
                    'type': 'address',
                },
                {'indexed': True, 'internalType': 'address', 'name': 'from', 'type': 'address'},
                {'indexed': True, 'internalType': 'address', 'name': 'to', 'type': 'address'},
                {'indexed': False, 'internalType': 'uint256', 'name': 'id', 'type': 'uint256'},
                {'indexed': False, 'internalType': 'uint256', 'name': 'value', 'type': 'uint256'},
            ],
            'name': 'TransferSingle',
            'type': 'event',
        },
    ]
    file_name1 = 'abi1.json'
    file_name2 = 'abi2.json'
    subdir_path = tmp_path / 'subdir'
    subdir_path.mkdir()
    (subdir_path / file_name1).write_text(json.dumps(abi))
    (subdir_path / file_name2).write_text(json.dumps(abi))

    load_abis_to_event_inventory(clickhouse_migrated_url, str(tmp_path), dry_run=False)

    event_inventory_records = list(
        clickhouse.query(
            'SELECT * FROM event_inventory LIMIT 10 SETTINGS asterisk_include_alias_columns=1'
        ).named_results()
    )

    assert event_inventory_records == [
        {
            'event_signature_hash_and_log_topic_count': (
                '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62',
                4,
            ),
            'event_abi_json': json.dumps(abi[0], separators=(',', ':')),
            'event_name': 'TransferSingle',
            'event_signature': 'TransferSingle(address,address,address,uint256,uint256)',
            'event_signature_hash': '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62',
            'event_topic_count': 4,
            'contract_name': [
                'abi1',
                'abi2',
            ],
            'namespace': [
                'subdir',
            ],
        },
    ]


def test_select_where_no_results(clickhouse_adapter):
    """Test that `select_where()` returns an empty tuple if there are no results."""
    assert clickhouse_adapter.select_where(EntityType.BLOCK, "number", number=-1) == ()


def test_select_where_with_results(clickhouse_adapter, clickhouse_migrated):
    """Test that `select_where()` returns a tuple of results."""
    insert_records(
        clickhouse_migrated,
        'blocks',
        [
            {
                'number': 12345,
                'is_reorged': False,
                'hash': 'hash_1',
            },
            {
                'number': 12346,
                'is_reorged': False,
                'hash': 'hash_2',
            },
            {
                'number': 12347,
                'is_reorged': True,
                'hash': 'hash_3',
            },
            {
                'number': 12348,
                'is_reorged': True,
                'hash': 'hash_4',
            },
        ],
    )
    results = clickhouse_adapter.select_where(
        EntityType.BLOCK, "number", number=[12345, 12346, 12347]
    )
    assert len(results) == 3
    assert results[0]["number"] == 12345
    assert results[1]["number"] == 12346
    assert results[2]["number"] == 12347

    results = clickhouse_adapter.select_where(
        EntityType.BLOCK, "number", number=[12345, 12346, 12347], is_reorged=False
    )
    assert len(results) == 2
    assert results[0]["number"] == 12345
    assert results[1]["number"] == 12346

    results = clickhouse_adapter.select_where(
        EntityType.BLOCK,
        "number",
        number=[12345, 12346, 12347],
        is_reorged=True,
        hash='hash_3',
    )
    assert len(results) == 1
    assert results[0]["number"] == 12347
