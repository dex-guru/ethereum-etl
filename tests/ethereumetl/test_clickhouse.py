import json
from collections.abc import Sequence

from clickhouse_connect.driver.client import Client

from blockchainetl.jobs.exporters.clickhouse_exporter import ClickHouseItemExporter
from ethereumetl.scripts.execute_clickhouse_sql import execute_clickhouse_sql
from ethereumetl.scripts.load_abi_to_event_inventory import load_abis_to_event_inventory
from ethereumetl.streaming.item_exporter_creator import make_item_type_to_table_mapping


def create_entity_tables(clickhouse_url):
    ch_exporter = ClickHouseItemExporter(
        connection_url=clickhouse_url,
        item_type_to_table_mapping=make_item_type_to_table_mapping(chain_id=1),
        chain_id=1,
    )
    ch_exporter.open()
    try:
        ch_exporter.create_tables()
    finally:
        ch_exporter.close()


def insert_records(clickhouse: Client, table: str, records: Sequence[dict]):
    columns = records[0].keys()
    data = [[r[col] for col in columns] for r in records]
    clickhouse.insert(table, data, columns)


def test_events_mat_view(clickhouse_url, clickhouse):
    create_entity_tables(clickhouse_url)

    execute_clickhouse_sql(
        chain_id=1,
        clickhouse_url=clickhouse_url,
        on_cluster='',
        replacing_merge_tree='ReplacingMergeTree',
        dry_run=False,
    )

    event_infos = [
        {
            'event_signature_hash': '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62',
            'event_signature': 'TransferSingle(address,address,address,uint256,uint256)',
            'event_topic_count': 4,
            'event_name': 'TransferSingle',
            'abi_type': "/path/to/file1.json",
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

    logs = [
        {
            'log_index': 68,
            'transaction_hash': "0x54c60539f2dcca7b61924440413bb29139cbaa2a2931f539e1a31edbb6d7cc2e",
            'transaction_index': 54,
            'address': "0x03bf9f1f807967002c4f9feed1dd4ea542275947",
            'data': (
                "0x"
                "0000000000000000000000000000000000000000000000000000000000000003"
                "0000000000000000000000000000000000000000000000000000000000000001"
            ),
            'topics': [
                "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62",
                "0x000000000000000000000000fa1b15df09c2944a91a2f9f10a6133090d4119bd",
                "0x0000000000000000000000000000000000000000000000000000000000000000",
                "0x000000000000000000000000fb3a85aff7cab85e7e2b7461703f57ed0105645e",
            ],
            'block_number': 16896735,
            'block_hash': '0x2aee15c56ee9b2bccbfccbfe62d6cebcb2af7bf736ab59259804398f502d7fe7',
        },
    ]

    # Add some event infos to be captured my the MaterializedView
    insert_records(clickhouse, '1_event_inventory_src', event_infos)

    # 1_event_inventory_src
    assert list(clickhouse.query('SELECT * FROM 1_event_inventory_src').named_results()) == [
        {
            'event_abi_json': event_infos[0]['event_abi_json'],
            'event_name': event_infos[0]['event_name'],
            'event_signature': event_infos[0]['event_signature'],
            'event_signature_hash': event_infos[0]['event_signature_hash'],
            'event_topic_count': event_infos[0]['event_topic_count'],
            'abi_type': event_infos[0]['abi_type'],
        },
    ]

    # 1_event_inventory
    assert list(clickhouse.query('SELECT * FROM 1_event_inventory').named_results()) == [
        {
            'event_abi_json': event_infos[0]['event_abi_json'],
            'event_name': event_infos[0]['event_name'],
            'event_signature': event_infos[0]['event_signature'],
            'event_signature_hash_and_log_topic_count': (
                '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62',
                4,
            ),
            'abi_types': [event_infos[0]['abi_type']],
        },
    ]

    # Insert logs. This should join with event_infos and insert into 1_events
    insert_records(clickhouse, '1_logs', logs)

    events_records = list(
        clickhouse.query(
            'SELECT * FROM 1_events LIMIT 10 SETTINGS asterisk_include_alias_columns=1'
        ).named_results()
    )

    # Ensure that the event was captured by the MaterializedView
    assert events_records == [
        {
            'event_signature_hash': '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62',
            'topic_count': 4,
            'block_hash': '0x2aee15c56ee9b2bccbfccbfe62d6cebcb2af7bf736ab59259804398f502d7fe7',
            'block_number': 16896735,
            'contract_address': '0x03bf9f1f807967002c4f9feed1dd4ea542275947',
            'data': (
                '0x'
                '0000000000000000000000000000000000000000000000000000000000000003'
                '0000000000000000000000000000000000000000000000000000000000000001'
            ),
            'event_name': 'TransferSingle',
            'log_index': 68,
            'topics': [
                '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62',
                '0x000000000000000000000000fa1b15df09c2944a91a2f9f10a6133090d4119bd',
                '0x0000000000000000000000000000000000000000000000000000000000000000',
                '0x000000000000000000000000fb3a85aff7cab85e7e2b7461703f57ed0105645e',
            ],
            'transaction_hash': '0x54c60539f2dcca7b61924440413bb29139cbaa2a2931f539e1a31edbb6d7cc2e',
            'transaction_index': 54,
        },
    ]


def test_load_abis_to_event_inventory(clickhouse_url, clickhouse, tmp_path):
    create_entity_tables(clickhouse_url)
    execute_clickhouse_sql(
        chain_id=1,
        clickhouse_url=clickhouse_url,
        on_cluster='',
        replacing_merge_tree='ReplacingMergeTree',
        dry_run=False,
    )

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

    load_abis_to_event_inventory(1, clickhouse_url, str(tmp_path), dry_run=False)

    event_inventory_records = list(
        clickhouse.query(
            'SELECT * FROM 1_event_inventory LIMIT 10 SETTINGS asterisk_include_alias_columns=1'
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
            'abi_types': [
                'subdir/abi1.json',
                'subdir/abi2.json',
            ],
        },
    ]
