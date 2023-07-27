import json
from unittest.mock import Mock

from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
from ethereumetl.enumeration.entity_type import EntityType
from ethereumetl.jobs.export_native_balances_job import ExportNativeBalancesJob
from ethereumetl.providers.rpc import BatchHTTPProvider


def test_export_native_balances_job():
    exporter = InMemoryItemExporter(item_types=(EntityType.NATIVE_BALANCE,))

    fake_batch_web3_provider = Mock(spec=BatchHTTPProvider)

    def handle_rpc_request(request_json: str):
        data: list[dict] = json.loads(request_json)

        block_number_hex: str
        address: str

        match data:
            case [
                {
                    'id': int(request_id),
                    'jsonrpc': '2.0',
                    'method': 'eth_getBalance',
                    'params': [str(address), str(block_number_hex)],
                }
            ] if block_number_hex == hex(123):
                return [
                    {
                        'id': request_id,
                        'jsonrpc': '2.0',
                        'result': hex(blockchain_balances_by_block[block_number_hex][address]),
                    }
                ]
            case _:
                raise ValueError(f'Unexpected request: {data}')

    fake_batch_web3_provider.make_batch_request.side_effect = handle_rpc_request

    transactions = [
        {
            "type": "transaction",
            "hash": "tx_hash_1",
            "block_number": 123,
            # the rest of the fields are irrelevant for this test
        },
    ]
    internal_transfers = [
        {
            "type": "internal_transfer",
            "from_address": "address_1",
            "to_address": "address_2",
            "transaction_hash": "tx_hash_1",
            "value": 1,
            # the rest of the fields are irrelevant for this test
        },
        {
            "type": "internal_transfer",
            "from_address": "address_2",
            "to_address": "0x0000000000000000000000000000000000000000",  # NULL_ADDRESS should be ignored
            "transaction_hash": "tx_hash_1",
            "value": 2,
            # the rest of the fields are irrelevant for this test
        },
    ]
    blockchain_balances_by_block = {
        hex(123): {
            "address_1": 11,
            "address_2": 12,
        }
    }

    job = ExportNativeBalancesJob(
        batch_web3_provider=fake_batch_web3_provider,
        batch_size=1,
        max_workers=2,
        item_exporter=exporter,
        transactions=transactions,
        internal_transfers=internal_transfers,
    )
    job.run()

    exported_native_balances = exporter.get_items(EntityType.NATIVE_BALANCE)
    exported_native_balances.sort(key=lambda x: x["address"])

    assert exported_native_balances == [
        {
            "type": "native_balance",
            "block_number": 123,
            "address": "address_1",
            "value": 11,
        },
        {
            "type": "native_balance",
            "block_number": 123,
            "address": "address_2",
            "value": 12,
        },
    ]
