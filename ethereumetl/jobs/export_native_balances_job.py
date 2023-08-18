from collections.abc import Collection, Iterable
from typing import Any

from eth_utils import to_int

from blockchainetl.exporters import BaseItemExporter
from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.domain.native_balance import EthNativeBalance
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from ethereumetl.json_rpc_requests import generate_get_native_balance_json_rpc
from ethereumetl.mappers.native_balance_mapper import NativeBalanceMapper
from ethereumetl.misc.info import NULL_ADDRESSES
from ethereumetl.utils import execute_in_batches, rpc_response_to_result

InternalTransferItem = dict[str, Any]
TransactionItem = dict[str, Any]
RPCRequest = dict
BlockAddress = tuple[int, str]


class ExportNativeBalancesJob(BaseJob):
    def __init__(
        self,
        *,
        batch_web3_provider,
        batch_size: int,
        max_workers: int,
        item_exporter: BaseItemExporter,
        transactions: Collection[TransactionItem],
        internal_transfers: Collection[InternalTransferItem],
    ):
        self.batch_web3_provider = batch_web3_provider
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.item_exporter = item_exporter
        self.transactions = transactions
        self.internal_transfers = [
            transfer for transfer in internal_transfers if transfer.get('value')
        ]
        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)

    def _start(self):
        self.item_exporter.open()

    def _end(self):
        try:
            self.batch_work_executor.shutdown()
        finally:
            self.item_exporter.close()

    def _export(self):
        block_address_pairs = self.get_block_address_pairs(
            self.transactions, self.internal_transfers
        )
        rpc_requests = tuple(self.generate_rpc_requests(block_address_pairs))

        rpc_responses = execute_in_batches(  # i/o
            self.batch_web3_provider, self.batch_work_executor, rpc_requests
        )

        native_balance_items = []
        for rpc_response, block_address_pair in zip(rpc_responses, block_address_pairs):
            result = rpc_response_to_result(rpc_response)
            value = to_int(hexstr=result)
            block_number, address = block_address_pair

            native_balance = EthNativeBalance(
                block_number=block_number,
                address=address,
                value=value,
            )

            native_balance_item = NativeBalanceMapper.native_balance_to_dict(native_balance)
            native_balance_items.append(native_balance_item)

        self.item_exporter.export_items(native_balance_items)

    @staticmethod
    def get_block_address_pairs(
        transactions: Collection[TransactionItem],
        internal_transfers: Collection[InternalTransferItem],
    ) -> set[BlockAddress]:
        transaction_by_hash = {t['hash']: t for t in transactions}
        block_address_pairs: set[BlockAddress] = set()
        for transfer in internal_transfers:
            transaction = transaction_by_hash.get(transfer['transaction_hash'])
            if transaction is None:
                continue
            block_number = transaction['block_number']
            for address in (transfer['from_address'], transfer['to_address']):
                if address is None or address in NULL_ADDRESSES:
                    continue
                block_address_pairs.add((block_number, address))
        return block_address_pairs

    @staticmethod
    def generate_rpc_requests(
        block_address_pairs: Collection[BlockAddress],
    ) -> Iterable[RPCRequest]:
        for request_id, (block_number, address) in enumerate(block_address_pairs):
            yield generate_get_native_balance_json_rpc(address, block_number, request_id)
