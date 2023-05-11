import json
import threading
from typing import Iterable, NamedTuple

from eth_utils import to_int

from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.domain.token_balance import EthTokenBalance
from ethereumetl.domain.token_transfer import EthTokenTransferItem
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from ethereumetl.json_rpc_requests import generate_balance_of_json_rpc
from ethereumetl.mappers.token_balance_mapper import EthTokenBalanceMapper
from ethereumetl.utils import rpc_response_to_result


class TokenBalanceParams(NamedTuple):
    token_address: str
    holder_address: str
    block_number: int
    token_id: int | None


class ExportTokenBalancesJob(BaseJob):
    def __init__(
        self,
        *,
        batch_size,
        batch_web3_provider,
        token_transfer_items_iterable: Iterable[EthTokenTransferItem],
        max_workers,
        item_exporter,
    ):
        self.item_exporter = item_exporter
        self.token_transfers_iterable = token_transfer_items_iterable
        self.batch_web3_provider = batch_web3_provider
        self.token_balance_mapper = EthTokenBalanceMapper()

        self.batch_size = batch_size
        self.max_workers = max_workers

    def _start(self):
        self.item_exporter.open()

    def _end(self):
        self.item_exporter.close()

    def _export(self):
        unique_rpc_params = set()
        for t in self.token_transfers_iterable:
            for params in self.prepare_params(t):
                unique_rpc_params.add(params)

        all_rpc_params = list(unique_rpc_params)

        rpc_responses = self.execute_rpcs(all_rpc_params)  # i/o

        for rpc_params, rpc_response in zip(all_rpc_params, rpc_responses):
            token_balance = self.make_token_balance(rpc_params, rpc_response)
            token_balance_item = self.token_balance_mapper.token_balance_to_dict(token_balance)
            self.item_exporter.export_item(token_balance_item)

    @staticmethod
    def make_token_balance(rpc_params: TokenBalanceParams, rpc_response: dict) -> EthTokenBalance:
        balance_value = to_int(hexstr=rpc_response['result'])

        token_balance = EthTokenBalance(
            token_address=rpc_params.token_address,
            holder_address=rpc_params.holder_address,
            token_id=rpc_params.token_id,
            block_number=rpc_params.block_number,
            value=balance_value,
        )

        return token_balance

    @staticmethod
    def make_rpc_request(rpc_id: int, rpc_params: TokenBalanceParams) -> dict:
        rpc = generate_balance_of_json_rpc(
            rpc_params.token_address,
            rpc_params.holder_address,
            rpc_params.token_id,
            rpc_params.block_number,
            rpc_id,
        )
        return rpc

    @staticmethod
    def prepare_params(token_transfer: EthTokenTransferItem) -> Iterable[TokenBalanceParams]:
        rpc_params = []
        for address in (token_transfer['from_address'], token_transfer['to_address']):
            if address == '0x0000000000000000000000000000000000000000':
                # probably a mint or burn, skip
                continue
            if token_transfer['token_standard'] == 'ERC-1155':
                # ERC-1155: balanceOf(address,tokenId)
                token_id = token_transfer['token_id']
            else:
                assert token_transfer['token_standard'] in ('ERC-20', 'ERC-721')
                # ERC-20, ERC-721: balanceOf(address)
                # Cannot get balance for a specific token_id.
                token_id = None

            params = TokenBalanceParams(
                token_address=token_transfer['token_address'],
                holder_address=address,
                block_number=token_transfer['block_number'],
                token_id=token_id,
            )
            rpc_params.append(params)
        return rpc_params

    def execute_rpcs(self, rpc_params: list[TokenBalanceParams]) -> list[dict]:
        """
        The order and count of the results is guaranteed to match the order of `rpc_params`.
        """
        rpc_params_indexed = list(enumerate(rpc_params))
        responses_ordered = [None] * len(rpc_params_indexed)

        lock = threading.Lock()

        def handle_one_batch(rpc_params_indexed_batch):
            requests = []
            params_idxs = []

            for rpc_id, (params_idx, params) in enumerate(rpc_params_indexed_batch):
                request = self.make_rpc_request(rpc_id, params)
                requests.append(request)
                params_idxs.append(params_idx)

            responses = self.batch_web3_provider.make_batch_request(json.dumps(requests))

            # Here a RetriableValueError can be raised and the BatchWorkExecutor will retry the
            # batch.
            for response in responses:
                rpc_response_to_result(response)  # checks for errors

            response_by_rpc_id = {r['id']: r for r in responses}
            if len(response_by_rpc_id) != len(requests):
                raise Exception('batch JSON-RPC error: response ids do not match request ids')

            with lock:
                for rpc_id, params_idx in enumerate(params_idxs):
                    try:
                        response = response_by_rpc_id[rpc_id]
                    except KeyError:
                        raise Exception(
                            'batch JSON-RPC error: response ids do not match request ids'
                        )
                    else:
                        responses_ordered[params_idx] = response

        executor = BatchWorkExecutor(self.batch_size, self.max_workers)
        try:
            executor.execute(rpc_params_indexed, handle_one_batch)
        finally:
            executor.shutdown()

        return responses_ordered  # type: ignore
