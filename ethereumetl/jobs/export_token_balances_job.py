import json
from typing import Iterable, TypedDict

from ethereumetl.domain.token_balance import EthTokenBalance
from ethereumetl.domain.token_transfer import EthTokenTransferItem
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from ethereumetl.json_rpc_requests import generate_balance_of_json_rpc
from ethereumetl.mappers.token_balance_mapper import EthTokenBalanceMapper
from ethereumetl.utils import rpc_response_batch_to_results


class TokenBalanceParams(TypedDict):
    token_address: str
    address: str
    block_number: int
    token_id: int | None


class ExportTokenBalancesJob:
    def __init__(
        self,
        *,
        batch_size,
        batch_web3_provider,
        token_transfers_iterable,
        max_workers,
        item_exporter,
    ):
        self.token_balance_mapper = EthTokenBalanceMapper()
        self.token_transfers_iterable = token_transfers_iterable
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.item_exporter = item_exporter
        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.batch_web3_provider = batch_web3_provider

    def _start(self):
        self.item_exporter.open()

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()

    def _export(self):
        self.batch_work_executor.execute(
            self.token_transfers_iterable, self._export_token_balances
        )

    def _export_token_balances(self, token_transfer_items: Iterable[EthTokenTransferItem]):
        rpc_params = self.prepare_params(token_transfer_items)
        rpcs = self.prepare_rpcs(rpc_params)
        rpc_responses = self.execute_batch_rpc(rpcs)
        token_balances = self.make_token_balances(rpc_params, rpc_responses)
        for token_balance in token_balances:
            token_balance_item = self.token_balance_mapper.token_balance_to_dict(token_balance)
            self.item_exporter.export_item(token_balance_item)

    @staticmethod
    def make_token_balances(rpc_params: list[TokenBalanceParams], rpc_responses: list[dict]):
        for rpc_response in rpc_responses:
            params = rpc_params[rpc_response['id']]
            balance_value = rpc_response['result']

            token_balance = EthTokenBalance(
                token_address=params['token_address'],
                holder_address=params['address'],
                token_id=params['token_id'],
                block_number=params['block_number'],
                value=balance_value,
            )

            yield token_balance

    @staticmethod
    def prepare_rpcs(rpc_params: list[TokenBalanceParams]):
        rpcs = []
        for rpc_id, params in enumerate(rpc_params):
            rpc = generate_balance_of_json_rpc(
                params['token_address'],
                params['address'],
                params['token_id'],
                params['block_number'],
                rpc_id,
            )
            rpcs.append(rpc)
        return rpcs

    @staticmethod
    def prepare_params(
        token_transfer_items: Iterable[EthTokenTransferItem],
    ) -> list[TokenBalanceParams]:
        rpc_params = []
        for transfer in token_transfer_items:
            for address in (transfer['from_address'], transfer['to_address']):
                params: TokenBalanceParams = {
                    'token_address': transfer['token_address'],
                    'address': address,
                    'block_number': transfer['block_number'],
                    'token_id': transfer['token_id'],
                }
                rpc_params.append(params)
        return rpc_params

    def execute_batch_rpc(self, rpcs):
        response = self.batch_web3_provider.make_batch_request(json.dumps(rpcs))
        rpc_responses = rpc_response_batch_to_results(response)
        return rpc_responses
