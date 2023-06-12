import json
import threading
import time
from typing import Iterable, NamedTuple

from eth_utils import to_int

from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.domain.error import EthError
from ethereumetl.domain.token_balance import EthTokenBalance
from ethereumetl.domain.token_transfer import EthTokenTransferItem
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from ethereumetl.json_rpc_requests import generate_balance_of_json_rpc
from ethereumetl.mappers.error_mapper import EthErrorMapper
from ethereumetl.mappers.token_balance_mapper import EthTokenBalanceMapper
from ethereumetl.misc.info import NULL_ADDRESSES
from ethereumetl.misc.retriable_value_error import RetriableValueError
from ethereumetl.utils import rpc_response_to_result

MAX_UINT256 = 2**256 - 1


class TokenBalanceParams(NamedTuple):
    token_address: str
    holder_address: str
    block_number: int
    token_id: int | None


class ExportTokenBalancesJob(BaseJob):
    BALANCE_OF_OUT_OF_GAS_ERROR = {"message": "out of gas", "code": -32000}

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
        rpc_requests = [
            self.make_rpc_request(i, params) for i, params in enumerate(all_rpc_params)
        ]

        rpc_responses = self.execute_balance_of_rpcs(rpc_requests)  # i/o

        token_balances, errors = self.process_balance_of_rpc_responses(
            zip(all_rpc_params, rpc_responses)
        )
        token_balance_items = [
            EthTokenBalanceMapper.token_balance_to_dict(token_balance)
            for token_balance in token_balances
        ]
        error_items = [EthErrorMapper.error_to_dict(error) for error in errors]
        self.item_exporter.export_items(token_balance_items)
        self.item_exporter.export_items(error_items)

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
        if token_transfer['token_standard'] == 'ERC-1155':
            # ERC-1155: balanceOf(address,tokenId)
            token_id = token_transfer['token_id']
        elif token_transfer['token_standard'] in ('ERC-20', 'ERC-721'):
            # ERC-20, ERC-721: balanceOf(address)
            # Cannot get balance for a specific token_id.
            token_id = None
        else:
            raise ValueError(f'Unknown token standard: {token_transfer["token_standard"]}')

        rpc_params = []
        for address in (token_transfer['from_address'], token_transfer['to_address']):
            if address in NULL_ADDRESSES:
                # probably a mint or burn, skip
                continue

            params = TokenBalanceParams(
                token_address=token_transfer['token_address'],
                holder_address=address,
                block_number=token_transfer['block_number'],
                token_id=token_id,
            )
            rpc_params.append(params)
        return rpc_params

    def check_balance_of_rpc_response(self, response):
        if response.get('error') == self.BALANCE_OF_OUT_OF_GAS_ERROR:
            return

        # Here a RetriableValueError can be raised and the BatchWorkExecutor will
        # retry the batch.
        rpc_response_to_result(response)

    def execute_balance_of_rpcs(self, rpc_requests: list[dict]) -> list[dict]:
        """
        Returns responses, the order and count of which is guaranteed to
        match the order of `rpc_requests`.
        """
        rpc_requests_indexed = list(enumerate(rpc_requests))
        responses_ordered = [None] * len(rpc_requests_indexed)

        lock = threading.Lock()

        def handle_one_batch(rpc_requests_indexed_batch):
            requests = []
            request_idxs = []

            for rpc_id, (request_idx, request) in enumerate(rpc_requests_indexed_batch):
                request = {**request, 'id': rpc_id}
                requests.append(request)
                request_idxs.append(request_idx)

            responses = self.batch_web3_provider.make_batch_request(json.dumps(requests))

            for response in responses:
                try:
                    self.check_balance_of_rpc_response(response)
                except RetriableValueError as e:
                    try:
                        request_id = response['id']
                        request = requests[request_id]
                    except (KeyError, TypeError, IndexError):
                        raise e
                    else:
                        request_json = json.dumps(request)
                        raise RetriableValueError(f'{e} request: {request_json}') from e

            response_by_rpc_id = {r['id']: r for r in responses}
            if len(response_by_rpc_id) != len(requests):
                raise Exception('batch JSON-RPC error: response ids do not match request ids')

            with lock:
                for rpc_id, request_idx in enumerate(request_idxs):
                    try:
                        response = response_by_rpc_id[rpc_id]
                    except KeyError:
                        raise Exception(
                            'batch JSON-RPC error: response ids do not match request ids'
                        )
                    else:
                        responses_ordered[request_idx] = response

        executor = BatchWorkExecutor(self.batch_size, self.max_workers)
        try:
            executor.execute(rpc_requests_indexed, handle_one_batch)
        finally:
            executor.shutdown()

        return responses_ordered  # type: ignore

    @staticmethod
    def process_balance_of_rpc_responses(
        rpc_param_response_pairs: Iterable[tuple[TokenBalanceParams, dict]],
        timestamp_func=lambda: int(time.time()),
    ) -> tuple[list[EthTokenBalance], list[EthError]]:
        token_balances = []
        errors = []

        for rpc_params, rpc_response in rpc_param_response_pairs:
            if (rpc_response_error := rpc_response.get('error')) is not None:
                error = EthError(
                    timestamp=timestamp_func(),
                    block_number=rpc_params.block_number,
                    kind='rpc_response_error',
                    data={
                        'rpc_params': rpc_params,
                        'rpc_response_error': rpc_response_error,
                        'rpc_method': 'balanceOf',
                    },
                )
                errors.append(error)
                continue

            balance_value = to_int(hexstr=rpc_response['result'])

            if not (0 <= balance_value <= MAX_UINT256):
                error = EthError(
                    timestamp=timestamp_func(),
                    block_number=rpc_params.block_number,
                    kind='token_balance_value_invalid_uint256',
                    data={
                        'rpc_params': rpc_params,
                        'rpc_response_result': rpc_response['result'],
                        'rpc_method': 'balanceOf',
                        'message': 'token_balance_value does not fit in uint256',
                    },
                )
                errors.append(error)
                continue

            token_balance = EthTokenBalance(
                token_address=rpc_params.token_address,
                holder_address=rpc_params.holder_address,
                token_id=rpc_params.token_id,
                block_number=rpc_params.block_number,
                value=balance_value,
            )

            token_balances.append(token_balance)

        return token_balances, errors
