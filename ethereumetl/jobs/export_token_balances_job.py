import time
from collections.abc import Iterable
from typing import NamedTuple

from eth_utils import to_int

from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.domain.error import EthError
from ethereumetl.domain.token_balance import EthTokenBalance
from ethereumetl.domain.token_transfer import EthTokenTransferItem, TokenStandard
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from ethereumetl.json_rpc_requests import generate_balance_of_json_rpc
from ethereumetl.mappers.error_mapper import EthErrorMapper
from ethereumetl.mappers.token_balance_mapper import EthTokenBalanceMapper
from ethereumetl.misc.info import NULL_ADDRESSES
from ethereumetl.utils import execute_in_batches

MAX_UINT256 = 2**256 - 1


class TokenBalanceParams(NamedTuple):
    token_address: str
    token_standard: TokenStandard
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

        self.batch_size = batch_size
        self.max_workers = max_workers

    def _start(self):
        self.item_exporter.open()

    def _end(self):
        self.item_exporter.close()

    def _export(self):
        unique_rpc_params: set[TokenBalanceParams] = set()
        token_balances: list[EthTokenBalance] = []

        for t in self.token_transfers_iterable:
            params, balances = self.parse_transfer(t)
            unique_rpc_params.update(params)
            token_balances.extend(balances)

        all_rpc_params = tuple(unique_rpc_params)
        rpc_requests = [
            self.make_rpc_request(i, params) for i, params in enumerate(all_rpc_params)
        ]

        rpc_responses = execute_in_batches(  # i/o
            self.batch_web3_provider,
            BatchWorkExecutor(
                self.batch_size, self.max_workers, job_name='Export Token Balances Job'
            ),
            rpc_requests,
        )

        balances, errors = self.process_balance_of_rpc_responses(
            zip(all_rpc_params, rpc_responses)
        )
        token_balances.extend(balances)

        token_balance_items = tuple(
            EthTokenBalanceMapper.token_balance_to_dict(token_balance)
            for token_balance in token_balances
        )
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
    def parse_transfer(
        token_transfer: EthTokenTransferItem,
    ) -> tuple[list[TokenBalanceParams], list[EthTokenBalance]]:
        erc = TokenStandard(token_transfer['token_standard'])

        erc721_token_balances: list[EthTokenBalance] = []
        rpc_params: list[TokenBalanceParams] = []

        if erc == TokenStandard.ERC721:
            # NFT-only transfer. The tokens are unique, so balance should be 0 or 1.

            assert token_transfer['token_id'] is not None, 'ERC-721 transfers must have a token ID'

            for holder_address, value in (
                (token_transfer['to_address'], 1),
                (token_transfer['from_address'], 0),
            ):
                if holder_address in NULL_ADDRESSES:
                    # probably a mint or burn, skip
                    continue

                balance = EthTokenBalance(
                    token_standard=TokenStandard.ERC721,
                    holder_address=holder_address,
                    value=value,
                    block_number=token_transfer['block_number'],
                    token_address=token_transfer['token_address'],
                    token_id=token_transfer['token_id'],
                )
                erc721_token_balances.append(balance)

            return rpc_params, erc721_token_balances

        elif erc in (TokenStandard.ERC20, TokenStandard.ERC1155):
            if erc == TokenStandard.ERC20:
                assert (
                    token_transfer['token_id'] is None
                ), 'ERC-20 transfers must not have a token ID'
            elif erc == TokenStandard.ERC1155:
                assert (
                    token_transfer['token_id'] is not None
                ), 'ERC-1155 transfers must have a token ID'

            for holder_address in (token_transfer['from_address'], token_transfer['to_address']):
                if holder_address in NULL_ADDRESSES:
                    # probably a mint or burn, skip
                    continue

                params = TokenBalanceParams(
                    token_address=token_transfer['token_address'],
                    holder_address=holder_address,
                    block_number=token_transfer['block_number'],
                    token_id=token_transfer['token_id'],
                    token_standard=erc,
                )
                rpc_params.append(params)

            return rpc_params, erc721_token_balances
        else:
            raise ValueError(f'Unknown token standard: {erc}')

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
                        'rpc_response_result': rpc_response.get('result'),
                        'rpc_method': 'balanceOf',
                    },
                )
                errors.append(error)
                continue

            if rpc_response.get('result') is None:
                error = EthError(
                    timestamp=timestamp_func(),
                    block_number=rpc_params.block_number,
                    kind='rpc_response_no_result',
                    data={
                        'rpc_params': rpc_params,
                        'rpc_response': rpc_response,
                        'rpc_method': 'balanceOf',
                    },
                )
                errors.append(error)
                continue

            balance_value = hexstr_to_int(rpc_response['result'])

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
                token_id=rpc_params.token_id or 0,
                block_number=rpc_params.block_number,
                value=balance_value,
                token_standard=rpc_params.token_standard,
            )

            token_balances.append(token_balance)

        return token_balances, errors


def hexstr_to_int(hexstr: str) -> int:
    if hexstr == '0x':
        return 0
    return to_int(hexstr=hexstr)
