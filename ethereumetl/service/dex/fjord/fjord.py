import json
import logging
from collections.abc import Callable
from functools import lru_cache
from pathlib import Path
from typing import Literal

from web3 import Web3
from web3.exceptions import BadFunctionCallOutput, ContractLogicError

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.domain.dex_trade import EthDexTrade
from ethereumetl.domain.receipt_log import ParsedReceiptLog
from ethereumetl.domain.token import EthToken
from ethereumetl.domain.token_transfer import EthTokenTransfer
from ethereumetl.service.dex.base.base_dex_client import BaseDexClient
from ethereumetl.utils import get_prices_for_two_pool

to_checksum = Web3.to_checksum_address

logs = logging.getLogger(__name__)


class FjordLBP(BaseDexClient):
    ASSET_INDEX = 0
    SHARE_INDEX = 1

    def __init__(self, web3: Web3, chain_id: int | None = None, file_path: str = __file__):
        super().__init__(web3, chain_id, file_path)
        self.web3 = web3
        pool_abi_path = Path(file_path).parent / "Pool.json"
        self.pool_contract_abi = self.web3.eth.contract(abi=json.loads(pool_abi_path.read_text()))

    @property
    def event_resolver(self):
        return {
            'Buy': self.get_swap_from_buy_share_event,
            'Sell': self.get_swap_from_sell_share_event,
        }

    def resolve_asset_from_log(self, parsed_log: ParsedReceiptLog) -> EthDexPool | None:
        pool_address = parsed_log.address
        checksum_address = to_checksum(pool_address)
        logs.debug(f"Resolving pool addresses for {pool_address}")
        factory_address = self.get_factory_address(checksum_address)
        if not factory_address:
            return None
        tokens_addresses = self.get_tokens_addresses_for_pool(checksum_address)
        if not tokens_addresses:
            return None
        swap_fee = self.get_swap_fee(checksum_address)
        return EthDexPool(
            address=pool_address,
            token_addresses=[token.lower() for token in tokens_addresses],
            fee=(swap_fee / 10**18) * 10_000,
            factory_address=factory_address.lower(),
            lp_token_addresses=[pool_address],
        )

    def resolve_receipt_log(
        self,
        parsed_receipt_log: ParsedReceiptLog,
        dex_pool: EthDexPool | None = None,
        tokens_for_pool: list[EthToken] | None = None,
        transfers_for_transaction: list[EthTokenTransfer] | None = None,
    ) -> EthDexTrade | None:
        logging.debug(f"Resolving receipt log {parsed_receipt_log}")
        if not dex_pool or not tokens_for_pool:
            return None
        event_name = parsed_receipt_log.event_name
        if not self.event_resolver.get(event_name):
            logging.debug(f"Event {event_name} not found in resolver")
            return None
        token_scalars = self._get_scalars_for_tokens(tokens_for_pool, dex_pool)
        resolve_func: Callable = self.event_resolver[event_name]
        parsed_receipt_log.parsed_event = self.normalize_event(
            self._get_events_abi(self.pool_contract_abi, event_name)['inputs'],
            parsed_receipt_log.parsed_event,
        )
        try:
            finance_info = self._resolve_finance_info(parsed_receipt_log, dex_pool, token_scalars)
        except (ValueError, TypeError, BadFunctionCallOutput, ContractLogicError) as e:
            logging.debug(f"Finance info not found for {parsed_receipt_log}: {e}")
            finance_info = {
                'reserve_0': 0,
                'reserve_1': 0,
                'price_0': 0,
                'price_1': 0,
            }
        try:
            resolved_log = resolve_func(parsed_receipt_log, dex_pool, finance_info, token_scalars)
        except (ValueError, TypeError, KeyError, BadFunctionCallOutput, ContractLogicError) as e:
            logging.debug(f"Resolved log not found for {parsed_receipt_log}: {e}")
            resolved_log = None
        logging.debug(f"Resolved receipt log {resolved_log}")
        return resolved_log

    def _resolve_finance_info(
        self,
        parsed_receipt_log,
        dex_pool: EthDexPool,
        token_scalars: list[int],
    ):
        asset_reserve, share_reserve, asset_weight, share_weight = self.get_reserves_and_weights(
            dex_pool, parsed_receipt_log.block_number
        )
        reserves = [
            asset_reserve / token_scalars[self.ASSET_INDEX],
            share_reserve / token_scalars[self.SHARE_INDEX],
        ]

        finance_info = {
            'reserve_0': reserves[0],
            'reserve_1': reserves[1],
            'price_0': 0,
            'price_1': 0,
        }
        return finance_info

    def get_reserves_and_weights(self, dex_pool: EthDexPool, block_number: int):
        return self.pool_contract_abi.functions.reservesAndWeights().call(
            {"to": to_checksum(dex_pool.address)}, block_number
        )

    def get_swap_from_buy_share_event(
        self,
        parsed_receipt_log: ParsedReceiptLog,
        base_pool: EthDexPool,
        finance_info: dict,
        token_scalars: list[int],
    ):
        parsed_event = parsed_receipt_log.parsed_event
        swap = EthDexTrade(
            pool_address=base_pool.address,
            token_amounts=[
                -parsed_event["assets"] / token_scalars[self.ASSET_INDEX],
                parsed_event["shares"] / token_scalars[self.SHARE_INDEX],
            ],
            transaction_hash=parsed_receipt_log.transaction_hash,
            log_index=parsed_receipt_log.log_index,
            block_number=parsed_receipt_log.block_number,
            event_type='swap',
            token_reserves=[finance_info['reserve_0'], finance_info['reserve_1']],
            token_prices=get_prices_for_two_pool(finance_info['price_0'], finance_info['price_1']),
            token_addresses=base_pool.token_addresses,
        )
        return swap

    def get_swap_from_sell_share_event(
        self,
        parsed_receipt_log: ParsedReceiptLog,
        base_pool: EthDexPool,
        finance_info: dict,
        token_scalars: list[int],
    ):
        parsed_event = parsed_receipt_log.parsed_event
        swap = EthDexTrade(
            pool_address=base_pool.address,
            token_amounts=[
                parsed_event["assets"] / token_scalars[self.ASSET_INDEX],
                -parsed_event["shares"] / token_scalars[self.SHARE_INDEX],
            ],
            transaction_hash=parsed_receipt_log.transaction_hash,
            log_index=parsed_receipt_log.log_index,
            block_number=parsed_receipt_log.block_number,
            event_type='swap',
            token_reserves=[finance_info['reserve_0'], finance_info['reserve_1']],
            token_prices=get_prices_for_two_pool(finance_info['price_0'], finance_info['price_1']),
            token_addresses=base_pool.token_addresses,
        )
        return swap

    @lru_cache(maxsize=128)
    def get_factory_address(self, pool_address: str) -> str | None:
        try:
            factory_address = self.pool_contract_abi.functions.SABLIER().call(
                {"to": pool_address}, "latest"
            )
        except (ValueError, TypeError, BadFunctionCallOutput, ContractLogicError):
            return None
        return factory_address

    def get_tokens_addresses_for_pool(self, pool_address: str) -> list | None:
        try:
            tokens_addresses = [
                (
                    self.pool_contract_abi.functions.asset().call({"to": pool_address}, "latest")
                ).lower(),
                (
                    self.pool_contract_abi.functions.share().call({"to": pool_address}, "latest")
                ).lower(),
            ]
        except (ValueError, TypeError, BadFunctionCallOutput, ContractLogicError):
            return None
        return tokens_addresses

    @lru_cache(maxsize=128)
    def get_swap_fee(
        self,
        pool_address,
        block_identifier: Literal['latest'] | int = "latest",
    ):
        try:
            return self.pool_contract_abi.functions.swapFee().call(
                {"to": pool_address}, block_identifier
            )
        except (ValueError, TypeError, BadFunctionCallOutput, ContractLogicError):
            return 0
