import json
import logging
from collections.abc import Callable
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
from ethereumetl.service.dex.enums import DexPoolFeeAmount
from ethereumetl.utils import get_prices_for_two_pool

logs = logging.getLogger(__name__)
to_checksum = Web3.toChecksumAddress

TICK_BASE = 1.0001
MIN_TICK = -887272
MAX_TICK = -MIN_TICK
MIN_SQRT_RATIO = 4295128739
MAX_SQRT_RATIO = 1461446703485210103287273052203988822378723970342


class UniswapV3Amm(BaseDexClient):

    def __init__(self, web3: Web3, chain_id: int | None = None, file_path: str = __file__):
        super().__init__(web3, chain_id, file_path)
        self.web3 = web3
        pool_abi_path = Path(file_path).parent / "Pool.json"
        self.pool_contract_abi = self.web3.eth.contract(abi=json.loads(pool_abi_path.read_text()))

    @property
    def event_resolver(self):
        return {
            'Swap': self.get_swap_from_swap_event,
            'Burn': self.get_burn_from_event,
            'Mint': self.get_mint_from_event,
            'Collect': self.get_burn_from_event,
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
        ticks_spacing = self.get_ticks_spacing(checksum_address)
        return EthDexPool(
            address=pool_address,
            token_addresses=[token.lower() for token in tokens_addresses],
            fee=self.tick_spacing_to_fee(ticks_spacing),
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
        token_scalars = self._get_token_scalars(tokens_for_pool, dex_pool)
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
        parsed_event = parsed_receipt_log.parsed_event
        sqrt_price_x96 = parsed_event.get("sqrtPriceX96")
        if not sqrt_price_x96:
            slot0 = self.get_slot0(to_checksum(dex_pool.address))
            sqrt_price_x96 = slot0["sqrtPriceX96"]
            if sqrt_price_x96 < MIN_SQRT_RATIO or sqrt_price_x96 > MAX_SQRT_RATIO:
                raise ValueError(f"Invalid sqrt price {sqrt_price_x96}")

        token0_price = self.calculate_token0_price_from_sqrt_price_x96(
            sqrt_price_x96, token_scalars
        )
        token1_price = 1 / token0_price

        reserves = []
        for idx, token in enumerate(dex_pool.token_addresses):
            reserve = self._get_balance_of(
                token, dex_pool.address, parsed_receipt_log.block_number
            )
            if reserve:
                reserves.append(reserve / token_scalars[idx])
            else:
                reserves.append(0)
        finance_info = {
            'reserve_0': reserves[0],
            'reserve_1': reserves[1],
            'price_0': token0_price,
            'price_1': token1_price,
        }
        return finance_info

    @staticmethod
    def _get_token_scalars(tokens_for_pool, dex_pool: EthDexPool):
        token_scalars = []
        for token_address in dex_pool.token_addresses:
            token = next(
                (token for token in tokens_for_pool if token.address == token_address), None
            )
            if not token:
                logging.debug(f"Token {token_address} not found in tokens")
                return None
            token_scalars.append(10**token.decimals)
        return token_scalars

    @staticmethod
    def get_swap_from_swap_event(
        parsed_receipt_log: ParsedReceiptLog,
        base_pool: EthDexPool,
        finance_info: dict,
        token_scalars: list[int],
    ):
        parsed_event = parsed_receipt_log.parsed_event
        swap = EthDexTrade(
            pool_address=base_pool.address,
            token_amounts=[
                parsed_event["amount0"] / token_scalars[0],
                parsed_event["amount1"] / token_scalars[1],
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

    @staticmethod
    def get_burn_from_event(
        parsed_receipt_log: ParsedReceiptLog,
        base_pool: EthDexPool,
        finance_info: dict,
        token_scalars: list[int],
    ):
        parsed_event = parsed_receipt_log.parsed_event
        burn = EthDexTrade(
            pool_address=base_pool.address,
            token_amounts=[
                parsed_event["amount0"] / token_scalars[0],
                parsed_event["amount1"] / token_scalars[1],
            ],
            transaction_hash=parsed_receipt_log.transaction_hash,
            log_index=parsed_receipt_log.log_index,
            block_number=parsed_receipt_log.block_number,
            event_type='burn',
            token_reserves=[finance_info['reserve_0'], finance_info['reserve_1']],
            token_prices=get_prices_for_two_pool(finance_info['price_0'], finance_info['price_1']),
            token_addresses=base_pool.token_addresses,
        )
        return burn

    @staticmethod
    def get_mint_from_event(
        parsed_receipt_log: ParsedReceiptLog,
        base_pool: EthDexPool,
        finance_info: dict,
        token_scalars: list[int],
    ):
        parsed_event = parsed_receipt_log.parsed_event
        mint = EthDexTrade(
            pool_address=base_pool.address,
            token_amounts=[
                parsed_event["amount0"] / token_scalars[0],
                parsed_event["amount1"] / token_scalars[1],
            ],
            transaction_hash=parsed_receipt_log.transaction_hash,
            log_index=parsed_receipt_log.log_index,
            block_number=parsed_receipt_log.block_number,
            event_type='mint',
            token_reserves=[finance_info['reserve_0'], finance_info['reserve_1']],
            token_prices=get_prices_for_two_pool(finance_info['price_0'], finance_info['price_1']),
            token_addresses=base_pool.token_addresses,
        )
        return mint

    def get_slot0(self, pool_address, block_identifier: Literal['latest'] | int = "latest"):
        values = self.pool_contract_abi.functions.slot0().call(
            {"to": pool_address}, block_identifier
        )
        names = [
            "sqrtPriceX96",
            "tick",
            "observationIndex",
            "observationCardinality",
            "observationCardinalityNext",
            "feeProtocol",
            "unlocked",
        ]
        return dict(zip(names, values))

    def get_liquidity(self, pool_address, block_identifier: Literal['latest'] | int = "latest"):
        return self.pool_contract_abi.functions.liquidity().call(
            {"to": pool_address}, block_identifier
        )

    def get_factory_address(self, pool_address: str) -> str | None:
        try:
            factory_address = self.pool_contract_abi.functions.factory().call(
                {"to": pool_address}, "latest"
            )
        except (ValueError, TypeError, BadFunctionCallOutput, ContractLogicError):
            return None
        return factory_address

    @staticmethod
    def tick_spacing_to_fee(tick_spacing: int) -> DexPoolFeeAmount:
        fees_spacings = {
            1: DexPoolFeeAmount.VERY_LOW,
            10: DexPoolFeeAmount.LOW,
            60: DexPoolFeeAmount.MEDIUM,
            200: DexPoolFeeAmount.HIGH,
        }
        try:
            return fees_spacings[tick_spacing]
        except KeyError:
            return DexPoolFeeAmount.UNDEFINED

    def get_tokens_addresses_for_pool(self, pool_address: str) -> list | None:
        try:
            tokens_addresses = [
                (
                    self.pool_contract_abi.functions.token0().call({"to": pool_address}, "latest")
                ).lower(),
                (
                    self.pool_contract_abi.functions.token1().call({"to": pool_address}, "latest")
                ).lower(),
            ]
        except (ValueError, TypeError, BadFunctionCallOutput, ContractLogicError):
            return None
        return tokens_addresses

    @staticmethod
    def calculate_token0_price_from_sqrt_price_x96(
        sqrt_price_x96: int, token_decimals_scalars: list[int]
    ) -> float:
        # TODO: failing with point in some cases, check
        token_scalars_diff = token_decimals_scalars[1] / token_decimals_scalars[0]
        price = ((sqrt_price_x96**2) / (2**192)) / token_scalars_diff
        return price

    def get_ticks_spacing(
        self,
        pool_address,
        block_identifier: Literal['latest'] | int = "latest",
    ):
        return self.pool_contract_abi.functions.tickSpacing().call(
            {"to": pool_address}, block_identifier
        )
