import logging
from functools import lru_cache
from typing import Literal

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.domain.dex_trade import EthDexTrade
from ethereumetl.domain.receipt_log import ParsedReceiptLog
from ethereumetl.service.dex.uniswap_v3.uniswap_v3 import UniswapV3Amm
from ethereumetl.utils import get_prices_for_two_pool

logs = logging.getLogger(__name__)


class KyberSwapElasticAmm(UniswapV3Amm):

    def __init__(self, web3, chain_id: int | None = None, file_path: str = __file__):
        super().__init__(web3, chain_id, file_path)

    @lru_cache(maxsize=128)
    def get_ticks_spacing(
        self, pool_address, block_identifier: int | Literal['latest'] = "latest"
    ):
        return self.pool_contract_abi.functions.tickDistance().call(
            {"to": pool_address}, block_identifier
        )

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
                parsed_event["qty0"] / token_scalars[0],
                parsed_event["qty1"] / token_scalars[1],
            ],
            transaction_hash=parsed_receipt_log.transaction_hash,
            log_index=parsed_receipt_log.log_index,
            block_number=parsed_receipt_log.block_number,
            event_type='burn',
            token_reserves=[finance_info['reserve_0'], finance_info['reserve_1']],
            token_prices=get_prices_for_two_pool(finance_info['price_0'], finance_info['price_1']),
            token_addresses=base_pool.token_addresses,
            lp_token_address=base_pool.lp_token_addresses[0],
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
                parsed_event["qty0"] / token_scalars[0],
                parsed_event["qty1"] / token_scalars[1],
            ],
            transaction_hash=parsed_receipt_log.transaction_hash,
            log_index=parsed_receipt_log.log_index,
            block_number=parsed_receipt_log.block_number,
            event_type='mint',
            token_reserves=[finance_info['reserve_0'], finance_info['reserve_1']],
            token_prices=get_prices_for_two_pool(finance_info['price_0'], finance_info['price_1']),
            token_addresses=base_pool.token_addresses,
            lp_token_address=base_pool.lp_token_addresses[0],
        )
        return mint

    @staticmethod
    def get_swap_from_swap_event(
        parsed_receipt_log: ParsedReceiptLog,
        base_pool: EthDexPool,
        finance_info: dict,
        token_scalars: list[int],
    ):
        parsed_event = parsed_receipt_log.parsed_event
        amount0 = parsed_event["deltaQty0"] / token_scalars[0]
        amount1 = parsed_event["deltaQty1"] / token_scalars[1]
        swap = EthDexTrade(
            pool_address=base_pool.address,
            token_amounts=[amount0, amount1],
            transaction_hash=parsed_receipt_log.transaction_hash,
            log_index=parsed_receipt_log.log_index,
            block_number=parsed_receipt_log.block_number,
            event_type='swap',
            token_reserves=[finance_info['reserve_0'], finance_info['reserve_1']],
            token_prices=get_prices_for_two_pool(finance_info['price_0'], finance_info['price_1']),
            token_addresses=base_pool.token_addresses,
        )
        return swap

    def get_pool_details(self, pool_address, block_identifier: int | Literal['latest'] = "latest"):
        values = self.pool_contract_abi.functions.getPoolState().call(
            {"to": pool_address}, block_identifier
        )
        names = ["sqrtP", "currentTick", "nearestCurrentTick", "locked"]
        return dict(zip(names, values))

    def _resolve_finance_info(
        self,
        parsed_receipt_log: ParsedReceiptLog,
        dex_pool: EthDexPool,
        token_scalars: list[int],
    ):
        parsed_event = parsed_receipt_log.parsed_event
        sqrt_price_x96 = parsed_event.get("sqrtP")
        if not sqrt_price_x96:
            pool_details = self.get_pool_details(dex_pool.address)
            sqrt_price_x96 = pool_details["sqrtP"]

        token0_price = self.calculate_token0_price_from_sqrt_price_x96(
            sqrt_price_x96, token_scalars
        )
        token1_price = 1 / token0_price

        reserves = []
        for idx, token in enumerate(dex_pool.token_addresses):
            reserves.append(
                self._get_balance_of(token, dex_pool.address, parsed_receipt_log.block_number - 1)
                / token_scalars[idx]
            )

        finance_info = {
            'reserve_0': reserves[0],
            'reserve_1': reserves[1],
            'price_0': token0_price,
            'price_1': token1_price,
        }
        return finance_info
