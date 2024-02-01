from ethereumetl.domain.dex_trade import EthDexTrade
from ethereumetl.domain.receipt_log import ParsedReceiptLog
from ethereumetl.service.dex.uniswap_v2.uniswap_v2 import UniswapV2Amm
from ethereumetl.utils import get_prices_for_two_pool


class SushiSwapBentoAmm(UniswapV2Amm):
    @staticmethod
    def _get_trade_from_swap_event(
        parsed_receipt_log: ParsedReceiptLog,
        dex_pool,
        finance_info: dict,
        tokens_scalars: list[int],
    ) -> EthDexTrade:
        parsed_event = parsed_receipt_log.parsed_event
        if parsed_event["tokenIn"].lower() == dex_pool.token_addresses[0].lower():
            amount0 = parsed_event["amountIn"] / tokens_scalars[0]
            amount1 = -parsed_event["amountOut"] / tokens_scalars[1]
        else:
            amount0 = -parsed_event["amountOut"] / tokens_scalars[0]
            amount1 = parsed_event["amountIn"] / tokens_scalars[1]
        return EthDexTrade(
            pool_address=parsed_receipt_log.address,
            token_amounts=[amount0, amount1],
            transaction_hash=parsed_receipt_log.transaction_hash,
            log_index=parsed_receipt_log.log_index,
            block_number=parsed_receipt_log.block_number,
            event_type='swap',
            token_reserves=[finance_info['reserve_0'], finance_info['reserve_1']],
            token_prices=get_prices_for_two_pool(finance_info['price_0'], finance_info['price_1']),
            token_addresses=[dex_pool.token_addresses[0], dex_pool.token_addresses[1]],
        )
