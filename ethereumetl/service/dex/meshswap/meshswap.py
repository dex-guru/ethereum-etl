import logging
from collections.abc import Callable

from web3 import Web3

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.domain.dex_trade import EthDexTrade
from ethereumetl.domain.receipt_log import ParsedReceiptLog
from ethereumetl.service.dex.uniswap_v2.uniswap_v2 import UniswapV2Amm
from ethereumetl.utils import get_prices_for_two_pool

logs = logging.getLogger(__name__)

POOL_CONTRACT = "Pool"
FACTORY_CONTRACT = "UniswapV2Factory"

to_checksum = Web3.toChecksumAddress


class MeshswapAmm(UniswapV2Amm):

    MINT_EVENTS = ("AddLiquidity",)

    def __init__(self, web3: Web3, chain_id: int | None = None):
        super().__init__(web3, chain_id, __file__)

    @property
    def event_resolver(self) -> dict[str, Callable]:
        return {
            "ExchangePos": self._get_trade_from_swap_event,
            "ExchangeNeg": self._get_trade_from_swap_event,
            "RemoveLiquidity": self._get_trade_from_burn_event,
            "AddLiquidity": self._get_trade_from_mint_event,
        }

    def resolve_asset_from_log(self, parsed_log: ParsedReceiptLog) -> EthDexPool | None:
        if parsed_log.address.lower() in [
            "0x10f4a785f458bc144e3706575924889954946639",
            "0x650a6938ec6f96f8b62ae712e5f4ad9c3fffe956",
        ]:
            # This is router address on Polygon, as router can also issue ExchangePos events.
            return None
        return super().resolve_asset_from_log(parsed_log)

    @staticmethod
    def _get_trade_from_swap_event(
        parsed_receipt_log: ParsedReceiptLog,
        dex_pool: EthDexPool,
        finance_info: dict,
        tokens_scalars: list[int],
    ) -> EthDexTrade:
        """
        Get trade from ExchangePos or ExchangeNeg event.

        See docs:
        https://docs.meshswap.fi/developers/contract/exchange#exchangepos
        """
        parsed_event = parsed_receipt_log.parsed_event
        if parsed_event["token0"].lower() == dex_pool.token_addresses[0]:
            amount_0 = parsed_event["amount0"] / tokens_scalars[0]
            amount_1 = -parsed_event["amount1"] / tokens_scalars[1]
        else:
            amount_1 = parsed_event["amount0"] / tokens_scalars[1]
            amount_0 = -parsed_event["amount1"] / tokens_scalars[0]

        return EthDexTrade(
            pool_address=parsed_receipt_log.address,
            token_amounts=[amount_0, amount_1],
            transaction_hash=parsed_receipt_log.transaction_hash,
            log_index=parsed_receipt_log.log_index,
            block_number=parsed_receipt_log.block_number,
            event_type='swap',
            token_reserves=[finance_info['reserve_0'], finance_info['reserve_1']],
            token_prices=get_prices_for_two_pool(finance_info['price_0'], finance_info['price_1']),
            token_addresses=[dex_pool.token_addresses[0], dex_pool.token_addresses[1]],
        )
