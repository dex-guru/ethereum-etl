from clients.blockchain.models.pool import BasePool
from clients.blockchain.models.protocol_transaction import MintBurn, Swap
from utils.logger import get_logger

from .uniswap_v2 import UniswapV2Amm

logs = get_logger(__name__)

POOL_CONTRACT = "Pool"


class SushiSwapBentoAmm(UniswapV2Amm):
    @staticmethod
    def get_mint_burn_from_events(
        base_pool: BasePool, parsed_event: dict, tokens_scalars: list
    ) -> MintBurn:
        amount0 = parsed_event["amount0"] / tokens_scalars[0]
        amount1 = parsed_event["amount1"] / tokens_scalars[1]
        mint_burn = MintBurn(
            pool_address=base_pool.address,
            sender=parsed_event["sender"],
            owner=parsed_event["recipient"],
            amounts=[amount0, amount1],
        )
        return mint_burn

    @staticmethod
    def get_swap_from_swap_event(
        base_pool: BasePool, parsed_event: dict, tokens_scalars: list
    ) -> Swap:
        if parsed_event["tokenIn"].lower() == base_pool.tokens_addresses[0].lower():
            amount0 = parsed_event["amountIn"] / tokens_scalars[0]
            amount1 = parsed_event["amountOut"] / tokens_scalars[1]
        else:
            amount0 = parsed_event["amountOut"] / tokens_scalars[0]
            amount1 = parsed_event["amountIn"] / tokens_scalars[1]
        swap = Swap(
            pool_address=base_pool.address,
            sender=parsed_event["recipient"],
            to=parsed_event["recipient"],
            amounts=[amount0, amount1],
        )
        return swap
