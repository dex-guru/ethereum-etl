from clients.blockchain.models.pool import PoolFinances
from clients.blockchain.models.protocol_transaction import (
    MintBurn,
    Swap,
)
from utils.logger import get_logger
from utils.prices import get_prices_for_two_pool

from .uniswap_v3 import UniswapV3Amm

logs = get_logger(__name__)

POOL_CONTRACT = "Pool"


class KyberSwapElasticAmm(UniswapV3Amm):
    def get_ticks_spacing(self, pool_address, block_identifier: str | int = "latest"):
        return (
            self.abi[POOL_CONTRACT]
            .contract.functions.tickDistance()
            .call({"to": pool_address}, block_identifier)
        )

    @staticmethod
    def get_mint_burn_from_events(base_pool, parsed_event, tokens_scalars):
        mint_burn = MintBurn(
            pool_address=base_pool.address,
            sender=parsed_event["sender"] if parsed_event.get("sender") else parsed_event["owner"],
            owner=parsed_event["owner"] if parsed_event.get("owner") else parsed_event["sender"],
            amounts=[
                parsed_event["qty0"] / tokens_scalars[0],
                parsed_event["qty1"] / tokens_scalars[1],
            ],
        )
        return mint_burn

    @staticmethod
    def get_swap_from_swap_event(base_pool, parsed_event, tokens_scalars):
        amount0 = parsed_event["deltaQty0"] / tokens_scalars[0]
        amount1 = parsed_event["deltaQty1"] / tokens_scalars[1]

        swap = Swap(
            pool_address=base_pool.address,
            sender=parsed_event["sender"],
            to=parsed_event["recipient"],
            amounts=[amount0, amount1],
        )
        return swap

    def get_pool_details(self, pool_address, block_identifier: str | int = "latest"):
        values = (
            self.abi[POOL_CONTRACT]
            .contract.functions.getPoolState()
            .call({"to": pool_address}, block_identifier)
        )
        names = ["sqrtP", "currentTick", "nearestCurrentTick", "locked"]
        return dict(zip(names, values))

    def get_pool_finances(
        self,
        base_pool,
        parsed_event,
        tokens_scalars,
        block_identifier: str | int = "latest",
    ):
        sqrt_price_x96 = parsed_event.get("sqrtP")
        if not sqrt_price_x96:
            pool_details = self.get_pool_details(base_pool.address)
            sqrt_price_x96 = pool_details["sqrtP"]

        token0_price = self.calculate_token0_price_from_sqrt_price_x96(
            sqrt_price_x96, tokens_scalars
        )
        token1_price = 1 / token0_price

        reserves = []
        for idx, token in enumerate(base_pool.tokens_addresses):
            reserves.append(
                self.get_contract_reserve(base_pool.address, token, block_identifier)
                / tokens_scalars[idx]
            )

        pool = PoolFinances(**base_pool.dict())
        pool.reserves = [reserves[0], reserves[1]]
        pool.prices = get_prices_for_two_pool(token0_price, token1_price)
        return pool
