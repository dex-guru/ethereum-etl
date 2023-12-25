import logging
from enum import Enum

from web3 import Web3

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.domain.receipt_log import EthReceiptLog
from ethereumetl.domain.token import EthToken
from ethereumetl.service.dex.uniswap_v2.uniswap_v2 import UniswapV2Amm

logs = logging.getLogger(__name__)

POOL_CONTRACT = "Pool"
FACTORY_CONTRACT = "UniswapV2Factory"

to_checksum = Web3.toChecksumAddress


class MeshswapAmm(UniswapV2Amm):
    class MeshswapTransactionType(Enum):
        """supported events from meshswap contracts."""

        ExchangePos = "ExchangePos"  # analog of uniswap v2 swap
        ExchangeNeg = "ExchangeNeg"  # analog of uniswap v2 swap
        AddLiquidity = "AddLiquidity"  # mint
        RemoveLiquidity = "RemoveLiquidity"  # burn
        Sync = "Sync"

    def get_base_pool(self, address: str) -> EthDexPool | None:
        if address in [
            "0x10f4a785f458bc144e3706575924889954946639",
            "0x650a6938ec6f96f8b62ae712e5f4ad9c3fffe956",
        ]:
            # This is router address on Polygon, as router can also issue ExchangePos events.
            return None
        return super().get_base_pool(address)

    def resolve_receipt_log(
        self,
        receipt_log: EthReceiptLog,
        base_pool: EthDexPool,
        erc20_tokens: list[EthToken],
    ) -> dict | None:
        pass

    # def resolve_receipt_log(
    #     self,
    #     receipt_log: ReceiptLog,
    #     base_pool: BasePool,
    #     erc20_tokens: List[ERC20Token],
    # ) -> Optional[dict]:
    #     logs.debug(f"resolving {receipt_log.transaction_hash.hex()}-{receipt_log.log_index}")
    #     event_name = self._get_event_name(receipt_log.topics)
    #
    #     if not all((receipt_log.topics, event_name)):
    #         return None
    #
    #     tokens_scalars = []
    #     for erc20_token in erc20_tokens:
    #         tokens_scalars.append((erc20_token.address, 10**erc20_token.decimals))
    #     parsed_event = self.parse_event(self.abi[POOL_CONTRACT], event_name, receipt_log)
    #
    #     if event_name in self.pool_contracts_events_enum.swap_events():
    #         logs.debug("resolving swap from swap event")
    #         swap = self.get_swap_from_swap_event(base_pool, parsed_event, tokens_scalars)
    #         pool = self.get_pool_finances(
    #             base_pool, receipt_log.block_number, [i[1] for i in tokens_scalars]
    #         )
    #         logs.debug(f"resolved swap from swap event {swap}")
    #         swap.log_index = receipt_log.log_index
    #         return {
    #             "swaps": [swap],
    #             "pools": [pool],
    #         }
    #
    #     if event_name == self.pool_contracts_events_enum.RemoveLiquidity.name:
    #         logs.debug(f"resolving burn from burn event")
    #         burn = self.get_mint_burn_from_events(base_pool, parsed_event, tokens_scalars)
    #         pool = self.get_pool_finances(
    #             base_pool, receipt_log.block_number - 1, [i[1] for i in tokens_scalars]
    #         )
    #         logs.debug(f"resolving burn from burn event")
    #         burn.log_index = receipt_log.log_index
    #         return {
    #             "burns": [burn],
    #             "pools": [pool],
    #         }
    #
    #     if event_name == self.pool_contracts_events_enum.AddLiquidity.name:
    #         logs.debug(f"resolving burn from mint event")
    #         mint = self.get_mint_burn_from_events(base_pool, parsed_event, tokens_scalars)
    #         pool = self.get_pool_finances(
    #             base_pool, receipt_log.block_number, [i[1] for i in tokens_scalars]
    #         )
    #         logs.debug(f"resolving burn from mint event")
    #         mint.log_index = receipt_log.log_index
    #         return {
    #             "mints": [mint],
    #             "pools": [pool],
    #         }
    #
    #     # if event_name == self.pool_contracts_events_enum.Sync.name:
    #     #     logs.debug(f'resolving pool finances from sync event')
    #     #     pool = self.get_pool_finances_from_sync_event(base_pool, parsed_event, tokens_scalars)
    #     #     logs.debug(f'resolved pool finances from sync event {pool}')
    #     #     return {
    #     #         "pools": [pool]
    #     #     }
    #
    # @staticmethod
    # def get_mint_burn_from_events(base_pool, parsed_event, tokens_scalars):
    #     if parsed_event["token0"] == tokens_scalars[0][0]:
    #         amount0 = parsed_event["amount0"] / tokens_scalars[0][1]
    #         amount1 = parsed_event["amount1"] / tokens_scalars[1][1]
    #     else:
    #         amount0 = parsed_event["amount1"] / tokens_scalars[0][1]
    #         amount1 = parsed_event["amount0"] / tokens_scalars[1][1]
    #     mint_burn = MintBurn(
    #         pool_address=base_pool.address,
    #         sender=parsed_event["user"],
    #         owner=parsed_event["user"],
    #         amounts=[amount0, amount1],
    #     )
    #     return mint_burn
    #
    # @staticmethod
    # def get_swap_from_swap_event(base_pool, parsed_event, tokens_scalars):
    #     if parsed_event["token0"].lower() == tokens_scalars[0][0].lower():
    #         amount0 = parsed_event["amount0"] / tokens_scalars[0][1]
    #         amount1 = parsed_event["amount1"] / tokens_scalars[1][1]
    #     else:
    #         amount0 = parsed_event["amount1"] / tokens_scalars[0][1]
    #         amount1 = parsed_event["amount0"] / tokens_scalars[1][1]
    #     swap = Swap(
    #         pool_address=base_pool.address,
    #         sender="0x000000000000000000000000000000000000dead",
    #         to="0x000000000000000000000000000000000000dead",
    #         amounts=[amount0, amount1],
    #     )
    #     return swap
    #
    # @staticmethod
    # def get_pool_finances_from_sync_event(base_pool, parsed_event, tokens_scalars):
    #     reserve0 = parsed_event["reserveA"] / tokens_scalars[0][1]
    #     reserve1 = parsed_event["reserveB"] / tokens_scalars[1][1]
    #
    #     if reserve0:
    #         token0_price = float(reserve1 / reserve0)
    #     else:
    #         logs.error("cant get price, as reserve0 = 0")
    #         token0_price = 0
    #     if reserve1:
    #         token1_price = float(reserve0 / reserve1)
    #     else:
    #         logs.error("cant get price, as reserve1 = 0")
    #         token1_price = 0
    #     if token0_price >= float(9.999999999999999e45):
    #         logs.error("cant get price, as it's infinite")
    #         token0_price = 0
    #     if token1_price >= float(9.999999999999999e45):
    #         logs.error("cant get price, as it's infinite")
    #         token1_price = 0
    #     pool = PoolFinances(**base_pool.dict())
    #     pool.reserves = [reserve0, reserve1]
    #     pool.prices = get_prices_for_two_pool(token0_price, token1_price)
    #     return pool
