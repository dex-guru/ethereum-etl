import logging
from typing import Literal

from web3 import Web3

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.domain.dex_trade import EthDexTrade
from ethereumetl.domain.receipt_log import ParsedReceiptLog
from ethereumetl.domain.token import EthToken
from ethereumetl.domain.token_transfer import EthTokenTransfer
from ethereumetl.service.dex.uniswap_v3.uniswap_v3 import UniswapV3Amm

logs = logging.getLogger(__name__)
to_checksum = Web3.to_checksum_address

POOL_CONTRACT = "Pool"


class QuickswapV3Amm(UniswapV3Amm):

    def __init__(self, web3: Web3, chain_id: int | None = None):
        super().__init__(web3, chain_id, __file__)

    def resolve_receipt_log(
        self,
        parsed_receipt_log: ParsedReceiptLog,
        dex_pool: EthDexPool | None = None,
        tokens_for_pool: list[EthToken] | None = None,
        transfers_for_transaction: list[EthTokenTransfer] | None = None,
    ) -> EthDexTrade | None:
        return super().resolve_receipt_log(
            parsed_receipt_log, dex_pool, tokens_for_pool, transfers_for_transaction
        )

    def _resolve_finance_info(
        self,
        parsed_receipt_log,
        dex_pool: EthDexPool,
        token_scalars: list[int],
    ):
        parsed_event = parsed_receipt_log.parsed_event
        price = parsed_event.get("price")
        if not price:
            state = self.get_state(to_checksum(dex_pool.address), parsed_receipt_log.block_number)
            price = state["price"]
        token0_price = self.calculate_token0_price_from_sqrt_price_x96(price, token_scalars)
        token1_price = 1 / token0_price
        reserves = []
        for idx, token in enumerate(dex_pool.token_addresses):
            reserves.append(
                self.erc20_contract_abi.functions.balanceOf(to_checksum(dex_pool.address)).call(
                    {"to": to_checksum(token)},
                    parsed_receipt_log.block_number,
                )
                / token_scalars[idx]
            )
        finance_info = {
            'reserve_0': reserves[0] / token_scalars[0],
            'reserve_1': reserves[1] / token_scalars[1],
            'price_0': token0_price,
            'price_1': token1_price,
        }
        return finance_info

    def get_state(self, pool_address: str, block_identifier: int | Literal['latest'] = "latest"):
        values = self.pool_contract_abi.functions.globalState().call(
            {"to": pool_address}, block_identifier
        )
        names = [
            "price",
            "tick",
            "fee",
            "timepointIndex",
            "communityFeeToken0",
            "communityFeeToken1",
            "unlocked",
        ]
        return dict(zip(names, values))

    def resolve_asset_from_log(self, parsed_log: ParsedReceiptLog) -> EthDexPool | None:
        return super().resolve_asset_from_log(parsed_log)
