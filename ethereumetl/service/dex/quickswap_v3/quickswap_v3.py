from clients.blockchain import BaseBlockchainClient
from clients.blockchain.amm.base.base_contract import BaseContract
from clients.blockchain.interfaces import AmmClientI
from clients.blockchain.models.pool import BasePool, PoolFinances, V3Pool
from clients.blockchain.models.protocol_transaction import (
    BaseProtocolTransaction,
    MintBurn,
    ProtocolType,
    Swap,
    TransactionType,
)
from clients.blockchain.models.tokens import ERC20Token
from clients.blockchain.models.transaction import ReceiptLog
from eth_typing import ChecksumAddress
from utils.logger import get_logger
from utils.prices import get_prices_for_two_pool
from web3 import Web3

from .uniswap_v3 import UniswapV3Amm

logs = get_logger(__name__)

POOL_CONTRACT = "Pool"


class QuickswapV3Amm(UniswapV3Amm, BaseContract, BaseBlockchainClient, AmmClientI):
    pool_contract_names = (POOL_CONTRACT,)
    pool_contracts_events_enum = TransactionType

    def resolve_receipt_log(
        self,
        receipt_log: ReceiptLog,
        base_pool: BasePool,
        erc20_tokens: list[ERC20Token],
    ) -> dict | None:
        logs.debug(f"resolving {receipt_log.transaction_hash.hex()}-{receipt_log.log_index}")
        try:
            topic = receipt_log.topics[0][0:4]
        except IndexError:
            logs.error(f"Cant get receipt_log.topics[0][0:4], index error, log: {receipt_log}")
            return None
        event_name = self.abi[POOL_CONTRACT].topic_keccaks.get(topic, None)
        if receipt_log.topics and event_name:
            tokens_scalars = []
            for erc20_token in erc20_tokens:
                tokens_scalars.append(10**erc20_token.decimals)
            parsed_event = self.parse_event(self.abi[POOL_CONTRACT], event_name, receipt_log)

            if event_name.lower() == self.pool_contracts_events_enum.swap.name:
                logs.debug("resolving swap from swap event")
                swap = self.get_swap_from_swap_event(base_pool, parsed_event, tokens_scalars)
                logs.debug(f"resolved swap from swap event {swap}")
                swap.log_index = receipt_log.log_index
                return {
                    "swaps": [swap],
                    "pools": [
                        self.get_pool_finances(
                            base_pool,
                            parsed_event,
                            tokens_scalars,
                            receipt_log.block_number,
                        )
                    ],
                }

            if event_name.lower() == self.pool_contracts_events_enum.burn.name:
                logs.debug("resolving burn from burn event")
                burn = self.get_mint_burn_from_events(base_pool, parsed_event, tokens_scalars)
                logs.debug("resolving burn from burn event")
                burn.log_index = receipt_log.log_index
                if not all(burn.amounts):
                    logs.info(f"burn amounts are zero. Probably it is a collect event. {burn}")
                    return None
                return {
                    "burns": [burn],
                    "pools": [
                        self.get_pool_finances(
                            base_pool,
                            parsed_event,
                            tokens_scalars,
                            receipt_log.block_number - 1,
                        )
                    ],
                }

            if event_name.lower() == self.pool_contracts_events_enum.mint.name:
                logs.debug("resolving burn from mint event")
                mint = self.get_mint_burn_from_events(base_pool, parsed_event, tokens_scalars)
                logs.debug("resolving burn from mint event")
                mint.log_index = receipt_log.log_index
                return {
                    "mints": [mint],
                    "pools": [
                        self.get_pool_finances(
                            base_pool,
                            parsed_event,
                            tokens_scalars,
                            receipt_log.block_number,
                        )
                    ],
                }
            if event_name.lower() == "collect":
                logs.debug("resolving collect from collect event")
                collect = self.get_mint_burn_from_events(base_pool, parsed_event, tokens_scalars)
                logs.debug("resolving collect from collect event")
                collect.log_index = receipt_log.log_index
                return {
                    "burns": [collect],
                    "pools": [
                        self.get_pool_finances(
                            base_pool,
                            parsed_event,
                            tokens_scalars,
                            receipt_log.block_number - 1,
                        )
                    ],
                }

    def get_pool_finances(
        self,
        base_pool,
        parsed_event,
        tokens_scalars,
        block_number: int | str = "latest",
    ):
        price = parsed_event.get("price")
        if not price:
            state = self.get_state(base_pool.address)
            price = state["price"]

        token0_price = self.calculate_token0_price_from_sqrt_price_x96(price, tokens_scalars)
        token1_price = 1 / token0_price

        reserves = []
        for idx, token in enumerate(base_pool.tokens_addresses):
            reserves.append(
                self.get_contract_reserve(base_pool.address, token, block_number)
                / tokens_scalars[idx]
            )

        pool = PoolFinances(**base_pool.dict())
        pool.reserves = [reserves[0], reserves[1]]
        pool.prices = get_prices_for_two_pool(token0_price, token1_price)
        return pool

    def get_state(self, pool_address: str, block_identifier: int | str = "latest"):
        values = (
            self.abi[POOL_CONTRACT]
            .contract.functions.globalState()
            .call({"to": pool_address}, block_identifier)
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

    @staticmethod
    def get_mint_burn_from_events(base_pool, parsed_event, tokens_scalars):
        mint_burn = MintBurn(
            pool_address=base_pool.address,
            sender=parsed_event["owner"]
            if parsed_event.get("owner")
            else parsed_event["recipient"],
            owner=parsed_event["recipient"]
            if parsed_event.get("recipient")
            else parsed_event["owner"],
            amounts=[
                parsed_event["amount0"] / tokens_scalars[0],
                parsed_event["amount1"] / tokens_scalars[1],
            ],
        )
        return mint_burn

    @staticmethod
    def get_swap_from_swap_event(base_pool, parsed_event, tokens_scalars):
        amount0 = parsed_event["amount0"] / tokens_scalars[0]
        amount1 = parsed_event["amount1"] / tokens_scalars[1]

        swap = Swap(
            pool_address=base_pool.address,
            sender=parsed_event["sender"],
            to=parsed_event["recipient"],
            amounts=[amount0, amount1],
        )
        return swap

    def get_base_pool(self, address: ChecksumAddress) -> BasePool | None:
        logs.debug(f"Resolving pool addresses for {address}")
        tokens_addresses = self.get_tokens_addresses_for_pool(address)
        ticks_spacing = self.get_ticks_spacing(address)

        if tokens_addresses:
            return BasePool(
                address=address,
                tokens_addresses=tokens_addresses,
                fee=V3Pool.tick_spacing_to_fee(ticks_spacing),  # import info for swap
            )

    def get_tokens_addresses_for_pool(self, pool_address: str) -> list | None:
        tokens_addresses = [
            (
                self.abi[POOL_CONTRACT]
                .contract.functions.token0()
                .call({"to": pool_address}, "latest")
            ).lower(),
            (
                self.abi[POOL_CONTRACT]
                .contract.functions.token1()
                .call({"to": pool_address}, "latest")
            ).lower(),
        ]
        return tokens_addresses

    def resolve_base_transaction(
        self, event_name, receipt_log, transaction
    ) -> BaseProtocolTransaction:
        if event_name.lower() in [
            self.pool_contracts_events_enum.mint.name,
            self.pool_contracts_events_enum.burn.name,
            self.pool_contracts_events_enum.swap.name,
        ]:
            tokens_addresses = self.get_tokens_addresses_for_pool(
                Web3.toChecksumAddress(receipt_log.address)
            )
        elif event_name.lower() == self.pool_contracts_events_enum.transfer.name:
            tokens_addresses = [receipt_log.address]
        else:
            tokens_addresses = []
        base_transaction = BaseProtocolTransaction(
            id=f"{transaction.address}-{receipt_log.log_index}",
            pool_address=receipt_log.address,
            log_index=receipt_log.log_index,
            transaction_type=event_name.lower(),
            transaction_address=transaction.address,
            timestamp=transaction.timestamp,
            protocol_type=ProtocolType.uniswap_v3,
            tokens_addresses=tokens_addresses,
            wallet_address=transaction.from_address,
        )
        return base_transaction

    def get_ticks_spacing(self, pool_address, block_identifier: str | int = "latest"):
        return (
            self.abi[POOL_CONTRACT]
            .contract.functions.tickSpacing()
            .call({"to": pool_address}, block_identifier)
        )
