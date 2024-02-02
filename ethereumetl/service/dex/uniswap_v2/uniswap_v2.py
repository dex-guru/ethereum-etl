import json
import logging
from collections.abc import Callable
from functools import cache
from pathlib import Path

from eth_typing import ChecksumAddress
from web3 import Web3
from web3.exceptions import BadFunctionCallOutput, ContractLogicError

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.domain.dex_trade import EthDexTrade
from ethereumetl.domain.receipt_log import ParsedReceiptLog
from ethereumetl.domain.token import EthToken
from ethereumetl.domain.token_transfer import EthTokenTransfer
from ethereumetl.misc.info import INFINITE_PRICE_THRESHOLD
from ethereumetl.service.dex.base.interface import DexClientInterface
from ethereumetl.service.dex.enums import DexPoolFeeAmount
from ethereumetl.utils import get_prices_for_two_pool

to_checksum = Web3.toChecksumAddress


class UniswapV2Amm(DexClientInterface):
    def __init__(self, web3: Web3, chain_id: int | None = None):
        pool_abi_path = Path(__file__).parent / "Pool.json"
        abi = json.loads(pool_abi_path.read_text())
        self._w3: Web3 = web3
        self.pool_contract = self._w3.eth.contract(abi=abi)

    @property
    def event_resolver(self) -> dict[str, Callable]:
        return {
            "Swap": self._get_trade_from_swap_event,
            "Burn": self._get_trade_from_burn_event,
            "Mint": self._get_trade_from_mint_event,
            # "Sync": self.get_pool_finances_from_sync_event,
        }

    def resolve_asset_from_log(self, parsed_log: ParsedReceiptLog) -> EthDexPool | None:
        address = parsed_log.address
        logging.debug(f"Resolving pool addresses for {address}")
        factory_address = self.get_factory_address(to_checksum(address))
        if not factory_address:
            logging.debug(f"Factory address not found for {address}, resolving {parsed_log}")
            return None
        tokens_addresses = self.get_tokens_addresses_for_pool(to_checksum(address))
        if not tokens_addresses:
            logging.debug(f"Tokens addresses not found for {address}, resolving {parsed_log}")
            return None
        return EthDexPool(
            address=address,
            token_addresses=[token_address.lower() for token_address in tokens_addresses],
            fee=DexPoolFeeAmount.MEDIUM.value,
            lp_token_addresses=[address],
            factory_address=factory_address.lower(),
        )

    @cache
    def get_factory_address(self, pool_address: str) -> str | None:
        try:
            factory_address = self.pool_contract.functions.factory().call(
                {"to": to_checksum(pool_address)}, "latest"
            )
            if not self._w3.is_address(factory_address):
                raise ValueError(f"Factory address is not valid: {factory_address}")
            return factory_address.lower()
        except (TypeError, ContractLogicError, ValueError, BadFunctionCallOutput) as e:
            logging.debug(f"Not found factory, fallback to maintainer. Error: {e}")
        return None

    @cache
    def get_tokens_addresses_for_pool(self, pool_address: ChecksumAddress) -> list | None:
        logging.debug(f"Resolving tokens addresses for {pool_address}")
        try:
            tokens_addresses = [
                (self.pool_contract.functions.token0().call({"to": pool_address}, "latest")),
                (self.pool_contract.functions.token1().call({"to": pool_address}, "latest")),
            ]
            for token_address in tokens_addresses:
                if not self._w3.is_address(token_address):
                    raise ValueError(f"Token address is not valid: {token_address}")
            return tokens_addresses
        except (TypeError, ContractLogicError, ValueError, BadFunctionCallOutput) as e:
            logging.debug(f"Cant resolve tokens_addresses for pair {pool_address}, {e}")
        return None

    @staticmethod
    def _get_scalars_for_tokens(tokens: list[EthToken], dex_pool: EthDexPool) -> list[int]:
        token_scalars = []
        for token_address in dex_pool.token_addresses:
            token = next((token for token in tokens if token.address == token_address), None)
            if not token:
                logging.debug(f"Token {token_address} not found in tokens")
                return []
            token_scalars.append(10**token.decimals)
        return token_scalars

    def resolve_receipt_log(
        self,
        parsed_receipt_log: ParsedReceiptLog,
        dex_pool: EthDexPool | None = None,
        tokens_for_pool: list[EthToken] | None = None,
        transfers_for_transaction: list[EthTokenTransfer] | None = None,
    ) -> EthDexTrade | None:
        logging.debug(f"Resolving receipt log {parsed_receipt_log}")
        event_name = parsed_receipt_log.event_name
        if not self.event_resolver.get(event_name):
            logging.debug(f"Event {event_name} not found in resolver")
            return None
        if not dex_pool or not tokens_for_pool:
            logging.debug(f"Pool or tokens not found for {parsed_receipt_log}")
            return None

        token_scalars = self._get_scalars_for_tokens(tokens_for_pool, dex_pool)

        resolve_func: Callable = self.event_resolver[event_name]
        finance_info = self.resolve_finance_info(parsed_receipt_log, token_scalars)
        resolved_log = resolve_func(parsed_receipt_log, dex_pool, finance_info, token_scalars)
        logging.debug(f"Resolved receipt log {resolved_log}")
        return resolved_log

    def resolve_finance_info(
        self, parsed_receipt_log: ParsedReceiptLog, token_scalars: list[int]
    ) -> dict | None:
        try:
            reserves = self.pool_contract.functions.getReserves().call(
                {"to": to_checksum(parsed_receipt_log.address)},
                block_identifier=parsed_receipt_log.block_number - 1,
            )
        except (TypeError, ContractLogicError, ValueError, BadFunctionCallOutput) as e:
            logging.debug(f"Not found reserves for {parsed_receipt_log.address}. Error: {e}")
            return {
                'reserve_0': 0,
                'reserve_1': 0,
                'price_0': 0,
                'price_1': 0,
            }
        reserve_0 = reserves[0] / token_scalars[0]
        reserve_1 = reserves[1] / token_scalars[1]
        price_0 = float(reserve_1 / reserve_0)
        price_1 = float(reserve_0 / reserve_1)
        if price_0 >= INFINITE_PRICE_THRESHOLD:
            logging.debug(f"Price is infinite for {parsed_receipt_log.address}")
            price_0 = 0
        if price_1 >= INFINITE_PRICE_THRESHOLD:
            logging.debug(f"Price is infinite for {parsed_receipt_log.address}")
            price_1 = 0
        return {
            'reserve_0': reserve_0,
            'reserve_1': reserve_1,
            'price_0': price_0,
            'price_1': price_1,
        }

    @staticmethod
    def _get_trade_from_mint_event(
        parsed_receipt_log: ParsedReceiptLog,
        dex_pool,
        finance_info: dict,
        tokens_scalars: list[int],
    ) -> EthDexTrade:
        parsed_event = parsed_receipt_log.parsed_event
        return EthDexTrade(
            pool_address=parsed_receipt_log.address,
            token_amounts=[
                parsed_event["amount0"] / tokens_scalars[0],
                parsed_event["amount1"] / tokens_scalars[1],
            ],
            transaction_hash=parsed_receipt_log.transaction_hash,
            log_index=parsed_receipt_log.log_index,
            block_number=parsed_receipt_log.block_number,
            event_type='mint',
            token_reserves=[finance_info['reserve_0'], finance_info['reserve_1']],
            token_prices=get_prices_for_two_pool(finance_info['price_0'], finance_info['price_1']),
            lp_token_address=parsed_receipt_log.address,
            token_addresses=[dex_pool.token_addresses[0], dex_pool.token_addresses[1]],
        )

    @staticmethod
    def _get_trade_from_burn_event(
        parsed_receipt_log: ParsedReceiptLog,
        dex_pool,
        finance_info: dict,
        tokens_scalars: list[int],
    ) -> EthDexTrade:
        parsed_event = parsed_receipt_log.parsed_event

        return EthDexTrade(
            pool_address=parsed_receipt_log.address,
            token_amounts=[
                parsed_event["amount0"] / tokens_scalars[0],
                parsed_event["amount1"] / tokens_scalars[1],
            ],
            transaction_hash=parsed_receipt_log.transaction_hash,
            log_index=parsed_receipt_log.log_index,
            block_number=parsed_receipt_log.block_number,
            event_type='burn',
            token_reserves=[finance_info['reserve_0'], finance_info['reserve_1']],
            token_prices=get_prices_for_two_pool(finance_info['price_0'], finance_info['price_1']),
            lp_token_address=parsed_receipt_log.address,
            token_addresses=[dex_pool.token_addresses[0], dex_pool.token_addresses[1]],
        )

    @staticmethod
    def _get_trade_from_swap_event(
        parsed_receipt_log: ParsedReceiptLog,
        dex_pool,
        finance_info: dict,
        tokens_scalars: list[int],
    ) -> EthDexTrade:
        parsed_event = parsed_receipt_log.parsed_event
        return EthDexTrade(
            pool_address=parsed_receipt_log.address,
            token_amounts=[
                parsed_event["amount0In"] / tokens_scalars[0]
                - parsed_event["amount0Out"] / tokens_scalars[0],
                parsed_event["amount1In"] / tokens_scalars[1]
                - parsed_event["amount1Out"] / tokens_scalars[1],
            ],
            transaction_hash=parsed_receipt_log.transaction_hash,
            log_index=parsed_receipt_log.log_index,
            block_number=parsed_receipt_log.block_number,
            event_type='swap',
            token_reserves=[finance_info['reserve_0'], finance_info['reserve_1']],
            token_prices=get_prices_for_two_pool(finance_info['price_0'], finance_info['price_1']),
            token_addresses=[dex_pool.token_addresses[0], dex_pool.token_addresses[1]],
        )

        # if event_name.lower() == self.pool_contracts_events_enum.sync.name:
        #     logging.debug(f'resolving pool finances from sync event')
        #     pool = self.get_pool_finances_from_sync_event(base_pool, parsed_event, tokens_scalars)
        #     logging.debug(f'resolved pool finances from sync event {pool}')
        #     return {
        #         "pools": [pool]
        #     }

    # # def get_pool_finances(
    # #     self,
    # #     base_pool: BasePool,
    # #     block_number: Union[str, int],
    # #     tokens_scalars: List[int],
    # # ):
    # #     try:
    # #         reserves = (
    # #             self.abi[POOL_CONTRACT]
    # #             .contract.functions.getReserves()
    # #             .call({"to": base_pool.address}, block_number)
    # #         )
    # #     except (TypeError, ContractLogicError, ValueError, BadFunctionCallOutput) as e:
    # #         logging.debug("Not found reserves for %s. Error: %s", base_pool.address, e)
    # #         if "backsync" in config.PIPELINE:
    # #             return None
    # #         reserves = (
    # #             self.abi[POOL_CONTRACT]
    # #             .contract.functions.getReserves()
    # #             .call({"to": base_pool.address}, "latest")
    # #         )
    # #     pool = PoolFinances(**base_pool.dict())
    # #     reserves = [reserves[0] / tokens_scalars[0], reserves[1] / tokens_scalars[1]]
    # #     reserve0 = reserves[0]
    # #     reserve1 = reserves[1]
    # #     if reserve0:
    # #         token0_price = float(reserve1 / reserve0)
    # #     else:
    # #         logging.warning(
    # #             "cant get price, as reserve0 = 0",
    # #             extra={
    # #                 "pool_address": base_pool.address,
    # #                 "block_number": block_number,
    # #             },
    # #         )
    # #         token0_price = 0
    # #     if reserve1:
    # #         token1_price = float(reserve0 / reserve1)
    # #     else:
    # #         logging.warning(
    # #             "cant get price, as reserve0 = 0",
    # #             extra={
    # #                 "pool_address": base_pool.address,
    # #                 "block_number": block_number,
    # #             },
    # #         )
    # #         token1_price = 0
    # #     if token0_price >= INFINITE_PRICE_THRESHOLD:
    # #         logging.warning(
    # #             "cant get price, as it's infinite",
    # #             extra={
    # #                 "pool_address": base_pool.address,
    # #                 "block_number": block_number,
    # #             },
    # #         )
    # #         token0_price = 0
    # #     if token1_price >= INFINITE_PRICE_THRESHOLD:
    # #         logging.warning(
    # #             "cant get price, as it's infinite",
    # #             extra={
    # #                 "pool_address": base_pool.address,
    # #                 "block_number": block_number,
    # #             },
    # #         )
    # #         token1_price = 0
    # #     pool.prices = get_prices_for_two_pool(token0_price, token1_price)
    # #     pool.reserves = reserves
    # #     return pool
    #
    # # @staticmethod
    # # def get_mint_burn_from_events(base_pool, parsed_event, tokens_scalars):
    # #     amount0 = parsed_event["amount0"] / tokens_scalars[0]
    # #     amount1 = parsed_event["amount1"] / tokens_scalars[1]
    # #     mint_burn = MintBurn(
    # #         pool_address=base_pool.address,
    # #         sender=parsed_event["sender"] if parsed_event.get("sender") else parsed_event["owner"],
    # #         owner=parsed_event["owner"] if parsed_event.get("owner") else parsed_event["sender"],
    # #         amounts=[amount0, amount1],
    # #     )
    # #     return mint_burn
    # #
    # # @staticmethod
    # # def get_swap_from_swap_event(base_pool, parsed_event, tokens_scalars):
    # #     amount0 = (
    # #         parsed_event["amount0In"] / tokens_scalars[0]
    # #         - parsed_event["amount0Out"] / tokens_scalars[0]
    # #     )
    # #     amount1 = (
    # #         parsed_event["amount1In"] / tokens_scalars[1]
    # #         - parsed_event["amount1Out"] / tokens_scalars[1]
    # #     )
    # #     swap = Swap(
    # #         pool_address=base_pool.address,
    # #         sender=parsed_event["sender"],
    # #         to=parsed_event["to"],
    # #         amounts=[amount0, amount1],
    # #     )
    # #     return swap
    # #
    # # @staticmethod
    # # def get_pool_finances_from_sync_event(base_pool, parsed_event, tokens_scalars):
    # #     reserve0 = parsed_event["reserve0"] / tokens_scalars[0]
    # #     reserve1 = parsed_event["reserve1"] / tokens_scalars[1]
    # #     if reserve0:
    # #         token0_price = float(reserve1 / reserve0)
    # #     else:
    # #         logging.error("cant get price, as reserve0 = 0")
    # #         token0_price = 0
    # #     if reserve1:
    # #         token1_price = float(reserve0 / reserve1)
    # #     else:
    # #         logging.error("cant get price, as reserve1 = 0")
    # #         token1_price = 0
    # #     if token0_price >= INFINITE_PRICE_THRESHOLD:
    # #         logging.error("cant get price, as it's infinite")
    # #         token0_price = 0
    # #     if token1_price >= INFINITE_PRICE_THRESHOLD:
    # #         logging.error("cant get price, as it's infinite")
    # #         token1_price = 0
    # #     pool = PoolFinances(**base_pool.dict())
    # #     pool.reserves = [reserve0, reserve1]
    #     pool.prices = get_prices_for_two_pool(token0_price, token1_price)
    #     return pool
