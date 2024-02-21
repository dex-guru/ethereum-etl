import json
import logging
from enum import Enum
from pathlib import Path

from eth_typing import ChecksumAddress
from web3 import Web3
from web3.exceptions import BadFunctionCallOutput, ContractLogicError

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.domain.dex_trade import EthDexTrade
from ethereumetl.domain.receipt_log import ParsedReceiptLog
from ethereumetl.domain.token import EthToken
from ethereumetl.domain.token_transfer import EthTokenTransfer
from ethereumetl.misc.info import INFINITE_PRICE_THRESHOLD, get_chain_config
from ethereumetl.service.dex.base.base_dex_client import BaseDexClient
from ethereumetl.service.dex.enums import DexPoolFeeAmount
from ethereumetl.utils import get_prices_for_two_pool

logger = logging.getLogger(__name__)

AMM_TYPE = "1inch"
POOL_CONTRACT = "OneInchPool"
FACTORY_CONTRACT = "OneInchFactory"


to_checksum = Web3.to_checksum_address


class OneInchTransactionType(Enum):
    swapped = "swapped"
    withdrawn = "withdrawn"
    deposited = "deposited"


class OneInchAmm(BaseDexClient):

    pool_contracts_events_enum = OneInchTransactionType
    POOL_ABI_PATH = "OneInchPool.json"

    def __init__(self, web3: Web3, chain_id: int | None = None, file_path: str = __file__):
        super().__init__(web3, chain_id)
        _path_to_abi = (Path(file_path).parent.parent,)
        pool_abi_path = Path(file_path).parent / self.POOL_ABI_PATH
        self._w3 = web3
        pool_abi_json = json.loads(pool_abi_path.read_text())
        self.pool_contract_abi = self._w3.eth.contract(abi=pool_abi_json)
        chain_config = get_chain_config(chain_id)
        self.native_token: dict = chain_config['native_token']

    def _get_factory_address(self, pool_address: ChecksumAddress) -> ChecksumAddress | None:
        try:
            return self.pool_contract_abi.functions.factory().call({"to": pool_address})
        except (
            ValueError,
            TypeError,
            BadFunctionCallOutput,
            ContractLogicError,
        ):
            return None

    def resolve_asset_from_log(self, parsed_log: ParsedReceiptLog) -> EthDexPool | None:
        pool_address = parsed_log.address
        checksum_address = to_checksum(pool_address)
        factory = self._get_factory_address(to_checksum(checksum_address))
        if not factory:
            return None
        tokens_addresses = self.get_tokens_addresses_for_pool(checksum_address)
        if not tokens_addresses:
            return None
        return EthDexPool(
            address=pool_address,
            token_addresses=[address.lower() for address in tokens_addresses],
            factory_address=factory.lower(),
            fee=DexPoolFeeAmount.MEDIUM.value,
            lp_token_addresses=[pool_address],
        )

    def get_tokens_addresses_for_pool(self, pool_address: ChecksumAddress) -> list | None:
        logger.debug(f"Resolving tokens addresses for {pool_address}")
        try:
            tokens_addresses = self.pool_contract_abi.functions.getTokens().call(
                {"to": pool_address}, "latest"
            )
        except (TypeError, ValueError, BadFunctionCallOutput, ContractLogicError) as e:
            logger.error(f"Cant resolve tokens_addressese for pair {pool_address}, {e}")
            return None

        return tokens_addresses

    def resolve_receipt_log(
        self,
        parsed_receipt_log: ParsedReceiptLog,
        dex_pool: EthDexPool,
        tokens_for_pool: list[EthToken],
        transfers_for_transaction: list[EthTokenTransfer],
    ) -> EthDexTrade | None:
        event_name = parsed_receipt_log.event_name
        if event_name.lower() == self.pool_contracts_events_enum.withdrawn.name.lower():
            block_number = parsed_receipt_log.block_number - 1
        else:
            block_number = parsed_receipt_log.block_number

        parsed_receipt_log.parsed_event = self.normalize_event(
            self._get_events_abi(self.pool_contract_abi, event_name)['inputs'],
            parsed_receipt_log.parsed_event,
        )
        parsed_event = parsed_receipt_log.parsed_event

        tokens_scalars = self._get_scalars_for_tokens(tokens_for_pool, dex_pool)
        for i, erc20_token in enumerate(tokens_for_pool):
            if erc20_token.address.lower() == self.native_token['address'].lower():
                parsed_event[f"reserve{i}"] = (
                    self._w3.eth.get_balance(dex_pool.address, block_identifier=block_number)
                    / self.native_token['decimals']
                )
            else:
                parsed_event[f"reserve{i}"] = (
                    self._get_balance_of(erc20_token.address, dex_pool.address, block_number)
                    / tokens_scalars[i]
                )

        if event_name.lower() == self.pool_contracts_events_enum.swapped.name:
            return self._parse_swapped(dex_pool, tokens_scalars, parsed_receipt_log)

        elif event_name.lower() == self.pool_contracts_events_enum.withdrawn.name:
            return self._parse_withdrawn(
                dex_pool,
                tokens_for_pool,
                tokens_scalars,
                parsed_receipt_log,
                transfers_for_transaction,
            )

        elif event_name.lower() == self.pool_contracts_events_enum.deposited.name:
            return self._parse_deposited(
                dex_pool,
                tokens_for_pool,
                tokens_scalars,
                parsed_receipt_log,
                transfers_for_transaction,
            )

    def _parse_swapped(
        self,
        base_pool: EthDexPool,
        tokens_scalars: list[int],
        receipt_log: ParsedReceiptLog,
    ) -> EthDexTrade:
        logger.debug("resolving swap from swap event")
        parsed_event = receipt_log.parsed_event
        if base_pool.token_addresses[1].lower() == parsed_event["dst"]:
            amount0 = parsed_event["amount"] / tokens_scalars[0]
            amount1 = parsed_event["amount1"] = parsed_event["result"] / tokens_scalars[1]
        else:
            amount0 = parsed_event["result"] / tokens_scalars[0]
            amount1 = parsed_event["amount1"] = parsed_event["amount"] / tokens_scalars[1]

        finance_info = self._get_finance_info(parsed_event)

        return EthDexTrade(
            token_amounts=[amount0, amount1],
            pool_address=base_pool.address,
            transaction_hash=receipt_log.transaction_hash,
            log_index=receipt_log.log_index,
            block_number=receipt_log.block_number,
            event_type="swap",
            token_reserves=[finance_info["reserve_0"], finance_info["reserve_1"]],
            token_prices=get_prices_for_two_pool(finance_info["price_0"], finance_info["price_1"]),
            token_addresses=base_pool.token_addresses,
        )

    def _parse_deposited(
        self,
        base_pool: EthDexPool,
        erc20_tokens: list[EthToken],
        tokens_scalars: list[int],
        receipt_log: ParsedReceiptLog,
        transfers_for_transaction: list,
    ) -> EthDexTrade:
        logger.debug("resolving mint from mint event")
        parsed_event = receipt_log.parsed_event
        amount1 = parsed_event["amount"] / tokens_scalars[1]
        amount0_transfers = list(
            filter(
                lambda x: x.token_address == erc20_tokens[1].address.lower(),
                transfers_for_transaction,
            )
        ).pop()

        amount0 = amount0_transfers.value / tokens_scalars[0]
        finance_info = self._get_finance_info(parsed_event)

        return EthDexTrade(
            pool_address=base_pool.address,
            token_amounts=[amount0, amount1],
            transaction_hash=receipt_log.transaction_hash,
            log_index=receipt_log.log_index,
            block_number=receipt_log.block_number,
            event_type="mint",
            token_reserves=[finance_info["reserve_0"], finance_info["reserve_1"]],
            token_prices=get_prices_for_two_pool(finance_info["price_0"], finance_info["price_1"]),
            token_addresses=base_pool.token_addresses,
            lp_token_address=base_pool.lp_token_addresses[0],
        )

    def _parse_withdrawn(
        self,
        base_pool: EthDexPool,
        erc20_tokens: list[EthToken],
        tokens_scalars: list[int],
        receipt_log: ParsedReceiptLog,
        transfers_for_transaction: list[EthTokenTransfer],
    ) -> EthDexTrade:
        logger.debug("resolving burn from burn event")
        parsed_event = receipt_log.parsed_event
        amount0 = parsed_event["amount"] / tokens_scalars[0]
        amount1_transfers = list(
            filter(
                lambda x: x.token_address == erc20_tokens[1].address.lower(),
                transfers_for_transaction,
            )
        ).pop()

        amount1 = amount1_transfers.value / tokens_scalars[1]

        finance_info = self._get_finance_info(parsed_event)
        logger.debug("resolving burn from burn event")
        return EthDexTrade(
            pool_address=base_pool.address,
            token_amounts=[amount0, amount1],
            transaction_hash=receipt_log.transaction_hash,
            log_index=receipt_log.log_index,
            block_number=receipt_log.block_number,
            event_type="burn",
            token_reserves=[finance_info["reserve_0"], finance_info["reserve_1"]],
            token_prices=get_prices_for_two_pool(finance_info["price_0"], finance_info["price_1"]),
            token_addresses=base_pool.token_addresses,
            lp_token_address=base_pool.lp_token_addresses[0],
        )

    @staticmethod
    def _get_finance_info(parsed_event):
        reserve0 = parsed_event["reserve0"]
        reserve1 = parsed_event["reserve1"]
        if reserve0:
            token0_price = float(reserve1 / reserve0)
        else:
            logger.error("cant get price, as reserve0 = 0")
            token0_price = 0
        if reserve1:
            token1_price = float(reserve0 / reserve1)
        else:
            logger.error("cant get price, as reserve1 = 0")
            token1_price = 0
        if token0_price >= INFINITE_PRICE_THRESHOLD:
            logger.error("cant get price, as it's infinite")
            token0_price = 0
        if token1_price >= INFINITE_PRICE_THRESHOLD:
            logger.error("cant get price, as it's infinite")
            token1_price = 0
        finance_info = {
            "reserve_0": reserve0,
            "reserve_1": reserve1,
            "price_0": token0_price,
            "price_1": token1_price,
        }
        return finance_info
