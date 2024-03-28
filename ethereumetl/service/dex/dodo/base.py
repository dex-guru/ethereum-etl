import json
import logging
from functools import lru_cache
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
from ethereumetl.service.dex.base.base_dex_client import BaseDexClient
from ethereumetl.service.dex.enums import DexPoolFeeAmount

logs = logging.getLogger(__name__)
to_checksum = Web3.toChecksumAddress


class BaseDODOAmmClient(BaseDexClient):
    """Base class for DODO AMM clients."""

    pool_contract_name = ''

    def __init__(self, web3: Web3, chain_id: int | None = None, file_path: str = __file__):
        super().__init__(web3, chain_id, file_path)
        assert self.pool_contract_name, "pool_contract_name is not set"
        pool_abi_path = Path(file_path).parent / self.pool_contract_name
        abi = json.loads(pool_abi_path.read_text())
        self.pool_contract = self._w3.eth.contract(abi=abi)

    def _get_factory_address(self, pool_address: ChecksumAddress) -> str | None:
        try:
            factory_address = self.pool_contract.functions._MAINTAINER_()().call(
                {"to": pool_address}, "latest"
            )
            return factory_address.lower()
        except (ValueError, TypeError, BadFunctionCallOutput, ContractLogicError):
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

    def get_tokens_addresses_for_pool(self, pool_address: ChecksumAddress) -> list | None:
        logs.debug(f"Resolving tokens addresses for {pool_address}")
        try:
            tokens_addresses = [
                self.pool_contract.functions._BASE_TOKEN_().call({"to": pool_address}, "latest"),
                self.pool_contract.functions._QUOTE_TOKEN_().call({"to": pool_address}, "latest"),
            ]
        except (TypeError, ContractLogicError, ValueError, BadFunctionCallOutput) as e:
            logs.error(f"Cant resolve tokens_addresses for pair {pool_address}, {e}")
            return None

        return tokens_addresses

    def resolve_receipt_log(
        self,
        parsed_receipt_log: ParsedReceiptLog,
        dex_pool: EthDexPool,
        tokens_for_pool: list[EthToken],
        transfers_for_transaction: list[EthTokenTransfer],
    ) -> EthDexTrade | None:
        raise NotImplementedError()

    def _get_normalized_event(self, event_name: str, parsed_event: dict) -> dict:
        return self.normalize_event(
            self._get_events_abi(self.pool_contract, event_name)['inputs'],
            parsed_event,
        )

    def resolve_asset_from_log(self, parsed_log: ParsedReceiptLog) -> EthDexPool | None:
        address = to_checksum(parsed_log.address)
        factory_address = self._get_factory_address(address)
        if not factory_address:
            return None
        tokens_addresses = self.get_tokens_addresses_for_pool(address)
        if not tokens_addresses:
            return None
        lp_token_addresses = self.get_lp_token_address_for_pool(address)
        if not lp_token_addresses:
            lp_token_addresses = [parsed_log.address]
        return EthDexPool(
            address=parsed_log.address,
            token_addresses=[token.lower() for token in tokens_addresses],
            fee=DexPoolFeeAmount.MEDIUM.value,
            factory_address=factory_address.lower(),
            lp_token_addresses=lp_token_addresses,
        )

    def get_lp_token_address_for_pool(self, pool_address):
        return [pool_address.lower()]

    def get_pool_finances_from_event(
        self,
        base_pool: EthDexPool,
        parsed_receipt_log: ParsedReceiptLog,
        tokens_scalars: list[int],
    ) -> dict:
        reserves = []
        for idx, token in enumerate(base_pool.token_addresses):
            reserves.append(
                self._get_balance_of(token, base_pool.address, parsed_receipt_log.block_number - 1)
            )
        reserve0 = reserves[0] / tokens_scalars[0]
        reserve1 = reserves[1] / tokens_scalars[1]

        @lru_cache(maxsize=128)
        def get_pool_version(pool_address):
            return self.pool_contract.functions.version().call(
                {"to": to_checksum(pool_address)}, "latest"
            )

        pool_version = get_pool_version(base_pool.address)

        if isinstance(pool_version, str) and "DSP" in pool_version:
            # In case its DSP (fixed pool, DODO Stable Pool), we can't relly on the reserves, only on 1-1 swapping
            token0_price = token1_price = 1.0
        else:
            if reserve0:
                token0_price = float(reserve1 / reserve0)
            else:
                logs.error("cant get price, as reserve0 = 0")
                token0_price = 0

            if reserve1:
                token1_price = float(reserve0 / reserve1)
            else:
                logs.error("cant get price, as reserve1 = 0")
                token1_price = 0

            if token0_price >= INFINITE_PRICE_THRESHOLD:
                logs.error("cant get price, as it's infinite")
                token0_price = 0
            if token1_price >= INFINITE_PRICE_THRESHOLD:
                logs.error("cant get price, as it's infinite")
                token1_price = 0

        finance_info = {
            'reserve_0': reserve0,
            'reserve_1': reserve1,
            'price_0': token0_price,
            'price_1': token1_price,
        }
        return finance_info
