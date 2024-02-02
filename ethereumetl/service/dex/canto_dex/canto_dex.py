import json
import logging
from functools import cache
from pathlib import Path

from web3 import Web3
from web3.exceptions import BadFunctionCallOutput, ContractLogicError

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.domain.dex_trade import EthDexTrade
from ethereumetl.domain.receipt_log import ParsedReceiptLog
from ethereumetl.domain.token import EthToken
from ethereumetl.domain.token_transfer import EthTokenTransfer
from ethereumetl.service.dex.enums import DexPoolFeeAmount
from ethereumetl.service.dex.uniswap_v2.uniswap_v2 import UniswapV2Amm

logs = logging.getLogger(__name__)
to_checksum = Web3.toChecksumAddress


class CantoDexAmm(UniswapV2Amm):

    def __init__(self, web3: Web3, chain_id: int | None = None):
        super().__init__(web3, chain_id)
        pool_abi_path = Path(__file__).parent / 'BaseV1Pair.json'
        factory_abi_path = Path(__file__).parent / 'BaseV1Factory.json'
        pool_abi = json.loads(pool_abi_path.read_text())
        factory_abi = json.loads(factory_abi_path.read_text())

        self._w3: Web3 = web3
        self.pool_contract = self._w3.eth.contract(abi=pool_abi)
        self.factory_contract = self._w3.eth.contract(abi=factory_abi)
        self.chain_id = chain_id

    def _init_metadata(self):
        if not self.chain_id:
            self.chain_id = self._w3.eth.chain_id
        path = Path(__file__).parent / 'deploys' / str(self.chain_id) / 'metadata.json'
        if not path.exists():
            raise ValueError(f'Metadata file not found: {path}')
        with open(Path(__file__).parent / 'deploys' / str(self.chain_id) / 'metadata.json') as f:
            self.metadata = json.load(f)
        self.factory_addresses = []

        for metadata in self.metadata:
            factory_address = metadata['contracts'].get('BaseV1Factory')
            if factory_address:
                self.factory_addresses.append(factory_address.lower())

    def resolve_asset_from_log(self, parsed_log: ParsedReceiptLog) -> EthDexPool | None:
        pool_address_checksum = to_checksum(parsed_log.address)
        factory_address = self.get_factory_address(pool_address_checksum)
        if not factory_address:
            return None
        tokens_addresses = self.get_tokens_addresses_for_pool(pool_address_checksum)
        if not tokens_addresses:
            return None

        return EthDexPool(
            address=parsed_log.address.lower(),
            token_addresses=[address.lower() for address in tokens_addresses],
            fee=DexPoolFeeAmount.MEDIUM.value,
            lp_token_addresses=[parsed_log.address.lower()],
            factory_address=factory_address.lower(),
        )

    @cache
    def get_factory_address(self, pool_address: str) -> str | None:
        for factory_address in self.factory_addresses:
            try:
                # Canto's slingshot and other dexes, added isPair method to factory
                is_pair = self.factory_contract.functions.isPair(to_checksum(pool_address)).call(
                    {'to': to_checksum(factory_address)}, 'latest'
                )

                if is_pair:
                    return factory_address
            except (TypeError, ContractLogicError, ValueError, BadFunctionCallOutput) as e:
                logs.debug(f'Not found factory, fallback to maintainer. Error: {e}')
        return None

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
