from web3 import Web3

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.domain.receipt_log import EthReceiptLog
from ethereumetl.domain.token import EthToken
from ethereumetl.service.dex.base.interface import DexClientInterface


class BaseDexClient(DexClientInterface):
    def __init__(self, web3: Web3):
        self._w3 = web3
        self.factory_address_to_dex_name: dict[str, dict[str, str]] = {}
        self.contracts_by_dex_name: dict[str, dict[str, str]] = {}

    def get_base_pool(self, pool_address: str) -> EthDexPool | None:
        raise NotImplementedError()

    def resolve_receipt_log(
        self,
        receipt_log: EthReceiptLog,
        base_pool: EthDexPool,
        erc20_tokens: list[EthToken],
    ) -> dict | None:
        raise NotImplementedError()
