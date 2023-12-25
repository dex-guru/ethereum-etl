from abc import ABC, abstractmethod

from web3 import Web3

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.domain.receipt_log import EthReceiptLog
from ethereumetl.domain.token import EthToken


class DexClientInterface(ABC):
    @abstractmethod
    def __init__(self, web3: Web3):
        ...

    @abstractmethod
    def get_base_pool(self, pool_address: str) -> EthDexPool | None:
        ...

    @abstractmethod
    def resolve_receipt_log(
        self,
        receipt_log: EthReceiptLog,
        base_pool: EthDexPool,
        erc20_tokens: list[EthToken],
    ) -> dict | None:
        ...
