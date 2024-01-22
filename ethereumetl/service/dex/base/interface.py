from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from web3 import Web3

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.domain.dex_trade import EthDexTrade
from ethereumetl.domain.receipt_log import ParsedReceiptLog
from ethereumetl.domain.token import EthToken
from ethereumetl.domain.token_transfer import EthTokenTransfer

TDexClient = TypeVar("TDexClient", bound="DexClientInterface")


class DexClientInterface(ABC, Generic[TDexClient]):
    @abstractmethod
    def __init__(self, web3: Web3, chain_id: int | None = None):
        ...

    @abstractmethod
    def resolve_asset_from_log(self, parsed_log: ParsedReceiptLog) -> EthDexPool | None:
        ...

    @abstractmethod
    def resolve_receipt_log(
        self,
        parsed_receipt_log: ParsedReceiptLog,
        dex_pool: EthDexPool | None = None,
        tokens_for_pool: list[EthToken] | None = None,
        transfers_for_transaction: list[EthTokenTransfer] | None = None,
    ) -> EthDexTrade | None:
        ...
