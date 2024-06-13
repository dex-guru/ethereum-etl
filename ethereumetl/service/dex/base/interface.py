from abc import ABC, abstractmethod
from collections.abc import Sequence
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
    def __init__(
        self, web3: Web3, chain_id: int | None = None, file_path: str | None = __file__
    ): ...

    @abstractmethod
    def resolve_asset_from_log(self, parsed_log: ParsedReceiptLog) -> EthDexPool | None: ...

    @abstractmethod
    def resolve_receipt_log(
        self,
        parsed_receipt_log: ParsedReceiptLog,
        dex_pool: EthDexPool,
        tokens_for_pool: list[EthToken],
        transfers_for_transaction: list[EthTokenTransfer],
    ) -> EthDexTrade | None: ...

    @staticmethod
    def normalize_event(inputs: Sequence, event_to_fix: dict) -> dict:
        return {k['name']: v for k, v in zip(inputs, event_to_fix.values())}
