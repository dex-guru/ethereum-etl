from abc import ABC, abstractmethod
from collections.abc import Collection

from ethereumetl.domain.price import Price


class PriceImporterInterface(ABC):
    @abstractmethod
    def __init__(self, *args, **kwargs): ...

    @abstractmethod
    def open(self): ...

    @abstractmethod
    def close(self): ...

    @abstractmethod
    def get_prices_for_tokens(
        self,
        token_addresses: Collection[str],
        timestamp: int | None = None,
        block_number: int | None = None,
    ) -> Collection[Price]: ...

    @abstractmethod
    def get_stable_price_for_token(
        self, token_address: str, timestamp: int | None = None, block_number: int | None = None
    ) -> float:
        pass

    @abstractmethod
    def get_native_price_for_token(
        self,
        token_address: str,
        timestamp: int | None = None,
        block_number: int | None = None,
    ) -> float:
        pass

    @abstractmethod
    def get_token_score(self, token_address: str) -> int:
        pass
