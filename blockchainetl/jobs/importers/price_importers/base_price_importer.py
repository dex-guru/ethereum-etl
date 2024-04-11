from collections.abc import Collection

from blockchainetl.jobs.importers.price_importers.interface import PriceImporterInterface
from ethereumetl.domain.price import Price
from ethereumetl.misc.info import get_chain_config


class BasePriceImporter(PriceImporterInterface):
    def __init__(self, chain_id: int):
        self.chain_id = chain_id
        self.stablecoin_addresses = ()
        self.native_token_address = ''

    def open(self):
        chain_config = get_chain_config(self.chain_id)
        self.stablecoin_addresses = chain_config['stablecoin_addresses']
        self.native_token_address = chain_config['native_token']['address']

    def close(self): ...

    def get_prices_for_tokens(
        self,
        token_addresses: Collection[str],
        timestamp: int | None = None,
        block_number: int | None = None,
    ) -> Collection[Price]:
        prices = []
        for token in token_addresses:
            prices.append(
                Price(
                    token_address=token,
                    price_stable=self.get_stable_price_for_token(token),
                    price_native=self.get_native_price_for_token(token),
                    score=self.get_token_score(token),
                )
            )
        return prices

    def get_stable_price_for_token(
        self, token_address: str, timestamp: int | None = None, block_number: int | None = None
    ) -> float:
        if token_address in self.stablecoin_addresses:
            return 1.0
        return 0.0

    def get_native_price_for_token(
        self,
        token_address: str,
        timestamp: int | None = None,
        block_number: int | None = None,
    ) -> float:
        if token_address == self.native_token_address:
            return 1.0
        return 0.0

    def get_token_score(self, token_address: str) -> int:
        if token_address in self.stablecoin_addresses:
            return 1
        if token_address == self.native_token_address:
            return 1
        return 0
