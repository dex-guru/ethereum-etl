from collections.abc import Collection

from blockchainetl.jobs.importers.price_importers.interface import PriceImporterInterface
from ethereumetl.domain.price import Price


class MultiPriceImporter(PriceImporterInterface):
    def __init__(self, chain_id: int, item_importers: Collection[PriceImporterInterface]):
        self.chain_id = chain_id
        self.item_importers = item_importers

    def get_prices_for_tokens(
        self,
        token_addresses: Collection[str],
        timestamp: int | None = None,
        block_number: int | None = None,
    ):
        prices_from_all_sources: list[Price] = []
        for item_importer in self.item_importers:
            prices_from_all_sources.extend(
                item_importer.get_prices_for_tokens(token_addresses, timestamp, block_number)
            )
        return prices_from_all_sources

    def get_stable_price_for_token(
        self, token_address: str, timestamp: int | None = None, block_number: int | None = None
    ) -> float:
        for item_importer in self.item_importers:
            price = item_importer.get_stable_price_for_token(
                token_address, timestamp, block_number
            )
            if price:
                return price
        return 0

    def get_native_price_for_token(
        self,
        token_address: str,
        timestamp: int | None = None,
        block_number: int | None = None,
    ) -> float:
        for item_importer in self.item_importers:
            price = item_importer.get_native_price_for_token(
                token_address, timestamp, block_number
            )
            if price:
                return price
        return 0

    def open(self):
        for item_importer in self.item_importers:
            item_importer.open()

    def close(self):
        for item_importer in self.item_importers:
            item_importer.close()

    def get_token_score(self, token_address: str) -> int:
        for item_importer in self.item_importers:
            score = item_importer.get_token_score(token_address)
            if score:
                return score
        return 0
