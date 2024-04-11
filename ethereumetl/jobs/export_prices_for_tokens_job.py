from collections.abc import Collection
from dataclasses import asdict

from blockchainetl.jobs.base_job import BaseJob
from blockchainetl.jobs.importers.price_importers.interface import PriceImporterInterface


class ExportPricesForTokensJob(BaseJob):
    def __init__(
        self,
        token_addresses_iterable: Collection[str],
        price_importer: PriceImporterInterface,
        item_exporter,
        chain_id,
    ):
        self.token_addresses_iterable = token_addresses_iterable
        self.price_importer = price_importer
        self.item_exporter = item_exporter
        self.chain_id = chain_id

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self._export_prices_for_tokens(self.token_addresses_iterable)

    def _export_prices_for_tokens(self, token_addresses_iterable):
        prices = self.price_importer.get_prices_for_tokens(
            token_addresses=token_addresses_iterable,
        )
        self.item_exporter.export_items(
            [{**asdict(price), 'type': 'base_token_price'} for price in prices]
        )

    def _end(self):
        self.item_exporter.close()
