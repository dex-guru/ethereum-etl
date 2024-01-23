from collections.abc import Collection

from blockchainetl.jobs.base_job import BaseJob
from blockchainetl.jobs.importers.price_importers.interface import PriceImporterInterface
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor


class ExportPricesForTokensJob(BaseJob):
    def __init__(
        self,
        token_addresses_iterable: Collection[str],
        price_importer: PriceImporterInterface,
        item_exporter,
        batch_size,
        max_workers,
        chain_id,
    ):
        self.token_addresses_iterable = token_addresses_iterable
        self.price_importer = price_importer
        self.item_exporter = item_exporter
        self.chain_id = chain_id
        self.batch_work_executor = BatchWorkExecutor(1, max_workers)
        self.batch_size = batch_size

    def _start(self):
        self.item_exporter.open()
        self.price_importer.open()

    def _export(self):
        self.batch_work_executor.execute(
            self.token_addresses_iterable,
            self._export_prices_for_tokens,
            len(self.token_addresses_iterable),
        )

    def _export_prices_for_tokens(self, token_addresses_iterable):
        for token_address in token_addresses_iterable:
            stable_price = self.price_importer.get_stable_price_for_token(token_address, 0)
            native_price = self.price_importer.get_native_price_for_token(token_address, 0)

            price = {
                'token_address': token_address,
                'price_stable': stable_price,
                'price_native': native_price,
                'score': 1,
                'type': 'base_token_price',
            }
            self.item_exporter.export_item(price)

    def _end(self):
        self.item_exporter.close()
        self.price_importer.close()
