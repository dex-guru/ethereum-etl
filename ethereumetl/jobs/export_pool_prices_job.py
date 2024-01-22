from blockchainetl.jobs.base_job import BaseJob


class ExportPoolPricesJob(BaseJob):
    def __init__(
        self,
        dex_pools_iterable,
        dex_trades_iterable,
        item_exporter,
        batch_size,
        max_workers,
    ):
        self.item_exporter = item_exporter
        self.batch_size = batch_size
        self.dex_pools_iterable = dex_pools_iterable
        self.dex_trades_iterable = dex_trades_iterable

    def _start(self):
        self.item_exporter.open()
        self._export()

    def _export(self):
        ...

    def _export_dex_pool_prices(self, dex_pool, dex_trades):
        ...

    def _end(self):
        self.item_exporter.close()
