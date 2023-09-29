from datetime import datetime

from elasticsearch import Elasticsearch, NotFoundError

from blockchainetl.exporters import BaseItemExporter
from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from ethereumetl.mappers.transfer_priced_mapper import TokenTransferPricedMapper


class ExtractTokenTransfersPricedJob(BaseJob):
    def __init__(
        self,
        token_transfers: list[dict],
        tokens: list[dict],
        chain_id: int,
        batch_size: int,
        max_workers: int,
        item_exporter: BaseItemExporter,
        elastic_client: Elasticsearch,
    ):
        self.token_transfers = token_transfers
        self.tokens = {token['address']: token for token in tokens}
        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.item_exporter = item_exporter
        self.chain_id = chain_id
        self.transfer_priced_mapper = TokenTransferPricedMapper()
        self.elastic_client = elastic_client

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(
            self.token_transfers,
            self._extract_transfers_priced,
            len(self.token_transfers),
        )

    def _extract_transfers_priced(self, token_transfers):
        try:
            prices = self._get_prices(self.tokens.keys())
        except NotFoundError:
            prices = {}
        items = []
        for transfer in token_transfers:
            token = self.tokens.get(transfer['token_address'], {})
            priced_transfer = self.transfer_priced_mapper.token_transfer_to_transfer_priced(
                token_transfer=transfer,
                price=prices.get(transfer['token_address'], 0),
                decimals=token.get('decimals', 0),
                symbol=token.get('symbol', token.get('name', 'UNKNOWN').replace(' ', '')),
                chain_id=self.chain_id,
            )
            items.append(self.transfer_priced_mapper.transfer_priced_to_dict(priced_transfer))

        self.item_exporter.export_items(items)

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()

    def _get_prices(self, token_addresses):
        prices = {}
        today_date = datetime.now().strftime('%Y%m%d')
        candles = self.elastic_client.search(
            index=f'rounded_candle-{today_date}',
            source_includes=['address', 'c', 't_rounded'],
            query={
                "bool": {
                    "must": [
                        {"term": {"chain_id": self.chain_id}},
                        {"terms": {"address": list(token_addresses)}},
                        {"term": {"cur": "S"}},
                        {"term": {"amm": "all"}},
                        {"term": {"interval": 60}},
                    ]
                }
            },
            collapse={
                "field": "address",
                "inner_hits": {
                    "name": "latest",
                    "size": 1,
                    "sort": [{"t_rounded": {"order": "desc"}}],
                    "_source": ['c'],
                },
            },
            size=10000,
        )
        for candle in candles['hits']['hits']:
            prices[candle['_source']['address']] = candle['inner_hits']['latest']['hits']['hits'][
                0
            ]['_source']['c']
        return prices
