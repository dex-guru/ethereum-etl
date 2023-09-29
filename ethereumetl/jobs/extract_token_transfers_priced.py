import threading
from datetime import datetime

from elasticsearch import (
    Elasticsearch,
    NotFoundError,
    TransportError,
    ConnectionError,
    ConnectionTimeout,
)

from blockchainetl.exporters import BaseItemExporter
from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from ethereumetl.mappers.transfer_priced_mapper import TokenTransferPricedMapper

ELASTIC_RETRY_EXCEPTIONS = (TransportError, ConnectionError, ConnectionTimeout)


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
        self.batch_work_executor = BatchWorkExecutor(
            batch_size,
            max_workers,
            retry_exceptions=ELASTIC_RETRY_EXCEPTIONS,
        )
        self.prices = {}
        self.item_exporter = item_exporter
        self.chain_id = chain_id
        self.transfer_priced_mapper = TokenTransferPricedMapper()
        self.elastic_client = elastic_client

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(
            tuple(self.tokens.keys()),
            self._get_prices,
            len(self.tokens),
        )
        # wait for all futures to complete
        self.batch_work_executor.executor._check_completed_futures()
        self._extract_transfers_priced(self.token_transfers)

    def _extract_transfers_priced(self, token_transfers):
        items = []
        for transfer in token_transfers:
            token = self.tokens.get(transfer['token_address'], {})
            priced_transfer = self.transfer_priced_mapper.token_transfer_to_transfer_priced(
                token_transfer=transfer,
                price=self.prices.get(transfer['token_address'], 0),
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
        search_body = {
            "index": f'rounded_candle-{today_date}',
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"chain_id": self.chain_id}},
                        {"terms": {"address": list(token_addresses)}},
                        {"term": {"cur": "S"}},
                        {"term": {"amm": "all"}},
                        {"term": {"interval": 600}},
                    ]
                }
            },
            "aggs": {
                "group_by_address": {
                    "terms": {"field": "address", "size": 1000},
                    "aggs": {
                        "latest": {
                            "top_hits": {
                                "sort": [{"t_rounded": {"order": "desc"}}],
                                "_source": ["c"],
                                "size": 1,
                            }
                        }
                    },
                }
            },
        }
        try:
            candles = self.elastic_client.search(**search_body)
        except NotFoundError:
            return
        for candle in candles['aggregations']['group_by_address']['buckets']:
            try:
                prices[candle['key']] = candle['latest']['hits']['hits'][0]['_source']['c']
            except IndexError:
                pass
        threading.Lock()
        self.prices.update(prices)
