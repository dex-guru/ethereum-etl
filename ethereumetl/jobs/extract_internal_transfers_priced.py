from datetime import datetime

from elasticsearch import (
    ConnectionError,
    ConnectionTimeout,
    Elasticsearch,
    NotFoundError,
    TransportError,
)

from blockchainetl.exporters import BaseItemExporter
from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from ethereumetl.mappers.internal_transfer_priced_mapper import InternalTransferPricedMapper

ELASTIC_RETRY_EXCEPTIONS = (TransportError, ConnectionError, ConnectionTimeout)
ELASTIC_MAX_FLOAT = 3.402823466e38


class ExtractInternalTransfersPricedJob(BaseJob):
    # TODO add from config
    WRAPPED_TOKENS_BY_CHAIN = {
        1: {
            'address': '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            'decimals': 18,
            'name': 'Wrapped Ether',
            'symbol': 'ETH',
        },
        56: {
            'address': '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c',
            'decimals': 18,
            'name': 'Wrapped BNB',
            'symbol': 'BNB',
        },
        137: {
            'address': '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270',
            'decimals': 18,
            'name': 'Wrapped Matic',
            'symbol': 'MATIC',
        },
        250: {
            'address': '0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83',
            'decimals': 18,
            'name': 'Wrapped Fantom',
            'symbol': 'FTM',
        },
        10: {
            'address': '0x4200000000000000000000000000000000000006',
            'decimals': 18,
            'name': 'Wrapped Ether',
            'symbol': 'ETH',
        },
        100: {
            'address': '0xe91d153e0b41518a2ce8dd3d7944fa863463a97d',
            'decimals': 18,
            'name': 'Wrapped xDai',
            'symbol': 'XDAI',
        },
        7700: {
            'address': '0x826551890dc65655a0aceca109ab11abdbd7a07b',
            'decimals': 18,
            'name': 'wCanto',
            'symbol': 'CANTO',
        },
        8453: {
            'address': '0x4200000000000000000000000000000000000006',
            'decimals': 18,
            'name': 'Wrapped Ether',
            'symbol': 'ETH',
        },
        42161: {
            'address': '0x82af49447d8a07e3bd95bd0d56f35241523fbab1',
            'decimals': 18,
            'name': 'Wrapped Ether',
            'symbol': 'ETH',
        },
        43114: {
            'address': '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7',
            'decimals': 18,
            'name': 'Wrapped AVAX',
            'symbol': 'AVAX',
        },
        42170: {
            'address': '0x722e8bdd2ce80a4422e880164f2079488e115365',
            'decimals': 18,
            'name': 'Wrapped Ether',
            'symbol': 'ETH',
        },
    }

    def __init__(
        self,
        internal_transfers: list[dict],
        chain_id: int,
        batch_size: int,
        max_workers: int,
        item_exporter: BaseItemExporter,
        elastic_client: Elasticsearch,
    ):
        self.internal_transfers = internal_transfers
        self.batch_work_executor = BatchWorkExecutor(
            batch_size,
            max_workers,
            retry_exceptions=ELASTIC_RETRY_EXCEPTIONS,
        )
        self.item_exporter = item_exporter
        self.chain_id = chain_id
        self.transfer_priced_mapper = InternalTransferPricedMapper()
        self.elastic_client = elastic_client
        self.wrapped_token: dict = self.WRAPPED_TOKENS_BY_CHAIN[self.chain_id]
        self.candles_interval = 600

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self._extract_transfers_priced(self.internal_transfers)

    def _extract_transfers_priced(self, token_transfers):
        items = []
        if not token_transfers:
            return
        timestamps = {int(transfer['block_timestamp'].timestamp()) for transfer in token_transfers}
        prices = self._get_prices(
            timestamps=list(timestamps),
            candles_interval=self.candles_interval,
        )
        if not prices:
            prices = {int(datetime.utcnow().timestamp()): 0}
        for transfer in token_transfers:
            if transfer['value'] == 0:
                continue

            # get price for timestamp
            price = prices.get(
                self._round_timestamp_to_floor(
                    int(transfer['block_timestamp'].timestamp()), self.candles_interval
                )
            )
            if not price:
                # or get latest price
                price = prices.get(max(prices.keys()), 0)

            priced_transfer = self.transfer_priced_mapper.internal_transfer_to_transfer_priced(
                token_address=self.wrapped_token['address'],
                token_transfer=transfer,
                price=price,
                decimals=self.wrapped_token['decimals'],
                symbol=self.wrapped_token['symbol'],
                chain_id=self.chain_id,
            )
            if priced_transfer.amounts[0] >= ELASTIC_MAX_FLOAT:
                continue
            items.append(
                self.transfer_priced_mapper.internal_transfer_priced_to_dict(priced_transfer)
            )

        self.item_exporter.export_items(items)

    def _get_prices(
        self,
        timestamps: list[int],
        candles_interval: int = 600,
    ):
        if not timestamps:
            return self._get_latest_price(candles_interval)
        candles = self._get_price_by_timestamps(timestamps, candles_interval)
        if not candles or candles['hits']['total']['value'] == 0:
            candles = self._get_latest_price(candles_interval)
            if not candles:
                return {}
        return self._parse_candles(candles)

    @staticmethod
    def _round_timestamp_to_floor(ts, interval):
        t_rounded = ts - ts % interval
        # without that, 1w always rounding to Thursday (epoch start, January 1, 1970 is Thursday)
        if interval == 604800:
            t_rounded += 345600
            if t_rounded > ts:
                t_rounded -= 604800
        return t_rounded

    @staticmethod
    def _parse_candles(candles):
        prices = {}
        for candle in candles['hits']['hits']:
            prices[candle['_source']['t_rounded']] = candle['_source']['c']
        return prices

    def _get_price_by_timestamps(
        self,
        timestamps: list[int],
        candles_interval: int = 600,
    ):
        timestamps_rounded = {
            self._round_timestamp_to_floor(ts, candles_interval) for ts in timestamps
        }
        index_dates = {
            datetime.utcfromtimestamp(ts).strftime('%Y%m%d') for ts in timestamps_rounded
        }
        indices = [f'rounded_candle-{index_date}' for index_date in index_dates]
        search_body = {
            'index': list(indices),
            'size': len(timestamps_rounded),
            'sort': [{'t_rounded': {'order': 'desc'}}],
            'query': {
                'bool': {
                    'filter': [
                        {'term': {'chain_id': self.chain_id}},
                        {'term': {'address': self.wrapped_token['address']}},
                        {'term': {'cur': 'S'}},
                        {'term': {'amm': 'all'}},
                        {'term': {'interval': candles_interval}},
                        {'terms': {'t_rounded': list(timestamps_rounded)}},
                    ]
                }
            },
            '_source': ['c', 't_rounded', 'address'],
        }
        try:
            candles = self.elastic_client.search(**search_body)  # type: ignore
        except (NotFoundError, TransportError):
            return {}
        return candles

    def _get_latest_price(self, candles_interval=600):
        index = f'rounded_candle-{datetime.utcnow().strftime("%Y%m%d")}'
        search_body = {
            'index': index,
            'size': 1,
            'sort': [{'t_rounded': {'order': 'desc'}}],
            'query': {
                'bool': {
                    'filter': [
                        {'term': {'chain_id': self.chain_id}},
                        {'term': {'address': self.wrapped_token['address']}},
                        {'term': {'cur': 'S'}},
                        {'term': {'amm': 'all'}},
                        {'term': {'interval': candles_interval}},
                    ]
                }
            },
            '_source': ['c', 't_rounded', 'address'],
        }
        try:
            candles = self.elastic_client.search(**search_body)  # type: ignore
        except (NotFoundError, TransportError):
            return {}
        return candles

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
