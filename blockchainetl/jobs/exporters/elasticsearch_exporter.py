import logging

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError
from elasticsearch.helpers import BulkIndexError, parallel_bulk
from retry import retry

from blockchainetl.exporters import BaseItemExporter

logger = logging.getLogger(__name__)


class ElasticsearchItemExporter(BaseItemExporter):
    def __init__(self, elasticsearch_url, item_type_to_index_mapping, chain_id=1):
        super().__init__()
        self.item_type_to_index_mapping = item_type_to_index_mapping
        self.chain_id = chain_id
        self.connection_url = elasticsearch_url
        self.client: None | Elasticsearch = None
        self.bulk_data = []

    def close(self):
        assert (
            self.client is not None
        ), 'Cannot close ElasticsearchItemExporter, it has not been opened'
        self._flush_bulk_data()
        self.client.close()
        self.client = None

    def open(self):
        assert self.client is None, 'Cannot open ElasticsearchItemExporter, it is already opened'
        self.client = Elasticsearch(self.connection_url)
        assert self.client.ping(), 'Cannot connect to Elasticsearch'

    def export_item(self, item):
        self.export_items([item])

    def export_items(self, items):
        for item in items:
            item_type = item.get('type')
            index = self.item_type_to_index_mapping.get(item_type)
            if index:
                converted_item = self._convert_to_bulk_data(item)
                self.bulk_data.append(
                    {'_index': index, '_source': converted_item, '_id': converted_item['id']}
                )
        self._flush_bulk_data()

    @staticmethod
    def _convert_to_bulk_data(item):
        item_type = item.pop('type')
        item.pop('item_id', None)
        item.pop('item_timestamp', None)
        if item_type in ('token_transfer_priced', 'internal_transfer_priced'):
            item['type'] = item['transfer_type']
            item['doc_type'] = 'transaction'
            item.pop('transfer_type')
        return item

    @retry(TransportError, tries=5, delay=1, logger=logger)
    def _flush_bulk_data(self):
        assert (
            self.client is not None
        ), 'Cannot flush bulk data, ElasticsearchItemExporter is not opened'
        if not self.bulk_data:
            return
        logger.info('Flushing %s items to Elasticsearch', len(self.bulk_data))
        try:
            for success, info in parallel_bulk(
                client=self.client, actions=self.bulk_data, thread_count=8
            ):
                if not success:
                    logger.error('Failed to flush bulk data to Elasticsearch: %s', info)
        except BulkIndexError as e:
            logger.exception('Error while flushing bulk data to Elasticsearch: %s', e)
        finally:
            self.bulk_data = []
