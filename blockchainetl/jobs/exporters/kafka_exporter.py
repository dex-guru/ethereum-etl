import collections
import json
import logging

from kafka import KafkaProducer

from blockchainetl.exporters import BaseItemExporter
from blockchainetl.jobs.exporters.converters.composite_item_converter import CompositeItemConverter


class KafkaItemExporter(BaseItemExporter):
    def __init__(self, output, item_type_to_topic_mapping, converters=()):
        super().__init__()
        self.item_type_to_topic_mapping = item_type_to_topic_mapping
        self.converter = CompositeItemConverter(converters)
        self.connection_url = self.get_connection_url(output)
        print(self.connection_url)
        self.producer = KafkaProducer(bootstrap_servers=self.connection_url)

    @staticmethod
    def get_connection_url(output):
        try:
            return output.split('/')[1]
        except KeyError:
            raise Exception(
                'Invalid kafka output param, It should be in format of "kafka/127.0.0.1:9092"'
            )

    def export_item(self, item):
        item_type = item.get('type')
        if item_type is not None and item_type in self.item_type_to_topic_mapping:
            data = json.dumps(item).encode('utf-8')
            logging.debug(data)
            return self.producer.send(self.item_type_to_topic_mapping[item_type], value=data)
        else:
            logging.warning(f'Topic for item type "{item_type}" is not configured.')
            return None

    def convert_items(self, items):
        for item in items:
            yield self.converter.convert_item(item)


def group_by_item_type(items):
    result = collections.defaultdict(list)
    for item in items:
        result[item.get('type')].append(item)

    return result
