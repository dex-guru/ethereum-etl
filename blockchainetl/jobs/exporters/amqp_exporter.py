import logging
from typing import Any

import kombu
from retry import retry

from blockchainetl.exporters import BaseItemExporter


class AMQPItemExporter(BaseItemExporter):
    """
    Publishes items to AMQP broker.
    """

    def __init__(
        self,
        amqp_url: str,
        exchange: str,
        item_type_to_routing_key_mapping: dict[str, str] | None = None,
    ):
        super().__init__()
        self._item_type_to_topic_mapping = item_type_to_routing_key_mapping or {}

        self._exchange = exchange
        self._amqp_url = amqp_url

        self._connection: kombu.Connection = ...
        self._producer: kombu.Producer = ...

    def open(self):
        """
        raises: IOError.
        """
        if self._connection is not Ellipsis:
            raise RuntimeError('already opened')

        try:
            self._connection = kombu.Connection(
                self._amqp_url,
                connect_timeout=5,
                transport_options={'max_retries': 0},
            )
            exchange = kombu.Exchange(
                self._exchange,
                type='direct',
                durable=True,
            )
            self._producer = kombu.Producer(
                self._connection.channel(),
                exchange=exchange,
                auto_declare=True,
            )
        except OSError as e:
            logging.error('Failed to connect to AMQP broker: %s', e)
            raise

    def close(self):
        if self._connection is not Ellipsis:
            self._connection.close()
            self._connection = ...
            self._producer = ...

    @retry(ConnectionError, tries=3)
    def export_item(self, item: dict[str, Any]):
        """
        ,
        raises: ConnectionError.
        """
        if self._producer is Ellipsis:
            self.open()
        self._connection.ensure_connection(errback=self._reopen)

        item_type = item['type']
        routing_key = self._item_type_to_topic_mapping.get(item_type)

        if routing_key is None:
            logging.debug('Routing key for item type "%s" is not configured.', item_type)
            return

        try:
            self._producer.publish(item, routing_key=routing_key, serializer='json')
        except OSError as e:
            msg = f'Failed to publish item to AMQP broker: {e}'
            logging.error(msg)
            self._reopen()
            raise ConnectionError(msg) from e

    @retry(ConnectionError, tries=3)
    def export_items(self, items):
        """
        raises: ConnectionError.
        """
        if self._producer is Ellipsis:
            self.open()
        self._connection.ensure_connection(errback=self._reopen)

        items_grouped_by_routing_key = self._group_items_by_routing_key(items)

        for routing_key, grouped_items in items_grouped_by_routing_key.items():
            if routing_key == 'pre_event':
                # pre_events are published one by one
                for item in grouped_items:
                    try:
                        self._producer.publish(item, routing_key=routing_key, serializer='json')
                    except OSError as e:
                        msg = f'Failed to publish item to AMQP broker: {e}'
                        logging.error(msg)
                        self._reopen()
                        raise ConnectionError(msg) from e
            else:
                try:
                    self._producer.publish(
                        grouped_items, routing_key=routing_key, serializer='json'
                    )
                except OSError as e:
                    msg = f'Failed to publish items to AMQP broker: {e}'
                    logging.error(msg)
                    self._reopen()
                    raise ConnectionError(msg) from e

    def _group_items_by_routing_key(self, items):
        items_grouped_by_routing_key: dict[str, list] = {}
        for item in items:
            item_type = item['type']
            routing_key = self._item_type_to_topic_mapping.get(item_type)
            if routing_key is None:
                logging.debug('Routing key for item type "%s" is not configured.', item_type)
                continue
            items_grouped_by_routing_key.setdefault(routing_key, []).append(item)
        return items_grouped_by_routing_key

    def _reopen(self):
        self.close()
        self.open()
