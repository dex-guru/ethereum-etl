import logging
from typing import Any, Iterable, Optional

import kombu

from ethereumetl.enumeration.entity_type import EntityType

_DEFAULT_ITEM_TYPE_TO_ROUTING_KEY = {
    EntityType.BLOCK: 'block',
    EntityType.TRANSACTION: 'transaction',
    EntityType.RECEIPT: 'receipt',
    EntityType.LOG: 'log',
    EntityType.TOKEN_TRANSFER: 'token_transfer',
    EntityType.TRACE: 'trace',
    EntityType.CONTRACT: 'contract',
    EntityType.TOKEN: 'token',
}


class AMQPItemExporter:
    """
    Publishes items to AMQP broker.
    """

    def __init__(
        self,
        amqp_url: str,
        exchange: str,
        item_type_to_routing_key_mapping: Optional[dict[str, str]] = None,
    ):
        self._item_type_to_topic_mapping = (
            item_type_to_routing_key_mapping or _DEFAULT_ITEM_TYPE_TO_ROUTING_KEY
        )

        self._exchange = exchange
        self._amqp_url = amqp_url

        self._connection: Optional[kombu.Connection] = None
        self._producer: Optional[kombu.Producer] = None

    def open(self):
        """
        raises: IOError
        """
        if self._connection is not None:
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
        except IOError as e:
            logging.error('Failed to connect to AMQP broker: %s', e)
            raise

    def close(self):
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            self._producer = None

    def export_items(self, items: Iterable[dict[str, Any]]):
        """
        raises: ConnectionError
        """
        if self._producer is None:
            raise RuntimeError('not opened')

        for item in items:
            item_type = item['type']
            routing_key = self._item_type_to_topic_mapping.get(item_type)

            if routing_key is None:
                logging.warning('Routing key for item type "%s" is not configured.', item_type)
                return

            try:
                self._producer.publish(item, routing_key=routing_key, serializer='json')
            except IOError as e:
                msg = f'Failed to publish item to AMQP broker: {e}'
                logging.error(msg)
                raise ConnectionError(msg) from e
