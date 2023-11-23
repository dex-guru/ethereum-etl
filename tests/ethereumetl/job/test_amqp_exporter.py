from unittest.mock import MagicMock

import pytest

from blockchainetl.jobs.exporters.amqp_exporter import AMQPItemExporter


@pytest.fixture
def amqp_exporter():
    exporter = AMQPItemExporter(
        amqp_url='amqp://guest:guest@localhost:5672',
        exchange='test',
        item_type_to_routing_key_mapping={'block': 'block'},
    )
    exporter._connection = MagicMock()
    exporter._producer = MagicMock()
    return exporter


def test_amqp_exporter(amqp_exporter):
    amqp_exporter._producer.publish.side_effect = [ConnectionError, ConnectionError, None]
    amqp_exporter._reopen = MagicMock()
    amqp_exporter.export_item({'type': 'block', 'number': 1})
    assert amqp_exporter._producer.publish.call_count == 3
    assert amqp_exporter._reopen.call_count == 2
