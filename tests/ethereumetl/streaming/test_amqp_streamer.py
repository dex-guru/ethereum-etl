from unittest.mock import MagicMock

import pytest

from ethereumetl.cli.amqp_stream import AmqpStreamerAdapter


@pytest.fixture
def amqp_streamer_adapter():
    return AmqpStreamerAdapter(
        amqp_url='amqp://guest:guest@localhost:5672/dex',
        eth_streamer=MagicMock(),
        routing_key='routing_key',
        queue_name='queue_name',
        exchange_name='exchange_name',
    )


def test_dead_letter_exchange(amqp_streamer_adapter):
    amqp_streamer_adapter._consume = MagicMock()
    amqp_streamer_adapter._dlx_producer = MagicMock()
    amqp_streamer_adapter.closed = False
    amqp_streamer_adapter._eth_streamer.export_all.side_effect = Exception('error')
    amqp_streamer_adapter._process_item(1, 2, [{'type': 'block'}])
    amqp_streamer_adapter._dlx_producer.publish.assert_called_once()
