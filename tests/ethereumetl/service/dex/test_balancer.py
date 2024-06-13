from collections import OrderedDict

import pytest

from ethereumetl.domain.receipt_log import ParsedReceiptLog
from ethereumetl.service.dex.balancer.balancer import BalancerAmm


@pytest.fixture
def balancer_client(web3):
    return BalancerAmm(web3, 1)


@pytest.fixture
def parsed_receipt_log_swap():
    return ParsedReceiptLog(
        transaction_hash='0x223d9918964385d52a49e2550a80824d3e294206f83e90e00e82c2853df4d7fe',
        block_number=19419123,
        log_index=119,
        event_name='Swap',
        namespaces={'balancer'},
        address='0xba12222222228d8ba445958a75a0704d566bf2c8',
        parsed_event=OrderedDict(
            [
                (
                    'poolId',
                    b'\x93\xd1\x99&62\xa4\xefK\xb48\xf1\xfe\xb9\x9eW\xb4\xb5\xf0\xbd\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\xc2',
                ),
                ('tokenIn', '0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'),
                ('tokenOut', '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'),
                ('amountIn', 345564005007454774345),
                ('amountOut', 400719847524621304060),
            ]
        ),
    )


def test_resolve_asset(balancer_client, parsed_receipt_log_swap):
    asset = balancer_client.resolve_asset_from_log(parsed_receipt_log_swap)
    assert asset
    assert asset.address == '0x93d199263632a4ef4bb438f1feb99e57b4b5f0bd'
    assert asset.token_addresses == [
        '0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0',
        '0x93d199263632a4ef4bb438f1feb99e57b4b5f0bd',
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
    ]
