from collections import OrderedDict

import pytest

from ethereumetl.domain.receipt_log import ParsedReceiptLog
from ethereumetl.service.dex.fjord.fjord import FjordLBP


@pytest.fixture()
def fjord_client(web3):
    fjord = FjordLBP(web3, chain_id=1)
    return fjord


@pytest.fixture()
def fjord_buy_parsed_log():
    return ParsedReceiptLog(
        transaction_hash='0xa36e8c4c305f5fba1c337a70431dc9de929dd649d1e732bdc1558f3978669a3a',
        block_number=19983036,
        log_index=125,
        event_name='Buy',
        namespaces={'fjord'},
        address='0x017164dc0f6c316e1d1f0f274f300753312de7aa',
        parsed_event=OrderedDict(
            [
                ('caller', '0x6d1AeFc047d55C5d08c288a663711F7B7EFD82E0'),
                ('assets', 100000000),
                ('shares', 53388078393210679703973),
                ('swapFee', 2000000),
            ]
        ),
    )


def test_fjord_resolve_asset_from_log(fjord_client, fjord_buy_parsed_log):
    dex_pool = fjord_client.resolve_asset_from_log(fjord_buy_parsed_log)
    assert dex_pool.address == '0x017164dc0f6c316e1d1f0f274f300753312de7aa'
    assert dex_pool.token_addresses == [
        '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
        '0x2dcdc642d0694f1c6ebcb63efb6ee4015ba3a47f',
    ]
    assert dex_pool.fee == 200
    assert dex_pool.factory_address == '0xb10daee1fcf62243ae27776d7a92d39dc8740f95'
    assert dex_pool.lp_token_addresses == ['0x017164dc0f6c316e1d1f0f274f300753312de7aa']
