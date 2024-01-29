from unittest.mock import MagicMock

import pytest
from web3 import Web3

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.domain.receipt_log import ParsedReceiptLog
from ethereumetl.domain.token import EthToken
from ethereumetl.service.dex.dex_client_factory import ContractAdaptersFactory


@pytest.fixture
def dex_client_uniswap_v2():
    return ContractAdaptersFactory.get_dex_client('uniswap_v2', Web3(), 1)


log = ParsedReceiptLog(
    transaction_hash='0x4235fd4f96a42895cc0f143d2c2ad65a096e55d2400577c4b7fa6a7bfda3cc85',
    block_number=18962404,
    log_index=208,
    event_name='Swap',
    namespace={'canto_dex', 'solidly', 'uniswap_v2'},
    address='0xe4d96c90d2608a9e8efe2d7feee5a1feb8eead29',
    parsed_event={
        'sender': '0x80a64c6D7f12C47B7c66c5B4E20E72bc1FCd5d9e',
        'to': '0x80a64c6D7f12C47B7c66c5B4E20E72bc1FCd5d9e',
        'amount0In': 9157112000000000000000000,
        'amount1In': 0,
        'amount0Out': 0,
        'amount1Out': 204349302466713085,
    },
)


@pytest.fixture
def dex_pool():
    return EthDexPool(
        address='0xe4d96c90d2608a9e8efe2d7feee5a1feb8eead29',
        factory_address='0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f',
        token_addresses=['0x1', '0x2'],
        fee=3000,
        lp_token_addresses=['0xe4d96c90d2608a9e8efe2d7feee5a1feb8eead29'],
        underlying_token_addresses=[],
    )


@pytest.fixture
def tokens_for_pool():
    return [
        EthToken(
            address='0x1',
            symbol='T1',
            decimals=18,
            name='Token 1',
            total_supply=100000000000000000000000000,
        ),
        EthToken(
            address='0x2',
            symbol='T2',
            decimals=18,
            name='Token 2',
            total_supply=100000000000000000000000000,
        ),
    ]


def test_resolve_receipt_log(dex_client_uniswap_v2, dex_pool, tokens_for_pool):
    dex_client_uniswap_v2.pool_contract = MagicMock()
    dex_client_uniswap_v2.pool_contract.functions.getReserves.return_value.call.return_value = (
        10**18,
        10**18,
        1234,
    )
    res = dex_client_uniswap_v2.resolve_receipt_log(log, dex_pool, tokens_for_pool)
    assert res
    assert res.token_amounts == [
        9157112000000000000000000 / 10**18,
        -204349302466713085 / 10**18,
    ]
    assert res.token_reserves == [1, 1]
    assert res.token_prices == [[1.0, 1.0], [1.0, 1.0]]
