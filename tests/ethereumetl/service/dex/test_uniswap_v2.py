from unittest.mock import MagicMock

import pytest

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.domain.receipt_log import EthReceiptLog
from ethereumetl.domain.token import EthToken
from ethereumetl.service.dex.dex_client_factory import ContractAdaptersFactory


@pytest.fixture
def dex_client_uniswap_v2(web3):
    return ContractAdaptersFactory(web3, 1).get_dex_client('uniswap_v2')


log = EthReceiptLog(
    **{
        'log_index': 11,
        'transaction_hash': '0x4937857736a8512ff2f43d0b253b0fe28fe857d6d196b74d5211069951ef3315',
        'block_number': 19226988,
        'address': '0x68e4af213c49f320175116bff189c9ca452ce29c',
        'data': '0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000016345785d8a0000000000000000000000000000000000000000000000000000035658d07444f9b020000000000000000000000000000000000000000000000000000000000000000',
        'transaction_index': 1,
        'topics': [
            '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822',
            '0x0000000000000000000000003fc91a3afd70395cd496c647d5a6cc9d4b2b7fad',
            '0x000000000000000000000000e6a92cf7d584d238e34960fcb2b1386b0f6f4432',
        ],
        'block_hash': '0xabec07f193f5348e9955c954cb6b7cf11e083266e0339aae69624374bd975513',
    }
)


@pytest.fixture
def dex_pool():
    return EthDexPool(
        address='0x68e4af213c49f320175116bff189c9ca452ce29c',
        factory_address='0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f',
        token_addresses=[
            '0x0000000000ca73a6df4c58b84c5b4b847fe8ff39',
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
        ],
        fee=3000,
        lp_token_addresses=['0x68e4af213c49f320175116bff189c9ca452ce29c'],
        underlying_token_addresses=(),
    )


@pytest.fixture
def tokens_for_pool():
    return [
        EthToken(
            address='0x0000000000ca73a6df4c58b84c5b4b847fe8ff39',
            symbol='T1',
            decimals=18,
            name='Token 1',
            total_supply=100000000000000000000000000,
        ),
        EthToken(
            address='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            symbol='T2',
            decimals=18,
            name='Token 2',
            total_supply=100000000000000000000000000,
        ),
    ]


def test_resolve_receipt_log(
    eth_resolve_log_service, dex_client_uniswap_v2, dex_pool, tokens_for_pool
):
    client = eth_resolve_log_service._dex_client_factory.get_dex_client('uniswap_v2')
    client.pool_contract.functions.getReserves = MagicMock()
    client.pool_contract.functions.getReserves.return_value.call.return_value = (
        10**18,
        10**18,
        1234,
    )
    parsed_log = eth_resolve_log_service.parse_log(log)
    res = eth_resolve_log_service.resolve_log(parsed_log, dex_pool, tokens_for_pool, [])
    assert res
    assert res.token_amounts == [-3.847636519008312, 1.6]
    assert res.token_reserves == [1, 1]
    assert res.token_prices == [[1.0, 1.0], [1.0, 1.0]]


def test_resolve_asset_from_log(eth_resolve_log_service, dex_pool):
    parsed_log = eth_resolve_log_service.parse_log(log)
    res = eth_resolve_log_service.resolve_asset_from_log(parsed_log)
    assert res == dex_pool
