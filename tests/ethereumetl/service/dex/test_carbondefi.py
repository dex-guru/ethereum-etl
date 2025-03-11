from unittest.mock import MagicMock

import pytest

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.domain.receipt_log import EthReceiptLog
from ethereumetl.domain.token import EthToken
from ethereumetl.service.dex.dex_client_factory import ContractAdaptersFactory


@pytest.fixture
def dex_client_carbon_defi(web3):
    return ContractAdaptersFactory(web3, 1).get_dex_client('carbondefi')


log = EthReceiptLog(
    **{
        'log_index': 374,
        'transaction_hash': '0xa77327f7eb51ec5a32f9e988c50ede54e742ae601598650f2aaf8c1652298a82',
        'block_number': 22024113,
        'address': '0xC537e898CD774e2dCBa3B14Ea6f34C93d5eA45e1',
        'data': '0x00000000000000000000000000000000000000000000000054d22f9e033000000000000000000000000000000000000000000000000000000000000001cb8b14000000000000000000000000000000000000000000000000000000000000ebc20000000000000000000000000000000000000000000000000000000000000000',
        'transaction_index': 1,
        'topics': [
            '0x95f3b01351225fea0e69a46f68b164c9dea10284f12cd4a907ce66510ab7af6a',
            '0x000000000000000000000000d768d1fe6ef1449a54f9409400fe9d0e4954ea3f',
            '0x0000000000000000000000006f40d4a6237c257fff2db00fa0510deeecd303eb',
            '0x000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
        ],
        'block_hash': '0x9e43206856dbbd383af5087e7d74a24fd3b0be96ad6ade4e1b27c6d7bd403ef6',
    }
)


@pytest.fixture
def dex_pool():
    return EthDexPool(
        address='0xC537e898CD774e2dCBa3B14Ea6f34C93d5eA45e1',
        factory_address='0xC537e898CD774e2dCBa3B14Ea6f34C93d5eA45e1',
        token_addresses=[
            '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE',  # ETH
            '0x1F573D6Fb3F13d689FF844B4cE37794d79a7FF1C',  # BNT
            '0x6B3595068778DD592e39A122f4f5a5cF09C90fE2',  # SUSHI
            '0x6f40d4A6237C257fff2dB00FA0510DeEECd303eb',  # Fluid
            '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48',  # USDC
        ],
        fee=2000,
        lp_token_addresses=['0xC537e898CD774e2dCBa3B14Ea6f34C93d5eA45e1'],
    )


@pytest.fixture
def tokens_for_pool():
    return [
        EthToken(
            address='0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE',
            symbol='ETH',
            decimals=18,
            name='Ether',
            total_supply=100000000000000000000000000,
        ),
        EthToken(
            address='0x1F573D6Fb3F13d689FF844B4cE37794d79a7FF1C',
            symbol='BNT',
            decimals=18,
            name='Bancor Network Token',
            total_supply=200000000000000000000000000,
        ),
        EthToken(
            address='0x1F573D6Fb3F13d689FF844B4cE37794d79a7FF1C',
            symbol='SUSHI',
            decimals=18,
            name='Sushiswap',
            total_supply=300000000000000000000000000,
        ),
        EthToken(
            address='0x6f40d4A6237C257fff2dB00FA0510DeEECd303eb',
            symbol='FLUID',
            decimals=18,
            name='Fluid',
            total_supply=400000000000000000000000000,
        ),
        EthToken(
            address='0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48',
            symbol='USDC',
            decimals=6,
            name='Circle USDC',
            total_supply=500000000000000000000000000,
        ),
    ]


def test_resolve_receipt_log(
    eth_resolve_log_service, dex_client_carbon_defi, dex_pool, tokens_for_pool
):
    client = eth_resolve_log_service._dex_client_factory.get_dex_client('carbondefi')
    client.carbon_controller.functions.pairTradingFeePPM = MagicMock()
    client.carbon_controller.functions.pairTradingFeePPM.return_value.call.return_value = 3000

    client.carbon_controller.functions.pairs = MagicMock()
    client.carbon_controller.functions.pairs.return_value.call.return_value = [
        [
            '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE',
            '0x1F573D6Fb3F13d689FF844B4cE37794d79a7FF1C',
        ],
        [
            '0x1F573D6Fb3F13d689FF844B4cE37794d79a7FF1C',
            '0x6B3595068778DD592e39A122f4f5a5cF09C90fE2',
        ],
        [
            '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE',
            '0x6B3595068778DD592e39A122f4f5a5cF09C90fE2',
        ],
        [
            '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE',
            '0x6f40d4A6237C257fff2dB00FA0510DeEECd303eb',
        ],
        [
            '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE',
            '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48',
        ],
        [
            '0x6f40d4A6237C257fff2dB00FA0510DeEECd303eb',
            '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48',
        ],
    ]

    client._get_token_reserves = MagicMock()
    client._get_token_reserves.return_value = [1000, 2000]

    parsed_log = eth_resolve_log_service.parse_log(log)
    res = eth_resolve_log_service.resolve_log(parsed_log, dex_pool, tokens_for_pool, [])
    assert res
    assert res.token_amounts == [6.112, -30.116628]
    assert res.token_reserves == [1000, 2000]
    assert res.token_prices == [[1.0, 0.20294436681291148], [4.927458769633508, 1.0]]
    assert res.event_type == 'swap'
    assert res.block_number
    assert res.transaction_hash
    assert res.log_index
    assert res.token_addresses == [
        '0x6f40d4A6237C257fff2dB00FA0510DeEECd303eb',
        '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48',
    ]
