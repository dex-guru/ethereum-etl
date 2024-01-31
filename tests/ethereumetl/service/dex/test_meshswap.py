from unittest.mock import MagicMock

import pytest
from web3 import Web3

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.domain.receipt_log import ParsedReceiptLog
from ethereumetl.domain.token import EthToken
from ethereumetl.service.dex.dex_client_factory import ContractAdaptersFactory


@pytest.fixture
def dex_client_meshswap():
    return ContractAdaptersFactory.get_dex_client('meshswap', Web3(), 1)


@pytest.fixture
def meshswap_log_sample():
    return ParsedReceiptLog(
        transaction_hash='0x60a18de95d8f6d677d2d9cc731aa99fe260de25f329959bac33039d3ebac9778',
        block_number=52922548,
        log_index=135,
        event_name='ExchangePos',
        namespace={'meshswap'},
        address='0x67fa408a4cd3f23d1f14414e6292a01bb451c117',
        parsed_event={
            'token0': '0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063',
            'amount0': 5000000000000000000,
            'token1': '0xc2132D05D31c914a87C6611C10748AEb04B58e8F',
            'amount1': 5001335,
        },
    )


@pytest.fixture
def meshswap_dex_pool_sample():
    return EthDexPool(
        address='0x67fa408a4cd3f23d1f14414e6292a01bb451c117',
        factory_address='0x9f3044f7f9fc8bc9ed615d54845b4577b833282d',
        token_addresses=[
            '0xc2132d05d31c914a87c6611c10748aeb04b58e8f',
            '0x8f3cf7ad23cd3cadbd9735aff958023239c6a063',
        ],
        fee=3000,
        lp_token_addresses=['0x67fa408a4cd3f23d1f14414e6292a01bb451c117'],
        underlying_token_addresses=[],
    )


@pytest.fixture
def meshswap_tokens_for_pool_sample():
    return [
        EthToken(
            address='0xc2132d05d31c914a87c6611c10748aeb04b58e8f',
            symbol='USDT',
            name='(PoS) Tether USD',
            decimals=6,
            total_supply=550951863746367,
        ),
        EthToken(
            address='0x8f3cf7ad23cd3cadbd9735aff958023239c6a063',
            symbol='DAI',
            name='(PoS) Dai Stablecoin',
            decimals=18,
            total_supply=120293415811718075593020588,
        ),
    ]


def test_resolve_receipt_log(
    dex_client_meshswap,
    meshswap_dex_pool_sample,
    meshswap_log_sample,
    meshswap_tokens_for_pool_sample,
):
    dex_client_meshswap.pool_contract = MagicMock()
    dex_client_meshswap.pool_contract.functions.getReserves.return_value.call.return_value = (
        10**6,
        10**18,
        1234,
    )
    res = dex_client_meshswap.resolve_receipt_log(
        meshswap_log_sample, meshswap_dex_pool_sample, meshswap_tokens_for_pool_sample
    )
    assert res
    assert res.token_amounts == [
        -5001335 / 10**6,
        5000000000000000000 / 10**18,
    ]
    assert res.token_reserves == [1, 1]
    assert res.token_prices == [[1.0, 1.0], [1.0, 1.0]]
