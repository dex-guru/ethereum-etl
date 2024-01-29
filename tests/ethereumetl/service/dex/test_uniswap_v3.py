from unittest.mock import MagicMock

import pytest
from web3 import Web3

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.domain.receipt_log import ParsedReceiptLog
from ethereumetl.domain.token import EthToken
from ethereumetl.domain.token_transfer import EthTokenTransfer
from ethereumetl.service.dex.dex_client_factory import ContractAdaptersFactory


@pytest.fixture
def dex_client_uniswap_v3():
    return ContractAdaptersFactory.get_dex_client('uniswap_v3', Web3(), 1)


@pytest.fixture
def dex_pool_sample():
    return EthDexPool(
        address='0x127452f3f9cdc0389b0bf59ce6131aa3bd763598',
        factory_address='0x1f98431c8ad98523631ae4a59f267346ea31f984',
        token_addresses=[
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            '0xd31a59c85ae9d8edefec411d448f90841571b89c',
        ],
        fee=3000,
        lp_token_addresses=['0x127452f3f9cdc0389b0bf59ce6131aa3bd763598'],
        underlying_token_addresses=[],
    )


@pytest.fixture
def tokens_sample():
    return [
        EthToken(
            address='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            symbol='WETH',
            name='Wrapped Ether',
            decimals=18,
            total_supply=3135835689358593052435655,
        ),
        EthToken(
            address='0xd31a59c85ae9d8edefec411d448f90841571b89c',
            symbol='SOL',
            name='Wrapped SOL',
            decimals=9,
            total_supply=150605554799470,
        ),
    ]


@pytest.fixture
def parsed_log_sample():
    return ParsedReceiptLog(
        transaction_hash='0x9f73eb8393c6047cb07afaf2f2e065e3899a5ccb5066e6433494d3268d2ec325',
        block_number=19111377,
        log_index=150,
        event_name='Swap',
        namespace={'kyberswap_elastic', 'uniswap_v3', 'quickswap_v3'},
        address='0x127452f3f9cdc0389b0bf59ce6131aa3bd763598',
        parsed_event={
            'sender': '0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD',
            'recipient': '0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD',
            'amount0': -8554334621384010,
            'amount1': 200000000,
            'sqrtPriceX96': 12096211644547842549031838,
            'liquidity': 850244952391465198,
            'tick': -175753,
        },
    )


@pytest.fixture()
def transfers_sample():
    return [
        EthTokenTransfer(
            token_address='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            from_address='0x127452f3f9cdc0389b0bf59ce6131aa3bd763598',
            to_address='0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad',
            value=8554334621384010,
            transaction_hash='0x9f73eb8393c6047cb07afaf2f2e065e3899a5ccb5066e6433494d3268d2ec325',
            log_index=147,
            block_number=19111377,
            token_standard='ERC-20',
            token_id=None,
            operator_address=None,
        ),
        EthTokenTransfer(
            token_address='0xd31a59c85ae9d8edefec411d448f90841571b89c',
            from_address='0xbced711c938e2dc7313954f133d0737a04c11251',
            to_address='0x127452f3f9cdc0389b0bf59ce6131aa3bd763598',
            value=200000000,
            transaction_hash='0x9f73eb8393c6047cb07afaf2f2e065e3899a5ccb5066e6433494d3268d2ec325',
            log_index=148,
            block_number=19111377,
            token_standard='ERC-20',
            token_id=None,
            operator_address=None,
        ),
    ]


def test_resolve_receipt_log(
    dex_client_uniswap_v3, dex_pool_sample, tokens_sample, parsed_log_sample, transfers_sample
):
    dex_client_uniswap_v3.pool_contract = MagicMock()
    dex_client_uniswap_v3.erc20_contract_abi = MagicMock()
    dex_client_uniswap_v3.erc20_contract_abi.functions.balanceOf.return_value.call.side_effect = [
        10**18,
        10**18,
    ]
    res = dex_client_uniswap_v3.resolve_receipt_log(
        parsed_log_sample, dex_pool_sample, tokens_sample, transfers_sample
    )
    assert res
    assert res.token_amounts == [-0.00855433462138401, 0.2]
    assert res.token_reserves == [1e-18, 1.0]
    assert res.token_prices == [[1.0, 0.04290030833158687], [23.30985577704379, 1.0]]
    assert res.event_type == 'swap'
    assert res.block_number
    assert res.transaction_hash
    assert res.log_index
    assert res.token_addresses == [
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
        '0xd31a59c85ae9d8edefec411d448f90841571b89c',
    ]
