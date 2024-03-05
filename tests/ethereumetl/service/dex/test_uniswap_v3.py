from unittest.mock import MagicMock

import pytest

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.domain.receipt_log import EthReceiptLog
from ethereumetl.domain.token import EthToken
from ethereumetl.domain.token_transfer import EthTokenTransfer


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
    return EthReceiptLog(
        log_index=45,
        transaction_hash='0x806a76df3e4c6ac716237455858f9f1f1938cff537ac15b3ab29a59925de78cc',
        block_number=18962404,
        address='0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640',
        data='0x00000000000000000000000000000000000000000000000000000060a91d0404fffffffffffffffffffffffffffffffffffffffffffffff608c3dd316b9138c100000000000000000000000000000000000052363be9e4be26d90c91d790ae55000000000000000000000000000000000000000000000000ae834c223176f5e900000000000000000000000000000000000000000000000000000000000309bb',
        transaction_index=4,
        topics=[
            '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67',
            '0x000000000000000000000000a69babef1ca67a37ffaf7a485dfff3382056e78c',
            '0x000000000000000000000000a69babef1ca67a37ffaf7a485dfff3382056e78c',
        ],
        block_hash='0x6245b001ea4096c6796f8c543e82effb00380b64dda23bb3e95760f95e76f4e8',
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
    eth_resolve_log_service,
    dex_pool_sample,
    tokens_sample,
    parsed_log_sample,
    transfers_sample,
):
    client = eth_resolve_log_service._dex_client_factory.get_dex_client('uniswap_v3')
    parsed_log = eth_resolve_log_service.parse_log(parsed_log_sample)
    client.erc20_contract_abi = MagicMock()
    client.erc20_contract_abi.functions.balanceOf.return_value.call.side_effect = [
        10**18,
        10**18,
    ]
    res = eth_resolve_log_service.resolve_log(
        parsed_log, dex_pool_sample, tokens_sample, transfers_sample
    )
    assert res
    assert res.token_amounts == [4.15154111492e-07, -183835849160.0057]
    assert res.token_reserves == [1.0, 1.0 * 10**9]
    assert res.token_prices == [[1.0, 2.2576218982992146e-18], [4.4294396716888365e17, 1.0]]
    assert res.event_type == 'swap'
    assert res.block_number
    assert res.transaction_hash
    assert res.log_index
    assert res.token_addresses == [
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
        '0xd31a59c85ae9d8edefec411d448f90841571b89c',
    ]
