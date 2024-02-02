import pytest
from web3 import Web3

from ethereumetl.domain.receipt_log import EthReceiptLog, ParsedReceiptLog
from ethereumetl.service.dex.dex_client_factory import ContractAdaptersFactory
from ethereumetl.service.eth_resolve_log_service import EthResolveLogService


@pytest.fixture
def web3():
    return Web3()


@pytest.fixture
def mock_dex_client_factory(web3):
    return ContractAdaptersFactory(web3, 1)


@pytest.fixture
def eth_resolve_log_service(web3, mock_dex_client_factory):
    return EthResolveLogService(web3, 1)


def test_init(eth_resolve_log_service, web3, mock_dex_client_factory):
    assert eth_resolve_log_service._web3 == web3
    assert (
        eth_resolve_log_service._possible_dex_types
        == mock_dex_client_factory.get_all_supported_dex_types()
    )
    assert eth_resolve_log_service.events_inventory


@pytest.mark.skip(reason='Parsed logs should have exact namespaces')
def test_parse_log(eth_resolve_log_service):
    log = EthReceiptLog(
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
    parsed_log = eth_resolve_log_service.parse_log(log)
    assert parsed_log == ParsedReceiptLog(
        transaction_hash='0x806a76df3e4c6ac716237455858f9f1f1938cff537ac15b3ab29a59925de78cc',
        block_number=18962404,
        log_index=45,
        event_name='Swap',
        namespace={'kyberswap_elastic', 'quickswap_v3', 'uniswap_v3'},
        address='0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640',
        parsed_event={
            'sender': '0xA69babEF1cA67A37Ffaf7a485DfFF3382056e78C',
            'recipient': '0xA69babEF1cA67A37Ffaf7a485DfFF3382056e78C',
            'amount0': 415154111492,
            'amount1': -183835849160005699391,
            'sqrtPriceX96': 1667454450650709917312126284770901,
            'liquidity': 12574978294291035625,
            'tick': 199099,
        },
    )
