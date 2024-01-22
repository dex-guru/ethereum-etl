from unittest.mock import MagicMock

import pytest

from ethereumetl.service.dex.base.interface import DexClientInterface
from ethereumetl.service.dex.dex_client_factory import ContractAdaptersFactory
from ethereumetl.service.dex.uniswap_v2.uniswap_v2 import UniswapV2Amm


@pytest.fixture()
def contract_adapters_factory():
    return ContractAdaptersFactory(chain_id=1, web3=MagicMock())


@pytest.fixture()
def logs_iterable():
    return [
        {
            'log_index': 26,
            'transaction_hash': '0xea06045ca6409d9ddb47da4ec14fceb27623f9a50f97626115b084c81c8a0322',
            'block_number': 18934076,
            'address': '0xdf6581b1da6a120882f5029939ebdd1ebd94aef5',
            'data': '0xfffffffffffffffffffffffffffffffffffffffffffffffffdf099fc2d7cf6cc0000000000000000000000000000000000000000000b48da2631c51f9a0d0d030000000000000000000000000000000000002543eb092a46693886807872506000000000000000000000000000000000000000000003713288e9494d0d83d9b6000000000000000000000000000000000000000000000000000000000002cbe9',
            'transaction_index': 4,
            'topics': [
                '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67',
                '0x00000000000000000000000080a64c6d7f12c47b7c66c5b4e20e72bc1fcd5d9e',
                '0x00000000000000000000000080a64c6d7f12c47b7c66c5b4e20e72bc1fcd5d9e',
            ],
            'block_hash': '0x2f432a7b50b4d0e0dc889f36bdd92fb6663747d033ee8e296bdc4ad59a5f8a30',
        }
    ]


def test_factory_init(contract_adapters_factory):
    # Test if initiated_adapters is populated correctly
    for adapter in contract_adapters_factory.initiated_adapters.values():
        assert isinstance(adapter, DexClientInterface)

    # Test if namespace_by_factory_address is populated
    assert isinstance(contract_adapters_factory.namespace_by_factory_address, dict)
    assert len(contract_adapters_factory.namespace_by_factory_address) > 0


def test_get_dex_client(contract_adapters_factory):
    # Test with valid AMM type
    amm_type = 'uniswap_v2'
    dex_client = contract_adapters_factory.get_dex_client(amm_type, MagicMock(), 1)
    assert isinstance(dex_client, UniswapV2Amm)

    # Test with invalid AMM type
    with pytest.raises(ValueError):
        contract_adapters_factory.get_dex_client('invalid_amm_type', MagicMock(), 1)
