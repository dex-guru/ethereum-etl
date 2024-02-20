import pytest

from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
from ethereumetl.jobs.enrich_dex_trades_job import EnrichDexTradeJob


@pytest.fixture()
def transfers_sample():
    return [
        {
            'block_number': 19234294,
            'from_address': '0x6cdff40fcad846328a8aa19ea928d3fa9412517f',
            'log_index': 57,
            'operator_address': None,
            'to_address': '0x43de4318b6eb91a7cf37975dbb574396a7b5b5c6',
            'token_address': '0x38e68a37e401f7271568cecaac63c6b1e19130b4',
            'token_id': None,
            'token_standard': 'ERC-20',
            'transaction_hash': '0x06284a665dd6b7f0afba08272a2040e5d8e3b220cd61e48e8c7df0ed1f8edb14',
            'type': 'token_transfer',
            'value': 124943201660504636845,
        },
        {
            'block_number': 19234294,
            'from_address': '0x0000000000a84d1a9b0063a910315c7ffa9cd248',
            'log_index': 58,
            'operator_address': None,
            'to_address': '0x6cdff40fcad846328a8aa19ea928d3fa9412517f',
            'token_address': '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            'token_id': None,
            'token_standard': 'ERC-20',
            'transaction_hash': '0x06284a665dd6b7f0afba08272a2040e5d8e3b220cd61e48e8c7df0ed1f8edb14',
            'type': 'token_transfer',
            'value': 1027002180000000000,
        },
        {
            'block_number': 19234294,
            'from_address': '0x43de4318b6eb91a7cf37975dbb574396a7b5b5c6',
            'log_index': 60,
            'operator_address': None,
            'to_address': '0x0000000000a84d1a9b0063a910315c7ffa9cd248',
            'token_address': '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            'token_id': None,
            'token_standard': 'ERC-20',
            'transaction_hash': '0x06284a665dd6b7f0afba08272a2040e5d8e3b220cd61e48e8c7df0ed1f8edb14',
            'type': 'token_transfer',
            'value': 1038726533961254388,
        },
        {
            'block_number': 19234294,
            'from_address': '0x6fa41f2620a2fa4189935fe1e81912cb3ff3935c',
            'log_index': 315,
            'operator_address': None,
            'to_address': '0x714a54d6aeb3a4b7c8ade17ee6c4dcef1dca0050',
            'token_address': '0xd0b19232245859aba2fe7deb57b237c9b5d1188e',
            'token_id': 8015,
            'token_standard': 'ERC-721',
            'transaction_hash': '0x0a89d2c28d96fbe2af0e1f7d25aa0fb1034061bc49e6d6cfcf4df9ab9e8d4394',
            'type': 'token_transfer',
            'value': 0,
        },
        {
            'block_number': 19234294,
            'from_address': '0x6ba211aa1348682c393173369cd2daab50032ede',
            'log_index': 21,
            'operator_address': None,
            'to_address': '0x80a64c6d7f12c47b7c66c5b4e20e72bc1fcd5d9e',
            'token_address': '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            'token_id': None,
            'token_standard': 'ERC-20',
            'transaction_hash': '0x0e517db26e2f740001327e128f0cc6d1578e84ded76ab8b8c35c64a2af498568',
            'type': 'token_transfer',
            'value': 99554755241287376,
        },
    ]


@pytest.fixture()
def internal_transfers_sample():
    return [
        {
            'from_address': '0x30a1b724c9dfe2e12a19ed84878312d199d1519e',
            'gas_limit': 250000,
            'id': 'call_0',
            'to_address': '0x0000000000a84d1a9b0063a910315c7ffa9cd248',
            'transaction_hash': '0x06284a665dd6b7f0afba08272a2040e5d8e3b220cd61e48e8c7df0ed1f8edb14',
            'type': 'internal_transfer',
            'value': 19234294,
        },
        {
            'from_address': '0x0000000000a84d1a9b0063a910315c7ffa9cd248',
            'gas_limit': 171346,
            'id': 'call_01',
            'to_address': '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            'transaction_hash': '0x06284a665dd6b7f0afba08272a2040e5d8e3b220cd61e48e8c7df0ed1f8edb14',
            'type': 'internal_transfer',
            'value': 1027002180000000000,
        },
        {
            'from_address': '0x0000000000a84d1a9b0063a910315c7ffa9cd248',
            'gas_limit': 149405,
            'id': 'call_02',
            'to_address': '0x6cdff40fcad846328a8aa19ea928d3fa9412517f',
            'transaction_hash': '0x06284a665dd6b7f0afba08272a2040e5d8e3b220cd61e48e8c7df0ed1f8edb14',
            'type': 'internal_transfer',
            'value': 0,
        },
        {
            'from_address': '0x6cdff40fcad846328a8aa19ea928d3fa9412517f',
            'gas_limit': 124707,
            'id': 'call_020',
            'to_address': '0x38e68a37e401f7271568cecaac63c6b1e19130b4',
            'transaction_hash': '0x06284a665dd6b7f0afba08272a2040e5d8e3b220cd61e48e8c7df0ed1f8edb14',
            'type': 'internal_transfer',
            'value': 0,
        },
        {
            'from_address': '0x6cdff40fcad846328a8aa19ea928d3fa9412517f',
            'gas_limit': 112611,
            'id': 'call_022',
            'to_address': '0x0000000000a84d1a9b0063a910315c7ffa9cd248',
            'transaction_hash': '0x06284a665dd6b7f0afba08272a2040e5d8e3b220cd61e48e8c7df0ed1f8edb14',
            'type': 'internal_transfer',
            'value': 0,
        },
    ]


@pytest.fixture()
def transactions_sample():
    return [
        {
            'hash': '0x06284a665dd6b7f0afba08272a2040e5d8e3b220cd61e48e8c7df0ed1f8edb14',
            'nonce': 4563,
            'block_hash': '0x396a2ee70d9456628df760708e5df9b0a284a877c650fa5853832683a46153b3',
            'block_number': 19234294,
            'transaction_index': 8,
            'from_address': '0x30a1b724c9dfe2e12a19ed84878312d199d1519e',
            'to_address': '0x0000000000a84d1a9b0063a910315c7ffa9cd248',
            'value': 19234294,
            'gas': 250000,
            'gas_price': 52893156368,
            'input': '0x0000000000061f14ba19feadbf026cdff40fcad846328a8aa19ea928d3fa9412517f1138e68a37e401f7271568cecaac63c6b1e19130b443de4318b6eb91a7cf37975dbb574396a7b5b5c60a1e0000000000000e6a4c477b8885f4',
            'block_timestamp': 1708011755,
            'max_fee_per_gas': 59504800914,
            'max_priority_fee_per_gas': 0,
            'transaction_type': 2,
            'receipt_cumulative_gas_used': 1780876,
            'receipt_gas_used': 163707,
            'receipt_contract_address': None,
            'receipt_root': None,
            'receipt_status': 1,
            'receipt_effective_gas_price': 52893156368,
            'receipt_logs_count': 8,
            'is_reorged': False,
            'type': 'transaction',
        },
        {
            'hash': '0x0a89d2c28d96fbe2af0e1f7d25aa0fb1034061bc49e6d6cfcf4df9ab9e8d4394',
            'nonce': 363,
            'block_hash': '0x396a2ee70d9456628df760708e5df9b0a284a877c650fa5853832683a46153b3',
            'block_number': 19234294,
            'transaction_index': 137,
            'from_address': '0x714a54d6aeb3a4b7c8ade17ee6c4dcef1dca0050',
            'to_address': '0x00000000000000adc04c56bf30ac9d3c0aaf14dc',
            'value': 14600000000000000,
            'gas': 227825,
            'gas_price': 52906637893,
            'input': '0x00000000000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003292a83660b0000000000000000000000000006fa41f2620a2fa4189935fe1e81912cb3ff3935c000000000000000000000000004c00500000ad104d7dbd00e3ae0a5c00560c00000000000000000000000000d0b19232245859aba2fe7deb57b237c9b5d1188e0000000000000000000000000000000000000000000000000000000000001f4f000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000065ce2fc20000000000000000000000000000000000000000000000000000000065f439130000000000000000000000000000000000000000000000000000000000000000360c6ebe0000000000000000000000000000000000000000e67a146f21b8b3800000007b02230091a7ed01230072f7006a004d60a8d4e71d599b8104250f00000000007b02230091a7ed01230072f7006a004d60a8d4e71d599b8104250f00000000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000024000000000000000000000000000000000000000000000000000000000000002a0000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000014bf72f57d0000000000000000000000000000000a26b00c1f0df003000390027140000faa71900000000000000000000000000000000000000000000000000000000000000407a20bb635ba6393be1e19f255021ec0087e0e55b3c74b2d6cd31607db609e91853898c1590b3c2d3d200664b78d539fd0df525368b02603dd080f4be6eec00db00000000360c6ebe',
            'block_timestamp': 1708011755,
            'max_fee_per_gas': 71664679716,
            'max_priority_fee_per_gas': 13481525,
            'transaction_type': 2,
            'receipt_cumulative_gas_used': 15045990,
            'receipt_gas_used': 159963,
            'receipt_contract_address': None,
            'receipt_root': None,
            'receipt_status': 1,
            'receipt_effective_gas_price': 52906637893,
            'receipt_logs_count': 2,
            'is_reorged': False,
            'type': 'transaction',
        },
    ]


@pytest.fixture()
def dex_trades_sample():
    return [
        {
            'block_number': 19234294,
            'event_type': 'swap',
            'log_index': 85,
            'lp_token_address': None,
            'pool_address': '0x02f95d16ce771145b23ad7b970973e33198a5e6b',
            'token_addresses': [
                '0x8b91f277501cf8322ebe34f137dd35b384b353c7',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            ],
            'token_amounts': [13429.450768193, -0.1229112291111839],
            'token_prices': [[1.0, 109383.45584180634], [9.14215035815133e-06, 1.0]],
            'token_reserves': [6819433.303498297, 62.34428461796606],
            'transaction_hash': '0x848cb8e35d26af85517156c82a48d7dd5d9a231f0b0a3ce77bca3de2fa96a090',
            'type': 'dex_trade',
        },
        {
            'block_number': 19234294,
            'event_type': 'swap',
            'log_index': 54,
            'lp_token_address': None,
            'pool_address': '0x43de4318b6eb91a7cf37975dbb574396a7b5b5c6',
            'token_addresses': [
                '0x38e68a37e401f7271568cecaac63c6b1e19130b4',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            ],
            'token_amounts': [-2143.6141861127994, 17.654691765584033],
            'token_prices': [[1.0, 123.78540813210962], [0.008078496610301213, 1.0]],
            'token_reserves': [132600.98752863967, 1071.2166282727092],
            'transaction_hash': '0xef264d1bf1d67880931f46ac7dc95dd2c8d8242a322e36993f9d0c3590168d17',
            'type': 'dex_trade',
        },
        {
            'block_number': 19234294,
            'event_type': 'swap',
            'log_index': 281,
            'lp_token_address': None,
            'pool_address': '0x197d7010147df7b99e9025c724f13723b29313f8',
            'token_addresses': [
                '0xa41d2f8ee4f47d3b860a149765a7df8c3287b7f0',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            ],
            'token_amounts': [-120634.112824464, 0.26915515110099136],
            'token_prices': [[1.0, 449718.5809029796], [2.2236128158016576e-06, 1.0]],
            'token_reserves': [310890427.8371009, 691.299939648638],
            'transaction_hash': '0xd2291ddd953eefc32251d284bd4e832962f62fd4967b0b2f841e44e94e0210dd',
            'type': 'dex_trade',
        },
        {
            'block_number': 19234294,
            'event_type': 'swap',
            'log_index': 44,
            'lp_token_address': None,
            'pool_address': '0xae750560b09ad1f5246f3b279b3767afd1d79160',
            'token_addresses': [
                '0x02f92800f57bcd74066f5709f1daa1a4302df875',
                '0x6b175474e89094c44da98b954eedeac495271d0f',
            ],
            'token_amounts': [-332.845375110366, 1975.2078979047046],
            'token_prices': [[1.0, 0.1700524265246482], [5.8805394338495685, 1.0]],
            'token_reserves': [3.555655600527435e-13, 2.129614550367697e-12],
            'transaction_hash': '0x790a7073ea8df685047bfc87bf7289e641f5902a4e1d85ed84dfcaa929ffad69',
            'type': 'dex_trade',
        },
        {
            'block_number': 19234294,
            'event_type': 'swap',
            'log_index': 150,
            'lp_token_address': None,
            'pool_address': '0x6ba211aa1348682c393173369cd2daab50032ede',
            'token_addresses': [
                '0xa72332af4a7efbce221903e7a09175be64f0400d',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            ],
            'token_amounts': [19.315033355693465, -0.41288087444951277],
            'token_prices': [[1.0, 46.452079067185664], [0.021527561738488743, 1.0]],
            'token_reserves': [5.0621381772025345e-15, 4.381456112268169e-17],
            'transaction_hash': '0xfe85aca51541430b2f8c908bf37d80658259281a9fe8595f81b96612d9aff6cd',
            'type': 'dex_trade',
        },
    ]


@pytest.fixture()
def tokens_sample():
    return [
        {
            'address': '0x38e68a37e401f7271568cecaac63c6b1e19130b4',
            'decimals': 18,
            'name': 'Banana',
            'symbol': 'BANANA',
            'total_supply': 10000000000000000000000000,
            'type': 'token',
        },
        {
            'address': '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            'decimals': 18,
            'name': 'Wrapped Ether',
            'symbol': 'WETH',
            'total_supply': 3135835689358593052435655,
            'type': 'token',
        },
        {
            'address': '0xd0b19232245859aba2fe7deb57b237c9b5d1188e',
            'decimals': 0,
            'name': 'Uncharted LandsX Spirits',
            'symbol': 'Spirit',
            'total_supply': 2,
            'type': 'token',
        },
    ]


@pytest.fixture()
def dex_pools_sample():
    return [
        {
            'address': '0x01b464bc83f09e13c0ab218692ad09f971d88608',
            'factory_address': '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f',
            'token_addresses': [
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
                '0xf2dfdbe1ea71bbdcb5a4662a16dbf5e487be3ebe',
            ],
            'lp_token_addresses': ['0x01b464bc83f09e13c0ab218692ad09f971d88608'],
            'fee': 3000,
            'underlying_token_addresses': [],
            'type': 'dex_pool',
        },
        {
            'address': '0x02f95d16ce771145b23ad7b970973e33198a5e6b',
            'factory_address': '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f',
            'token_addresses': [
                '0x8b91f277501cf8322ebe34f137dd35b384b353c7',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            ],
            'lp_token_addresses': ['0x02f95d16ce771145b23ad7b970973e33198a5e6b'],
            'fee': 3000,
            'underlying_token_addresses': [],
            'type': 'dex_pool',
        },
        {
            'address': '0x06da0fd433c1a5d7a4faa01111c044910a184553',
            'factory_address': '0xc0aee478e3658e2610c5f7a4a2e1777ce9e4f2ac',
            'token_addresses': [
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
                '0xdac17f958d2ee523a2206206994597c13d831ec7',
            ],
            'lp_token_addresses': ['0x06da0fd433c1a5d7a4faa01111c044910a184553'],
            'fee': 3000,
            'underlying_token_addresses': [],
            'type': 'dex_pool',
        },
        {
            'address': '0x0e304f5b7d89597f841631ba752f6dea55e6b095',
            'factory_address': '0x1f98431c8ad98523631ae4a59f267346ea31f984',
            'token_addresses': [
                '0x382edfe4c6168858c81893fe00fcb7b68914d929',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            ],
            'lp_token_addresses': ['0x0e304f5b7d89597f841631ba752f6dea55e6b095'],
            'fee': 10000,
            'underlying_token_addresses': [],
            'type': 'dex_pool',
        },
        {
            'address': '0x11b815efb8f581194ae79006d24e0d814b7697f6',
            'factory_address': '0x1f98431c8ad98523631ae4a59f267346ea31f984',
            'token_addresses': [
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
                '0xdac17f958d2ee523a2206206994597c13d831ec7',
            ],
            'lp_token_addresses': ['0x11b815efb8f581194ae79006d24e0d814b7697f6'],
            'fee': 500,
            'underlying_token_addresses': [],
            'type': 'dex_pool',
        },
        {
            'address': '0x197d7010147df7b99e9025c724f13723b29313f8',
            'factory_address': '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f',
            'token_addresses': [
                '0xa41d2f8ee4f47d3b860a149765a7df8c3287b7f0',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            ],
            'lp_token_addresses': ['0x197d7010147df7b99e9025c724f13723b29313f8'],
            'fee': 3000,
            'underlying_token_addresses': [],
            'type': 'dex_pool',
        },
        {
            'address': '0x331399c614ca67dee86733e5a2fba40dbb16827c',
            'factory_address': '0x1f98431c8ad98523631ae4a59f267346ea31f984',
            'token_addresses': [
                '0xb9f599ce614feb2e1bbe58f180f370d05b39344e',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            ],
            'lp_token_addresses': ['0x331399c614ca67dee86733e5a2fba40dbb16827c'],
            'fee': 10000,
            'underlying_token_addresses': [],
            'type': 'dex_pool',
        },
        {
            'address': '0x39f49254d6eaf6b2b2549dfec4ed93cf6bae167f',
            'factory_address': '0xc0aee478e3658e2610c5f7a4a2e1777ce9e4f2ac',
            'token_addresses': [
                '0x9e20461bc2c4c980f62f1b279d71734207a6a356',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            ],
            'lp_token_addresses': ['0x39f49254d6eaf6b2b2549dfec4ed93cf6bae167f'],
            'fee': 3000,
            'underlying_token_addresses': [],
            'type': 'dex_pool',
        },
        {
            'address': '0x43de4318b6eb91a7cf37975dbb574396a7b5b5c6',
            'factory_address': '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f',
            'token_addresses': [
                '0x38e68a37e401f7271568cecaac63c6b1e19130b4',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            ],
            'lp_token_addresses': ['0x43de4318b6eb91a7cf37975dbb574396a7b5b5c6'],
            'fee': 3000,
            'underlying_token_addresses': [],
            'type': 'dex_pool',
        },
        {
            'address': '0x53e79ef1cf6ac0cdf6f1743c3be3ad48fa3c5657',
            'factory_address': '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f',
            'token_addresses': [
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
                '0xe3dbc4f88eaa632ddf9708732e2832eeaa6688ab',
            ],
            'lp_token_addresses': ['0x53e79ef1cf6ac0cdf6f1743c3be3ad48fa3c5657'],
            'fee': 3000,
            'underlying_token_addresses': [],
            'type': 'dex_pool',
        },
        {
            'address': '0x59354356ec5d56306791873f567d61ebf11dfbd5',
            'factory_address': '0x1f98431c8ad98523631ae4a59f267346ea31f984',
            'token_addresses': [
                '0xb50721bcf8d664c30412cfbc6cf7a15145234ad1',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            ],
            'lp_token_addresses': ['0x59354356ec5d56306791873f567d61ebf11dfbd5'],
            'fee': 3000,
            'underlying_token_addresses': [],
            'type': 'dex_pool',
        },
        {
            'address': '0x68e4af213c49f320175116bff189c9ca452ce29c',
            'factory_address': '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f',
            'token_addresses': [
                '0x0000000000ca73a6df4c58b84c5b4b847fe8ff39',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            ],
            'lp_token_addresses': ['0x68e4af213c49f320175116bff189c9ca452ce29c'],
            'fee': 3000,
            'underlying_token_addresses': [],
            'type': 'dex_pool',
        },
        {
            'address': '0x6ba211aa1348682c393173369cd2daab50032ede',
            'factory_address': '0x1f98431c8ad98523631ae4a59f267346ea31f984',
            'token_addresses': [
                '0xa72332af4a7efbce221903e7a09175be64f0400d',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            ],
            'lp_token_addresses': ['0x6ba211aa1348682c393173369cd2daab50032ede'],
            'fee': 10000,
            'underlying_token_addresses': [],
            'type': 'dex_pool',
        },
        {
            'address': '0x6cdff40fcad846328a8aa19ea928d3fa9412517f',
            'factory_address': '0x1f98431c8ad98523631ae4a59f267346ea31f984',
            'token_addresses': [
                '0x38e68a37e401f7271568cecaac63c6b1e19130b4',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            ],
            'lp_token_addresses': ['0x6cdff40fcad846328a8aa19ea928d3fa9412517f'],
            'fee': 10000,
            'underlying_token_addresses': [],
            'type': 'dex_pool',
        },
        {
            'address': '0x79db69ab1ed51261a9fdc3bf0e6db3fa48b8cc52',
            'factory_address': '0x1f98431c8ad98523631ae4a59f267346ea31f984',
            'token_addresses': [
                '0x0800394f6e23dd539929c8b77a3d45c96f76aefc',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            ],
            'lp_token_addresses': ['0x79db69ab1ed51261a9fdc3bf0e6db3fa48b8cc52'],
            'fee': 10000,
            'underlying_token_addresses': [],
            'type': 'dex_pool',
        },
        {
            'address': '0x7fa640483993c968abc3d17b012f822441d1217e',
            'factory_address': '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f',
            'token_addresses': [
                '0x3cb48aeb3d1abadc23d2d8a6894b3a68338381c2',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            ],
            'lp_token_addresses': ['0x7fa640483993c968abc3d17b012f822441d1217e'],
            'fee': 3000,
            'underlying_token_addresses': [],
            'type': 'dex_pool',
        },
        {
            'address': '0x85aa97537837514a9229fcaeb66eeef7242122bf',
            'factory_address': '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f',
            'token_addresses': [
                '0x25b4f5d4c314bcd5d7962734936c957b947cb7cf',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            ],
            'lp_token_addresses': ['0x85aa97537837514a9229fcaeb66eeef7242122bf'],
            'fee': 3000,
            'underlying_token_addresses': [],
            'type': 'dex_pool',
        },
        {
            'address': '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640',
            'factory_address': '0x1f98431c8ad98523631ae4a59f267346ea31f984',
            'token_addresses': [
                '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            ],
            'lp_token_addresses': ['0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'],
            'fee': 500,
            'underlying_token_addresses': [],
            'type': 'dex_pool',
        },
        {
            'address': '0x89eeba49e12d06a26a25f83719914f173256ce72',
            'factory_address': '0x1f98431c8ad98523631ae4a59f267346ea31f984',
            'token_addresses': [
                '0x25931894a86d47441213199621f1f2994e1c39aa',
                '0xdac17f958d2ee523a2206206994597c13d831ec7',
            ],
            'lp_token_addresses': ['0x89eeba49e12d06a26a25f83719914f173256ce72'],
            'fee': 3000,
            'underlying_token_addresses': [],
            'type': 'dex_pool',
        },
        {
            'address': '0x8cd0f6551c79f9eb53fd6550ceac9abbde9f6506',
            'factory_address': '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f',
            'token_addresses': [
                '0x1fe03cd45839b0391358b719bb52993155430582',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            ],
            'lp_token_addresses': ['0x8cd0f6551c79f9eb53fd6550ceac9abbde9f6506'],
            'fee': 3000,
            'underlying_token_addresses': [],
            'type': 'dex_pool',
        },
        {
            'address': '0x95a45a87dd4d3a1803039072f37e075f37b23d75',
            'factory_address': '0x1f98431c8ad98523631ae4a59f267346ea31f984',
            'token_addresses': [
                '0x44ff8620b8ca30902395a7bd3f2407e1a091bf73',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            ],
            'lp_token_addresses': ['0x95a45a87dd4d3a1803039072f37e075f37b23d75'],
            'fee': 10000,
            'underlying_token_addresses': [],
            'type': 'dex_pool',
        },
        {
            'address': '0x97be09f2523b39b835da9ea3857cfa1d3c660cbb',
            'factory_address': '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f',
            'token_addresses': [
                '0x0fc6c0465c9739d4a42daca22eb3b2cb0eb9937a',
                '0x1bbf25e71ec48b84d773809b4ba55b6f4be946fb',
            ],
            'lp_token_addresses': ['0x97be09f2523b39b835da9ea3857cfa1d3c660cbb'],
            'fee': 3000,
            'underlying_token_addresses': [],
            'type': 'dex_pool',
        },
        {
            'address': '0x98409d8ca9629fbe01ab1b914ebf304175e384c8',
            'factory_address': '0x1f98431c8ad98523631ae4a59f267346ea31f984',
            'token_addresses': [
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
                '0xf411903cbc70a74d22900a5de66a2dda66507255',
            ],
            'lp_token_addresses': ['0x98409d8ca9629fbe01ab1b914ebf304175e384c8'],
            'fee': 3000,
            'underlying_token_addresses': [],
            'type': 'dex_pool',
        },
        {
            'address': '0xad9ef19e289dcbc9ab27b83d2df53cdeff60f02d',
            'factory_address': '0x1f98431c8ad98523631ae4a59f267346ea31f984',
            'token_addresses': [
                '0x15e6e0d4ebeac120f9a97e71faa6a0235b85ed12',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            ],
            'lp_token_addresses': ['0xad9ef19e289dcbc9ab27b83d2df53cdeff60f02d'],
            'fee': 10000,
            'underlying_token_addresses': [],
            'type': 'dex_pool',
        },
        {
            'address': '0xae750560b09ad1f5246f3b279b3767afd1d79160',
            'factory_address': '0x1f98431c8ad98523631ae4a59f267346ea31f984',
            'token_addresses': [
                '0x02f92800f57bcd74066f5709f1daa1a4302df875',
                '0x6b175474e89094c44da98b954eedeac495271d0f',
            ],
            'lp_token_addresses': ['0xae750560b09ad1f5246f3b279b3767afd1d79160'],
            'fee': 10000,
            'underlying_token_addresses': [],
            'type': 'dex_pool',
        },
        {
            'address': '0xb7f27e5ebf97d88f37e16eddecc59523361a60e1',
            'factory_address': '0x1f98431c8ad98523631ae4a59f267346ea31f984',
            'token_addresses': [
                '0x470c8950c0c3aa4b09654bc73b004615119a44b5',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            ],
            'lp_token_addresses': ['0xb7f27e5ebf97d88f37e16eddecc59523361a60e1'],
            'fee': 10000,
            'underlying_token_addresses': [],
            'type': 'dex_pool',
        },
        {
            'address': '0xba0c053250785a76cd85e789436b0208c1bf7c86',
            'factory_address': '0x1f98431c8ad98523631ae4a59f267346ea31f984',
            'token_addresses': [
                '0x6b0faca7ba905a86f221ceb5ca404f605e5b3131',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            ],
            'lp_token_addresses': ['0xba0c053250785a76cd85e789436b0208c1bf7c86'],
            'fee': 100,
            'underlying_token_addresses': [],
            'type': 'dex_pool',
        },
        {
            'address': '0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8',
            'factory_address': '0x1f98431c8ad98523631ae4a59f267346ea31f984',
            'token_addresses': [
                '0x6b175474e89094c44da98b954eedeac495271d0f',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            ],
            'lp_token_addresses': ['0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8'],
            'fee': 3000,
            'underlying_token_addresses': [],
            'type': 'dex_pool',
        },
        {
            'address': '0xdc900845732a53ee8df737efa282a6bc56976e62',
            'factory_address': '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f',
            'token_addresses': [
                '0x9e9fbde7c7a83c43913bddc8779158f1368f0413',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            ],
            'lp_token_addresses': ['0xdc900845732a53ee8df737efa282a6bc56976e62'],
            'fee': 3000,
            'underlying_token_addresses': [],
            'type': 'dex_pool',
        },
        {
            'address': '0xfb76331ddfffe1b42c15d02dcca63a76f04b693d',
            'factory_address': '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f',
            'token_addresses': [
                '0x659298aa4be6d761f6814cf935e8cd9ca85ac112',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            ],
            'lp_token_addresses': ['0xfb76331ddfffe1b42c15d02dcca63a76f04b693d'],
            'fee': 3000,
            'underlying_token_addresses': [],
            'type': 'dex_pool',
        },
    ]


@pytest.fixture()
def enrich_dex_trades_job(
    transfers_sample,
    internal_transfers_sample,
    transactions_sample,
    dex_trades_sample,
    tokens_sample,
):
    return EnrichDexTradeJob(
        dex_pools=[],
        base_tokens_prices=[],
        tokens=tokens_sample,
        token_transfers=transfers_sample,
        internal_transfers=internal_transfers_sample,
        transactions=transactions_sample,
        dex_trades=dex_trades_sample,
        stablecoin_addresses=[],
        native_token={
            'address': '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
        },
        item_exporter=InMemoryItemExporter(['enriched_dex_trade', 'enriched_transfer']),
    )


def test_build_transfers(
    enrich_dex_trades_job,
    transfers_sample,
    internal_transfers_sample,
    transactions_sample,
    dex_trades_sample,
):
    job = enrich_dex_trades_job
    transfers_by_hash = job._token_transfers_by_hash
    assert transfers_by_hash
    assert len(transfers_by_hash) == 3
    assert (
        len(
            transfers_by_hash['0x06284a665dd6b7f0afba08272a2040e5d8e3b220cd61e48e8c7df0ed1f8edb14']
        )
        == 6
    )
    assert (
        len(
            transfers_by_hash['0x0a89d2c28d96fbe2af0e1f7d25aa0fb1034061bc49e6d6cfcf4df9ab9e8d4394']
        )
        == 2
    )
    assert (
        len(
            transfers_by_hash['0x0e517db26e2f740001327e128f0cc6d1578e84ded76ab8b8c35c64a2af498568']
        )
        == 1
    )


def test_enrich_transfers(enrich_dex_trades_job):
    job = enrich_dex_trades_job
    job.item_exporter.open()
    transfers = []
    for transfer in job._token_transfers_by_hash.values():
        transfers.extend(transfer)
    job._enrich_transfers(transfers)
    assert len(job.item_exporter.get_items('enriched_transfer'))
    assert (
        len(job.item_exporter.get_items('enriched_transfer')) == 9 * 4
    )  # 9 transfers * 4 copies for filter_column
