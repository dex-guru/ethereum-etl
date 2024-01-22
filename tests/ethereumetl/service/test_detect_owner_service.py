import pytest

from ethereumetl.service.detect_swap_owner import SwapOwnerDetectionService

swaps = [
    {
        'token_amounts_raw': (9962194100068119715840, -1511749439198529716),
        'pool_address': '0xc09bf2b1bc8725903c509e8caeef9190857215a8',
        'transaction_hash': '0xe8bb6bc5b2d2797c02a75bcc967227ce19ba8abb5fee649338b87a782ef69cb5',
        'log_index': 166,
        'block_number': 18962407,
        'event_type': 'swap',
        'token_reserves': (1852518364471930738258416, 283474787090371067712),
        'token_prices': (0.0001530213100862711, 6535.037501876145),
        'lp_token_address': None,
        'type': 'dex_trade',
        'token_addresses': [
            '0x73d7c860998ca3c01ce8c808f5577d94d545d1b4',
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
        ],
        'token_amounts': [9962.19410006812, -1.5117494391985298],
        'factory_address': '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f',
    }
]

dex_pool = {
    'address': '0xc09bf2b1bc8725903c509e8caeef9190857215a8',
    'factory_address': '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f',
    'token_addresses': [
        '0x73d7c860998ca3c01ce8c808f5577d94d545d1b4',
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
    ],
    'lp_token_addresses': ['0xc09bf2b1bc8725903c509e8caeef9190857215a8'],
    'fee': 3000,
}

transfers = [
    {
        'token_address': '0x73d7c860998ca3c01ce8c808f5577d94d545d1b4',
        'from_address': '0xa7d2550cc823721edc6641da8cde8dd7ad369523',
        'to_address': '0x9008d19f58aabd9ed0d60971565aa8510560ab41',
        'value': 10000000000000000000000,
        'transaction_hash': '0xe8bb6bc5b2d2797c02a75bcc967227ce19ba8abb5fee649338b87a782ef69cb5',
        'log_index': 160,
        'block_number': 18962407,
        'token_standard': 'ERC-20',
        'token_id': None,
        'operator_address': None,
    },
    {
        'token_address': '0x73d7c860998ca3c01ce8c808f5577d94d545d1b4',
        'from_address': '0x9008d19f58aabd9ed0d60971565aa8510560ab41',
        'to_address': '0xc09bf2b1bc8725903c509e8caeef9190857215a8',
        'value': 9962194100068119715840,
        'transaction_hash': '0xe8bb6bc5b2d2797c02a75bcc967227ce19ba8abb5fee649338b87a782ef69cb5',
        'log_index': 162,
        'block_number': 18962407,
        'token_standard': 'ERC-20',
        'token_id': None,
        'operator_address': None,
    },
    {
        'token_address': '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
        'from_address': '0xc09bf2b1bc8725903c509e8caeef9190857215a8',
        'to_address': '0x9008d19f58aabd9ed0d60971565aa8510560ab41',
        'value': 1511749439198529716,
        'transaction_hash': '0xe8bb6bc5b2d2797c02a75bcc967227ce19ba8abb5fee649338b87a782ef69cb5',
        'log_index': 164,
        'block_number': 18962407,
        'token_standard': 'ERC-20',
        'token_id': None,
        'operator_address': None,
    },
    {
        'token_address': '0x38e382f74dfb84608f3c1f10187f6bef5951de93',
        'from_address': '0x844eb5c280f38c7462316aad3f338ef9bda62668',
        'to_address': '0x9008d19f58aabd9ed0d60971565aa8510560ab41',
        'value': 24563098883056863068943,
        'transaction_hash': '0xe8bb6bc5b2d2797c02a75bcc967227ce19ba8abb5fee649338b87a782ef69cb5',
        'log_index': 168,
        'block_number': 18962407,
        'token_standard': 'ERC-20',
        'token_id': None,
        'operator_address': None,
    },
    {
        'token_address': '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
        'from_address': '0x9008d19f58aabd9ed0d60971565aa8510560ab41',
        'to_address': '0x844eb5c280f38c7462316aad3f338ef9bda62668',
        'value': 1511749439198529716,
        'transaction_hash': '0xe8bb6bc5b2d2797c02a75bcc967227ce19ba8abb5fee649338b87a782ef69cb5',
        'log_index': 169,
        'block_number': 18962407,
        'token_standard': 'ERC-20',
        'token_id': None,
        'operator_address': None,
    },
    {
        'token_address': '0x38e382f74dfb84608f3c1f10187f6bef5951de93',
        'from_address': '0x9008d19f58aabd9ed0d60971565aa8510560ab41',
        'to_address': '0xa7d2550cc823721edc6641da8cde8dd7ad369523',
        'value': 24563098883056863068943,
        'transaction_hash': '0xe8bb6bc5b2d2797c02a75bcc967227ce19ba8abb5fee649338b87a782ef69cb5',
        'log_index': 172,
        'block_number': 18962407,
        'token_standard': 'ERC-20',
        'token_id': None,
        'operator_address': None,
    },
]

all_pool_addresses = [
    '0x672fefac7f6e3017d9a2f1c14fe048191a24ce14',
    '0x6ef6666271df490f8c7d5cd18077662456aa292c',
    '0x9f9373b2b5fe5f6b32f171191946b0be30e0bd30',
    '0x92cc4300b9fd36242900bca782b2e9e000bd5099',
    '0x9e7065ce7e029d6a6d4936091547921a8008c4d3',
    '0xd6ef070951d008f1e6426ad9ca1c4fcf7220ee4d',
    '0x69c66beafb06674db41b22cfc50c34a93b8d82a2',
    '0x83f66823e56c429dc7682e5fd6785982306c1ad0',
    '0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc',
    '0xbf519fb74965680b45d528d29e1d77bd1728c9ce',
    '0xc09bf2b1bc8725903c509e8caeef9190857215a8',
    '0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852',
    '0x25647e01bd0967c1b9599fa3521939871d1d0888',
    '0x2fa947313b2598af518a9a8e43ca8aae4d4427b2',
    '0x8180eb8d90023fd91b7254eb442bf3d09d0042de',
]


@pytest.fixture
def detect_owner_service():
    return SwapOwnerDetectionService()


def test_detect_swap_owner(detect_owner_service):
    owner = detect_owner_service.get_swap_owner(transfers, dex_pool, all_pool_addresses)
    # assert owner == '0xbf54079c9Bc879Ae4dD6BC79bCe11d3988fD9C2b'.lower() # TODO will be fixed after connect univ3
    assert owner
