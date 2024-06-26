# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
from typing import Literal

import eth_abi
from eth_utils import keccak, to_hex


def generate_get_block_by_number_json_rpc(block_numbers, include_transactions):
    for idx, block_number in enumerate(block_numbers):
        yield generate_json_rpc(
            method='eth_getBlockByNumber',
            params=[hex(block_number), include_transactions],
            request_id=idx,
        )


def generate_trace_block_by_number_json_rpc(block_numbers):
    for block_number in block_numbers:
        yield generate_json_rpc(
            method='debug_traceBlockByNumber',
            params=[hex(block_number), {'tracer': 'callTracer'}],
            # save block_number in request ID, so later we can identify block number in response
            request_id=block_number,
        )


def generate_trace_by_transaction_hashes_json_rpc(transaction_hashes):
    for i, transaction_hash in enumerate(transaction_hashes):
        yield generate_json_rpc(
            method='debug_traceTransaction',
            params=[transaction_hash, {'tracer': 'callTracer'}],
            request_id=i,
        )


def generate_get_receipt_json_rpc(transaction_hashes):
    for idx, transaction_hash in enumerate(transaction_hashes):
        yield generate_json_rpc(
            method='eth_getTransactionReceipt', params=[transaction_hash], request_id=idx
        )


def generate_get_code_json_rpc(contract_addresses, block='latest'):
    for idx, contract_address in enumerate(contract_addresses):
        yield generate_json_rpc(
            method='eth_getCode',
            params=[contract_address, hex(block) if isinstance(block, int) else block],
            request_id=idx,
        )


def generate_get_native_balance_json_rpc(address: str, block: int, request_id: int) -> dict:
    return generate_json_rpc(
        method='eth_getBalance',
        params=[address, to_hex(block)],
        request_id=request_id,
    )


ERC20_BALANCE_OF_SELECTOR = keccak(text='balanceOf(address)')[:4]
ERC1155_BALANCE_OF_SELECTOR = keccak(text='balanceOf(address,uint256)')[:4]


def generate_balance_of_json_rpc(
    contract_address: str,
    holder_address: str,
    token_id: int | None = None,
    block: int | Literal['latest'] = 'latest',
    request_id: int = 1,
) -> dict:
    """
    See:
        * https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_call
        * https://docs.soliditylang.org/en/latest/abi-spec.html
        * https://eips.ethereum.org/EIPS/eip-721
        * https://eips.ethereum.org/EIPS/eip-1155.

    Note:
    ----
        ERC-1155:        balanceOf(address,tokenId).
        ERC-20, ERC-721: balanceOf(address)         - cannot get balance for a specific token_id.

    """
    if token_id is None:  # ERC-20 or ERC-721 contract
        # [ selector: 4 bytes, address: 20 bytes zero padded to 32 bytes ]
        data = ERC20_BALANCE_OF_SELECTOR + eth_abi.encode_single('address', holder_address)

    else:  # ERC-1155 contract
        # [ selector: 4 bytes, address: 20 bytes zero padded to 32 bytes, token_id: uint256 ]
        data = ERC1155_BALANCE_OF_SELECTOR + eth_abi.encode(
            ('address', 'uint256'),
            (holder_address, token_id),
        )

    transaction = {'to': contract_address, 'data': to_hex(data)}

    eth_call_block: str | Literal['latest']
    if block == 'latest':
        eth_call_block = block
    else:
        eth_call_block = to_hex(block)
    return generate_json_rpc('eth_call', [transaction, eth_call_block], request_id)


def generate_json_rpc(method, params, request_id=1):
    return {
        'jsonrpc': '2.0',
        'method': method,
        'params': params,
        'id': request_id,
    }
