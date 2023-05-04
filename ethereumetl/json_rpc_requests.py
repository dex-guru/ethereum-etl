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


ERC20_BALANCE_OF_SELECTOR = keccak(text='balanceOf(address)')[:4]
ERC721_BALANCE_OF_SELECTOR = keccak(text='balanceOf(address,uint256)')[:4]


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
    """
    if token_id is None:  # ERC-20 contract
        # [ selector: 4 bytes, address: 20 bytes zero padded to 32 bytes ]
        data = ERC20_BALANCE_OF_SELECTOR + eth_abi.encode_single('address', holder_address)

    else:  # ERC-721 or ERC-1155 contract
        # [ selector: 4 bytes, address: 20 bytes zero padded to 32 bytes, token_id: uint256 ]
        data = ERC721_BALANCE_OF_SELECTOR + eth_abi.encode(
            ('address', 'uint256'),
            (holder_address, token_id),
        )

    transaction = {'to': contract_address, 'data': to_hex(data)}

    return generate_json_rpc('eth_call', [transaction, to_hex(block)], request_id)


def generate_json_rpc(method, params, request_id=1):
    return {
        'jsonrpc': '2.0',
        'method': method,
        'params': params,
        'id': request_id,
    }
