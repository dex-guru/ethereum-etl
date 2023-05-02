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

from eth_utils import is_address, keccak, to_bytes, to_hex


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


def generate_get_balance_json_rpc(
    contract_address: str,
    holder_address: str,
    token_id: int | None = None,
    block: int | None = None,
) -> dict:
    """
    See:
        * https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_call
        * https://docs.soliditylang.org/en/latest/abi-spec.html

    `eth_abi.encode` is too slow.
    """
    assert is_address(contract_address)
    assert is_address(holder_address)

    if token_id is None:  # ERC-20 token
        # [ selector: 4 bytes, address: 20 bytes zero padded to 32 bytes ]
        data = bytearray(4 + 32)
        data[:4] = ERC20_BALANCE_OF_SELECTOR  # 4 bytes of the function signature's keccak hash
        data[-20:] = to_bytes(hexstr=holder_address)  # 20 bytes of address

    else:  # ERC-721 or ERC-1155 token
        assert isinstance(token_id, int)
        # [ selector: 4 bytes, address: 20 bytes zero padded to 32 bytes, token_id: uint256 ]
        data = bytearray(4 + 32 + 32)
        data[:4] = ERC721_BALANCE_OF_SELECTOR  # 4 bytes of the function signature's keccak hash
        data[-32 - 20 : -32] = to_bytes(hexstr=holder_address)  # 20 bytes of address
        data[-32:] = token_id.to_bytes(32, 'big', signed=False)  # 32 bytes of uint256

    transaction = {'to': contract_address, 'data': to_hex(data)}

    if block is None:
        block = 'latest'
    else:
        assert isinstance(block, int)
        transaction['blockNumber'] = hex(block)

    return generate_json_rpc(method='eth_call', params=[transaction, block])


def generate_json_rpc(method, params, request_id=1):
    return {
        'jsonrpc': '2.0',
        'method': method,
        'params': params,
        'id': request_id,
    }
