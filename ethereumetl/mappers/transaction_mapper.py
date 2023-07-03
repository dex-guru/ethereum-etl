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
from dataclasses import asdict
from typing import Any

from ethereumetl.domain.transaction import EthTransaction
from ethereumetl.enumeration.entity_type import EntityType
from ethereumetl.utils import hex_to_dec, to_normalized_address


class EthTransactionMapper(object):
    @staticmethod
    def json_dict_to_transaction(json_dict: dict, **kwargs) -> EthTransaction:
        transaction = EthTransaction(
            hash=json_dict.get('hash'),
            nonce=hex_to_dec(json_dict.get('nonce')),
            block_hash=json_dict.get('blockHash'),
            block_number=hex_to_dec(json_dict.get('blockNumber')),
            block_timestamp=kwargs.get('block_timestamp'),
            transaction_index=hex_to_dec(json_dict.get('transactionIndex')),
            from_address=to_normalized_address(json_dict.get('from')),
            to_address=to_normalized_address(json_dict.get('to')),
            value=hex_to_dec(json_dict.get('value')),
            gas=hex_to_dec(json_dict.get('gas')),
            gas_price=hex_to_dec(json_dict.get('gasPrice')),
            input=json_dict.get('input'),
            max_fee_per_gas=hex_to_dec(json_dict.get('maxFeePerGas')),
            max_priority_fee_per_gas=hex_to_dec(json_dict.get('maxPriorityFeePerGas')),
            transaction_type=hex_to_dec(json_dict.get('type')),
        )
        return transaction

    @staticmethod
    def transaction_to_dict(transaction: EthTransaction) -> dict[str, Any]:
        result = asdict(transaction)
        result['type'] = EntityType.TRANSACTION.value
        return result
