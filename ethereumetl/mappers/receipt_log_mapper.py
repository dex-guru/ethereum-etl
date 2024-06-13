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

from ethereumetl.domain.receipt_log import EthReceiptLog
from ethereumetl.enumeration.entity_type import EntityType
from ethereumetl.utils import hex_to_dec


class EthReceiptLogMapper:
    @staticmethod
    def json_dict_to_receipt_log(json_dict: dict) -> EthReceiptLog:
        receipt_log = EthReceiptLog(
            log_index=hex_to_dec(json_dict.get('logIndex')),
            transaction_hash=json_dict.get('transactionHash'),  # type: ignore
            transaction_index=hex_to_dec(json_dict.get('transactionIndex')),
            block_hash=json_dict.get('blockHash'),
            block_number=hex_to_dec(json_dict.get('blockNumber')),
            address=json_dict.get('address'),  # type: ignore
            data=json_dict.get('data'),  # type: ignore
            topics=json_dict.get('topics'),  # type: ignore
        )
        return receipt_log

    @staticmethod
    def web3_dict_to_receipt_log(d: dict) -> EthReceiptLog:
        transaction_hash = d.get('transactionHash')
        if transaction_hash is not None:
            transaction_hash = transaction_hash.hex()
        block_hash = d.get('blockHash')
        if block_hash is not None:
            block_hash = block_hash.hex()
        if 'topics' in d:
            topics = [topic.hex() for topic in d['topics']]
        else:
            topics = []

        receipt_log = EthReceiptLog(
            log_index=d.get('logIndex'),  # type: ignore
            transaction_index=d.get('transactionIndex'),  # type: ignore
            transaction_hash=transaction_hash,
            block_hash=block_hash,
            block_number=d.get('blockNumber'),  # type: ignore
            address=d.get('address'),  # type: ignore
            data=d.get('data'),  # type: ignore
            topics=topics,
        )

        return receipt_log

    @staticmethod
    def receipt_log_to_dict(receipt_log: EthReceiptLog) -> dict[str, Any]:
        result = asdict(receipt_log)
        result['type'] = str(EntityType.LOG.value)
        return result

    @staticmethod
    def dict_to_receipt_log(d: dict) -> EthReceiptLog:
        topics: Any = d.get('topics')
        if isinstance(topics, str):
            if len(topics.strip()) == 0:
                receipt_log_topics = []
            else:
                receipt_log_topics = topics.strip().split(',')
        else:
            receipt_log_topics = topics

        receipt_log = EthReceiptLog(
            log_index=d.get('log_index'),  # type: ignore
            transaction_hash=d.get('transaction_hash'),  # type: ignore
            transaction_index=d.get('transaction_index'),  # type: ignore
            block_hash=d.get('block_hash'),
            block_number=d.get('block_number'),  # type: ignore
            address=d.get('address'),  # type: ignore
            data=d.get('data'),  # type: ignore
            topics=receipt_log_topics,
        )
        return receipt_log
