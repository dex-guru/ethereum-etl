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

from ethereumetl.domain.receipt_log import ParsedReceiptLog
from ethereumetl.enumeration.entity_type import EntityType
from ethereumetl.utils import hex_to_dec


class EthParsedReceiptLogMapper:
    @staticmethod
    def dict_to_parsed_receipt_log(json_dict: dict) -> ParsedReceiptLog:
        receipt_log = ParsedReceiptLog(
            transaction_hash=json_dict['transaction_hash'],
            block_number=hex_to_dec(json_dict['block_number']),
            log_index=hex_to_dec(json_dict['log_index']),
            event_name=json_dict['event_name'],
            namespaces=json_dict['namespaces'],
            address=json_dict['address'],
            parsed_event=json_dict.get('parsed_event', {}),
        )
        return receipt_log

    @staticmethod
    def parsed_receipt_log_to_dict(receipt_log: ParsedReceiptLog) -> dict[str, Any]:
        result = asdict(receipt_log)
        result['type'] = str(EntityType.PARSED_LOG.value)
        return result
