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

from ethereumetl.domain.token_transfer import EthTokenTransfer
from ethereumetl.enumeration.entity_type import EntityType


class EthTokenTransferMapper:
    @staticmethod
    def token_transfer_to_dict(token_transfer: EthTokenTransfer) -> dict[str, Any]:
        result = asdict(token_transfer)
        result['type'] = str(EntityType.TOKEN_TRANSFER.value)
        return result

    @staticmethod
    def dict_to_token_transfer(d: dict[str, Any]) -> EthTokenTransfer:
        return EthTokenTransfer(
            token_address=d['token_address'],
            from_address=d['from_address'],
            to_address=d['to_address'],
            value=d['value'],
            transaction_hash=d['transaction_hash'],
            log_index=d['log_index'],
            block_number=d['block_number'],
            token_standard=d['token_standard'],
            token_id=d['token_id'],
            operator_address=d['operator_address'],
        )
