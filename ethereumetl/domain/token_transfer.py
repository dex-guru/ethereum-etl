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
from dataclasses import dataclass
from enum import Enum
from typing import Literal, TypedDict


class TokenStandard(str, Enum):
    ERC20 = 'ERC-20'
    ERC721 = 'ERC-721'
    ERC1155 = 'ERC-1155'


@dataclass(slots=True)
class EthTokenTransfer:
    token_address: str
    from_address: str
    to_address: str
    value: int
    transaction_hash: str
    log_index: int
    block_number: int
    token_standard: TokenStandard
    token_id: int | None = None  # ERC-721, ERC-1155
    operator_address: str | None = None


class EthTokenTransferItem(TypedDict):
    type: str
    token_address: str
    from_address: str
    to_address: str
    value: int
    transaction_hash: str
    log_index: int
    block_number: int
    token_standard: Literal['ERC-20', 'ERC-721', 'ERC-1155']
    token_id: int | None
    operator_address: str | None
