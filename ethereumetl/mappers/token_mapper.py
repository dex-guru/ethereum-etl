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

from ethereumetl.domain.token import EthToken
from ethereumetl.enumeration.entity_type import EntityType


class EthTokenMapper:
    @staticmethod
    def token_to_dict(token: EthToken) -> dict[str, Any]:
        result = asdict(token)
        result['type'] = str(EntityType.TOKEN.value)
        return result

    @staticmethod
    def dict_to_token(d: dict[str, Any]) -> EthToken:
        return EthToken(
            address=d['address'].lower(),
            name=d['name'],
            symbol=d['symbol'],
            decimals=int(d['decimals']),
            total_supply=int(d['total_supply']),
        )
