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

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.enumeration.entity_type import EntityType


class EthDexPoolMapper:
    @staticmethod
    def pool_to_dict(pool: EthDexPool) -> dict[str, Any]:
        result = asdict(pool)
        result['fee'] = int(result['fee'])
        result['type'] = EntityType.DEX_POOL.value
        result['token_addresses'] = [
            token_address.lower() for token_address in result['token_addresses']
        ]
        result['address'] = result['address'].lower()
        result['factory_address'] = result['factory_address'].lower()
        result['lp_token_addresses'] = [
            lp_token_address.lower() for lp_token_address in result['lp_token_addresses']
        ]
        result['underlying_token_addresses'] = [
            underlying_token_address.lower()
            for underlying_token_address in result['underlying_token_addresses']
        ]

        return result

    @staticmethod
    def dict_to_pool(d: dict[str, Any]) -> EthDexPool:
        return EthDexPool(
            address=d['address'].lower(),
            factory_address=d['factory_address'].lower(),
            token_addresses=[token_address.lower() for token_address in d['token_addresses']],
            fee=int(d['fee']),
            lp_token_addresses=[
                lp_token_address.lower() for lp_token_address in d['lp_token_addresses']
            ],
            underlying_token_addresses=[
                underlying_token_address.lower()
                for underlying_token_address in d['underlying_token_addresses']
            ],
        )
