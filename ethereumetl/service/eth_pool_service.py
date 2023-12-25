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
import logging

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.service.dex.dex_client_factory import AmmClientFactory

logger = logging.getLogger('eth_pool_service')


class EthPoolService:
    def __init__(self, web3, chain_id: int):
        self._web3 = web3
        self._chain_id = chain_id
        self._possible_dex_types = AmmClientFactory.get_all_supported_dex_types()

    def get_dex_pool(
        self, pool_address: str, potential_dex_types: list[str] | None = None
    ) -> EthDexPool | None:
        if not potential_dex_types:
            potential_dex_types = self._possible_dex_types
        for dex_type in potential_dex_types:
            try:
                dex_client = AmmClientFactory.get_dex_client(dex_type, self._web3, self._chain_id)
            except ValueError:
                continue
            dex_pool = dex_client.get_base_pool(pool_address)
            if dex_pool:
                return dex_pool
        return None
