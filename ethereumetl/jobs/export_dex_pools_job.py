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
from collections.abc import Iterable

from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from ethereumetl.mappers.dex_pool_mapper import EthDexPoolMapper
from ethereumetl.service.eth_pool_service import EthPoolService


class ExportPoolsJob(BaseJob):
    def __init__(
        self,
        logs_iterable: Iterable[dict],
        sighash_to_namespace: dict | None,  # {(event_sig, topic_count): namespace}
        item_exporter,
        batch_size,
        batch_web3_provider,
        max_workers,
        chain_id,
    ):
        self.item_exporter = item_exporter
        self.batch_work_executor = BatchWorkExecutor(1, max_workers)
        self.batch_web3_provider = batch_web3_provider
        self.batch_size = batch_size
        logs_iterable = sorted(logs_iterable, key=lambda log: log['address'])
        self.sighash_to_namespace = sighash_to_namespace or {}
        self.pool_service = EthPoolService(batch_web3_provider, chain_id=chain_id)
        self.dex_pool_mapper = EthDexPoolMapper()
        logs_with_unique_addresses = []
        added_addresses = set()
        for log in logs_iterable:
            if log['address'] not in added_addresses:
                added_addresses.add(log['address'])
                logs_with_unique_addresses.append(log)
        self.logs_iterable = logs_with_unique_addresses

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(
            self.logs_iterable, self._export_pools, len(self.logs_iterable)
        )

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()

    def _export_pools(self, logs):
        pools = []
        for log in logs:
            potential_dex_types = self.sighash_to_namespace.get(
                (log['topics'][0], len(log['topics']))
            )
            if not potential_dex_types:
                continue
            pool = self.pool_service.get_dex_pool(log['address'], potential_dex_types)
            if pool:
                pools.append(self.dex_pool_mapper.pool_to_dict(pool))

        self.item_exporter.export_items(pools)


ASCII_0 = 0


def clean_user_provided_content(content):
    if isinstance(content, str):
        # This prevents this error in BigQuery
        # Error while reading data, error message: Error detected while parsing row starting at position: 9999.
        # Error: Bad character (ASCII 0) encountered.
        return content.translate({ASCII_0: None})
    else:
        return content
