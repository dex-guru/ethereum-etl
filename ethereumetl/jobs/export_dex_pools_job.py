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
from ethereumetl.mappers.parsed_log_mapper import EthParsedReceiptLogMapper
from ethereumetl.misc.info import PARSABLE_TRADE_EVENTS
from ethereumetl.service.eth_resolve_log_service import EthResolveLogService


class ExportPoolsJob(BaseJob):
    def __init__(
        self,
        parsed_logs_iterable: Iterable[dict],
        chain_id,
        item_exporter,
        batch_size,
        batch_web3_provider,
        max_workers,
    ):
        self.chain_id = chain_id
        self.item_exporter = item_exporter
        self.batch_work_executor = BatchWorkExecutor(
            batch_size, max_workers, job_name='Export Pools Job'
        )
        self.batch_web3_provider = batch_web3_provider
        self.pool_service = EthResolveLogService(batch_web3_provider, chain_id)
        self.dex_pool_mapper = EthDexPoolMapper()
        self.parsed_log_mapper = EthParsedReceiptLogMapper()
        self.logs_iterable = [
            EthParsedReceiptLogMapper.dict_to_parsed_receipt_log(log)
            for log in self._collect_logs_with_unique_addresses(parsed_logs_iterable)
            if log['event_name'] in PARSABLE_TRADE_EVENTS
        ]

    @staticmethod
    def _collect_logs_with_unique_addresses(parsed_logs_iterable):
        logs_with_unique_addresses = []
        added_addresses = set()

        for log in parsed_logs_iterable:
            # Balancer vault address is always in the log address
            pool_address = f"0x{log['parsed_event'].get('poolId', b'0x').hex().lower()[:40]}"
            if pool_address and pool_address not in added_addresses:
                added_addresses.add(pool_address)
                logs_with_unique_addresses.append(log)

            elif log['address'] not in added_addresses:
                added_addresses.add(log['address'])
                logs_with_unique_addresses.append(log)
        return logs_with_unique_addresses

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
            pool = self.pool_service.resolve_asset_from_log(log)
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
