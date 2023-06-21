# MIT License
#
# Copyright (c) 2018 Evgeniy Filatov, evgeniyfilatov@gmail.com
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

import json

from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from ethereumetl.json_rpc_requests import generate_trace_by_transaction_hashes_json_rpc
from ethereumetl.mappers.geth_trace_mapper import EthGethTraceMapper
from ethereumetl.utils import rpc_response_to_result


# Exports geth traces
class ExportGethTracesJob(BaseJob):
    def __init__(
        self, batch_size, batch_web3_provider, max_workers, item_exporter, transaction_hashes
    ):
        self.transaction_hashes = transaction_hashes
        self.batch_web3_provider = batch_web3_provider

        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.item_exporter = item_exporter

        self.geth_trace_mapper = EthGethTraceMapper()

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(
            self.transaction_hashes,
            self._export_batch,
            total_items=len(self.transaction_hashes),
        )

    def _export_batch(self, transaction_hashes: list[str]):
        transaction_hashes = sorted(transaction_hashes)
        trace_tx_rpc = list(generate_trace_by_transaction_hashes_json_rpc(transaction_hashes))
        response = self.batch_web3_provider.make_batch_request(json.dumps(trace_tx_rpc))

        for response_item in response:
            transaction_hash = transaction_hashes[response_item.get('id')]
            if (
                transaction_hash
                and response_item.get('error') is None
                and response_item.get('result') is not None
                and any([r for r in response_item.get('result')])
            ):
                continue

            tx_traces = rpc_response_to_result(response_item)

            geth_trace = self.geth_trace_mapper.json_dict_to_geth_trace(
                {
                    'transaction_hash': transaction_hash,
                    'transaction_traces': tx_traces,
                }
            )

            self.item_exporter.export_item(self.geth_trace_mapper.geth_trace_to_dict(geth_trace))

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
