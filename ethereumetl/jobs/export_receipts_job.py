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


import json
import time
from collections.abc import Iterable

from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.config.envs import envs
from ethereumetl.domain.error import EthError
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from ethereumetl.json_rpc_requests import generate_get_receipt_json_rpc
from ethereumetl.mappers.error_mapper import EthErrorMapper
from ethereumetl.mappers.receipt_log_mapper import EthReceiptLogMapper
from ethereumetl.mappers.receipt_mapper import EthReceiptMapper
from ethereumetl.utils import rpc_response_batch_to_results


# Exports receipts and logs
class ExportReceiptsJob(BaseJob):
    def __init__(
        self,
        transactions: Iterable[dict],
        batch_size,
        batch_web3_provider,
        max_workers,
        item_exporter,
        export_receipts=True,
        export_logs=True,
        skip_none_receipts=envs.SKIP_NONE_RECEIPTS,
    ):
        self.batch_web3_provider = batch_web3_provider
        self.transactions = transactions

        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers, job_name='Export Receipts Job')
        self.item_exporter = item_exporter

        self.export_receipts = export_receipts
        self.export_logs = export_logs
        if not self.export_receipts and not self.export_logs:
            raise ValueError('At least one of export_receipts or export_logs must be True')

        self.receipt_mapper = EthReceiptMapper()
        self.error_mapper = EthErrorMapper()
        self.receipt_log_mapper = EthReceiptLogMapper()
        self.skip_none_receipts = skip_none_receipts

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(self.transactions, self._export_receipts)

    def _export_receipts(self, transactions):
        transactions = tuple(transactions)
        receipts_rpc = list(generate_get_receipt_json_rpc(t['hash'] for t in transactions))
        responses = self.batch_web3_provider.make_batch_request(json.dumps(receipts_rpc))
        errors = []

        if self.skip_none_receipts:
            all_responses = responses
            responses = []
            receipt_rpc_transaction_by_id = {
                r['id']: (r, transactions[i]) for i, r in enumerate(receipts_rpc)
            }
            for response in all_responses:
                try:
                    receipt_rpc, transaction = receipt_rpc_transaction_by_id[response['id']]
                except KeyError as e:
                    raise KeyError(f'RPC response id does not match any request id: {e}') from e
                if response.get('result') is None and response.get('error') is None:
                    errors.append(
                        EthError(
                            block_number=transaction['block_number'],
                            timestamp=int(time.time()),
                            kind='get_receipt_result_none',
                            data={
                                'transaction_hash': transaction['hash'],
                                'rpc_request': receipt_rpc,
                                'rpc_response': response,
                                'env': envs.dict(),
                            },
                        )
                    )
                else:
                    responses.append(response)

        results = rpc_response_batch_to_results(responses)
        receipts = []
        for result in results:
            receipts.append(self.receipt_mapper.json_dict_to_receipt(result))
        for receipt in receipts:
            self._export_receipt(receipt)

        error_items = [self.error_mapper.error_to_dict(error) for error in errors]
        self.item_exporter.export_items(error_items)

    def _export_receipt(self, receipt):
        if self.export_receipts:
            self.item_exporter.export_item(self.receipt_mapper.receipt_to_dict(receipt))
        if self.export_logs:
            for log in receipt.logs:
                self.item_exporter.export_item(self.receipt_log_mapper.receipt_log_to_dict(log))

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
