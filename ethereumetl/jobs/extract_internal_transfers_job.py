from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from ethereumetl.mappers.geth_trace_mapper import EthGethTraceMapper
from ethereumetl.mappers.internal_transfer_mapper import InternalTransferMapper


class ExtractInternalTransfersJob(BaseJob):
    def __init__(self, geth_traces_iterable, batch_size, max_workers, item_exporter):
        self.geth_traces_iterable = geth_traces_iterable

        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.item_exporter = item_exporter

        self.geth_trace_mapper = EthGethTraceMapper()
        self.internal_transfer_mapper = InternalTransferMapper()

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(self.geth_traces_iterable, self._extract_internal_transfers)

    def _extract_internal_transfers(self, geth_traces):
        for geth_trace_str in geth_traces:
            geth_trace = self.geth_trace_mapper.json_dict_to_geth_trace(geth_trace_str)
            internal_transfers = self.internal_transfer_mapper.geth_trace_to_internal_transfers(geth_trace)
            for internal_transfer in internal_transfers:
                self.item_exporter.export_item(self.internal_transfer_mapper.internal_transfer_to_dict(internal_transfer))

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
