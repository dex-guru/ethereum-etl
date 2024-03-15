from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from ethereumetl.mappers.parsed_log_mapper import EthParsedReceiptLogMapper
from ethereumetl.mappers.receipt_log_mapper import EthReceiptLogMapper
from ethereumetl.misc.info import PARSABLE_TRADE_EVENTS
from ethereumetl.service.eth_resolve_log_service import EthResolveLogService


class ParseLogsJob(BaseJob):
    def __init__(
        self,
        logs_iterable,
        item_exporter,
        max_workers,
        batch_size,
        batch_web3_provider,
        chain_id,
    ):
        self.logs_iterable = logs_iterable
        self.item_exporter = item_exporter
        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.batch_web3_provider = batch_web3_provider
        self.resolve_logs_service = EthResolveLogService(batch_web3_provider, chain_id)
        self.parsed_log_mapper = EthParsedReceiptLogMapper()
        self.receipt_log_mapper = EthReceiptLogMapper()

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(self.logs_iterable, self._parse_logs)

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()

    def _parse_logs(self, logs):
        for log in logs:
            log = self.receipt_log_mapper.dict_to_receipt_log(log)
            parsed_log = self.resolve_logs_service.parse_log(
                log, filter_for_events=PARSABLE_TRADE_EVENTS
            )
            if parsed_log:
                self.item_exporter.export_item(
                    self.parsed_log_mapper.parsed_receipt_log_to_dict(parsed_log)
                )
