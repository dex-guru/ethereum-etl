from collections import defaultdict
from collections.abc import Collection

from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.domain.receipt_log import ParsedReceiptLog
from ethereumetl.domain.token_transfer import EthTokenTransfer
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from ethereumetl.mappers.dex_pool_mapper import EthDexPoolMapper
from ethereumetl.mappers.dex_trade_mapper import EthDexTradeMapper
from ethereumetl.mappers.parsed_log_mapper import EthParsedReceiptLogMapper
from ethereumetl.mappers.token_mapper import EthTokenMapper
from ethereumetl.mappers.token_transfer_mapper import EthTokenTransferMapper
from ethereumetl.misc.info import PARSABLE_TRADE_EVENTS
from ethereumetl.service.eth_resolve_log_service import EthResolveLogService


class ExportDexTradesJob(BaseJob):
    def __init__(
        self,
        logs_iterable: Collection[dict],
        tokens_iterable: Collection[dict],
        dex_pools_iterable: Collection[dict],
        token_transfers_iterable: Collection[dict],
        item_exporter,
        batch_size: int,
        batch_web3_provider,
        max_workers: int,
        chain_id: int,
    ):
        self.item_exporter = item_exporter
        self.logs_iterable = logs_iterable

        self.batch_work_executor = BatchWorkExecutor(
            batch_size, max_workers, job_name='Export Dex Trades Job'
        )
        self.log_resolve_service = EthResolveLogService(batch_web3_provider, chain_id)

        self.dex_trade_mapper = EthDexTradeMapper()
        self.token_mapper = EthTokenMapper()
        self.token_transfer_mapper = EthTokenTransferMapper()
        self.dex_pool_mapper = EthDexPoolMapper()
        self.receipt_log_mapper = EthParsedReceiptLogMapper()

        self.transfers_by_transaction_hash: dict[str, list[EthTokenTransfer]] = defaultdict(list)
        for transfer in token_transfers_iterable:
            self.transfers_by_transaction_hash[transfer['transaction_hash']].append(
                self.token_transfer_mapper.dict_to_token_transfer(transfer)
            )
        self.dex_pools_by_address: dict[str, EthDexPool] = {
            pool['address']: self.dex_pool_mapper.dict_to_pool(pool) for pool in dex_pools_iterable
        }
        self.tokens_by_address = {
            token['address']: self.token_mapper.dict_to_token(token) for token in tokens_iterable
        }

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        logs = (
            self.receipt_log_mapper.dict_to_parsed_receipt_log(log)
            for log in self.logs_iterable
            if log['event_name'] in PARSABLE_TRADE_EVENTS
        )
        self.batch_work_executor.execute(logs, self._export_trades, len(self.logs_iterable))

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()

    def _export_trades(self, parsed_logs: Collection[ParsedReceiptLog]):
        for log in parsed_logs:
            dex_pool = self._get_dex_pool_for_trade(log)
            if not dex_pool:
                continue
            tokens_for_pool = [
                self.tokens_by_address[token_address] for token_address in dex_pool.token_addresses
            ]
            transfers_for_transaction = self.transfers_by_transaction_hash.get(
                log.transaction_hash, []
            )
            resolved_log = self.log_resolve_service.resolve_log(
                log, dex_pool, tokens_for_pool, transfers_for_transaction
            )
            if not resolved_log:
                continue
            trade = self.dex_trade_mapper.dict_from_dex_trade(resolved_log)
            self.item_exporter.export_item(trade)

    def _get_dex_pool_for_trade(self, parsed_log: ParsedReceiptLog) -> EthDexPool | None:
        dex_pool = self.dex_pools_by_address.get(parsed_log.address.lower())
        if not dex_pool:
            # Try to get pool for balancer
            dex_pool = self.dex_pools_by_address.get(
                f"0x{parsed_log.parsed_event.get('poolId', b'').hex().lower()[:40]}"
            )
        return dex_pool
