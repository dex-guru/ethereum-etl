import logging
from datetime import datetime

from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
from ethereumetl.enumeration import entity_type
from ethereumetl.enumeration.entity_type import EntityType
from ethereumetl.jobs.export_blocks_job import ExportBlocksJob
from ethereumetl.jobs.export_geth_traces_job import ExportGethTracesJob
from ethereumetl.jobs.export_receipts_job import ExportReceiptsJob
from ethereumetl.jobs.export_token_balances_job import ExportTokenBalancesJob
from ethereumetl.jobs.export_traces_job import ExportTracesJob
from ethereumetl.jobs.extract_contracts_job import ExtractContractsJob
from ethereumetl.jobs.extract_internal_transfers_job import ExtractInternalTransfersJob
from ethereumetl.jobs.extract_token_transfers_job import ExtractTokenTransfersJob
from ethereumetl.jobs.extract_tokens_job import ExtractTokensJob
from ethereumetl.streaming.enrich import (
    enrich_contracts,
    enrich_logs,
    enrich_token_balances,
    enrich_token_transfers,
    enrich_tokens,
    enrich_traces,
    enrich_transactions,
    enrich_errors,
    enrich_geth_traces,
)
from ethereumetl.streaming.eth_item_id_calculator import EthItemIdCalculator
from ethereumetl.streaming.eth_item_timestamp_calculator import EthItemTimestampCalculator
from ethereumetl.thread_local_proxy import ThreadLocalProxy
from ethereumetl.web3_utils import build_web3


class EthStreamerAdapter:
    def __init__(
        self,
        batch_web3_provider,
        item_exporter=ConsoleItemExporter(),
        batch_size=100,
        max_workers=5,
        entity_types=tuple(entity_type.ALL_FOR_STREAMING),
    ):
        self.batch_web3_provider = batch_web3_provider
        self.item_exporter = item_exporter
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.entity_types = entity_types
        self.item_id_calculator = EthItemIdCalculator()
        self.item_timestamp_calculator = EthItemTimestampCalculator()

    def open(self):
        self.item_exporter.open()

    def get_current_block_number(self):
        w3 = build_web3(self.batch_web3_provider)
        return int(w3.eth.getBlock("latest").number)

    def export_all(self, start_block, end_block):
        errors = []

        # Export blocks and transactions
        blocks, transactions = [], []
        if self._should_export(EntityType.BLOCK) or self._should_export(EntityType.TRANSACTION):
            blocks, transactions = self._export_blocks_and_transactions(start_block, end_block)

        # Export receipts and logs
        receipts, logs = [], []
        if self._should_export(EntityType.RECEIPT) or self._should_export(EntityType.LOG):
            receipts, logs = self._export_receipts_and_logs(transactions)

        # Extract token transfers
        token_transfers = []
        if self._should_export(EntityType.TOKEN_TRANSFER):
            token_transfers = self._extract_token_transfers(logs)

        token_balances = []
        if self._should_export(EntityType.TOKEN_BALANCE):
            token_balances, token_balance_errors = self._export_token_balances(token_transfers)
            errors.extend(token_balance_errors)

        # Export traces
        traces = []
        if self._should_export(EntityType.TRACE):
            traces = self._export_traces(start_block, end_block)

        enriched_geth_traces = []
        if self._should_export(EntityType.GETH_TRACE):
            geth_traces = self._export_geth_traces([t["hash"] for t in transactions])
            enriched_geth_traces = enrich_geth_traces(transactions, geth_traces)

        # Export contracts
        contracts = []
        if self._should_export(EntityType.CONTRACT):
            contracts = self._export_contracts(traces)

        # Export tokens
        tokens = []
        if self._should_export(EntityType.TOKEN):
            tokens = self._extract_tokens(contracts)

        internal_transfers = []
        if self._should_export(EntityType.INTERNAL_TRANSFER):
            internal_transfers = self._extract_internal_transfers(enriched_geth_traces)

        enriched_blocks = blocks if EntityType.BLOCK in self.entity_types else []
        enriched_transactions = (
            enrich_transactions(transactions, receipts)
            if EntityType.TRANSACTION in self.entity_types
            else []
        )
        enriched_logs = enrich_logs(blocks, logs) if EntityType.LOG in self.entity_types else []
        enriched_token_transfers = (
            enrich_token_transfers(blocks, token_transfers)
            if EntityType.TOKEN_TRANSFER in self.entity_types
            else []
        )
        enriched_token_balances = (
            enrich_token_balances(blocks, token_balances)
            if EntityType.TOKEN_BALANCE in self.entity_types
            else []
        )
        enriched_traces = (
            enrich_traces(blocks, traces) if EntityType.TRACE in self.entity_types else []
        )
        enriched_contracts = (
            enrich_contracts(blocks, contracts) if EntityType.CONTRACT in self.entity_types else []
        )
        enriched_tokens = (
            enrich_tokens(blocks, tokens) if EntityType.TOKEN in self.entity_types else []
        )
        enriched_errors = (
            enrich_errors(blocks, errors) if EntityType.ERROR in self.entity_types else []
        )

        self.log_batch_export_progress(
            blocks=enriched_blocks,
            contracts=enriched_contracts,
            logs=enriched_logs,
            token_transfers=enriched_token_transfers,
            tokens=enriched_tokens,
            traces=enriched_traces,
            transactions=enriched_transactions,
            token_balances=token_balances,
            errors=enriched_errors,
        )

        all_items = (
            sort_by(enriched_blocks, ('number',))
            + sort_by(enriched_transactions, ('block_number', 'transaction_index'))
            + sort_by(enriched_logs, ('block_number', 'log_index'))
            + sort_by(enriched_token_transfers, ('block_number', 'log_index'))
            + sort_by(enriched_token_balances, ('block_number', 'token_address', 'address'))
            + sort_by(enriched_traces, ('block_number', 'trace_index'))
            + sort_by(enriched_geth_traces, ('transaction_hash', 'block_number'))
            + sort_by(enriched_contracts, ('block_number',))
            + sort_by(enriched_tokens, ('block_number',))
            + sort_by(enriched_errors, ('block_number',))
            + sort_by(internal_transfers, ('block_number', 'transaction_hash', 'id'))
        )

        self.calculate_item_ids(all_items)
        self.calculate_item_timestamps(all_items)

        self.item_exporter.export_items(all_items)

    def log_batch_export_progress(
        self,
        *,
        blocks,
        contracts,
        logs,
        token_transfers,
        token_balances,
        tokens,
        traces,
        transactions,
        errors,
    ):
        if blocks:
            last_synced_block_datetime = datetime.fromtimestamp(blocks[-1]["timestamp"])
            logging.info(
                (
                    f'Exporting batch {len(blocks)} with {type(self.item_exporter).__name__}, '
                    f'blocks up to {blocks[-1]["number"]}:{last_synced_block_datetime}, got'
                    f' {len(transactions)} transactions,'
                    f' {len(logs)} logs,'
                    f' {len(token_transfers)} token transfers,'
                    f' {len(token_balances)} token balances,'
                    f' {len(traces)} traces,'
                    f' {len(contracts)} contracts,'
                    f' {len(tokens)} tokens'
                    f' {len(errors)} tokens'
                ),
                extra={
                    'last_synced_block': blocks[-1]["number"],
                    'last_synced_block_datetime': last_synced_block_datetime,
                    'batch_size': len(blocks),
                    'transactions_per_batch': len(transactions),
                    'logs_per_batch': len(logs),
                    'token_transfers_per_batch': len(token_transfers),
                    'token_balances_per_batch': len(token_balances),
                    'traces_per_batch': len(traces),
                    'contracts_per_batch': len(contracts),
                    'tokens_per_batch': len(tokens),
                    'errors_per_batch': len(errors),
                },
            )
        else:
            logging.info(
                f"No blocks to export in this batch {len(blocks)} "
                f"with {type(self.item_exporter).__name__}"
            )

    def _export_blocks_and_transactions(self, start_block, end_block):
        blocks_and_transactions_item_exporter = InMemoryItemExporter(
            item_types=['block', 'transaction']
        )
        blocks_and_transactions_job = ExportBlocksJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            batch_web3_provider=self.batch_web3_provider,
            max_workers=self.max_workers,
            item_exporter=blocks_and_transactions_item_exporter,
            export_blocks=self._should_export(EntityType.BLOCK),
            export_transactions=self._should_export(EntityType.TRANSACTION),
        )
        blocks_and_transactions_job.run()
        blocks = blocks_and_transactions_item_exporter.get_items('block')
        transactions = blocks_and_transactions_item_exporter.get_items('transaction')
        return blocks, transactions

    def _export_receipts_and_logs(self, transactions):
        exporter = InMemoryItemExporter(item_types=['receipt', 'log'])
        job = ExportReceiptsJob(
            transaction_hashes_iterable=(transaction['hash'] for transaction in transactions),
            batch_size=self.batch_size,
            batch_web3_provider=self.batch_web3_provider,
            max_workers=self.max_workers,
            item_exporter=exporter,
            export_receipts=self._should_export(EntityType.RECEIPT),
            export_logs=self._should_export(EntityType.LOG),
        )
        job.run()
        receipts = exporter.get_items('receipt')
        logs = exporter.get_items('log')
        return receipts, logs

    def _extract_token_transfers(self, logs):
        exporter = InMemoryItemExporter(item_types=['token_transfer'])
        job = ExtractTokenTransfersJob(
            logs_iterable=logs,
            batch_size=self.batch_size,
            max_workers=self.max_workers,
            item_exporter=exporter,
        )
        job.run()
        token_transfers = exporter.get_items('token_transfer')
        return token_transfers

    def _export_token_balances(self, token_transfers):
        exporter = InMemoryItemExporter(item_types=['token_balance', 'error'])
        job = ExportTokenBalancesJob(
            token_transfer_items_iterable=token_transfers,
            batch_size=self.batch_size,
            max_workers=self.max_workers,
            item_exporter=exporter,
            batch_web3_provider=self.batch_web3_provider,
        )
        job.run()
        token_balances = exporter.get_items(EntityType.TOKEN_BALANCE)
        errors = exporter.get_items(EntityType.ERROR)
        return token_balances, errors

    def _export_traces(self, start_block, end_block):
        exporter = InMemoryItemExporter(item_types=['trace'])
        job = ExportTracesJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            web3=ThreadLocalProxy(lambda: build_web3(self.batch_web3_provider)),
            max_workers=self.max_workers,
            item_exporter=exporter,
        )
        job.run()
        traces = exporter.get_items('trace')
        return traces

    def _export_geth_traces(self, transaction_hashes):
        exporter = InMemoryItemExporter(item_types=['geth_trace'])
        job = ExportGethTracesJob(
            transaction_hashes=transaction_hashes,
            batch_size=self.batch_size,
            batch_web3_provider=self.batch_web3_provider,
            max_workers=self.max_workers,
            item_exporter=exporter,
        )
        job.run()
        traces = exporter.get_items('geth_trace')
        return traces

    def _export_contracts(self, traces):
        exporter = InMemoryItemExporter(item_types=['contract'])
        job = ExtractContractsJob(
            traces_iterable=traces,
            batch_size=self.batch_size,
            max_workers=self.max_workers,
            item_exporter=exporter,
        )
        job.run()
        contracts = exporter.get_items('contract')
        return contracts

    def _extract_tokens(self, contracts):
        exporter = InMemoryItemExporter(item_types=['token'])
        job = ExtractTokensJob(
            contracts_iterable=contracts,
            web3=ThreadLocalProxy(lambda: build_web3(self.batch_web3_provider)),
            max_workers=self.max_workers,
            item_exporter=exporter,
        )
        job.run()
        tokens = exporter.get_items('token')
        return tokens

    def _extract_internal_transfers(self, geth_traces):
        exporter = InMemoryItemExporter(item_types=['internal_transfer'])
        job = ExtractInternalTransfersJob(
            geth_traces_iterable=geth_traces,
            batch_size=self.batch_size,
            max_workers=self.max_workers,
            item_exporter=exporter,
        )
        job.run()
        internal_transfers = exporter.get_items('internal_transfer')
        return internal_transfers

    def _should_export(self, entity_type):
        if entity_type == EntityType.BLOCK:
            return True

        if entity_type == EntityType.TRANSACTION:
            return EntityType.TRANSACTION in self.entity_types or self._should_export(
                EntityType.LOG
            )

        if entity_type == EntityType.RECEIPT:
            return EntityType.TRANSACTION in self.entity_types or self._should_export(
                EntityType.TOKEN_TRANSFER
            )

        if entity_type == EntityType.LOG:
            return EntityType.LOG in self.entity_types or self._should_export(
                EntityType.TOKEN_TRANSFER
            )

        if entity_type == EntityType.TOKEN_TRANSFER:
            return EntityType.TOKEN_TRANSFER in self.entity_types or self._should_export(
                EntityType.TOKEN_BALANCE
            )

        if entity_type == EntityType.TRACE:
            return EntityType.TRACE in self.entity_types or self._should_export(
                EntityType.CONTRACT
            )

        if entity_type == EntityType.GETH_TRACE:
            return EntityType.GETH_TRACE in self.entity_types or self._should_export(
                EntityType.INTERNAL_TRANSFER
            )

        if entity_type == EntityType.CONTRACT:
            return EntityType.CONTRACT in self.entity_types or self._should_export(
                EntityType.TOKEN
            )

        if entity_type == EntityType.TOKEN:
            return EntityType.TOKEN in self.entity_types

        if entity_type == EntityType.TOKEN_BALANCE:
            return EntityType.TOKEN_BALANCE in self.entity_types

        if entity_type == EntityType.ERROR:
            return EntityType.ERROR in self.entity_types

        if entity_type == EntityType.INTERNAL_TRANSFER:
            return EntityType.INTERNAL_TRANSFER in self.entity_types

        raise ValueError('Unexpected entity type ' + entity_type)

    def calculate_item_ids(self, items):
        for item in items:
            item['item_id'] = self.item_id_calculator.calculate(item)

    def calculate_item_timestamps(self, items):
        for item in items:
            item['item_timestamp'] = self.item_timestamp_calculator.calculate(item)

    def close(self):
        self.item_exporter.close()


def sort_by(arr, fields):
    return sorted(arr, key=lambda item: tuple(item.get(f) for f in fields))
