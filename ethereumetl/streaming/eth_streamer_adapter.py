import logging
from datetime import datetime
from functools import cached_property

from blockchainetl.exporters import BaseItemExporter
from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
from ethereumetl.enumeration.entity_type import ALL_FOR_STREAMING, EntityType
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
    enrich_errors,
    enrich_geth_traces,
    enrich_internal_transfers,
    enrich_logs,
    enrich_token_balances,
    enrich_token_transfers,
    enrich_tokens,
    enrich_traces,
    enrich_transactions,
)
from ethereumetl.streaming.eth_item_id_calculator import EthItemIdCalculator
from ethereumetl.streaming.eth_item_timestamp_calculator import EthItemTimestampCalculator
from ethereumetl.thread_local_proxy import ThreadLocalProxy
from ethereumetl.web3_utils import build_web3

BLOCK = EntityType.BLOCK
TRANSACTION = EntityType.TRANSACTION
RECEIPT = EntityType.RECEIPT
LOG = EntityType.LOG
TOKEN_TRANSFER = EntityType.TOKEN_TRANSFER
TRACE = EntityType.TRACE
GETH_TRACE = EntityType.GETH_TRACE
CONTRACT = EntityType.CONTRACT
TOKEN = EntityType.TOKEN
INTERNAL_TRANSFER = EntityType.INTERNAL_TRANSFER
TOKEN_BALANCE = EntityType.TOKEN_BALANCE
ERROR = EntityType.ERROR


class EthStreamerAdapter:
    SORT_BY_FIELDS: dict[EntityType, tuple[str, ...]] = {
        BLOCK: ('number',),
        TRANSACTION: ('block_number', 'transaction_index'),
        LOG: ('block_number', 'log_index'),
        TOKEN_TRANSFER: ('block_number', 'log_index'),
        TOKEN_BALANCE: ('block_number', 'token_address', 'address'),
        TRACE: ('block_number', 'trace_index'),
        GETH_TRACE: ('transaction_hash', 'block_number'),
        CONTRACT: ('block_number',),
        TOKEN: ('block_number',),
        ERROR: ('block_number',),
        INTERNAL_TRANSFER: ('block_number', 'transaction_hash', 'id'),
        RECEIPT: ('block_number', 'transaction_index'),
    }

    ENRICH = {
        # entity_type: (enrich_with_entity_type, enrich_func(enrich_with_items, items))
        LOG: (BLOCK, enrich_logs),
        TOKEN_TRANSFER: (BLOCK, enrich_token_transfers),
        TOKEN_BALANCE: (BLOCK, enrich_token_balances),
        TRACE: (BLOCK, enrich_traces),
        CONTRACT: (BLOCK, enrich_contracts),
        TOKEN: (BLOCK, enrich_tokens),
        ERROR: (BLOCK, enrich_errors),
        GETH_TRACE: (TRANSACTION, enrich_geth_traces),
        INTERNAL_TRANSFER: (TRANSACTION, enrich_internal_transfers),
        # lambda because here the arg order is different
        TRANSACTION: (RECEIPT, lambda r, t: enrich_transactions(t, r)),
    }

    # both export and enrich dependencies
    # TODO: remove enrich dependencies from here, as they can be found in ENRICH
    DEPENDENCIES = {
        RECEIPT: (TRANSACTION,),
        TRANSACTION: (RECEIPT,),
        TRACE: (BLOCK,),
        LOG: (TRANSACTION, BLOCK),
        TOKEN_TRANSFER: (LOG, BLOCK),
        TOKEN_BALANCE: (TOKEN_TRANSFER, BLOCK),
        GETH_TRACE: (TRANSACTION, BLOCK),
        CONTRACT: (TRACE, BLOCK),
        TOKEN: (CONTRACT, BLOCK),
        INTERNAL_TRANSFER: (GETH_TRACE, BLOCK),
    }

    def __init__(
        self,
        batch_web3_provider,
        item_exporter: BaseItemExporter = ConsoleItemExporter(),
        batch_size=100,
        max_workers=5,
        entity_types=tuple(ALL_FOR_STREAMING),
    ):
        self.batch_web3_provider = batch_web3_provider
        self.item_exporter = item_exporter
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.entity_types = frozenset(entity_types)
        self.item_id_calculator = EthItemIdCalculator()
        self.item_timestamp_calculator = EthItemTimestampCalculator()

    def open(self):
        self.item_exporter.open()

    def get_current_block_number(self):
        w3 = build_web3(self.batch_web3_provider)
        return int(w3.eth.getBlock("latest").number)

    def export_batch(self, start_block: int, end_block: int) -> dict[EntityType, list[dict]]:
        exported: dict[EntityType, list[dict]] = {}

        export_plan = {
            entity_type: (export_func, export_types)
            for export_types, export_func in {
                (BLOCK, TRANSACTION): lambda: self._export_blocks_and_transactions(
                    start_block, end_block
                ),
                (RECEIPT, LOG, ERROR): lambda: self._export_receipts_and_logs(export(TRANSACTION)),
                (TOKEN_TRANSFER,): lambda: self._extract_token_transfers(export(LOG)),
                (TOKEN_BALANCE, ERROR): lambda: self._export_token_balances(
                    export(TOKEN_TRANSFER)
                ),
                (TRACE,): lambda: self._export_traces(start_block, end_block),
                (GETH_TRACE,): lambda: self._export_geth_traces(
                    [t["hash"] for t in export(TRANSACTION)]
                ),
                (CONTRACT,): lambda: self._export_contracts(export(TRACE)),
                (TOKEN,): lambda: self._extract_tokens(export(CONTRACT)),
                (INTERNAL_TRANSFER,): lambda: self._extract_internal_transfers(export(GETH_TRACE)),
            }.items()
            for entity_type in export_types
        }

        def export(entity_type):
            if entity_type in exported:
                return exported[entity_type]
            export_func, export_types = export_plan[entity_type]
            results = export_func()
            if len(export_types) == 1:
                results = (results,)
            for result, export_type in zip(results, export_types, strict=True):
                exported.setdefault(export_type, []).extend(result)
            return exported[entity_type]

        for entity_type in self.should_export:
            export(entity_type)

        return exported

    def export_all(self, start_block, end_block):
        exported = self.export_batch(start_block, end_block)

        all_items: list[dict] = []
        items_by_type: dict[EntityType, list[dict]] = {}
        for entity_type in self.entity_types:
            enriched_items = self.enrich(entity_type, exported.__getitem__)
            sorted_items = sort_by(enriched_items, self.SORT_BY_FIELDS[entity_type])
            items_by_type[entity_type] = sorted_items
            all_items.extend(sorted_items)

        self.log_batch_export_progress(items_by_type)

        self.calculate_item_ids(all_items)
        self.calculate_item_timestamps(all_items)

        self.item_exporter.export_items(all_items)

    @cached_property
    def should_export(self) -> set[EntityType]:
        should_export: set[EntityType] = set()
        stack = list(self.entity_types - {EntityType.ERROR})
        while stack:
            entity_type = stack.pop()
            if entity_type in should_export:
                continue
            should_export.add(entity_type)
            stack.extend(self.DEPENDENCIES.get(entity_type, ()))
        return should_export

    def enrich(self, entity_type, get_exported):
        try:
            enrich_with_type, enrich_func = self.ENRICH[entity_type]
        except KeyError:
            return get_exported(entity_type)
        else:
            enrich_with_items = get_exported(enrich_with_type)
            return enrich_func(enrich_with_items, get_exported(entity_type))

    def log_batch_export_progress(
        self,
        items_by_type: dict[EntityType, list[dict]],
    ):
        blocks = items_by_type.get(EntityType.BLOCK, ())
        if blocks:
            last_synced_block_datetime = datetime.fromtimestamp(blocks[-1]["timestamp"])
            logging.info(
                (
                    f'Exporting batch {len(blocks)} with {type(self.item_exporter).__name__}, '
                    f'blocks up to {blocks[-1]["number"]}:{last_synced_block_datetime}, got '
                    + ', '.join(
                        f'{len(items)} {entity_type.lower()}s'
                        for entity_type, items in sorted(items_by_type.items())
                    )
                ),
                extra={
                    'last_synced_block': blocks[-1]["number"],
                    'last_synced_block_datetime': last_synced_block_datetime,
                    'batch_size': len(blocks),
                    **{
                        f'{entity_type.lower()}s_per_batch': len(items)
                        for entity_type, items in sorted(items_by_type.items())
                    },
                },
            )
        else:
            logging.info(
                f"No blocks to export in this batch {len(blocks)} "
                f"with {type(self.item_exporter).__name__}"
            )

    def _export_blocks_and_transactions(
        self,
        start_block,
        end_block,
    ):
        blocks_and_transactions_item_exporter = InMemoryItemExporter(
            item_types=[EntityType.BLOCK, EntityType.TRANSACTION]
        )
        blocks_and_transactions_job = ExportBlocksJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            batch_web3_provider=self.batch_web3_provider,
            max_workers=self.max_workers,
            item_exporter=blocks_and_transactions_item_exporter,
            export_blocks=BLOCK in self.should_export,
            export_transactions=TRANSACTION in self.should_export,
        )
        blocks_and_transactions_job.run()
        blocks = blocks_and_transactions_item_exporter.get_items(EntityType.BLOCK)
        transactions = blocks_and_transactions_item_exporter.get_items(EntityType.TRANSACTION)
        return blocks, transactions

    def _export_receipts_and_logs(self, transactions) -> tuple[list[dict], list[dict], list[dict]]:
        exporter = InMemoryItemExporter(
            item_types=(EntityType.RECEIPT, EntityType.LOG, EntityType.ERROR)
        )
        job = ExportReceiptsJob(
            transactions=transactions,
            batch_size=self.batch_size,
            batch_web3_provider=self.batch_web3_provider,
            max_workers=self.max_workers,
            item_exporter=exporter,
            export_receipts=RECEIPT in self.should_export,
            export_logs=LOG in self.should_export,
        )
        job.run()
        receipts = exporter.get_items(EntityType.RECEIPT)
        logs = exporter.get_items(EntityType.LOG)
        errors = exporter.get_items(EntityType.ERROR)
        return receipts, logs, errors

    def _extract_token_transfers(self, logs):
        exporter = InMemoryItemExporter(item_types=[EntityType.TOKEN_TRANSFER])
        job = ExtractTokenTransfersJob(
            logs_iterable=logs,
            batch_size=self.batch_size,
            max_workers=self.max_workers,
            item_exporter=exporter,
        )
        job.run()
        token_transfers = exporter.get_items(EntityType.TOKEN_TRANSFER)
        return token_transfers

    def _export_token_balances(self, token_transfers):
        exporter = InMemoryItemExporter(item_types=[EntityType.TOKEN_BALANCE, EntityType.ERROR])
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
        exporter = InMemoryItemExporter(item_types=[EntityType.TRACE])
        job = ExportTracesJob(
            start_block=start_block,
            end_block=end_block,
            web3=ThreadLocalProxy(lambda: build_web3(self.batch_web3_provider)),
            max_workers=self.max_workers,
            item_exporter=exporter,
        )
        job.run()
        traces = exporter.get_items(EntityType.TRACE)
        return traces

    def _export_geth_traces(self, transaction_hashes):
        exporter = InMemoryItemExporter(item_types=[EntityType.GETH_TRACE])
        job = ExportGethTracesJob(
            transaction_hashes=transaction_hashes,
            batch_size=self.batch_size,
            batch_web3_provider=self.batch_web3_provider,
            max_workers=self.max_workers,
            item_exporter=exporter,
        )
        job.run()
        traces = exporter.get_items(EntityType.GETH_TRACE)
        return traces

    def _export_contracts(self, traces):
        exporter = InMemoryItemExporter(item_types=[EntityType.CONTRACT])
        job = ExtractContractsJob(
            traces_iterable=traces,
            batch_size=self.batch_size,
            max_workers=self.max_workers,
            item_exporter=exporter,
        )
        job.run()
        contracts = exporter.get_items(EntityType.CONTRACT)
        return contracts

    def _extract_tokens(self, contracts):
        exporter = InMemoryItemExporter(item_types=[EntityType.TOKEN])
        job = ExtractTokensJob(
            contracts_iterable=contracts,
            web3=ThreadLocalProxy(lambda: build_web3(self.batch_web3_provider)),
            max_workers=self.max_workers,
            item_exporter=exporter,
        )
        job.run()
        tokens = exporter.get_items(EntityType.TOKEN)
        return tokens

    def _extract_internal_transfers(self, geth_traces):
        exporter = InMemoryItemExporter(item_types=[EntityType.INTERNAL_TRANSFER])
        job = ExtractInternalTransfersJob(
            geth_traces_iterable=geth_traces,
            batch_size=self.batch_size,
            max_workers=self.max_workers,
            item_exporter=exporter,
        )
        job.run()
        internal_transfers = exporter.get_items(EntityType.INTERNAL_TRANSFER)
        return internal_transfers

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
