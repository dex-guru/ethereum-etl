import logging
from collections.abc import Collection
from copy import deepcopy
from datetime import datetime
from functools import cached_property
from typing import Any

from elasticsearch import Elasticsearch

from blockchainetl.exporters import BaseItemExporter
from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
from blockchainetl.jobs.importers.price_importers.interface import PriceImporterInterface
from ethereumetl.enumeration.entity_type import ALL_FOR_STREAMING, EntityType
from ethereumetl.jobs.enrich_dex_trades_job import EnrichDexTradeJob
from ethereumetl.jobs.export_blocks_job import ExportBlocksJob
from ethereumetl.jobs.export_dex_pools_job import ExportPoolsJob
from ethereumetl.jobs.export_dex_trades import ExportDexTradesJob
from ethereumetl.jobs.export_geth_traces_job import ExportGethTracesJob
from ethereumetl.jobs.export_native_balances_job import ExportNativeBalancesJob
from ethereumetl.jobs.export_prices_for_tokens_job import ExportPricesForTokensJob
from ethereumetl.jobs.export_receipts_job import ExportReceiptsJob
from ethereumetl.jobs.export_token_balances_job import ExportTokenBalancesJob
from ethereumetl.jobs.export_tokens_job import ExportTokensJob
from ethereumetl.jobs.export_traces_job import ExportTracesJob
from ethereumetl.jobs.extract_contracts_job import ExtractContractsJob
from ethereumetl.jobs.extract_events_job import PrepareForEventsJob
from ethereumetl.jobs.extract_internal_transfers_job import ExtractInternalTransfersJob
from ethereumetl.jobs.extract_internal_transfers_priced import ExtractInternalTransfersPricedJob
from ethereumetl.jobs.extract_token_transfers_job import ExtractTokenTransfersJob
from ethereumetl.jobs.extract_token_transfers_priced import ExtractTokenTransfersPricedJob
from ethereumetl.jobs.extract_tokens_job import ExtractTokensJob
from ethereumetl.jobs.parse_logs_job import ParseLogsJob
from ethereumetl.misc.info import get_chain_config
from ethereumetl.streaming.enrich import (
    enrich_dex_trades,
    enrich_errors,
    enrich_geth_traces,
    enrich_internal_transfers,
    enrich_logs,
    enrich_native_balances,
    enrich_parsed_logs,
    enrich_token_balances,
    enrich_token_transfers,
    enrich_token_transfers_priced,
    enrich_traces,
    enrich_transactions,
    enrich_transfers_for_trades,
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
NATIVE_BALANCE = EntityType.NATIVE_BALANCE
TOKEN_TRANSFER_PRICED = EntityType.TOKEN_TRANSFER_PRICED
INTERNAL_TRANSFER_PRICED = EntityType.INTERNAL_TRANSFER_PRICED
PRE_EVENT = EntityType.PRE_EVENT
DEX_POOL = EntityType.DEX_POOL
DEX_TRADE = EntityType.DEX_TRADE
PARSED_LOG = EntityType.PARSED_LOG
ENRICHED_DEX_TRADE = EntityType.ENRICHED_DEX_TRADE
ENRICHED_TRANSFER = EntityType.ENRICHED_TRANSFER


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
        TOKEN: ('address',),
        ERROR: ('block_number',),
        INTERNAL_TRANSFER: ('block_number', 'transaction_hash', 'id'),
        RECEIPT: ('block_number', 'transaction_index'),
        NATIVE_BALANCE: ('address', 'block_number'),
        TOKEN_TRANSFER_PRICED: ('block_number', 'log_index'),
        INTERNAL_TRANSFER_PRICED: ('block_number',),
        PRE_EVENT: ('block_number', 'log_index'),
        PARSED_LOG: ('block_number', 'log_index'),
        DEX_POOL: ('address',),
        DEX_TRADE: ('block_number', 'log_index'),
        ENRICHED_DEX_TRADE: ('block_number', 'transaction_hash', 'log_index'),
        ENRICHED_TRANSFER: ('block_number', 'transaction_hash', 'log_index'),
    }

    ENRICH = {
        # entity_type: (enrich_with_entity_type, enrich_func(enrich_with_items, items))
        LOG: (BLOCK, enrich_logs),
        TOKEN_TRANSFER: (BLOCK, enrich_token_transfers),
        TOKEN_BALANCE: (BLOCK, enrich_token_balances),
        TRACE: (BLOCK, enrich_traces),
        ERROR: (BLOCK, enrich_errors),
        GETH_TRACE: (TRANSACTION, enrich_geth_traces),
        INTERNAL_TRANSFER: (TRANSACTION, enrich_internal_transfers),
        NATIVE_BALANCE: (BLOCK, enrich_native_balances),
        # lambda because here the arg order is different
        TRANSACTION: (RECEIPT, lambda r, t: enrich_transactions(t, r)),
        TOKEN_TRANSFER_PRICED: (BLOCK, enrich_token_transfers_priced),
        ENRICHED_DEX_TRADE: (BLOCK, enrich_dex_trades),
        ENRICHED_TRANSFER: (TRANSACTION, enrich_transfers_for_trades),
        PARSED_LOG: (BLOCK, enrich_parsed_logs),
    }

    __EXPORT_DEPENDENCIES = {
        RECEIPT: (TRANSACTION,),
        TRANSACTION: (RECEIPT,),
        TRACE: (BLOCK,),
        LOG: (TRANSACTION,),
        TOKEN_TRANSFER: (LOG,),
        TOKEN_BALANCE: (TOKEN_TRANSFER,),
        GETH_TRACE: (TRANSACTION,),
        CONTRACT: (GETH_TRACE,),
        TOKEN: (TOKEN_TRANSFER,),
        INTERNAL_TRANSFER: (GETH_TRACE,),
        NATIVE_BALANCE: (TRANSACTION, INTERNAL_TRANSFER),
        TOKEN_TRANSFER_PRICED: (TOKEN_TRANSFER, TOKEN),
        PRE_EVENT: (BLOCK, LOG, TRANSACTION, TOKEN_TRANSFER, RECEIPT),
        PARSED_LOG: (LOG,),
        DEX_POOL: (PARSED_LOG,),
        DEX_TRADE: (PARSED_LOG, TOKEN, DEX_POOL, TOKEN_TRANSFER),
        ENRICHED_DEX_TRADE: (
            DEX_POOL,
            PARSED_LOG,
            TOKEN,
            TOKEN_TRANSFER,
            INTERNAL_TRANSFER,
            TRANSACTION,
        ),
        ENRICHED_TRANSFER: (ENRICHED_DEX_TRADE,),
    }

    def __init__(
        self,
        batch_web3_provider,
        item_exporter: BaseItemExporter = ConsoleItemExporter(),
        batch_size=100,
        max_workers=5,
        entity_types=tuple(ALL_FOR_STREAMING),
        chain_id=None,
        elastic_client: Elasticsearch | None = None,
        price_importer: PriceImporterInterface | None = None,
    ):
        self.EXPORT_DEPENDENCIES = deepcopy(self.__EXPORT_DEPENDENCIES)
        self.batch_web3_provider = batch_web3_provider
        self.w3 = build_web3(self.batch_web3_provider)
        self.item_exporter = item_exporter
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.entity_types = frozenset(entity_types)
        self.item_id_calculator = EthItemIdCalculator()
        self.item_timestamp_calculator = EthItemTimestampCalculator()
        self.chain_id = chain_id
        self.elastic_client = elastic_client
        self.price_importer = price_importer
        if CONTRACT in self.entity_types:
            self.EXPORT_DEPENDENCIES[TOKEN] = (CONTRACT,)
        self.chain_config: dict[str, Any] = {}

    def open(self):
        self.chain_config = get_chain_config(self.chain_id)
        self.item_exporter.open()

    def get_current_block_number(self):
        return int(self.w3.eth.getBlock("latest").number)

    def export_batch(self, start_block: int, end_block: int) -> dict[EntityType, list[dict]]:
        exported: dict[EntityType, list[dict]] = {}

        export_plan = {
            entity_type: (export_func, export_types)
            for export_types, export_func in {
                (BLOCK, TRANSACTION): lambda: self.export_blocks_and_transactions(
                    start_block, end_block
                ),
                (RECEIPT, LOG, ERROR): lambda: self.export_receipts_and_logs(export(TRANSACTION)),
                (TOKEN_TRANSFER,): lambda: self.extract_token_transfers(export(LOG)),
                (TOKEN_BALANCE, ERROR): lambda: self.export_token_balances(export(TOKEN_TRANSFER)),
                (TRACE,): lambda: self.export_traces(start_block, end_block),
                (GETH_TRACE,): lambda: self.export_geth_traces(
                    [t["hash"] for t in export(TRANSACTION)]
                ),
                (CONTRACT,): lambda: self.export_contracts(export(GETH_TRACE)),
                (TOKEN,): (
                    (
                        lambda: self.export_tokens(
                            {t["token_address"] for t in export(TOKEN_TRANSFER)}
                        )
                    )
                    if CONTRACT not in self.should_export
                    else lambda: self.extract_tokens(export(CONTRACT))
                ),
                (INTERNAL_TRANSFER,): lambda: self.extract_internal_transfers(export(GETH_TRACE)),
                (NATIVE_BALANCE,): lambda: self.export_native_balances(
                    transactions=export(TRANSACTION), internal_transfers=export(INTERNAL_TRANSFER)
                ),
                (TOKEN_TRANSFER_PRICED,): lambda: self.extract_token_transfers_priced(
                    export(TOKEN_TRANSFER), export(TOKEN)
                ),
                (INTERNAL_TRANSFER_PRICED,): lambda: self.extract_internal_transfers_priced(
                    export(INTERNAL_TRANSFER), export(TRANSACTION)
                ),
                (PRE_EVENT,): lambda: self.prepare_events(
                    export(BLOCK),
                    export(LOG),
                    export(TOKEN_TRANSFER),
                    export(TRANSACTION),
                    export(RECEIPT),
                ),
                (DEX_POOL,): lambda: self.export_dex_pools(export(PARSED_LOG)),
                (DEX_TRADE,): lambda: self.export_dex_trades(
                    export(PARSED_LOG), export(TOKEN), export(DEX_POOL), export(TOKEN_TRANSFER)
                ),
                (PARSED_LOG,): lambda: self.parse_logs(export(LOG)),
                (ENRICHED_DEX_TRADE, ENRICHED_TRANSFER): lambda: self.export_enriched_dex_trades(
                    export(DEX_TRADE),
                    export(DEX_POOL),
                    self.import_base_token_prices([t['address'] for t in export(TOKEN)]),
                    export(TOKEN),
                    export(TOKEN_TRANSFER),
                    export(INTERNAL_TRANSFER),
                    export(TRANSACTION),
                ),
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
            stack.extend(self.EXPORT_DEPENDENCIES.get(entity_type, ()))
            if entity_type in self.ENRICH:
                enrich_with_type, _enrich_fn = self.ENRICH[entity_type]
                stack.append(enrich_with_type)
        return should_export

    def enrich(self, entity_type, get_exported):
        enrich_with_type, enrich_func = self.ENRICH.get(entity_type, (None, None))
        if enrich_with_type is None or enrich_func is None:
            return get_exported(entity_type)
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

    def export_blocks_and_transactions(
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

    def export_receipts_and_logs(self, transactions) -> tuple[list[dict], list[dict], list[dict]]:
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

    def extract_token_transfers(self, logs):
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

    def extract_token_transfers_priced(self, token_transfers, tokens):
        assert (
            self.chain_id is not None
        ), 'Cannot extract token transfers priced. Chain id is required'
        assert (
            self.elastic_client is not None
        ), 'Cannot extract token transfers priced. Elastic client is required'
        exporter = InMemoryItemExporter(item_types=[EntityType.TOKEN_TRANSFER_PRICED])
        job = ExtractTokenTransfersPricedJob(
            token_transfers=token_transfers,
            tokens=tokens,
            batch_size=self.batch_size,
            max_workers=self.max_workers,
            item_exporter=exporter,
            chain_id=self.chain_id,
            elastic_client=self.elastic_client,
        )
        job.run()
        token_transfers_priced = exporter.get_items(EntityType.TOKEN_TRANSFER_PRICED)
        return token_transfers_priced

    def extract_internal_transfers_priced(self, internal_transfers, transactions):
        assert (
            self.chain_id is not None
        ), 'Cannot extract token transfers priced. Chain id is required'
        assert (
            self.elastic_client is not None
        ), 'Cannot extract token transfers priced. Elastic client is required'
        enriched_internal_transfers = enrich_internal_transfers(transactions, internal_transfers)
        exporter = InMemoryItemExporter(item_types=[EntityType.INTERNAL_TRANSFER_PRICED])
        job = ExtractInternalTransfersPricedJob(
            internal_transfers=enriched_internal_transfers,
            batch_size=self.batch_size,
            max_workers=self.max_workers,
            item_exporter=exporter,
            chain_id=self.chain_id,
            elastic_client=self.elastic_client,
            native_token=self.chain_config['native_token'],
        )
        job.run()
        internal_transfers_priced = exporter.get_items(EntityType.INTERNAL_TRANSFER_PRICED)
        return internal_transfers_priced

    def export_token_balances(self, token_transfers):
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

    def export_traces(self, start_block, end_block):
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

    def export_geth_traces(self, transaction_hashes):
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

    def export_contracts(self, geth_traces):
        exporter = InMemoryItemExporter(item_types=[EntityType.CONTRACT])
        job = ExtractContractsJob(
            traces_iterable=geth_traces,
            batch_size=self.batch_size,
            max_workers=self.max_workers,
            item_exporter=exporter,
        )
        job.run()
        contracts = exporter.get_items(EntityType.CONTRACT)
        return contracts

    def export_tokens(self, addresses):
        if not addresses:
            return []
        exporter = InMemoryItemExporter(item_types=[EntityType.TOKEN])
        job = ExportTokensJob(
            token_addresses_iterable=addresses,
            web3=ThreadLocalProxy(lambda: build_web3(self.batch_web3_provider)),
            item_exporter=exporter,
            max_workers=self.max_workers,
        )
        job.run()
        tokens = exporter.get_items(EntityType.TOKEN)
        return tokens

    def extract_tokens(self, contracts):
        if not contracts:
            return []
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

    def extract_internal_transfers(self, geth_traces):
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

    def export_native_balances(
        self,
        *,
        internal_transfers: Collection[dict],
        transactions: Collection[dict],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        exporter = InMemoryItemExporter(item_types=(EntityType.NATIVE_BALANCE,))
        job = ExportNativeBalancesJob(
            batch_web3_provider=self.batch_web3_provider,
            batch_size=self.batch_size,
            max_workers=self.max_workers,
            item_exporter=exporter,
            internal_transfers=internal_transfers,
            transactions=transactions,
        )
        job.run()
        native_balances = exporter.get_items(EntityType.NATIVE_BALANCE)
        return native_balances

    def prepare_events(self, blocks, logs, token_transfers, transactions, receipts):
        exporter = InMemoryItemExporter(item_types=[EntityType.PRE_EVENT])
        enriched_transfers = enrich_token_transfers(blocks, token_transfers)
        enriched_transactions = enrich_transactions(transactions, receipts)
        job = PrepareForEventsJob(
            logs=logs,
            token_transfers=enriched_transfers,
            transactions=enriched_transactions,
            batch_size=self.batch_size,
            batch_web3_provider=self.batch_web3_provider,
            max_workers=self.max_workers,
            item_exporter=exporter,
        )
        job.run()
        events = exporter.get_items(EntityType.PRE_EVENT)
        return events

    def export_dex_pools(self, parsed_logs):
        exporter = InMemoryItemExporter(item_types=[DEX_POOL])
        job = ExportPoolsJob(
            parsed_logs_iterable=parsed_logs,
            chain_id=self.chain_id,
            batch_web3_provider=ThreadLocalProxy(lambda: build_web3(self.batch_web3_provider)),
            item_exporter=exporter,
            max_workers=self.max_workers,
            batch_size=self.batch_size,
        )
        job.run()
        pools = exporter.get_items(DEX_POOL)
        return pools

    def export_dex_trades(
        self,
        parsed_logs: list,
        tokens: list,
        dex_pools: list,
        token_transfers: list,
    ):
        exporter = InMemoryItemExporter(item_types=[DEX_TRADE])
        job = ExportDexTradesJob(
            logs_iterable=parsed_logs,
            tokens_iterable=tokens,
            dex_pools_iterable=dex_pools,
            token_transfers_iterable=token_transfers,
            batch_web3_provider=ThreadLocalProxy(lambda: build_web3(self.batch_web3_provider)),
            batch_size=self.batch_size,
            item_exporter=exporter,
            max_workers=self.max_workers,
            chain_id=self.chain_id,
        )
        job.run()
        trades = exporter.get_items(DEX_TRADE)
        return trades

    def parse_logs(self, logs: Collection[dict]) -> list[dict]:
        exporter = InMemoryItemExporter(item_types=[PARSED_LOG])
        job = ParseLogsJob(
            logs_iterable=logs,
            batch_web3_provider=ThreadLocalProxy(lambda: build_web3(self.batch_web3_provider)),
            item_exporter=exporter,
            max_workers=self.max_workers,
            batch_size=self.batch_size,
            chain_id=self.chain_id,
        )
        job.run()
        parsed_logs = exporter.get_items(PARSED_LOG)
        return parsed_logs

    def export_enriched_dex_trades(
        self,
        dex_trades: list,
        dex_pools: list,
        base_tokens_prices: list,
        tokens: list,
        token_transfers: list,
        internal_transfers: list,
        transactions: list,
    ) -> tuple[list[dict], list[dict]]:
        item_exporter = InMemoryItemExporter([ENRICHED_DEX_TRADE, ENRICHED_TRANSFER])
        stablecoin_addresses = self.chain_config['stablecoin_addresses']
        native_token = self.chain_config['native_token']
        job = EnrichDexTradeJob(
            dex_pools=dex_pools,
            base_tokens_prices=base_tokens_prices,
            tokens=tokens,
            token_transfers=token_transfers,
            internal_transfers=internal_transfers,
            transactions=transactions,
            dex_trades=dex_trades,
            item_exporter=item_exporter,
            stablecoin_addresses=stablecoin_addresses,
            native_token=native_token,
        )
        job.run()
        enriched_dex_trades = item_exporter.get_items(ENRICHED_DEX_TRADE)
        enriched_transfers = item_exporter.get_items(ENRICHED_TRANSFER)
        return enriched_dex_trades, enriched_transfers

    def import_base_token_prices(
        self,
        token_addresses: list[str],
    ):
        assert (
            self.price_importer is not None
        ), 'Cannot import base token prices. Price importer is required'
        item_exporter = InMemoryItemExporter(['base_token_price'])
        job = ExportPricesForTokensJob(
            token_addresses_iterable=token_addresses,
            item_exporter=item_exporter,
            max_workers=self.max_workers,
            batch_size=self.batch_size,
            chain_id=self.chain_id,
            price_importer=self.price_importer,
        )
        job.run()
        tokens = item_exporter.get_items('base_token_price')
        return tokens

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
