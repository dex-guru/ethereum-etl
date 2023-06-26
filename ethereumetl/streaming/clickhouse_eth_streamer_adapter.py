import logging
from functools import cache, cached_property
from typing import Any, Iterable, Optional
from urllib.parse import parse_qs, urlparse

import clickhouse_connect

from blockchainetl.jobs.exporters.clickhouse_exporter import ClickHouseItemExporter
from blockchainetl.jobs.exporters.multi_item_exporter import MultiItemExporter
from ethereumetl.enumeration.entity_type import ALL, EntityType
from ethereumetl.streaming.eth_streamer_adapter import EthStreamerAdapter, sort_by

logger = logging.getLogger(__name__)


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


# noinspection PyProtectedMember
class ClickhouseEthStreamerAdapter:
    def __init__(
        self,
        eth_streamer_adapter: EthStreamerAdapter,
        clickhouse_url: str,
        chain_id: int,
        item_type_to_table_mapping: Optional[dict[EntityType, str]] = None,
        rewrite_entity_types: Iterable[EntityType] = ALL,
    ):
        self._eth_streamer_adapter = eth_streamer_adapter
        self._clickhouse_url = clickhouse_url
        self._clickhouse: Optional[clickhouse_connect.driver.HttpClient] = None
        self._rewrite_entity_types = frozenset(rewrite_entity_types)

        if item_type_to_table_mapping is None:
            self._item_type_to_table_mapping = {
                BLOCK: 'blocks',
                TRANSACTION: 'transactions',
                RECEIPT: 'receipts',
                LOG: 'logs',
                TOKEN_TRANSFER: 'token_transfers',
                TRACE: 'traces',
                CONTRACT: 'contracts',
                TOKEN: 'tokens',
                ERROR: 'errors',
                GETH_TRACE: 'geth_traces',
                INTERNAL_TRANSFER: 'internal_transfers',
            }
        else:
            self._item_type_to_table_mapping = item_type_to_table_mapping

        self._chain_id = chain_id
        self._entity_types = frozenset(eth_streamer_adapter.entity_types)

        if RECEIPT in self._entity_types:
            raise NotImplementedError("Receipt export is not implemented for ClickHouse")

    @staticmethod
    def clickhouse_client_from_url(url) -> clickhouse_connect.driver.HttpClient:
        connect_kwargs = ClickhouseEthStreamerAdapter.parse_url(url)
        return clickhouse_connect.get_client(
            **connect_kwargs, compress=False, query_limit=0, send_receive_timeout=600
        )

    @staticmethod
    def parse_url(url) -> dict[str, Any]:
        parsed = urlparse(url)
        settings = parse_qs(parsed.query)
        connect_kwargs = {
            'host': parsed.hostname,
            'port': parsed.port,
            'user': parsed.username,
            'password': parsed.password,
            'settings': settings,
        }
        if parsed.path:
            connect_kwargs['database'] = parsed.path[1:]
        if parsed.scheme == "https":
            connect_kwargs['secure'] = True
        return connect_kwargs

    def open(self):
        self._eth_streamer_adapter.open()
        self._clickhouse = self.clickhouse_client_from_url(self._clickhouse_url)

    def get_current_block_number(self) -> int:
        return self._eth_streamer_adapter.get_current_block_number()

    def _select_distinct(
        self, entity_type: EntityType, start_block: int, end_block: int, distinct_on: str
    ) -> tuple[dict[str, Any], ...]:
        assert self._clickhouse, "Clickhouse client is not initialized"

        table_name = self._item_type_to_table_mapping[entity_type]
        if entity_type == BLOCK:
            block_number_column = 'number'
        else:
            block_number_column = 'block_number'
        query = (
            f"select distinct on ({distinct_on}) * from `{table_name}`"
            f" where {block_number_column} >= {start_block}"
            f"   and {block_number_column} <= {end_block}"
        )
        try:
            return tuple(self._clickhouse.query(query).named_results())
        except clickhouse_connect.driver.exceptions.DatabaseError as e:
            if 'UNKNOWN_TABLE' in str(e):  # The error code is not exposed by the driver
                return ()
            raise

    @staticmethod
    def get_logs_count_from_transactions(transactions: tuple) -> int:
        none_value_receipt_logs_count = any(
            d.get('receipt_logs_count') is None for d in transactions
        )
        if none_value_receipt_logs_count:
            return -1
        want_logs_count = sum(b['receipt_logs_count'] for b in transactions)
        return want_logs_count

    def export_all(self, start_block, end_block):
        want_block_count = end_block - start_block + 1
        should_export = self._eth_streamer_adapter.should_export

        def get_transaction_count_from_blocks(blocks: tuple) -> int | float:
            if self._chain_id == 137:
                # workaround for Polygon where block.transaction_count doesn't match the number
                # of transactions in the db
                return float('-inf')
            return sum(b['transaction_count'] for b in blocks)

        @cache
        def export_blocks_and_transactions():
            logger.info("exporting BLOCKS and TRANSACTIONS...")
            blocks = self._select_distinct(BLOCK, start_block, end_block, 'number')
            transactions = self._select_distinct(TRANSACTION, start_block, end_block, 'hash')

            want_transaction_count = get_transaction_count_from_blocks(blocks)

            transaction_count = len(transactions)
            block_count = len(blocks)

            if block_count < want_block_count or (
                TRANSACTION in should_export and transaction_count < want_transaction_count
            ):
                logger.info(
                    f"Block/Transactions. Not enough data found in clickhouse: falling back to Eth node:"
                    f" entity_types=block,transaction block_range={start_block}-{end_block}"
                )
                blocks, transactions = self._eth_streamer_adapter._export_blocks_and_transactions(
                    start_block, end_block
                )

                want_transaction_count = get_transaction_count_from_blocks(blocks)

                assert len(blocks) >= want_block_count, "got less blocks than expected"
                assert (
                    len(transactions) >= want_transaction_count
                ), "got less transactions than expected"

                from_ch = False
                return blocks, transactions, from_ch

            from_ch = True

            for t in transactions:
                t['type'] = TRANSACTION
            for b in blocks:
                b['type'] = BLOCK

            return blocks, transactions, from_ch

        def blocks_previously_exported():
            blocks, _, _ = export_blocks_and_transactions()
            return len(blocks) == want_block_count

        @cache
        def export_receipts_and_logs():
            logger.info("exporting RECEIPTS and LOGS...")
            blocks, transactions, _ = export_blocks_and_transactions()
            receipts, logs = self._eth_streamer_adapter._export_receipts_and_logs(transactions)
            from_ch = False
            return receipts, logs, from_ch

        @cache
        def export_receipts():
            logger.info("exporting RECEIPTS...")

            blocks, transactions, from_ch = export_blocks_and_transactions()
            if from_ch:
                receipt_items = [
                    self._receipt_item_from_ch_transaction(transaction)
                    for transaction in transactions
                ]
                return receipt_items, from_ch

            receipt_items, _, from_ch = export_receipts_and_logs()
            return receipt_items, from_ch

        @cache
        def export_logs():
            logger.info("exporting LOGS...")
            if blocks_previously_exported():
                blocks, transactions, from_ch = export_blocks_and_transactions()
                want_transaction_count = get_transaction_count_from_blocks(blocks)
                if want_transaction_count == 0:
                    return (), from_ch
                logs = self._select_distinct(
                    LOG, start_block, end_block, 'transaction_hash,log_index'
                )
                want_logs_count = self.get_logs_count_from_transactions(transactions)
                if len(logs) == want_logs_count:
                    for l in logs:
                        l['type'] = LOG
                    from_ch = True
                    return logs, from_ch

            logger.info(
                f"Logs. Not enough data found in clickhouse: falling back to Eth node:"
                f" entity_types=receipt,log block_range={start_block}-{end_block}"
            )
            _, logs, from_ch = export_receipts_and_logs()
            return logs, from_ch

        @cache
        def export_traces():
            logger.info("exporting TRACES...")
            if blocks_previously_exported():
                traces = self._select_distinct(TRACE, start_block, end_block, 'trace_id')
                if len(traces) > 0:
                    for t in traces:
                        t['type'] = TRACE
                    from_ch = True
                    return traces, from_ch

            logger.info(
                f"Traces. Not enough data found in clickhouse: falling back to Eth node:"
                f" entity_type=trace block_range={start_block}-{end_block}"
            )
            traces = self._eth_streamer_adapter._export_traces(start_block, end_block)
            from_ch = False
            return traces, from_ch

        @cache
        def export_geth_traces():
            logger.info("exporting GETH_TRACES...")
            if blocks_previously_exported():
                geth_traces = self._select_distinct(
                    GETH_TRACE,
                    start_block,
                    end_block,
                    'transaction_hash',
                )
                if len(geth_traces) > 0:
                    for t in geth_traces:
                        t['type'] = GETH_TRACE
                    from_ch = True
                    return geth_traces, from_ch

            logging.info(
                f"Geth traces. Not enough data found in clickhouse: falling back to Eth node:"
                f" entity_type=geth_trace block_range={start_block}-{end_block}"
            )
            transaction_hashes = [t['hash'] for t in export_blocks_and_transactions()[1]]
            geth_traces = self._eth_streamer_adapter._export_geth_traces(transaction_hashes)
            from_ch = False
            return geth_traces, from_ch

        @cache
        def extract_token_transfers():
            logger.info("exporting TOKEN_TRANSFERS...")
            token_transfers_ch = self._select_distinct(
                TOKEN_TRANSFER, start_block, end_block, 'transaction_hash,log_index'
            )
            token_transfers = self._eth_streamer_adapter._extract_token_transfers(export_logs()[0])
            from_ch = True if len(token_transfers_ch) == len(token_transfers) else False
            return token_transfers, from_ch

        @cache
        def extract_contracts():
            logger.info("exporting CONTRACTS...")
            traces, from_ch = export_traces()
            contracts = self._eth_streamer_adapter._export_contracts(traces)
            return contracts, from_ch

        @cache
        def extract_tokens():
            logger.info("exporting TOKENS...")
            contracts, from_ch = extract_contracts()
            tokens = self._eth_streamer_adapter._extract_tokens(contracts)
            return tokens, from_ch

        @cache
        def export_token_balances():
            logger.info("exporting TOKEN_BALANCES...")
            if blocks_previously_exported():
                balances = self._select_distinct(
                    TOKEN_BALANCE,
                    start_block,
                    end_block,
                    'token_address,holder_address,block_number',
                )
                if len(balances) > 0:
                    for b in balances:
                        b['type'] = TOKEN_BALANCE
                    errors = ()
                    from_ch = True
                    return balances, errors, from_ch

            token_balances, errors = self._eth_streamer_adapter._export_token_balances(
                extract_token_transfers()[0]
            )
            from_ch = False
            return token_balances, errors, from_ch

        @cache
        def extract_internal_transfers():
            internal_transfers_ch = self._select_distinct(
                INTERNAL_TRANSFER, start_block, end_block, 'transaction_hash'
            )
            geth_traces, geth_traces_from_ch = export_geth_traces()
            internal_transfers = self._eth_streamer_adapter._extract_internal_transfers(
                geth_traces
            )
            from_ch = geth_traces_from_ch and len(internal_transfers_ch) == len(internal_transfers)
            return internal_transfers, from_ch

        exported: dict[EntityType, list] = {}
        from_ch: dict[EntityType, bool] = {}

        for entity_types, export_func in (
            ((BLOCK, TRANSACTION), export_blocks_and_transactions),
            ((RECEIPT,), export_receipts),
            ((LOG,), export_logs),
            ((TOKEN_TRANSFER,), extract_token_transfers),
            ((TOKEN_BALANCE, ERROR), export_token_balances),
            ((TRACE,), export_traces),
            ((GETH_TRACE,), export_geth_traces),
            ((CONTRACT,), extract_contracts),
            ((INTERNAL_TRANSFER,), extract_internal_transfers),
            ((TOKEN,), extract_tokens),
        ):
            for entity_type in entity_types:
                if entity_type not in self._eth_streamer_adapter.should_export:
                    continue

                *results, from_ch_ = export_func()

                for items, entity_type_ in zip(results, entity_types):
                    exported.setdefault(entity_type_, []).extend(items)
                    from_ch[entity_type_] = from_ch_

                break

        all_items = []
        items_by_type = {}
        for entity_type in self._entity_types:
            if (
                from_ch.get(entity_type)
                and self.exporting_to_the_same_clickhouse
                and entity_type not in self._rewrite_entity_types
            ):
                continue
            items = exported[entity_type]
            enriched_items = self._eth_streamer_adapter.enrich(entity_type, exported.__getitem__)
            sorted_items = sort_by(
                enriched_items, self._eth_streamer_adapter.SORT_BY_FIELDS[entity_type]
            )
            assert len(sorted_items) == len(items), entity_type
            all_items.extend(sorted_items)
            items_by_type[entity_type] = sorted_items

        self._eth_streamer_adapter.log_batch_export_progress(items_by_type)

        self._eth_streamer_adapter.calculate_item_ids(all_items)
        self._eth_streamer_adapter.calculate_item_timestamps(all_items)

        self._eth_streamer_adapter.item_exporter.export_items(all_items)

    def close(self):
        try:
            if self._clickhouse:
                self._clickhouse.close()
                self._clickhouse = None
        finally:
            self._eth_streamer_adapter.close()

    @cached_property
    def exporting_to_the_same_clickhouse(self) -> bool:
        exporter = self._eth_streamer_adapter.item_exporter
        if isinstance(exporter, MultiItemExporter):
            if len(exporter.item_exporters) != 1:
                return False
            exporter = exporter.item_exporters[0]

        if not isinstance(exporter, ClickHouseItemExporter):
            return False

        params = self.parse_url(self._clickhouse_url)

        return (
            params['host'] == exporter.host
            and params['port'] == exporter.port
            and params.get('database', exporter.database) == exporter.database
            and self._item_type_to_table_mapping == exporter.item_type_to_table_mapping
        )

    @staticmethod
    def _receipt_item_from_ch_transaction(transaction):
        return {
            'type': RECEIPT,
            'transaction_hash': transaction['hash'],
            'transaction_index': transaction['transaction_index'],
            'block_hash': transaction['block_hash'],
            'block_number': transaction['block_number'],
            'cumulative_gas_used': transaction['receipt_cumulative_gas_used'],
            'gas_used': transaction['receipt_gas_used'],
            'contract_address': transaction['receipt_contract_address'],
            'root': transaction['receipt_root'],
            'status': transaction['receipt_status'],
            'effective_gas_price': transaction['receipt_effective_gas_price'],
            'logs_count': transaction['receipt_logs_count'],
        }
