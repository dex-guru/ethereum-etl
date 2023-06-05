import logging
from functools import cache
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

import clickhouse_connect

from ethereumetl.enumeration.entity_type import EntityType
from ethereumetl.streaming.enrich import (
    enrich_contracts,
    enrich_logs,
    enrich_token_transfers,
    enrich_tokens,
    enrich_traces,
    enrich_transactions,
)
from ethereumetl.streaming.eth_streamer_adapter import EthStreamerAdapter, sort_by


# noinspection PyProtectedMember
class ClickhouseEthStreamerAdapter:
    def __init__(
        self,
        eth_streamer_adapter: EthStreamerAdapter,
        clickhouse_url: str,
        chain_id: int,
        item_type_to_table_mapping: Optional[dict[str, str]] = None,
    ):
        self._eth_streamer_adapter = eth_streamer_adapter
        self._clickhouse_url = clickhouse_url
        self._clickhouse: Optional[clickhouse_connect.driver.HttpClient] = None

        if item_type_to_table_mapping is None:
            self._item_type_to_table_mapping = {
                EntityType.BLOCK: 'blocks',
                EntityType.TRANSACTION: 'transactions',
                EntityType.RECEIPT: 'receipts',
                EntityType.LOG: 'logs',
                EntityType.TOKEN_TRANSFER: 'token_transfers',
                EntityType.TRACE: 'traces',
                EntityType.CONTRACT: 'contracts',
                EntityType.TOKEN: 'tokens',
            }
        else:
            self._item_type_to_table_mapping = item_type_to_table_mapping

        self._chain_id = chain_id
        self._entity_types = set(eth_streamer_adapter.entity_types)

    @staticmethod
    def clickhouse_client_from_url(url) -> clickhouse_connect.driver.HttpClient:
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
        return clickhouse_connect.get_client(**connect_kwargs, compress=False, query_limit=0)

    def open(self):
        self._eth_streamer_adapter.open()
        self._clickhouse = self.clickhouse_client_from_url(self._clickhouse_url)

    def get_current_block_number(self) -> int:
        return self._eth_streamer_adapter.get_current_block_number()

    def _select_distinct(
        self, entity_type: str, start_block: int, end_block: int, distinct_on: str
    ) -> tuple[dict[str, Any], ...]:
        assert self._clickhouse, "Clickhouse client is not initialized"

        table_name = self._item_type_to_table_mapping[entity_type]
        if entity_type == EntityType.BLOCK:
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

    def export_all(self, start_block, end_block):
        want_block_count = end_block - start_block + 1

        eth_export_blocks_and_transactions = cache(
            self._eth_streamer_adapter._export_blocks_and_transactions
        )

        def get_transaction_count_from_blocks(blocks: tuple) -> int:
            if self._chain_id == 137:
                # TODO: That's not correct, need to fix
                # workaround for Polygon where block.transaction_count doesn't match the number
                # of transactions in the db
                want_transaction_count = 1
            else:
                want_transaction_count = sum(b['transaction_count'] for b in blocks)
            return want_transaction_count

        @cache
        def export_blocks_and_transactions():
            blocks = self._select_distinct(EntityType.BLOCK, start_block, end_block, 'number')
            transactions = self._select_distinct(
                EntityType.TRANSACTION, start_block, end_block, 'hash'
            )

            want_transaction_count = get_transaction_count_from_blocks(blocks)

            transaction_count = len(transactions)
            block_count = len(blocks)

            if block_count < want_block_count or (
                EntityType.TRANSACTION in self._entity_types
                and transaction_count < want_transaction_count
            ):
                logging.info(
                    f"Block/Transactions. Not enough data found in clickhouse: falling back to Eth node:"
                    f" entity_types=block,transaction block_range={start_block}-{end_block}"
                )
                blocks, transactions = eth_export_blocks_and_transactions(start_block, end_block)

                want_transaction_count = get_transaction_count_from_blocks(blocks)

                assert len(blocks) == want_block_count, "got less blocks than expected"
                assert (
                    len(transactions) == want_transaction_count
                ), "got less transactions than expected"

                from_ch = False
                return blocks, transactions, from_ch

            from_ch = True

            for t in transactions:
                t['type'] = EntityType.TRANSACTION
            for b in blocks:
                b['type'] = EntityType.BLOCK

            return blocks, transactions, from_ch

        @cache
        def export_blocks_and_transactions_enriched():
            blocks, transactions, from_ch = export_blocks_and_transactions()
            # TODO: To fill up receipt_log_count remove after sync
            none_value_receipt_log_count = False
            if from_ch:
                none_value_receipt_log_count = any(
                    d.get('receipt_logs_count') is None for d in transactions
                )
            if EntityType.TRANSACTION in self._entity_types and (
                not from_ch or none_value_receipt_log_count
            ):
                transactions = enrich_transactions(transactions, export_receipts_and_logs()[0])
                return blocks, transactions, False
            return blocks, transactions, True

        def blocks_previously_exported():
            blocks, _, _ = export_blocks_and_transactions()
            return len(blocks) == want_block_count

        @cache
        def export_receipts_and_logs():
            blocks, transactions, _ = export_blocks_and_transactions()
            receipts, logs = self._eth_streamer_adapter._export_receipts_and_logs(transactions)
            logs = enrich_logs(blocks, logs)
            return receipts, logs

        def get_logs_count_from_transactions(transactions: tuple) -> int:
            none_value_receipt_logs_count = any(
                d.get('receipt_logs_count') is None for d in transactions
            )
            if none_value_receipt_logs_count:
                return -1
            want_logs_count = sum(b['receipt_logs_count'] for b in transactions)
            return want_logs_count

        @cache
        def export_logs():
            if blocks_previously_exported():
                blocks, transactions, from_ch = export_blocks_and_transactions_enriched()
                want_transaction_count = get_transaction_count_from_blocks(blocks)
                if want_transaction_count == 0:
                    return ()
                logs = self._select_distinct(
                    EntityType.LOG, start_block, end_block, 'transaction_hash,log_index'
                )
                want_logs_count = get_logs_count_from_transactions(transactions)
                if len(logs) == want_logs_count:
                    for l in logs:
                        l['type'] = EntityType.LOG
                    logs = enrich_logs(blocks, logs)
                    return logs, True

            logging.info(
                f"Logs. Not enough data found in clickhouse: falling back to Eth node:"
                f" entity_types=receipt,log block_range={start_block}-{end_block}"
            )
            _, logs = export_receipts_and_logs()
            logs = enrich_logs(blocks, logs)
            return logs, False

        @cache
        def export_traces():
            if blocks_previously_exported():
                traces = self._select_distinct(
                    EntityType.TRACE, start_block, end_block, 'trace_id'
                )
                if len(traces) > 0:
                    for t in traces:
                        t['type'] = EntityType.TRACE
                    return traces

            logging.info(
                f"Traces. Not enough data found in clickhouse: falling back to Eth node:"
                f" entity_type=trace block_range={start_block}-{end_block}"
            )
            traces = self._eth_streamer_adapter._export_traces(start_block, end_block)
            traces = enrich_traces(export_blocks_and_transactions()[0], traces)
            return traces

        @cache
        def extract_token_transfers():
            token_transfers_ch = self._select_distinct(
                EntityType.TOKEN_TRANSFER, start_block, end_block, 'transaction_hash,log_index'
            )
            token_transfers = self._eth_streamer_adapter._extract_token_transfers(export_logs()[0])
            token_transfers = enrich_token_transfers(
                export_blocks_and_transactions_enriched()[0], token_transfers
            )
            from_ch = True if len(token_transfers_ch) == len(token_transfers) else False
            return token_transfers, from_ch

        @cache
        def extract_contracts():
            contracts = self._eth_streamer_adapter._export_contracts(export_traces())
            contracts = enrich_contracts(export_blocks_and_transactions_enriched()[0], contracts)
            return contracts

        @cache
        def extract_tokens():
            tokens = self._eth_streamer_adapter._extract_tokens(extract_contracts())
            tokens = enrich_tokens(export_blocks_and_transactions_enriched()[0], tokens)
            return tokens

        blocks = transactions = logs = token_transfers = traces = contracts = tokens = tuple()
        blocks_from_ch = transactions_from_ch = logs_from_ch = token_transfers_from_ch = False
        if EntityType.BLOCK in self._entity_types:
            blocks, transactions, blocks_from_ch = export_blocks_and_transactions()

        if EntityType.TRANSACTION in self._entity_types:
            blocks, transactions, transactions_from_ch = export_blocks_and_transactions_enriched()

        if EntityType.LOG in self._entity_types:
            logs, logs_from_ch = export_logs()

        if EntityType.TOKEN_TRANSFER in self._entity_types:
            token_transfers, token_transfers_from_ch = extract_token_transfers()

        if EntityType.TRACE in self._entity_types:
            traces = export_traces()

        if EntityType.CONTRACT in self._entity_types:
            contracts = extract_contracts()

        if EntityType.TOKEN in self._entity_types:
            tokens = extract_tokens()

        logging.info('Exporting with ' + type(self._eth_streamer_adapter.item_exporter).__name__)

        # Emptying the lists, if they from CH to skip exporting them
        if blocks_from_ch:
            blocks = []
        if transactions_from_ch:
            transactions = []
        if logs_from_ch:
            logs = []
        if token_transfers_from_ch:
            token_transfers = []

        all_items = [
            *sort_by(blocks, 'number'),
            *sort_by(transactions, ('block_number', 'transaction_index')),
            *sort_by(logs, ('block_number', 'log_index')),
            *sort_by(token_transfers, ('block_number', 'log_index')),
            *sort_by(traces, ('block_number', 'trace_index')),
            *sort_by(contracts, ('block_number',)),
            *sort_by(tokens, ('block_number',)),
        ]

        self._eth_streamer_adapter.calculate_item_ids(all_items)
        self._eth_streamer_adapter.calculate_item_timestamps(all_items)

        self._eth_streamer_adapter.item_exporter.export_items(all_items)

    def close(self):
        try:
            self._clickhouse.close()
        finally:
            self._eth_streamer_adapter.close()
