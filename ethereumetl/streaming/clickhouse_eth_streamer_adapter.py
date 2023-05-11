import logging
from functools import cache
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

import clickhouse_connect

from ethereumetl.enumeration.entity_type import EntityType
from ethereumetl.streaming.enrich import (
    enrich_contracts,
    enrich_logs,
    enrich_token_balances,
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

        @cache
        def export_blocks_and_transactions():
            logging.info("exporting BLOCKS and TRANSACTIONS...")
            blocks = self._select_distinct(EntityType.BLOCK, start_block, end_block, 'number')
            transactions = self._select_distinct(
                EntityType.TRANSACTION, start_block, end_block, 'hash'
            )

            if self._chain_id == 137:
                # workaround for Polygon where block.transaction_count doesn't match the number
                # of transactions in the db
                want_transaction_count = 1
            else:
                want_transaction_count = sum(b['transaction_count'] for b in blocks)

            transaction_count = len(transactions)
            block_count = len(blocks)

            if block_count < want_block_count or (
                EntityType.TRANSACTION in self._entity_types
                and transaction_count < want_transaction_count
            ):
                logging.info(
                    f"Not enough data found in clickhouse: falling back to Eth node:"
                    f" entity_types=block,transaction block_range={start_block}-{end_block}"
                )
                blocks, transactions = eth_export_blocks_and_transactions(start_block, end_block)

                assert len(blocks) == want_block_count, "got less blocks than expected"

                if EntityType.TRANSACTION in self._entity_types:
                    transactions = enrich_transactions(transactions, export_receipts_and_logs()[0])
                return blocks, transactions

            for t in transactions:
                t['type'] = EntityType.TRANSACTION
            for b in blocks:
                b['type'] = EntityType.BLOCK

            return blocks, transactions

        def blocks_previously_exported():
            blocks, _ = export_blocks_and_transactions()
            return len(blocks) == want_block_count

        @cache
        def export_receipts_and_logs():
            logging.info("exporting RECEIPTS and LOGS...")
            blocks, transactions = eth_export_blocks_and_transactions(start_block, end_block)
            receipts, logs = self._eth_streamer_adapter._export_receipts_and_logs(transactions)
            logs = enrich_logs(blocks, logs)
            return receipts, logs

        @cache
        def export_logs():
            logging.info("exporting LOGS...")
            if blocks_previously_exported():
                logs = self._select_distinct(
                    EntityType.LOG, start_block, end_block, 'transaction_hash,log_index'
                )
                if len(logs) > 0:
                    for l in logs:
                        l['type'] = EntityType.LOG
                    logs = enrich_logs(export_blocks_and_transactions()[0], logs)
                    return logs

            logging.info(
                f"Not enough data found in clickhouse: falling back to Eth node:"
                f" entity_types=receipt,log block_range={start_block}-{end_block}"
            )
            _, logs = export_receipts_and_logs()
            logs = enrich_logs(export_blocks_and_transactions()[0], logs)
            return logs

        @cache
        def export_traces():
            logging.info("exporting TRACES...")
            if blocks_previously_exported():
                traces = self._select_distinct(
                    EntityType.TRACE, start_block, end_block, 'trace_id'
                )
                if len(traces) > 0:
                    for t in traces:
                        t['type'] = EntityType.TRACE
                    enrich_traces(export_blocks_and_transactions()[0], traces)
                    return traces

            logging.info(
                f"Not enough data found in clickhouse: falling back to Eth node:"
                f" entity_type=trace block_range={start_block}-{end_block}"
            )
            traces = self._eth_streamer_adapter._export_traces(start_block, end_block)
            traces = enrich_traces(export_blocks_and_transactions()[0], traces)
            return traces

        @cache
        def extract_token_transfers():
            logging.info("exporting TOKEN_TRANSFERS...")
            token_transfers = self._eth_streamer_adapter._extract_token_transfers(export_logs())
            token_transfers = enrich_token_transfers(
                export_blocks_and_transactions()[0], token_transfers
            )
            return token_transfers

        @cache
        def extract_contracts():
            logging.info("exporting CONTRACTS...")
            contracts = self._eth_streamer_adapter._export_contracts(export_traces())
            contracts = enrich_contracts(export_blocks_and_transactions()[0], contracts)
            return contracts

        @cache
        def extract_tokens():
            logging.info("exporting TOKENS...")
            tokens = self._eth_streamer_adapter._extract_tokens(extract_contracts())
            tokens = enrich_tokens(export_blocks_and_transactions()[0], tokens)
            return tokens

        @cache
        def export_token_balances():
            logging.info("exporting TOKEN_BALANCES...")

            if blocks_previously_exported():
                balances = self._select_distinct(
                    EntityType.TOKEN_BALANCE,
                    start_block,
                    end_block,
                    'token_address,holder_address,block_number',
                )
                if len(balances) > 0:
                    for b in balances:
                        b['type'] = EntityType.TOKEN_BALANCE
                    balances = enrich_token_balances(export_blocks_and_transactions()[0], balances)
                    return balances

            token_balances = self._eth_streamer_adapter._export_token_balances(
                extract_token_transfers()
            )
            token_balances = enrich_token_balances(
                export_blocks_and_transactions()[0], token_balances
            )
            return token_balances

        blocks = ()
        transactions = ()
        logs = ()
        token_transfers = ()
        token_balances = ()
        traces = ()
        contracts = ()
        tokens = ()

        if EntityType.BLOCK in self._entity_types:
            blocks = export_blocks_and_transactions()[0]

        if EntityType.TRANSACTION in self._entity_types:
            transactions = export_blocks_and_transactions()[1]

        if EntityType.LOG in self._entity_types:
            logs = export_logs()

        if EntityType.TOKEN_TRANSFER in self._entity_types:
            token_transfers = extract_token_transfers()

        if EntityType.TOKEN_BALANCE in self._entity_types:
            token_balances = export_token_balances()

        if EntityType.TRACE in self._entity_types:
            traces = export_traces()

        if EntityType.CONTRACT in self._entity_types:
            contracts = extract_contracts()

        if EntityType.TOKEN in self._entity_types:
            tokens = extract_tokens()

        self._eth_streamer_adapter.log_batch_export_progress(
            blocks=blocks,
            contracts=contracts,
            logs=logs,
            token_transfers=token_transfers,
            tokens=tokens,
            traces=traces,
            transactions=transactions,
            token_balances=token_balances,
        )

        all_items = [
            *sort_by(blocks, 'number'),
            *sort_by(transactions, ('block_number', 'transaction_index')),
            *sort_by(logs, ('block_number', 'log_index')),
            *sort_by(token_transfers, ('block_number', 'log_index')),
            *sort_by(token_balances, ('block_number', 'token_address', 'address')),
            *sort_by(traces, ('block_number', 'trace_index')),
            *sort_by(contracts, ('block_number',)),
            *sort_by(tokens, ('block_number',)),
        ]

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
