import logging
from collections.abc import Iterable, Sequence
from functools import cache, cached_property
from itertools import groupby
from typing import Any

import clickhouse_connect
from clickhouse_connect.driver import Client
from clickhouse_connect.driver.exceptions import DatabaseError

from blockchainetl.jobs.exporters.clickhouse_exporter import ClickHouseItemExporter
from blockchainetl.jobs.exporters.multi_item_exporter import MultiItemExporter
from ethereumetl.enumeration.entity_type import ALL, EntityType
from ethereumetl.streaming.eth_streamer_adapter import EthStreamerAdapter, sort_by
from ethereumetl.utils import parse_clickhouse_url

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
        item_type_to_table_mapping: dict[EntityType, str] | None = None,
        rewrite_entity_types: Iterable[EntityType] = ALL,
    ):
        self._eth_streamer_adapter = eth_streamer_adapter
        self._clickhouse_url = clickhouse_url
        self._clickhouse: Client | None = None
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
    def clickhouse_client_from_url(url) -> Client:
        connect_kwargs = parse_clickhouse_url(url)
        return clickhouse_connect.create_client(
            **connect_kwargs, compress=False, query_limit=0, send_receive_timeout=600
        )

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
        except DatabaseError as e:
            if 'UNKNOWN_TABLE' in str(e):  # The error code is not exposed by the driver
                logger.warning("Cannot export %s items from clickhouse: %s", entity_type, e)
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

            if (BLOCK in should_export and block_count < want_block_count) or (
                TRANSACTION in should_export and transaction_count < want_transaction_count
            ):
                logger.info(
                    f"Block/Transactions. Not enough data found in clickhouse: falling back to Eth node:"
                    f" entity_types=block,transaction block_range={start_block}-{end_block}"
                )
                blocks, transactions = self._eth_streamer_adapter._export_blocks_and_transactions(
                    start_block, end_block
                )

                if BLOCK in should_export:
                    assert len(blocks) >= want_block_count, "got less blocks than expected"

                if TRANSACTION in should_export:
                    want_transaction_count = get_transaction_count_from_blocks(blocks)
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
            receipts, logs, errors = self._eth_streamer_adapter._export_receipts_and_logs(
                transactions
            )
            from_ch = False
            return receipts, logs, errors, from_ch

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

            receipt_items, _logs, errors, from_ch = export_receipts_and_logs()
            return receipt_items, errors, from_ch

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
                    return logs, (), from_ch

            logger.info(
                f"Logs. Not enough data found in clickhouse: falling back to Eth node:"
                f" entity_types=receipt,log block_range={start_block}-{end_block}"
            )
            _receipts, logs, errors, from_ch = export_receipts_and_logs()
            return logs, errors, from_ch

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

                if len(geth_traces) == len(export_blocks_and_transactions()[1]):
                    for t in geth_traces:
                        t['type'] = GETH_TRACE
                        t['transaction_traces'] = t['traces_json']
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
            from_ch = len(token_transfers_ch) == len(token_transfers)
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

        exported: dict[EntityType, list] = {ERROR: []}
        from_ch: dict[EntityType, bool] = {}

        for entity_types, export_func in (
            ((BLOCK, TRANSACTION), export_blocks_and_transactions),
            ((RECEIPT, ERROR), export_receipts),
            ((LOG, ERROR), export_logs),
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
                and entity_type != EntityType.ERROR
            ):
                continue
            items = exported[entity_type]
            enriched_items = self._eth_streamer_adapter.enrich(entity_type, exported.__getitem__)
            if len(enriched_items) != len(items):
                logger.warning(
                    "'%s' item count has changed after enrichment: %i -> %i",
                    entity_type,
                    len(items),
                    len(enriched_items),
                    extra={
                        "start_block": start_block,
                        "end_block": end_block,
                        "entity_type": entity_type,
                        "count_before_enrichment": len(items),
                        "count_after_enrichment": len(enriched_items),
                    },
                )
            sorted_items = sort_by(
                enriched_items, self._eth_streamer_adapter.SORT_BY_FIELDS[entity_type]
            )
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

        params = parse_clickhouse_url(self._clickhouse_url)

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


class VerifyingClickhouseEthStreamerAdapter:
    def __init__(self, clickhouse_eth_streamer_adapter: ClickhouseEthStreamerAdapter):
        self.ch_streamer = clickhouse_eth_streamer_adapter
        assert (
            self.ch_streamer.exporting_to_the_same_clickhouse
        ), 'VerifyingClickhouseEthStreamerAdapter can be used only when exporting to the same ClickHouse instance'

    def open(self):
        self.ch_streamer.open()

    def close(self):
        self.ch_streamer.close()

    def get_current_block_number(self) -> int:
        return self.ch_streamer.get_current_block_number()

    def export_all(self, start_block: int, end_block: int):
        def find_inconsistent_blocks(blocks_from_storage: Sequence, blocks_from_w3: Sequence):
            if len(blocks_from_storage) != len(blocks_from_w3):
                # check absent blocks
                missing_blocks.update(
                    block['number']
                    for block in blocks_from_w3
                    if block['number'] not in [block_['number'] for block_ in blocks_from_storage]
                )
                blocks_from_w3 = [
                    block for block in blocks_from_w3 if block['number'] not in missing_blocks
                ]
                if missing_blocks:
                    logger.info("BLOCKS absent: %s", ', '.join(map(str, missing_blocks)))

            # check block hashes
            for block_ch, block_w3 in zip(blocks_from_storage, blocks_from_w3):
                try:
                    check_blocks_consistency(block_ch, block_w3)
                except AssertionError:
                    inconsistent_blocks.add(block_ch['number'])
            if inconsistent_blocks:
                logger.info("BLOCKS mismatch: %s", ', '.join(map(str, inconsistent_blocks)))

        def find_inconsistent_transactions(txns_from_storage: Sequence, txns_from_w3: Sequence):
            if len(txns_from_storage) != len(txns_from_w3):
                # check absent transactions
                missing_blocks.update(
                    txn['block_number']
                    for txn in txns_from_w3
                    if txn['block_number']
                    not in [txn_['block_number'] for txn_ in txns_from_storage]
                )
                txns_from_w3 = [
                    txn for txn in txns_from_w3 if txn['block_number'] not in missing_blocks
                ]
                logger.info("TRANSACTIONS mismatch: %s", ', '.join(map(str, missing_blocks)))

            # check transaction hashes
            for txn_ch, txn_w3 in zip(txns_from_storage, txns_from_w3):
                try:
                    check_transactions_consistency(txn_ch, txn_w3)
                except AssertionError:
                    inconsistent_blocks.add(txn_ch['block_number'])

        def check_transactions_consistency(txn_ch, txn_w3):
            assert txn_ch['hash'] == txn_w3['hash']
            assert txn_ch['block_hash'] == txn_w3['block_hash']
            assert txn_ch['block_number'] == txn_w3['block_number']
            assert txn_ch['transaction_index'] == txn_w3['transaction_index']
            assert txn_ch['from_address'] == txn_w3['from_address']
            assert txn_ch['to_address'] == txn_w3['to_address']
            assert txn_ch['value'] == txn_w3['value']
            assert txn_ch['gas'] == txn_w3['gas']
            assert txn_ch['gas_price'] == txn_w3['gas_price']
            assert txn_ch['input'] == txn_w3['input']
            assert txn_ch['nonce'] == txn_w3['nonce']

        def check_blocks_consistency(block_ch, block_w3):
            assert block_ch['number'] == block_w3['number']
            assert block_ch['hash'] == block_w3['hash']
            assert block_ch['transaction_count'] == block_w3['transaction_count']

        def delete_inconsistent_records_from_ch(blocks: Iterable):
            if not blocks:
                return

            assert self.ch_streamer._clickhouse, 'ClickHouse is not connected'
            for entity, table in self.ch_streamer._item_type_to_table_mapping.items():
                if entity == ERROR:
                    continue
                if entity == BLOCK:
                    self.ch_streamer._clickhouse.command(
                        f"DELETE FROM {table} WHERE number IN {tuple(blocks)}"
                    )
                else:
                    self.ch_streamer._clickhouse.command(
                        f"DELETE FROM {table} WHERE block_number IN {tuple(blocks)}"
                    )
            logger.info("Inconsistent records were deleted from ClickHouse")

        logger.info("Checking BLOCKS and TRANSACTIONS... from %s to %s", start_block, end_block)
        inconsistent_blocks: set[int] = set()
        missing_blocks: set[int] = set()

        blocks_ch = self.ch_streamer._select_distinct(BLOCK, start_block, end_block, 'number')
        transactions_ch = self.ch_streamer._select_distinct(
            TRANSACTION, start_block, end_block, 'hash'
        )

        (
            blocks_w3,
            transactions_w3,
        ) = self.ch_streamer._eth_streamer_adapter._export_blocks_and_transactions(
            start_block, end_block
        )
        blocks_w3 = sorted(blocks_w3, key=lambda x: x['number'])
        blocks_ch = sorted(blocks_ch, key=lambda x: x['number'])  # type: ignore
        transactions_w3 = sorted(transactions_w3, key=lambda x: x['hash'])
        transactions_ch = sorted(transactions_ch, key=lambda x: x['hash'])  # type: ignore

        find_inconsistent_blocks(blocks_ch, blocks_w3)
        find_inconsistent_transactions(transactions_ch, transactions_w3)

        if inconsistent_blocks or missing_blocks:
            delete_inconsistent_records_from_ch(inconsistent_blocks)
            blocks_to_resync = sorted(inconsistent_blocks | missing_blocks)
            block_sequences = [
                list(g)
                for k, g in groupby(blocks_to_resync, key=lambda x: x - blocks_to_resync.index(x))
            ]
            for seq in block_sequences:
                self.ch_streamer.export_all(start_block=min(seq), end_block=max(seq))
                logger.info("Inconsistent records were exported to ClickHouse")
