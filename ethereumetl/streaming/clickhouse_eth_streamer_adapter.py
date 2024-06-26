import logging
from collections.abc import Iterable, Sequence
from functools import cache, cached_property
from itertools import groupby
from time import sleep
from typing import Any

from clickhouse_connect.driver import Client
from clickhouse_connect.driver.exceptions import DatabaseError
from eth_utils import is_address

from blockchainetl.jobs.exporters.clickhouse_exporter import ClickHouseItemExporter
from blockchainetl.jobs.exporters.multi_item_exporter import MultiItemExporter
from ethereumetl.clickhouse import ITEM_TYPE_TO_TABLE_MAPPING
from ethereumetl.enumeration.entity_type import ALL, ALL_STATIC, EntityType
from ethereumetl.streaming.eth_streamer_adapter import EthStreamerAdapter, sort_by
from ethereumetl.utils import clickhouse_client_from_url, parse_clickhouse_url

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
NATIVE_BALANCE = EntityType.NATIVE_BALANCE
TOKEN_TRANSFER_PRICED = EntityType.TOKEN_TRANSFER_PRICED
INTERNAL_TRANSFER_PRICED = EntityType.INTERNAL_TRANSFER_PRICED
PRE_EVENT = EntityType.PRE_EVENT
DEX_POOL = EntityType.DEX_POOL
DEX_TRADE = EntityType.DEX_TRADE
PARSED_LOG = EntityType.PARSED_LOG
ENRICHED_DEX_TRADE = EntityType.ENRICHED_DEX_TRADE
ENRICHED_TRANSFER = EntityType.ENRICHED_TRANSFER


# noinspection PyProtectedMember
class ClickhouseEthStreamerAdapter:
    def __init__(
        self,
        eth_streamer: EthStreamerAdapter,
        clickhouse_url: str,
        chain_id: int,
        rewrite_entity_types: Iterable[EntityType] = ALL,
    ):
        self.eth_streamer = eth_streamer
        self.clickhouse_url = clickhouse_url
        self.clickhouse: Client | None = None
        self.rewrite_entity_types = frozenset(rewrite_entity_types)
        self.item_type_to_table_mapping = ITEM_TYPE_TO_TABLE_MAPPING
        self.chain_id = chain_id
        self.entity_types = frozenset(eth_streamer.entity_types)

        if RECEIPT in self.entity_types:
            raise NotImplementedError("Receipt export is not implemented for ClickHouse")

    def open(self):
        self.eth_streamer.open()
        self.clickhouse = clickhouse_client_from_url(self.clickhouse_url)

    def get_current_block_number(self) -> int:
        return self.eth_streamer.get_current_block_number()

    def select_distinct(
        self, entity_type: EntityType, start_block: int, end_block: int, distinct_on: str
    ) -> tuple[dict[str, Any], ...]:
        assert self.clickhouse, "Clickhouse client is not initialized"

        table_name = self.item_type_to_table_mapping[entity_type]
        if entity_type == BLOCK:
            block_number_column = 'number'
        else:
            block_number_column = 'block_number'
        query = (
            f"select distinct on ({distinct_on}) * from `{table_name}` final"
            f" where {block_number_column} >= {start_block}"
            f"   and {block_number_column} <= {end_block}"
            f"   and not is_reorged"
        )
        try:
            res = tuple(self.clickhouse.query(query).named_results())
            for item in res:
                item['type'] = entity_type
            return res
        except DatabaseError as e:
            if 'UNKNOWN_TABLE' in str(e):  # The error code is not exposed by the driver
                logger.warning("Cannot export %s items from clickhouse: %s", entity_type, e)
                return ()
            raise

    def select_where(self, entity_type: EntityType, distinct_on: str, **kwargs):
        """
        Select items from ClickHouse.

        As kwargs, you pass the field names and the values to filter on.
        For example, to import all transactions for a block, you would call:
        import_items(EntityType.TRANSACTION, 'hash', block_number=12345)
        Or to import few blocks:
        import_items(EntityType.BLOCK, 'number', number=[12345, 12346, 12347])
        """
        assert self.clickhouse, "Clickhouse client is not initialized"

        def _get_where_clause(kwargs):
            where_clause = []
            for key, value in kwargs.items():
                if isinstance(value, str):
                    where_clause.append(f"{key} = '{value}'")
                elif isinstance(value, Iterable):
                    assert value, f"Empty iterable for key {key}"
                    where_clause.append(f"{key} in {tuple(value)}")
                elif value is None:
                    where_clause.append(f"{key} is NULL")
                elif isinstance(value, bool):
                    where_clause.append(f"{key} = {int(value)}")
                else:
                    where_clause.append(f"{key} = {value}")
            assert where_clause, "No where clause"
            return ' and '.join(where_clause)

        table_name = self.item_type_to_table_mapping.get(entity_type)
        if table_name is None:
            return ()

        distinct_clause = ""
        if distinct_on is not None:
            distinct_clause = f"distinct on ({distinct_on})"

        try:
            where_clause = _get_where_clause(kwargs)
        except AssertionError as e:
            logger.warning(
                "Cannot make where clause for %s items from clickhouse: %s", entity_type, e
            )
            return ()
        query = f"select {distinct_clause} * from `{table_name}`" f" where {where_clause}"
        try:
            return tuple(self.clickhouse.query(query).named_results())
        except DatabaseError as e:
            if 'UNKNOWN_TABLE' in str(e):  # The error code is not exposed by the driver
                logger.warning("Cannot export %s items from clickhouse: %s", entity_type, e)
                return ()

    def select_where_with_type_assignment(
        self, entity_type: EntityType, distinct_on: str, **kwargs
    ):
        """Assigning the types for compatibility with the eth_streamer."""
        items = self.select_where(entity_type, distinct_on, **kwargs)
        for item in items:
            item['type'] = entity_type
        return items

    @staticmethod
    def get_logs_count_from_transactions(transactions: tuple) -> int:
        none_value_receipt_logs_count = any(
            d.get('receipt_logs_count') is None for d in transactions
        )
        if none_value_receipt_logs_count:
            return -1
        want_logs_count = sum(b['receipt_logs_count'] for b in transactions)
        return want_logs_count

    def _calculate_pools_count_for_tokens(self, tokens) -> dict:
        # here we calculating token score based on it's relationships ration
        # more pools existing with token - bigger score,
        # could be explored further using liquidity/volume as weights in
        # links between pools (nodes) in vector representaion of it.
        assert self.clickhouse, "Clickhouse client is not initialized"
        if not tokens:
            return {}
        query = f"""
            SELECT 
                token_address, 
                uniqMerge(pools_count) as pool_count
            FROM pools_counts
            WHERE token_address IN {tuple(tokens)}
            GROUP BY token_address
        """
        return {
            d['token_address']: d['pool_count']
            for d in self.clickhouse.query(query).named_results()
        }

    def export_all(self, start_block, end_block):
        want_block_count = end_block - start_block + 1
        should_export = self.eth_streamer.should_export

        def get_transaction_count_from_blocks(blocks: tuple) -> int | float:
            if self.chain_id == 137:
                # workaround for Polygon where block.transaction_count doesn't match the number
                # of transactions in the db
                return float('-inf')
            return sum(b['transaction_count'] for b in blocks)

        @cache
        def export_blocks_and_transactions():
            logger.info("exporting BLOCKS and TRANSACTIONS...")
            blocks = self.select_distinct(BLOCK, start_block, end_block, 'number')
            transactions = self.select_distinct(TRANSACTION, start_block, end_block, 'hash')

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
                blocks, transactions = self.eth_streamer.export_blocks_and_transactions(
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
            blocks, _, from_ch = export_blocks_and_transactions()
            return from_ch and len(blocks) == want_block_count

        @cache
        def export_receipts_and_logs():
            logger.info("exporting RECEIPTS and LOGS...")
            blocks, transactions, _ = export_blocks_and_transactions()
            receipts, logs, errors = self.eth_streamer.export_receipts_and_logs(transactions)
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
                logs = self.select_distinct(
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
                traces = self.select_distinct(TRACE, start_block, end_block, 'trace_id')
                if len(traces) > 0:
                    for t in traces:
                        t['type'] = TRACE
                    from_ch = True
                    return traces, from_ch

            logger.info(
                f"Traces. Not enough data found in clickhouse: falling back to Eth node:"
                f" entity_type=trace block_range={start_block}-{end_block}"
            )
            traces = self.eth_streamer.export_traces(start_block, end_block)
            from_ch = False
            return traces, from_ch

        @cache
        def export_geth_traces():
            logger.info("exporting GETH_TRACES...")
            if blocks_previously_exported():
                geth_traces = self.select_distinct(
                    GETH_TRACE,
                    start_block,
                    end_block,
                    'transaction_hash',
                )
                if geth_traces or not export_blocks_and_transactions()[1]:
                    for t in geth_traces:
                        t['type'] = GETH_TRACE
                        t['transaction_traces'] = t['traces_json']
                    from_ch = True
                    return geth_traces, from_ch

            logging.info(
                f"Geth traces. Not enough data found in clickhouse: falling back to Eth node:"
                f" entity_type=geth_trace block_range={start_block}-{end_block}"
            )
            transaction_hashes = [
                t['hash']
                for t in export_blocks_and_transactions()[1]
                if t.get('receipt_status') != 0
            ]
            geth_traces = self.eth_streamer.export_geth_traces(transaction_hashes)
            from_ch = False
            return geth_traces, from_ch

        @cache
        def extract_token_transfers():
            logger.info("extracting TOKEN_TRANSFERS...")
            if blocks_previously_exported():
                token_transfers_ch = self.select_distinct(
                    TOKEN_TRANSFER, start_block, end_block, 'transaction_hash,log_index'
                )
            else:
                token_transfers_ch = ()
            token_transfers = self.eth_streamer.extract_token_transfers(export_logs()[0])
            from_ch = len(token_transfers_ch) == len(token_transfers)
            return token_transfers, from_ch

        @cache
        def extract_contracts():
            logger.info("extracting CONTRACTS...")
            traces, from_ch = export_geth_traces()
            contracts = self.eth_streamer.export_contracts(traces)
            return contracts, from_ch

        @cache
        def extract_tokens():
            logger.info("extracting TOKENS...")
            contracts, from_ch = extract_contracts()
            tokens = self.eth_streamer.extract_tokens(contracts)
            return tokens, from_ch

        @cache
        def export_tokens():
            logger.info("exporting TOKENS...")
            transfers = extract_token_transfers()[0]
            token_addresses = {t['token_address'] for t in transfers}
            if not token_addresses:
                return (), False
            tokens_ch = self.select_where_with_type_assignment(
                entity_type=TOKEN, distinct_on='address', address=token_addresses
            )
            if len(tokens_ch) == len(token_addresses):
                from_ch = True
                return tokens_ch, from_ch
            from_ch = False
            absent_token_addresses = token_addresses - {t['address'] for t in tokens_ch}
            logger.info(
                f"Tokens. Not enough data found in clickhouse: falling back to Eth node:"
                f" entity_type=token token_addresses={absent_token_addresses}"
            )
            tokens = self.eth_streamer.export_tokens(absent_token_addresses)
            return tokens + list(tokens_ch), from_ch

        @cache
        def export_token_balances():
            logger.info("exporting TOKEN_BALANCES...")
            if blocks_previously_exported():
                balances = self.select_distinct(
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

            token_balances, errors = self.eth_streamer.export_token_balances(
                extract_token_transfers()[0]
            )
            from_ch = False
            return token_balances, errors, from_ch

        @cache
        def extract_internal_transfers():
            logger.info("extracting INTERNAL_TRANSFERS...")
            if blocks_previously_exported():
                internal_transfers_ch = self.select_distinct(
                    INTERNAL_TRANSFER, start_block, end_block, 'transaction_hash'
                )
                if internal_transfers_ch:
                    return internal_transfers_ch, True
            else:
                internal_transfers_ch = ()
            geth_traces, geth_traces_from_ch = export_geth_traces()
            internal_transfers = self.eth_streamer.extract_internal_transfers(geth_traces)
            from_ch = geth_traces_from_ch and len(internal_transfers_ch) == len(internal_transfers)
            return internal_transfers, from_ch

        @cache
        def export_native_balances():
            logger.info("exporting NATIVE_BALANCES...")
            if blocks_previously_exported():
                native_balances_ch = self.select_distinct(
                    NATIVE_BALANCE, start_block, end_block, 'address,block_number'
                )
            else:
                native_balances_ch = ()
            _blocks, transactions, transactions_from_ch = export_blocks_and_transactions()
            internal_transfers, internal_transfers_from_ch = extract_internal_transfers()

            if transactions_from_ch and internal_transfers_from_ch and native_balances_ch:
                from_ch = True
                for nb in native_balances_ch:
                    nb['type'] = NATIVE_BALANCE
                return native_balances_ch, from_ch

            logger.info(
                f"Native balances. Not enough data found in clickhouse: falling back to Eth node:"
                f" entity_type=native_balance block_range={start_block}-{end_block}"
            )

            native_balances = self.eth_streamer.export_native_balances(
                internal_transfers=internal_transfers,
                transactions=transactions,
            )

            from_ch = False
            return native_balances, from_ch

        @cache
        def extract_token_transfers_priced():
            logger.info(f"exporting {TOKEN_TRANSFER_PRICED.upper()}...")
            token_transfers = extract_token_transfers()[0]
            token_transfers_priced = self.eth_streamer.extract_token_transfers_priced(
                token_transfers, export_tokens()[0]
            )
            return token_transfers_priced, False

        @cache
        def extract_internal_transfers_priced():
            logger.info(f"exporting {INTERNAL_TRANSFER_PRICED.upper()}...")
            internal_transfers = extract_internal_transfers()[0]
            internal_transfers_priced = self.eth_streamer.extract_internal_transfers_priced(
                internal_transfers=internal_transfers,
                transactions=export_blocks_and_transactions()[1],
            )
            return internal_transfers_priced, False

        @cache
        def prepare_events():
            logger.info("preparing EVENTS...")
            blocks, transactions, _ = export_blocks_and_transactions()
            events = self.eth_streamer.prepare_events(
                blocks=blocks,
                logs=export_logs()[0],
                token_transfers=extract_token_transfers()[0],
                transactions=transactions,
                receipts=export_receipts()[0],
            )
            return events, False

        @cache
        def parse_logs():
            logger.info("parsing LOGS...")
            logs = export_logs()[0]
            parsed_logs = self.eth_streamer.parse_logs(logs)
            return parsed_logs, False

        @cache
        def export_dex_pools():
            assert self.clickhouse, "Clickhouse client is not initialized"
            _from_ch = False
            logger.info("exporting DEX_POOLS_INVENTORY...")
            parsed_logs = parse_logs()[0]
            if not parsed_logs:
                return (), _from_ch

            log_addresses = set()
            for log in parsed_logs:
                # check if balancer Vault is in the logs
                pool_id = log['parsed_event'].get('poolId')
                if pool_id:
                    log_address = f'0x{pool_id.hex().lower()[:40]}'
                else:
                    log_address = log['address'].lower()
                if not is_address(log_address):
                    logger.warning(
                        f"Invalid address from poolId: {log_address} for log: {log}",
                    )
                    continue
                log_addresses.add(log_address)

            existing_dex_pools = self.select_where_with_type_assignment(
                entity_type=DEX_POOL,
                distinct_on='address',
                address=log_addresses,
            )

            existing_dex_pool_addresses = {p['address'] for p in existing_dex_pools}

            logs_to_export = [
                log
                for log in parsed_logs
                if (log['address'] not in existing_dex_pool_addresses)
                or (
                    log['parsed_event'].get('poolId')
                    and f"0x{log['parsed_event']['poolId'].hex()[:40].lower()}"
                    not in existing_dex_pool_addresses
                )
            ]
            if not logs_to_export:
                _from_ch = True
                return existing_dex_pools, _from_ch

            dex_pools_inventory = self.eth_streamer.export_dex_pools(logs_to_export)

            return dex_pools_inventory + list(existing_dex_pools), _from_ch

        @cache
        def export_dex_trades():
            assert self.clickhouse, "Clickhouse client is not initialized"
            _from_ch = False
            logger.info("exporting DEX_TRADES...")
            parsed_logs = parse_logs()[0]
            if not parsed_logs:
                return (), _from_ch
            token_transfers = extract_token_transfers()[0]
            tokens = export_tokens_from_pools()[0]
            dex_pools = export_dex_pools()[0]
            dex_trades = self.eth_streamer.export_dex_trades(
                parsed_logs=parsed_logs,
                token_transfers=token_transfers,
                tokens=tokens,
                dex_pools=dex_pools,
            )
            return dex_trades, _from_ch

        def get_latest_prices_from_trades_for_tokens(base_tokens_addresses):
            # Here we are recieving prices for base tokens from last trades on those tokens
            # logic needs to be improved in future os we either have trust index for prices saved
            # along dex_trade or we would be able to calculate closest path to stable based on
            # pools route
            assert self.clickhouse, "Clickhouse client is not initialized"
            query = f"""
                        SELECT max(c_s).2 AS latest_price_stable,
                                 max(c_n).2 AS latest_price_native,
                                 token_address
                        FROM candles_5m
                        WHERE token_address IN {tuple(base_tokens_addresses)}
                        GROUP BY token_address
                    """
            return {
                d['token_address']: {
                    'token_address': d['token_address'],
                    'price_stable': d['latest_price_stable'],
                    'price_native': d['latest_price_native'],
                }
                for d in self.clickhouse.query(query).named_results()
            }

        @cache
        def import_base_tokens_prices() -> list:
            assert self.clickhouse, "Clickhouse client is not initialized"
            _from_ch = False
            logger.info("Importing TOKEN_PRICES...")
            dex_pools = export_dex_pools()[0]
            if not dex_pools:
                return []
            all_token_addresses = set()

            for pool in dex_pools:
                all_token_addresses.update(pool['token_addresses'])

            prices = self.eth_streamer.import_base_token_prices(list(all_token_addresses))
            return prices

        @cache
        def export_tokens_from_pools() -> tuple[list, bool]:
            tokens = export_tokens()[0]
            dex_pools = export_dex_pools()[0]
            _from_ch = False
            token_addresses_from_pools = set()
            for pool in dex_pools:
                token_addresses_from_pools.update(pool['token_addresses'])
                token_addresses_from_pools.update(pool['underlying_token_addresses'])
                token_addresses_from_pools.update(pool['lp_token_addresses'])
            absent_tokens = token_addresses_from_pools - {t['address'] for t in tokens}
            if absent_tokens:
                tokens_from_pools = self.select_where_with_type_assignment(
                    entity_type=TOKEN, distinct_on='address', address=absent_tokens
                )
                tokens_from_pools = list(tokens_from_pools)
                if len(tokens_from_pools) != len(absent_tokens):
                    _from_ch = False
                    absent_tokens = absent_tokens - {t['address'] for t in tokens_from_pools}
                    tokens_from_pools.extend(self.eth_streamer.export_tokens(absent_tokens))
                tokens = list(tokens) + tokens_from_pools
            return tokens, _from_ch

        @cache
        def export_enriched_dex_trades() -> tuple[Iterable, Iterable, bool]:
            assert self.clickhouse, "Clickhouse client is not initialized"
            from_ch_ = False
            logger.info("enriching DEX_TRADES...")
            # enriched_dex_trades = self.select_distinct(
            #     ENRICHED_DEX_TRADE, start_block, end_block, 'transaction_hash,log_index'
            # )
            # if enriched_dex_trades:
            #     for t in enriched_dex_trades:
            #         t['type'] = ENRICHED_DEX_TRADE
            #     return enriched_dex_trades, from_ch_
            dex_trades = export_dex_trades()[0]
            dex_pools = export_dex_pools()[0]
            tokens = export_tokens_from_pools()[0]
            token_transfers = extract_token_transfers()[0]
            # internal_transfers = extract_internal_transfers()[0]
            transactions = export_blocks_and_transactions()[1]
            base_tokens_prices = import_base_tokens_prices()
            enriched_dex_trades, enriched_transfers = self.eth_streamer.export_enriched_dex_trades(
                dex_trades=dex_trades,
                dex_pools=dex_pools,
                base_tokens_prices=base_tokens_prices,
                tokens=tokens,
                token_transfers=token_transfers,
                # internal_transfers=[],
                transactions=transactions,
            )
            return enriched_dex_trades, enriched_transfers, from_ch_

        exported: dict[EntityType, list] = {ERROR: []}
        from_ch: dict[EntityType, bool] = {}
        if CONTRACT in self.entity_types:
            export_tokens_job = export_tokens
        elif DEX_POOL in self.entity_types:
            export_tokens_job = export_tokens_from_pools
        else:
            export_tokens_job = export_tokens
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
            ((NATIVE_BALANCE,), export_native_balances),
            # if contract export is enabled, export tokens based on CREATE action from traces (indexing on create)
            # otherwise, export tokens based on token transfers (indexing on event)
            ((TOKEN,), export_tokens_job),
            ((TOKEN_TRANSFER_PRICED,), extract_token_transfers_priced),
            ((INTERNAL_TRANSFER_PRICED,), extract_internal_transfers_priced),
            ((PRE_EVENT,), prepare_events),
            ((DEX_POOL,), export_dex_pools),
            ((PARSED_LOG,), parse_logs),
            ((DEX_TRADE,), export_dex_trades),
            ((ENRICHED_DEX_TRADE, ENRICHED_TRANSFER), export_enriched_dex_trades),
        ):
            for entity_type in entity_types:
                if entity_type not in self.eth_streamer.should_export:
                    continue

                *results, from_ch_ = export_func()

                for items, entity_type_ in zip(results, entity_types):
                    exported.setdefault(entity_type_, []).extend(items)
                    from_ch[entity_type_] = from_ch_

                break

        all_items = []
        items_by_type = {}
        for entity_type in self.entity_types:
            if (
                from_ch.get(entity_type)
                and self.exporting_to_the_same_clickhouse
                and entity_type not in self.rewrite_entity_types
                and entity_type != EntityType.ERROR
            ):
                continue
            items = exported[entity_type]
            enriched_items = self.eth_streamer.enrich(entity_type, exported.__getitem__)
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
            sorted_items = sort_by(enriched_items, self.eth_streamer.SORT_BY_FIELDS[entity_type])
            all_items.extend(sorted_items)
            items_by_type[entity_type] = sorted_items

        self.eth_streamer.log_batch_export_progress(items_by_type)

        self.eth_streamer.calculate_item_ids(all_items)
        self.eth_streamer.calculate_item_timestamps(all_items)

        self.eth_streamer.item_exporter.export_items(all_items)

    def close(self):
        try:
            if self.clickhouse:
                self.clickhouse.close()
                self.clickhouse = None
        finally:
            self.eth_streamer.close()

    @cached_property
    def exporting_to_the_same_clickhouse(self) -> bool:
        exporter = self.eth_streamer.item_exporter
        if isinstance(exporter, MultiItemExporter):
            for exporter in exporter.item_exporters:
                if isinstance(exporter, ClickHouseItemExporter):
                    break

        if not isinstance(exporter, ClickHouseItemExporter):
            return False

        params = parse_clickhouse_url(self.clickhouse_url)

        return (
            params['host'] == exporter.host
            and params['port'] == exporter.port
            and params.get('database', exporter.database) == exporter.database
            and self.item_type_to_table_mapping == exporter.item_type_to_table_mapping
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
        self._validate_export_destination()
        self.skip_entity_types = [ERROR, *ALL_STATIC]

    def _validate_export_destination(self):
        msg = 'VerifyingClickhouseEthStreamerAdapter can be used only when exporting to the same ClickHouse instance'
        if isinstance(self.ch_streamer.eth_streamer.item_exporter, MultiItemExporter):
            for exporter in self.ch_streamer.eth_streamer.item_exporter.item_exporters:
                if isinstance(exporter, ClickHouseItemExporter):
                    assert self.ch_streamer.exporting_to_the_same_clickhouse, msg

        elif isinstance(self.ch_streamer.eth_streamer.item_exporter, ClickHouseItemExporter):
            assert self.ch_streamer.exporting_to_the_same_clickhouse, msg

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
                block_numbers_from_storage = [block_['number'] for block_ in blocks_from_storage]
                for block in blocks_from_w3:
                    if block['number'] not in block_numbers_from_storage:
                        missing_blocks.add(block['number'])
                blocks_from_w3 = [
                    block for block in blocks_from_w3 if block['number'] not in missing_blocks
                ]

            # check block hashes
            for block_ch, block_w3 in zip(blocks_from_storage, blocks_from_w3):
                try:
                    check_blocks_consistency(block_ch, block_w3)
                except AssertionError:
                    inconsistent_blocks.add(block_ch['number'])
                    inconsistent_timestamps.add(block_ch['timestamp'])
                    inconsistent_hashes.add(block_ch['hash'])

        def check_blocks_consistency(block_ch, block_w3):
            assert block_ch['number'] == block_w3['number']
            assert block_ch['hash'] == block_w3['hash']

        def mark_records_as_reorged(
            blocks: Iterable,
            timestamps: Iterable,
            hashes: Iterable,
        ):
            """
            Mark all records with the given block numbers and block hashes as reorged.
            Timestamps are used to speed up some queries.
            """
            if not blocks or not timestamps or not hashes:
                return

            def safe_execute(command):
                assert self.ch_streamer.clickhouse
                while True:
                    try:
                        self.ch_streamer.clickhouse.command(command)
                        break
                    except DatabaseError as e:
                        if 'UNKNOWN_TABLE' in str(
                            e
                        ):  # The error code is not exposed by the driver
                            logger.warning("Skip unsupported entity %s: %s", entity, e)
                            self.skip_entity_types.append(entity)
                            break
                    except Exception as e:
                        logger.warning('Error while executing command: %s', e)
                        logger.warning('Retrying in 2 seconds')
                        sleep(2)

            for entity, table in self.ch_streamer.item_type_to_table_mapping.items():
                if entity in self.skip_entity_types:
                    continue
                if entity == BLOCK:
                    where_condition = (
                        f"WHERE number IN {tuple(blocks)}"
                        f" AND timestamp IN {tuple(timestamps)}"
                        f" AND hash IN {tuple(hashes)}"
                    )
                elif entity == LOG:
                    where_condition = (
                        f"WHERE block_number IN {tuple(blocks)} AND block_hash IN {tuple(hashes)}"
                    )
                else:
                    where_condition = (
                        f"WHERE block_number IN {tuple(blocks)}"
                        f" AND block_timestamp IN {tuple(timestamps)}"
                        f" AND block_hash IN {tuple(hashes)}"
                    )
                query = f"INSERT INTO {table} SELECT * EXCEPT is_reorged, 1 FROM {table} {where_condition}"
                logger.warning('Marking records as reorged: %s', query)
                safe_execute(query.strip())

        inconsistent_blocks: set[int] = set()
        inconsistent_timestamps: set[int] = set()
        inconsistent_hashes: set[str] = set()
        missing_blocks: set[int] = set()

        blocks_ch = self.ch_streamer.select_distinct(BLOCK, start_block, end_block, 'number')

        (
            blocks_w3,
            transactions_w3,
        ) = self.ch_streamer.eth_streamer.export_blocks_and_transactions(start_block, end_block)
        blocks_w3 = sorted(blocks_w3, key=lambda x: x['number'])
        blocks_ch = sorted(blocks_ch, key=lambda x: x['number'])  # type: ignore

        find_inconsistent_blocks(blocks_ch, blocks_w3)

        if inconsistent_blocks or missing_blocks:
            if inconsistent_blocks:
                logger.warning(
                    "Inconsistent blocks were found: %s count %i",
                    inconsistent_blocks,
                    len(inconsistent_blocks),
                    extra={
                        'inconsistent_blocks': inconsistent_blocks,
                        'inconsistent_blocks_count': len(inconsistent_blocks),
                    },
                )
            if missing_blocks:
                logger.warning(
                    "Missing blocks were found: %s count: %i",
                    missing_blocks,
                    len(missing_blocks),
                    extra={
                        'missing_blocks': missing_blocks,
                        'missing_blocks_count': len(missing_blocks),
                    },
                )
            all_blocks = sorted(inconsistent_blocks | missing_blocks)
            block_sequences = [
                list(g) for k, g in groupby(all_blocks, key=lambda x: x - all_blocks.index(x))
            ]
            for seq in block_sequences:
                mark_records_as_reorged(
                    inconsistent_blocks & set(seq),
                    sorted(inconsistent_timestamps),
                    sorted(inconsistent_hashes),
                )
                self.ch_streamer.eth_streamer.export_all(
                    start_block=min(seq),
                    end_block=max(seq),
                )
            logger.info("Inconsistent records were exported to ClickHouse")
