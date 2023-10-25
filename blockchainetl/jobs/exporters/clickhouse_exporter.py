import collections
import json
import logging
from dataclasses import dataclass

import clickhouse_connect
import clickhouse_connect.datatypes.numeric as types
from clickhouse_connect.driver.exceptions import DatabaseError
from clickhouse_connect.driver.models import ColumnDef

from blockchainetl.exporters import BaseItemExporter
from ethereumetl.clickhouse import ITEM_TYPE_TO_TABLE_MAPPING
from ethereumetl.config.envs import envs
from ethereumetl.utils import parse_clickhouse_url

logger = logging.getLogger(__name__)


@dataclass
class Table:
    column_names: list[str]
    column_types: list[str]


MIN_INSERT_BATCH_SIZE = envs.MIN_INSERT_BATCH_SIZE

NUMERIC_TYPE_MAX_VALUES = {
    types.UInt8: 2**8 - 1,
    types.UInt16: 2**16 - 1,
    types.UInt32: 2**32 - 1,
    types.UInt64: 2**64 - 1,
    types.UInt128: 2**128 - 1,
    types.UInt256: 2**256 - 1,
    types.Int8: 2**7 - 1,
    types.Int16: 2**15 - 1,
    types.Int32: 2**31 - 1,
    types.Int64: 2**63 - 1,
    types.Int128: 2**127 - 1,
    types.Int256: 2**255 - 1,
}


class ClickHouseItemExporter(BaseItemExporter):
    def __init__(self, connection_url):
        super().__init__()
        parsed = parse_clickhouse_url(connection_url)
        self.username = parsed['user']
        self.password = parsed['password']
        self.host = parsed['host']
        self.port = parsed['port']
        self.database = parsed['database']
        self.settings = parsed['settings']
        self.connection: clickhouse_connect.driver.HttpClient | None = None
        self.tables = {}
        self.cached_batches = {}
        self.item_type_to_table_mapping = ITEM_TYPE_TO_TABLE_MAPPING

    def open(self):
        if self.connection:
            raise RuntimeError('Connection already opened.')
        self.connection = self.create_connection()

        ## for each time grab the schema to save a prefetch of the columns on each insert
        for table in self.item_type_to_table_mapping.values():
            try:
                describe_result = self.connection.query(f'DESCRIBE TABLE {self.database}.{table}')
                column_defs = [
                    ColumnDef(**row)
                    for row in describe_result.named_results()
                    if row['default_type'] not in ('ALIAS', 'MATERIALIZED')
                ]
                column_names = [cd.name for cd in column_defs]
                column_types = [cd.ch_type for cd in column_defs]
                self.tables[table] = Table(column_names, column_types)
                self.cached_batches[table] = []
            except DatabaseError as de:
                # this may not be critical since the user may not be exporting the type and hence the table likely
                # won't exist
                logger.warning(
                    f'Unable to read columns for table "{table}". This column will not be exported.'
                )
                logger.debug(de)
                pass

    def export_item(self, item):
        self.export_items([item])

    def export_items(self, items):
        items_grouped_by_table = self.group_items_by_table(items)
        for item_type, table in self.item_type_to_table_mapping.items():
            table_data = items_grouped_by_table.get(table)
            if table_data:
                cached = self.cached_batches[table]
                batch = cached + table_data
                if len(batch) >= MIN_INSERT_BATCH_SIZE:
                    logger.info(f'Flushing batch for "{item_type}" with {len(batch)} items.')
                    column_names = self.tables[table].column_names
                    column_types = self.tables[table].column_types
                    self._insert(column_names, column_types, table, batch)
                    self.cached_batches[table] = []
                else:
                    # insufficient size, so cache
                    logger.debug(
                        f'Batch for "{item_type}" is too small to be flushed'
                        f' ({len(batch)}<{MIN_INSERT_BATCH_SIZE}), caching.'
                    )
                    self.cached_batches[table] = batch

    def _insert(self, column_names, column_types, table, table_data):
        if self.connection is None:
            raise RuntimeError('Connection is not open.')
        try:
            self.connection.insert(
                table,
                data=table_data,
                column_names=column_names,
                column_types=column_types,
                database=self.database,
            )
        except clickhouse_connect.driver.exceptions.ProgrammingError as e:
            for row in table_data:
                for i, column_value in enumerate(row):
                    column_type = column_types[i]
                    column_name = column_names[i]
                    if isinstance(column_value, int):
                        max_value = NUMERIC_TYPE_MAX_VALUES.get(type(column_type))
                        if max_value is None:
                            raise
                        if column_value > max_value:
                            logger.warning(
                                "Insert error: too large column value:"
                                " table=%s column=%s column_type=%s"
                                " column_value=%s row_data_json=%s",
                                table,
                                column_name,
                                column_type.__class__.__name__,
                                column_value,
                                json.dumps(dict(zip(column_names, row))),
                            )
                            raise OverflowError(
                                f"Too large column value: table={table} column={column_name}"
                            ) from e
                    elif column_value is None and not column_type.nullable:
                        logger.error(
                            "Insert error: cannot insert null value into non-nullable column:"
                            " table=%s column=%s column_type=%s row_data_json=%s",
                            table,
                            column_name,
                            column_type.__class__.__name__,
                            json.dumps(dict(zip(column_names, row))),
                        )
                        raise TypeError(
                            "Cannot insert null value into non-nullable column:"
                            f" table={table} column={column_name}"
                        ) from e
            raise

    def create_connection(self):
        return clickhouse_connect.create_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
            settings=self.settings,
            compress=False,
            send_receive_timeout=600,
        )

    def close(self):
        try:
            # clear the cache
            logger.info("Flushing remaining batches")
            for table, batch in self.cached_batches.items():
                self._insert(
                    self.tables[table].column_names, self.tables[table].column_types, table, batch
                )
        finally:
            self.cached_batches.clear()
            if self.connection:
                self.connection.close()

    def group_items_by_table(self, items):
        results = collections.defaultdict(list)
        for item in items:
            type_ = item.get('type')
            if type_ in self.item_type_to_table_mapping:
                table = self.item_type_to_table_mapping[type_]
                if table not in self.tables:
                    logger.error(
                        f'Table "{table}" does not exist. Type "{type_}" cannot be exported.'
                    )
                result = []
                # only insert the columns which we have in the database
                for column in self.tables[table].column_names:
                    result.append(item.get(column))
                results[table].append(result)
            else:
                continue
        return results
