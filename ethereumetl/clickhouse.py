import json
import logging
import os
import random
import re
import string
from collections import defaultdict
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from string import ascii_uppercase
from textwrap import dedent
from typing import TextIO
from unittest import mock

from alembic import command as alembic_command
from alembic.config import Config as AlembicConfig
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection

import ethereumetl

logger = logging.getLogger(__name__)

SCHEMA_FILE_PATH = Path(ethereumetl.__file__).parent.parent / 'db/migrations/schema.sql'


table_names_sort_key = re.compile(r'(\d+|_)').split


def get_schema_ddls(url: str, strip_db_prefix: bool = True, parallel=40) -> Iterable[str]:
    engine = create_engine(url, pool_size=parallel)
    queries = list(schema_ddl_queries(engine))
    with ThreadPoolExecutor(parallel) as pool:
        ddls = pool.map(engine.scalar, queries)

        if strip_db_prefix:
            db_name = engine.scalar("SELECT currentDatabase()")
            db_prefix1 = f'{db_name}.'
            db_prefix2 = f'`{db_name}`.'

            def rm_db_prefix(ddl):
                return ddl.replace(db_prefix1, '').replace(db_prefix2, '')

            ddls = map(rm_db_prefix, ddls)

        yield from ddls


def schema_ddl_queries(connection: Connection) -> Iterable[str]:
    table_dependents = {
        name: json.loads(deps)
        for name, deps in connection.execute(
            "SELECT name, toJSONString(dependencies_table)"
            " FROM system.tables"
            " WHERE database = currentDatabase()"
            " ORDER BY name"
        )
    }

    table_dependencies: dict[str, list[str]] = defaultdict(list)
    for table, dependents in table_dependents.items():
        for dependent in dependents:
            table_dependencies[dependent].append(table)

    for deps in table_dependencies.values():
        deps[:] = set(deps)
        deps.sort(key=table_names_sort_key)

    processed = set()

    def process_table(table):
        if table in processed:
            return

        for dependent in table_dependencies[table]:
            yield from process_table(dependent)

        yield f"SHOW CREATE TABLE `{table}`"

        processed.add(table)

    for table in sorted(table_dependents, key=table_names_sort_key):
        yield from process_table(table)

    for dictionary_name in connection.execute("SHOW DICTIONARIES").scalars():
        yield f"SHOW CREATE DICTIONARY `{ dictionary_name }`"


def write_schema_ddl(url: str, f: TextIO, strip_db_prefix=True):
    ddls = iter(get_schema_ddls(url, strip_db_prefix))
    for ddl in ddls:
        f.write(ddl)
        f.write(';\n')
        break
    for ddl in ddls:
        f.write('\n')
        f.write(ddl)
        f.write(';\n')


def write_schema_ddl_by_url_to_file_path(
    clickhouse_url: str, file_path: str, strip_db_prefix=True
):
    with open(file_path, 'w') as f:
        write_schema_ddl(clickhouse_url, f, strip_db_prefix)


def migrate_up(clickhouse_url: str, revision: str = 'head'):
    """
    Applies Alembic migrations up to the given revision.
    """
    alembic_ini_path = Path(ethereumetl.__file__).parents[1] / 'alembic.ini'
    with mock.patch(
        'os.environ',
        {**os.environ.copy(), 'CLICKHOUSE_URL': clickhouse_url},
    ):
        alembic_cfg = AlembicConfig(alembic_ini_path)
        alembic_command.upgrade(alembic_cfg, revision)


def dump_migration_schema(clickhouse_url, schema_file_path, temp_db_name: str | None = None):
    if temp_db_name is None:
        random_suffix = "".join(random.sample(ascii_uppercase, 6))
        temp_db_name = '_tmp_ethereumetl_migration_' + random_suffix

    with (
        create_engine(clickhouse_url).connect() as connection,
        open(schema_file_path, 'w') as f,
    ):
        connection.execute(f'create database {temp_db_name}')
        try:
            connection.execute(f'USE {temp_db_name}')
            assert connection.scalar('SELECT currentDatabase()') == temp_db_name

            temp_db_url = connection.engine.url.set(database=temp_db_name)
            logger.info(
                f'Exporting schema DDL'
                f' from "{temp_db_url.render_as_string()}"'
                f' to "{schema_file_path}"...'
            )

            migrate_up(temp_db_url.render_as_string(hide_password=False), 'head')
            write_schema_ddl(temp_db_url.render_as_string(hide_password=False), f)
        finally:
            connection.execute(f'drop database {temp_db_name} sync')


def render_sql_template(is_replicated_clickhouse: bool, template: str, **kwargs) -> str:
    if is_replicated_clickhouse:
        replicated_merge_tree_prefix = "Replicated"
        replicated_merge_tree_params = (
            "'/clickhouse/tables/{shard}/{database}/{table}', '{replica}'"
        )
        replicated_merge_tree_params_with_comma = replicated_merge_tree_params + ","
        on_cluster = "ON CLUSTER '{cluster}'"

        replacing_merge_tree = "ReplicatedReplacingMergeTree"
    else:
        replicated_merge_tree_params = ""
        replicated_merge_tree_params_with_comma = ""
        replicated_merge_tree_prefix = ""
        on_cluster = ""
        replacing_merge_tree = "ReplacingMergeTree"

    params = dict(
        replacing_merge_tree=replacing_merge_tree,
        replicated_merge_tree_prefix=replicated_merge_tree_prefix,
        replicated_merge_tree_params=replicated_merge_tree_params,
        replicated_merge_tree_params_with_comma=replicated_merge_tree_params_with_comma,
        on_cluster=on_cluster,
        **kwargs,
    )

    return dedent(string.Template(template).substitute(params))
