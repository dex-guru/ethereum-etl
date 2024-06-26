"""
add underlying tokens for pools.

Revision ID: 8567893486ad
Revises: 3abace44b1b2
Create Date: 2024-01-16 14:04:00.378693
"""
import os
import string
from functools import cache

from alembic import op

# revision identifiers, used by Alembic.
revision = '8567893486ad'
down_revision = '3abace44b1b2'
branch_labels = None
depends_on = None


@cache
def is_clickhouse_replicated():
    if os.getenv('CLICKHOUSE_REPLICATED', '').lower() in ('true', '1'):
        return True

    result = op.get_bind().execute("SELECT count() FROM system.replicas")
    if result:
        return result.one()[0] > 0

    return False


def upgrade() -> None:
    schema_template = """
    DROP TABLE dex_pools $on_cluster SYNC;
    create table dex_pools
    (
        address            String CODEC(ZSTD(1)),
        factory_address LowCardinality(String),
        token_addresses    Array(String) CODEC(ZSTD(1)),
        lp_token_addresses Array(String) CODEC(ZSTD(1)),
        fee                UInt16,
        underlying_token_addresses  Array(String)
    )
    engine = EmbeddedRocksDB PRIMARY KEY address;
    """

    if is_clickhouse_replicated():
        on_cluster = "ON CLUSTER '{cluster}'"
        replicated = "Replicated"
    else:
        on_cluster = ""
        replicated = ""

    sql = string.Template(schema_template).substitute(
        on_cluster=on_cluster,
        replicated=replicated,
    )
    statements = filter(None, map(str.strip, sql.split(";\n")))
    for statement in statements:
        op.execute(statement)


def downgrade() -> None:
    schema_template = """
    DROP TABLE dex_pools $on_cluster SYNC;
    CREATE TABLE `dex_pools` $on_cluster
    (
        `address` String CODEC(ZSTD(1)),
        `factory_address` LowCardinality(String),
        `token_addresses` Array(String) CODEC(ZSTD(1)),
        `lp_token_addresses` Array(String) CODEC(ZSTD(1)),
        `fee` UInt16
    )
    ENGINE = EmbeddedRocksDB
    PRIMARY KEY address;
    """

    if is_clickhouse_replicated():
        on_cluster = "ON CLUSTER '{cluster}'"
        replicated = "Replicated"
    else:
        on_cluster = ""
        replicated = ""

    _ = replicated

    sql = string.Template(schema_template).substitute(
        on_cluster=on_cluster,
    )

    statements = filter(None, map(str.strip, sql.split(";\n")))
    for statement in statements:
        op.execute(statement)
