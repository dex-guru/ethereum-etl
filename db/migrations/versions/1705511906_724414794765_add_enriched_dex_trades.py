"""
add enriched dex trades.

Revision ID: 724414794765
Revises: 8567893486ad
Create Date: 2024-01-17 18:18:26.976311
"""
import os
import string
from functools import cache

from alembic import op

# revision identifiers, used by Alembic.
revision = '724414794765'
down_revision = '8567893486ad'
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
    CREATE TABLE dex_trades $on_cluster
    (
        block_number UInt64 CODEC(ZSTD(1)),
        block_hash String CODEC(ZSTD(1)),
        block_timestamp UInt64 CODEC(ZSTD(1)),
        transaction_hash String CODEC(ZSTD(1)),
        log_index UInt64 CODEC(ZSTD(1)),
        transaction_type String CODEC(ZSTD(1)),
        token_addresses Array(String) CODEC(ZSTD(1)),
        amounts Array(Float64) CODEC(ZSTD(1)),
        amount_stable Float64 CODEC(ZSTD(1)),
        amount_native Float64 CODEC(ZSTD(1)),
        prices_stable Array(Float64) CODEC(ZSTD(1)),
        prices_native Array(Float64) CODEC(ZSTD(1)),
        pool_address String CODEC(ZSTD(1)),
        wallet_address String CODEC(ZSTD(1)),
        is_reorged Bool DEFAULT 0,
        INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
    )
    ENGINE = ${replicated}ReplacingMergeTree
    ORDER BY (block_number, transaction_hash, log_index)
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
    schema_template = "DROP TABLE dex_trades $on_cluster SYNC"

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

    op.execute(sql)
