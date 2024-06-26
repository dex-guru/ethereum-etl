"""
add categories table.

Revision ID: dcc43e5b67a3
Revises: 4103ad73e38e
Create Date: 2024-03-11 16:27:27.470731
"""
import os
import string
from functools import cache

from alembic import op

# revision identifiers, used by Alembic.
revision = 'dcc43e5b67a3'
down_revision = '4103ad73e38e'
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
        create table traders_categories $on_cluster
        (
            wallet_address     String,
            day                Date,
            volume             Float64,
            swaps_count        UInt64,
            liquidity          Float64,
            mints_count        UInt64,
            volume_category    LowCardinality(String),
            lp_category        LowCardinality(String)
        )
        engine = ${replicated}MergeTree PARTITION BY day
            ORDER BY wallet_address
            SETTINGS index_granularity = 512;
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
    schema_template = "DROP TABLE traders_categories $on_cluster SYNC"

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