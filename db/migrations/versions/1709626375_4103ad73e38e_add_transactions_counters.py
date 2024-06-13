"""
add transactions counters.

Revision ID: 4103ad73e38e
Revises: e53d5b572cab
Create Date: 2024-03-05 15:12:55.521340
"""
import os
import string
from functools import cache

from alembic import op

# revision identifiers, used by Alembic.
revision = '4103ad73e38e'
down_revision = 'e53d5b572cab'
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
        CREATE TABLE transactions_count $on_cluster
        (
            `date` Date,
            `transactions_count` Int256
        )
        engine = ${replicated}SummingMergeTree ORDER BY date
        SETTINGS index_granularity = 8192;
        
        CREATE MATERIALIZED VIEW transactions_count_mv TO transactions_count AS
        SELECT
            date AS date,
            sum(count) AS transactions_count
        FROM
        (
            SELECT if(is_reorged > 0,
                   sum(toInt256(-transaction_count)),
                   sum(toInt256(transaction_count))) AS count,
                   toDate(toStartOfDay(FROM_UNIXTIME(timestamp))) AS date
            FROM blocks
            GROUP BY is_reorged, date
        )
        GROUP BY date;
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
    schema_template = """DROP VIEW transactions_count_mv $on_cluster SYNC;
                          DROP TABLE transactions_count $on_cluster SYNC;"""

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

