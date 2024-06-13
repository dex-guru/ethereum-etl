"""
add factory address in dex trades.

Revision ID: e3ce81fe0110
Revises: 3799feafd328
Create Date: 2024-01-24 15:01:04.905561
"""
import os
import string
from functools import cache

from alembic import op

# revision identifiers, used by Alembic.
revision = 'e3ce81fe0110'
down_revision = '3799feafd328'
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
    ALTER TABLE dex_trades $on_cluster ADD COLUMN factory_address LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)) AFTER pool_address;
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
    schema_template = "ALTER TABLE dex_trades $on_cluster DROP COLUMN factory_address;"

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
