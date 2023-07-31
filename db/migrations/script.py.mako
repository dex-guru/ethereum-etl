"""
${message}.

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}
"""
import os
from functools import cache

from alembic import op

from ethereumetl.clickhouse import render_sql_template
${imports if imports else ""}

# revision identifiers, used by Alembic.
revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}
branch_labels = ${repr(branch_labels)}
depends_on = ${repr(depends_on)}


@cache
def is_clickhouse_replicated():
    if os.getenv('CLICKHOUSE_REPLICATED', '').lower() in ('true', '1'):
        return True

    result = op.get_bind().execute("SELECT count() FROM system.replicas")
    if result:
        return result.one()[0] > 0

    return False


def upgrade() -> None:
    sql = render_sql_template(
        is_clickhouse_replicated(),
        """
        CREATE TABLE ${"${chain_id}"}_example
        (
            `a` UInt64,
            `b` String,
            `version` UInt32,
        )
        ENGINE = ${"${replicated_merge_tree_prefix}"}ReplacingMergeTree(${"${replicated_merge_tree_params_with_comma}"} version)
        ORDER BY a
        """,
        chain_id=1,
    )

    op.execute(sql)


def downgrade() -> None:
    op.execute(
        render_sql_template(
            is_clickhouse_replicated(),
            "DROP TABLE ${"${chain_id}"}_example SYNC",
            chain_id=1,
        )
    )
