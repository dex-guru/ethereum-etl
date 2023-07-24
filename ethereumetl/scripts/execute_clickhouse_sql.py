import logging
from pathlib import Path
from string import Template
from textwrap import indent

import click
from clickhouse_connect import create_client

from ethereumetl.utils import parse_clickhouse_url

log = logging.getLogger(__name__)


def render_template(template: str, variables: dict) -> str:
    return Template(template).substitute(variables)


def execute_clickhouse_sql(
    chain_id: int,
    clickhouse_url: str,
    on_cluster: str,
    replacing_merge_tree: str,
    dry_run: bool = True,
):
    sql_file_path = Path(__file__).parent / 'clickhouse.sql.tpl'

    params = {
        'chain_id': chain_id,
        'on_cluster': on_cluster,
        'replacing_merge_tree': replacing_merge_tree,
    }
    sqls = render_template(sql_file_path.read_text(), params)

    connect_kwargs = parse_clickhouse_url(clickhouse_url)
    host, port, database = (
        connect_kwargs['host'],
        connect_kwargs['port'],
        connect_kwargs['database'],
    )
    assert host
    assert port
    assert database

    client = create_client(**connect_kwargs)

    for sql in sqls.split(';'):
        sql = sql.strip()
        if not sql:
            continue

        log.info('SQL:\n%s\n', indent(sql, '    '))

        if dry_run:
            continue

        log.info('Executing on %s:%s/%s', host, port, database)
        client.command(sql)


@click.command()
@click.option('-c', '--chain-id', required=True, type=int, help='The chain id.')
@click.option('-u', '--clickhouse-url', required=True, type=str, help='The Clickhouse URL.')
@click.option(
    '-r',
    '--replacing-merge-tree',
    default='ReplacingMergeTree',
    type=str,
    help='ReplacingMergeTree clause. Default: "ReplacingMergeTree"',
)
@click.option('-k', '--on-cluster', default='', type=str, help='ON CLUSTER clause. Default: ""')
@click.option('-n', '--dry-run', is_flag=True, help='Do not execute SQL.')
def main(*, chain_id, clickhouse_url, dry_run, on_cluster, replacing_merge_tree):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
    execute_clickhouse_sql(chain_id, clickhouse_url, on_cluster, replacing_merge_tree, dry_run)


if __name__ == '__main__':
    main()
