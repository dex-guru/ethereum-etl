import logging

import click

from ethereumetl.clickhouse import write_schema_ddl_by_url_to_file_path


@click.command()
@click.option('-u', '--clickhouse-url', envvar='CLICKHOUSE_URL', required=True)
@click.option(
    '-o',
    '--output-file-path',
    type=click.Path(dir_okay=False, writable=True),
    default='/dev/stdout',
    show_default=True,
)
def cli(clickhouse_url: str, output_file_path: str):
    logging.basicConfig(level=logging.INFO, format='%(levelname)-5.5s [%(name)s] %(message)s')

    logging.info(f'Exporting schema DDL to "{output_file_path}"...')

    write_schema_ddl_by_url_to_file_path(clickhouse_url, output_file_path)

    logging.info(f'Done. Schema DDL is written to "{output_file_path}".')


if __name__ == '__main__':
    cli()
