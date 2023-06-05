import click

from ethereumetl.config.envs import envs
from ethereumetl.scripts.optimize_tables import optimize_tables_service


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option(
    '-c', '--chain-id', default=envs.CHAIN_ID, show_default=True, type=int, help='Chain ID'
)
@click.option(
    '-p',
    '--by-partition',
    default=True,
    show_default=True,
    type=bool,
    help='by_partition',
)
def optimize_tables(by_partition, chain_id):
    """Extracts field from given CSV or JSON newline-delimited file."""
    optimize_tables_service(chain_id, by_partition)
