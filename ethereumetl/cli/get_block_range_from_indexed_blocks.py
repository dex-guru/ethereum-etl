import click

from blockchainetl.file_utils import smart_open
from blockchainetl.logging_utils import logging_basic_config
from blockchainetl.streaming.streamer import LastSyncedBlockProviderSQL
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.web3_utils import build_web3

logging_basic_config()


@click.command(context_settings={'help_option_names': ['-h', '--help']})
@click.option(
    '-p',
    '--provider-uri',
    default='https://mainnet.infura.io',
    show_default=True,
    type=str,
    help='The URI of the web3 provider e.g. '
    'file://$HOME/Library/Ethereum/geth.ipc or https://mainnet.infura.io',
)
@click.option(
    '-o',
    '--output',
    default='-',
    show_default=True,
    type=str,
    help='The output file. If not specified stdout is used.',
)
@click.option(
    '-c',
    '--chain_id',
    required=True,
    show_default=True,
    type=int,
    help='The chain network to connect to.',
)
@click.option(
    '-d',
    '--last_synced_block_provider',
    required=True,
    type=str,
)
def get_block_range_to_index(provider_uri, output, chain_id, last_synced_block_provider):
    """Outputs start and end blocks for given date."""
    provider = get_provider_from_uri(provider_uri)
    web3 = build_web3(provider)
    last_synced_block_provider = LastSyncedBlockProviderSQL(
        last_synced_block_provider,
        chain_id,
        'last_synced_block',
    )

    end_block = web3.eth.getBlock('latest').number
    start_block = last_synced_block_provider.get_last_synced_block() + 1

    with smart_open(output, 'w') as output_file:
        output_file.write(f'{start_block},{end_block}\n')


#
# get_block_range_to_index.callback(
#     provider_uri='http://rpc-gw-stage.dexguru.biz/full/1',
#     output='-',
#     chain_id=1,
#     last_synced_block_provider='clickhouse://testuser3:testplpassword@stage-ch-eth-01.dexguru.biz/ethereum?table_name=last_synced_block',
# )
