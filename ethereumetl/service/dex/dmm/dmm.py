import logging

from web3 import Web3

from ethereumetl.service.dex.uniswap_v2.uniswap_v2 import UniswapV2Amm

logs = logging.getLogger(__name__)

FACTORY_CONTRACT = "IDMMFactory"
POOL_CONTRACT = "IDMMPool"


class DMMAmm(UniswapV2Amm):

    POOL_ABI_PATH = "IDMMPool.json"

    def __init__(self, web3: Web3, chain_id: int | None = None, file_path: str = __file__):
        super().__init__(web3, chain_id, file_path)
