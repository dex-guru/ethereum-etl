from ethereumetl.service.dex.uniswap_v2.uniswap_v2 import UniswapV2Amm


class SolidlyAmm(UniswapV2Amm):

    POOL_ABI_PATH = "Pool.json"

    def __init__(self, web3, chain_id: int | None = None, file_path: str = __file__):
        super().__init__(web3, chain_id, file_path)
