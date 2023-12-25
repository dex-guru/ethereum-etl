import json
from pathlib import Path

from web3 import Web3

from ethereumetl.service.dex.base.interface import DexClientInterface
from ethereumetl.service.dex.meshswap.meshswap import MeshswapAmm
from ethereumetl.service.dex.uniswap_v2.uniswap_v2 import UniswapV2Amm
from ethereumetl.service.dex.uniswap_v3.uniswap_v3 import UniswapV3Amm


class AmmClientFactory:
    """Creates an instance of an AMM client based on the AMM type."""

    METADATA_FILE_NAME = 'metadata.json'

    default_adapters = {
        "uniswap_v2": UniswapV2Amm,
        "uniswap_v3": UniswapV3Amm,
        "meshswap": MeshswapAmm,
        "solidly": UniswapV2Amm,
        # "dmm": DMMAmm,
        # "dodo": DODOv1Amm,
        # "dodo_v2": DODOv2Amm,
        # "1inch": OneInchAmm,
        # "curve": CurveAmm,
        # "bancor_v2": BancorV2Amm,
        # "ellipsis": EllipsisAmm,
        # "balancer": BalancerAmm,
        # "sushiswap_bento": SushiSwapBentoAmm,
        # "saddle": SaddleAmm,
        # "platypus": PlatypusAmm,
        # "kyberswap_elastic": KyberSwapElasticAmm,
        # "wombat": WombatAmm,
        # "canto_dex": CantoDexAmm,
        "pancakeswap_v3": UniswapV3Amm,
        # "quickswap_v3": QuickswapV3Amm,
        # "traderjoe_v2_1": TraderJoeV21Amm,
    }

    @classmethod
    def get_dex_client(cls, amm_type: str, web3: Web3, chain_id: int) -> DexClientInterface:
        adapter_class = cls.default_adapters.get(amm_type)
        if not adapter_class:
            raise ValueError(f"Unsupported AMM type: {amm_type}")
        adapter_instance = adapter_class(web3=web3)
        path_to_metadata = (
            Path(__file__).parent / amm_type / 'deploys' / str(chain_id) / cls.METADATA_FILE_NAME
        )
        if not path_to_metadata.exists():
            raise ValueError(f"Metadata file not found: {path_to_metadata}")
        with open(path_to_metadata) as f:
            metadatas = json.load(f)
            for metadata in metadatas:
                for address in metadata['contracts'].values():
                    adapter_instance.factory_address_to_dex_name[address.lower()] = {
                        'name': metadata['name'],
                        'type': metadata['type'],
                    }
                    adapter_instance.contracts_by_dex_name[metadata['name']] = metadata[
                        'contracts'
                    ]

        return adapter_instance

    @classmethod
    def get_all_supported_dex_types(cls) -> list[str]:
        return list(cls.default_adapters.keys())
