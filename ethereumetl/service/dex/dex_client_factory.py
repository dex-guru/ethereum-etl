import json
import logging
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from web3 import Web3

from ethereumetl.service.dex.base.base_dex_client import BaseDexClient
from ethereumetl.service.dex.base.interface import DexClientInterface
from ethereumetl.service.dex.canto_dex.canto_dex import CantoDexAmm
from ethereumetl.service.dex.dmm.dmm import DMMAmm
from ethereumetl.service.dex.dodo.proxy import DODOAmm
from ethereumetl.service.dex.fjord.fjord import FjordLBP
from ethereumetl.service.dex.kyberswap_elastic.kyberswap_elastic import KyberSwapElasticAmm
from ethereumetl.service.dex.meshswap.meshswap import MeshswapAmm
from ethereumetl.service.dex.one_inch.oneinch import OneInchAmm
from ethereumetl.service.dex.quickswap_v3.quickswap_v3 import QuickswapV3Amm
from ethereumetl.service.dex.solidly.solidly import SolidlyAmm
from ethereumetl.service.dex.sushiswap_bento.sushiswap_bento import SushiSwapBentoAmm
from ethereumetl.service.dex.traderjoe_v2_1.traderjoe_v2_1 import TraderJoeV21Amm
from ethereumetl.service.dex.uniswap_v2.uniswap_v2 import UniswapV2Amm
from ethereumetl.service.dex.uniswap_v3.uniswap_v3 import UniswapV3Amm
from ethereumetl.service.dex.wombat.wombat import WombatAmm
from ethereumetl.service.dex.carbondefi.carbondefi import CarbonDeFiAmm
from ethereumetl.utils import Singleton

ABI = Sequence[Mapping[str, Any]]
EventABI = dict[str, Any]
FilePath = str
to_checksum = Web3.to_checksum_address


class ContractAdaptersFactory(metaclass=Singleton):
    """Creates an instance of an AMM client based on the AMM type."""

    METADATA_FILE_NAME = 'metadata.json'

    # Mapping of namespace to the corresponding adapter class.
    default_adapters: dict[str, type[DexClientInterface]] = {
        'base': BaseDexClient,
        "uniswap_v2": UniswapV2Amm,
        "uniswap_v3": UniswapV3Amm,
        "meshswap": MeshswapAmm,
        "solidly": SolidlyAmm,
        "dmm": DMMAmm,
        "dodo": DODOAmm,
        "dodo_v2": DODOAmm,
        "1inch": OneInchAmm,
        # "curve": CurveAmm,
        # "bancor_v2": BancorV2Amm,
        # "ellipsis": EllipsisAmm,
        # "balancer": BalancerAmm,
        "sushiswap_bento": SushiSwapBentoAmm,
        # "saddle": SaddleAmm,
        # "platypus": PlatypusAmm,
        "kyberswap_elastic": KyberSwapElasticAmm,
        "wombat": WombatAmm,
        "canto_dex": CantoDexAmm,
        "pancakeswap_v3": UniswapV3Amm,
        "quickswap_v3": QuickswapV3Amm,
        "traderjoe_v2_1": TraderJoeV21Amm,
        "fjord": FjordLBP,
        "carbondefi": CarbonDeFiAmm,
    }

    def __init__(self, web3: Web3, chain_id: int | None = None):
        self.initiated_adapters: dict[str, DexClientInterface] = {}
        self.web3 = web3
        self.chain_id = chain_id
        self._namespace_by_factory_address: dict[str, str] = {}
        self._factory_address_to_dex_name: dict[str, str] = {}

        self._initiate_adapters()
        self._init_metadata()

    def get_namespace_by_factory(self, factory_address: str) -> str | None:
        return self._namespace_by_factory_address.get(factory_address.lower())

    def get(self, amm_type: str) -> DexClientInterface | None:
        return self.initiated_adapters.get(amm_type)

    def _initiate_adapters(self):
        for contract_type, contract_instance in self.default_adapters.items():
            try:
                self.initiated_adapters[contract_type] = contract_instance(
                    web3=self.web3, chain_id=self.chain_id
                )
            except (ValueError, OSError) as e:
                logging.error(f"Failed to initiate adapter for {contract_type}: {e}")

    def _init_metadata(self):
        path = Path(__file__).parent
        for file_path in path.rglob(f'{self.chain_id}/metadata.json'):
            with file_path.open() as f:
                data = json.load(f)
                for metadata in data:
                    for address in metadata['contracts'].values():
                        _address = address.lower()
                        self._namespace_by_factory_address[_address] = metadata['type']
                        self._factory_address_to_dex_name[_address] = metadata['name'].lower()

    def get_dex_name_by_factory_address(self, factory_address: str):
        return self._factory_address_to_dex_name.get(factory_address.lower(), 'unknown')

    def get_dex_client(self, amm_type: str) -> DexClientInterface:
        adapter_instance = self.initiated_adapters.get(amm_type)
        if not adapter_instance:
            raise ValueError(f"Unsupported AMM type: {amm_type}")
        # path_to_metadata = (
        #     Path(__file__).parent / amm_type / 'deploys' / str(chain_id) / cls.METADATA_FILE_NAME
        # )
        # if not path_to_metadata.exists():
        #     raise ValueError(f"Metadata file not found: {path_to_metadata}")
        # with open(path_to_metadata) as f:
        #     metadatas = json.load(f)
        #     for metadata in metadatas:
        #         for address in metadata['contracts'].values():
        #             adapter_instance.factory_address_to_dex_name[address.lower()] = {
        #                 'name': metadata['name'],
        #                 'type': metadata['type'],
        #             }
        #             adapter_instance.contracts_by_dex_name[metadata['name']] = metadata[
        #                 'contracts'
        #             ]

        return adapter_instance

    @classmethod
    def get_all_supported_dex_types(cls) -> list[str]:
        return list(cls.default_adapters.keys())
