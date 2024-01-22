import json
import logging
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from web3 import Web3

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.domain.dex_trade import EthDexTrade
from ethereumetl.domain.receipt_log import ParsedReceiptLog
from ethereumetl.domain.token import EthToken
from ethereumetl.domain.token_transfer import EthTokenTransfer
from ethereumetl.service.dex.base.base_dex_client import BaseDexClient
from ethereumetl.service.dex.base.interface import DexClientInterface
from ethereumetl.service.dex.uniswap_v2.uniswap_v2 import UniswapV2Amm

ABI = Sequence[Mapping[str, Any]]
EventABI = dict[str, Any]
FilePath = str
to_checksum = Web3.to_checksum_address


class ContractAdaptersFactory:
    """Creates an instance of an AMM client based on the AMM type."""

    METADATA_FILE_NAME = 'metadata.json'

    default_adapters: dict[str, type[DexClientInterface]] = {
        'base': BaseDexClient,
        "uniswap_v2": UniswapV2Amm,
        # "uniswap_v3": UniswapV3Amm,
        # "meshswap": MeshswapAmm,
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
        # "pancakeswap_v3": UniswapV3Amm,
        # "quickswap_v3": QuickswapV3Amm,
        # "traderjoe_v2_1": TraderJoeV21Amm,
    }

    def __init__(self, web3: Web3, chain_id: int | None = None):
        self.initiated_adapters: dict[str, DexClientInterface] = {}
        self.web3 = web3
        self.chain_id = chain_id
        self.namespace_by_factory_address: dict[str, str] = {}
        self.__initiate_adapters()
        self._init_metadata()

    def __initiate_adapters(self):
        for contract_type, contract_instance in self.default_adapters.items():
            self.initiated_adapters[contract_type] = contract_instance(
                web3=self.web3, chain_id=self.chain_id
            )

    def _init_metadata(self):
        path = Path(__file__).parent
        for file_path in path.rglob('metadata.json'):
            with file_path.open() as f:
                data = json.load(f)
                for metadata in data:
                    for address in metadata['contracts'].values():
                        self.namespace_by_factory_address[address.lower()] = metadata['type']

    # @staticmethod
    # def get_dex_by_factory_address(factory_address: str) -> str | None:
    #     path = Path(__file__).parent
    #     for file_path in path.rglob('metadata.json'):
    #         with file_path.open() as f:
    #             data = json.load(f)
    #             for metadata in data:
    #                 for address in metadata['contracts'].values():
    #                     if address.lower() == factory_address.lower():
    #                         return metadata['name']

    @classmethod
    def get_dex_client(cls, amm_type: str, web3: Web3, chain_id: int) -> DexClientInterface:
        adapter_class = cls.default_adapters.get(amm_type)
        if not adapter_class:
            raise ValueError(f"Unsupported AMM type: {amm_type}")
        adapter_instance = adapter_class(web3=web3, chain_id=chain_id)
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

    def resolve_log(
        self,
        parsed_log: ParsedReceiptLog,
        dex_pool: EthDexPool | None = None,
        tokens_for_pool: list[EthToken] | None = None,
        transfers_for_transaction: list[EthTokenTransfer] | None = None,
    ) -> EthDexTrade | None:
        namespaces = list(parsed_log.namespace)
        if dex_pool:
            namespace = self.namespace_by_factory_address.get(dex_pool.factory_address.lower())
            if namespace:
                # Move the namespace to the front of the list so that it is checked first
                namespaces.remove(namespace)
                namespaces.insert(0, namespace)

        for namespace in namespaces:
            dex_client = self.initiated_adapters.get(namespace)
            if not dex_client:
                logging.info(f"Failed to get dex client for namespace: {namespace}")
                continue
            try:
                resolved_log = dex_client.resolve_receipt_log(
                    parsed_receipt_log=parsed_log,
                    dex_pool=dex_pool,
                    tokens_for_pool=tokens_for_pool,
                    transfers_for_transaction=transfers_for_transaction,
                )
            except Exception as e:
                logging.info(f"Failed to resolve log: {e}")
                continue
            if resolved_log:
                return resolved_log

    def resolve_asset_from_log(self, parsed_log: ParsedReceiptLog) -> EthDexPool | None:
        for namespace in parsed_log.namespace:
            dex_client = self.initiated_adapters.get(namespace)
            if not dex_client:
                logging.info(f"Failed to get dex client for namespace: {namespace}")
                continue
            try:
                asset = dex_client.resolve_asset_from_log(parsed_log)
            except Exception as e:
                logging.info(f"Failed to resolve asset from log: {e}")
                continue
            if asset:
                parsed_log.namespace = (namespace,)
                return asset
        return None
