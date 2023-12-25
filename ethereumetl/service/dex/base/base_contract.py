# import os
# from pathlib import Path
# from typing import Any
#
# import eth_abi
# from clients.blockchain.base.evm_base import EVMBase
# from clients.blockchain.models.abi_contract import ABIContract
# from clients.blockchain.models.transaction import ReceiptLog
# from eth_utils import decode_hex
# from utils.logger import get_logger
# from web3 import Web3
#
# logs = get_logger(__name__)
#
#
# class InvalidToken(Exception):
#     def __init__(self, address: Any) -> None:
#         super(f"Invalid token address: {address}")
#
#
# class BaseContract(EVMBase):
#     pool_contract_names = []
#     pool_contracts_events_enum = None
#
#     def __init__(self, uri: str, amm_type: str, contracts: dict):
#         super().__init__(uri=uri)
#         self.amm_type = amm_type
#
#         _path_to_this_file = Path(__file__)
#         _path_to_abi = Path(_path_to_this_file.parent.parent.parent, "abi")
#         abi_path = f"{_path_to_abi}/{amm_type}/"
#         self.abi = {}
#
#         for contract, address in contracts.items():
#             contract_abi_path = abi_path + f"{contract}.json"
#             if not os.path.isfile(contract_abi_path):
#                 logs.error(f"Contract ABI {contract} in not found at {contract_abi_path}")
#                 continue
#             self.abi[contract]: ABIContract = self._initiate_contract(contract_abi_path, address)
#
#     @property
#     def pool_contracts_events_names(self):
#         return self.pool_contracts_events_enum.__members__.values()
#
#     def get_pool_contracts(self) -> tuple[list[ABIContract], dict[str, str]]:
#         pool_contracts = [self.abi.get(name) for name in self.pool_contract_names]
#         addresses = {}
#         for contract_name, contract in self.abi.items():
#             if contract.address:
#                 addresses[contract.address] = contract_name
#         return list(filter(None, pool_contracts)), addresses
#
#     @staticmethod
#     def parse_event(contract: ABIContract, event_name: str, receipt_log: ReceiptLog):
#         encoded_topics = [decode_hex(Web3.toHex(topic)) for topic in receipt_log.topics[1:]]
#         indexed_values = [
#             eth_abi.decode_single(t, v)
#             for t, v in zip(contract.topic_indexed_types[event_name], encoded_topics)
#         ]
#         values = eth_abi.decode_abi(contract.topic_types[event_name], decode_hex(receipt_log.data))
#         return {
#             **{
#                 **dict(zip(contract.topic_names[event_name], values)),
#                 **dict(zip(contract.topic_indexed_names[event_name], indexed_values)),
#             }
#         }
#
#     def get_lp_token_address_for_pool(self, pool_address: str) -> list[str]:
#         """
#         For regular pools, LP address == pool address.
#         For Curve amm this method overloaded.
#         """
#         return [pool_address.lower()]
