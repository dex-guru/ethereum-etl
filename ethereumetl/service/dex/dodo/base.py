#
# from clients.blockchain import BaseBlockchainClient
# from clients.blockchain.amm.base.base_contract import BaseContract
# from clients.blockchain.interfaces import AmmClientI
# from clients.blockchain.models.pool import BasePool, PoolFinances
# from clients.blockchain.models.transaction import ReceiptLog
# from eth_typing import ChecksumAddress
# from utils.common import INFINITE_PRICE_THRESHOLD
# from utils.logger import get_logger
# from utils.prices import get_prices_for_two_pool
#
# logs = get_logger(__name__)
#
#
# class BaseDODOAmmClient(BaseContract, BaseBlockchainClient, AmmClientI):
#     """Base class for DODO AMM clients."""
#
#     pool_contract_name = None
#     pool_contract_names = []
#
#     def __init__(self, uri: str, amm_type: str, contracts: dict):
#         super().__init__(uri=uri, amm_type=amm_type, contracts=contracts)
#
#     def get_tokens_addresses_for_pool(self, pool_address: ChecksumAddress) -> list | None:
#         logs.debug(f"Resolving tokens addresses for {pool_address}")
#         try:
#             tokens_addresses = [
#                 (
#                     self.abi[self.pool_contract_name]
#                     .contract.functions._BASE_TOKEN_()
#                     .call({"to": pool_address}, "latest")
#                 ),
#                 (
#                     self.abi[self.pool_contract_name]
#                     .contract.functions._QUOTE_TOKEN_()
#                     .call({"to": pool_address}, "latest")
#                 ),
#             ]
#         except TypeError as e:
#             logs.error(f"Cant resolve tokens_addresses for pair {pool_address}, {e}")
#             return None
#
#         return tokens_addresses
#
#     def get_base_pool(self, address: ChecksumAddress) -> BasePool | None:
#         tokens_addresses = self.get_tokens_addresses_for_pool(address)
#         if tokens_addresses:
#             return BasePool(address=address, tokens_addresses=tokens_addresses, fee=0.003)
#
#     def get_pool_finances_from_event(
#         self, base_pool: BasePool, parsed_event: dict
#     ) -> PoolFinances:
#         reserve0 = parsed_event["reserve0"]
#         reserve1 = parsed_event["reserve1"]
#
#         pool_version = (
#             self.abi[self.pool_contract_name]
#             .contract.functions.version()
#             .call({"to": self.w3.toChecksumAddress(base_pool.address)}, "latest")
#         )
#         if isinstance(pool_version, str) and "DSP" in pool_version:
#             # In case its DSP (fixed pool, DODO Stable Pool), we can't relly on the reserves, only on 1-1 swapping
#             token0_price = token1_price = 1
#         else:
#             if reserve0:
#                 token0_price = float(reserve1 / reserve0)
#             else:
#                 logs.error("cant get price, as reserve0 = 0")
#                 token0_price = 0
#
#             if reserve1:
#                 token1_price = float(reserve0 / reserve1)
#             else:
#                 logs.error("cant get price, as reserve1 = 0")
#                 token1_price = 0
#
#             if token0_price >= INFINITE_PRICE_THRESHOLD:
#                 logs.error("cant get price, as it's infinite")
#                 token0_price = 0
#             if token1_price >= INFINITE_PRICE_THRESHOLD:
#                 logs.error("cant get price, as it's infinite")
#                 token1_price = 0
#
#         pool = PoolFinances(**base_pool.dict())
#         pool.reserves = [reserve0, reserve1]
#         pool.prices = get_prices_for_two_pool(token0_price, token1_price)
#         return pool
#
#     def _get_event_name_from_receipt_log(
#         self, receipt_log: ReceiptLog
#     ) -> tuple[str | None, str | None]:
#         try:
#             topic = receipt_log.topics[0][0:4]
#         except IndexError:
#             logs.error(f"Cant get receipt_log.topics[0][0:4], index error, log: {receipt_log}")
#             return None, None
#
#         for contract_name in self.pool_contract_names:
#             event_name = self.abi[contract_name].topic_keccaks.get(topic, None)
#             if event_name:
#                 return event_name, contract_name
#
#         logs.error(f"Error on resolving receipt log: receipt_log={receipt_log}")
#
#     def _get_parsed_event_from_receipt_log(
#         self, receipt_log: ReceiptLog
#     ) -> tuple[dict | None, str | None]:
#         event_name, contract = self._get_event_name_from_receipt_log(receipt_log)
#         if not event_name:
#             return None, None
#
#         parsed_event = self.parse_event(self.abi[contract], event_name, receipt_log)
#         return parsed_event, event_name
