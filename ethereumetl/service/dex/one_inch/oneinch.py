# from pathlib import Path
#
# from clients.blockchain import BaseBlockchainClient
# from clients.blockchain.amm.base.base_contract import BaseContract
# from clients.blockchain.interfaces import AmmClientI
# from clients.blockchain.models.oneinch_transaction import OneInchTransactionType
# from clients.blockchain.models.pool import BasePool, PoolFinances
# from clients.blockchain.models.protocol_transaction import MintBurn, Swap
# from clients.blockchain.models.tokens import ERC20Token
# from clients.blockchain.models.transaction import ReceiptLog
# from dexguru_utils.enums import NativeTokenAddresses
# from eth_typing import ChecksumAddress
# from utils.common import INFINITE_PRICE_THRESHOLD
# from utils.logger import get_logger
# from utils.prices import get_prices_for_two_pool
# from web3 import Web3
# from web3.exceptions import BadFunctionCallOutput
#
# logger = get_logger(__name__)
#
# AMM_TYPE = "1inch"
# POOL_CONTRACT = "OneInchPool"
# FACTORY_CONTRACT = "OneInchFactory"
#
#
# class OneInchAmm(BaseContract, BaseBlockchainClient, AmmClientI):
#     pool_contract_names = (POOL_CONTRACT,)
#     pool_contracts_events_enum = OneInchTransactionType
#
#     def __init__(self, uri: str, amm_type: str, contracts: dict):
#         super().__init__(uri=uri, amm_type=amm_type, contracts=contracts)
#         _path_to_this_file = Path(__file__)
#         _path_to_abi = Path(_path_to_this_file.parent.parent, "abi")
#         abi_path = f"{_path_to_abi}/{amm_type}/"
#         pool_abi_path = abi_path + "OneInchPool.json"
#         self.abi[POOL_CONTRACT] = self._initiate_contract(pool_abi_path)
#
#     def get_base_pool(self, address: ChecksumAddress) -> BasePool | None:
#         logger.debug(f"Resolving token addresses for pool {address}")
#         tokens_addresses = self.get_tokens_addresses_for_pool(address)
#         if tokens_addresses:
#             return BasePool(address=address, tokens_addresses=tokens_addresses, fee=0.003)
#
#     def get_pair_tokens_by_index(
#         self, pair_index: int, block_identifier: str | int = "latest"
#     ) -> dict | None:
#         """
#         Gets pair_address and tokens addresses for pair_index.
#
#         :param pair_index: Index of the pair in the factory.
#         :param block_identifier: block number(int) or 'latest'
#         :return: Pair dict pair address, tokens addresses
#         """
#         pair_address = self.get_pair_address_by_index(
#             pair_index=pair_index, block_identifier=block_identifier
#         )
#
#         pair_address = Web3.toChecksumAddress(pair_address)
#
#         if not pair_address:
#             logger.error(
#                 f"Can't get pair by index for AMM:{self.factory_address}, index:{pair_index},"
#                 f" at {block_identifier}"
#             )
#             return None
#         tokens = (
#             self.abi[POOL_CONTRACT]
#             .contract.functions.getTokens()
#             .call({"to": pair_address}, "latest")
#         )
#
#         return {"address": pair_address.lower(), "tokens_addresses": tokens}
#
#     def get_tokens_addresses_for_pool(self, pool_address: ChecksumAddress) -> list | None:
#         logger.debug(f"Resolving tokens addresses for {pool_address}")
#         try:
#             tokens_addresses = (
#                 self.abi[POOL_CONTRACT]
#                 .contract.functions.getTokens()
#                 .call({"to": pool_address}, "latest")
#             )
#         except TypeError as e:
#             logger.error(f"Cant resolve tokens_addressese for pair {pool_address}, {e}")
#             return None
#
#         return tokens_addresses
#
#     def get_pair_address_by_index(
#         self, pair_index: int, block_identifier: str | int = "latest"
#     ) -> str | None:
#         """
#         Gets the address of the nth pair (0-indexed) created through
#         the factory, or 0x0 if not enough pairs have been created yet.
#
#         :param pair_index: Index of the pair in the factory.
#         :param block_identifier: block number(int) or 'latest'
#         :return: Address of the indexed pair.
#         """
#         try:
#             return Web3.toHex(
#                 hexstr=self.abi[FACTORY_CONTRACT]
#                 .contract.functions.allPools(pair_index)
#                 .call(block_identifier=block_identifier)
#             )
#         except BadFunctionCallOutput:
#             logger.error(
#                 "BadFunctionCallOutput while requesting address for pair with"
#                 f" pair_index: {pair_index}"
#                 f" in block: {block_identifier}"
#             )
#             return None
#         except ValueError:
#             logger.error(
#                 "Found no address for pair with"
#                 f" pair_index: {pair_index}"
#                 f" in block: {block_identifier}"
#             )
#             return None
#
#     def get_num_pairs(self, block_identifier: str | int = "latest") -> int:
#         """
#         Gets the total number of pairs created through the factory so far.
#
#         :param block_identifier: block number(int) or 'latest'
#         :return: Total number of pairs.
#         """
#         try:
#             data = (
#                 self.abi[FACTORY_CONTRACT]
#                 .contract.functions.getAllPools()
#                 .call(block_identifier=block_identifier)
#             )
#             return len(data)
#         except (BadFunctionCallOutput, ValueError) as e:
#             logger.error(
#                 f"Cant get pairs length on {self.factory_address}:{block_identifier}, trying latest:{e}"
#             )
#             try:
#                 data = (
#                     self.abi[FACTORY_CONTRACT]
#                     .contract.functions.getAllPools()
#                     .call(block_identifier="latest")
#                 )
#                 return len(data)
#             except (BadFunctionCallOutput, ValueError) as e:
#                 logger.error(
#                     f"Cant get pairs length on {self.factory_address}: 'latest', error: {e}"
#                 )
#
#     def resolve_receipt_log(
#         self,
#         receipt_log: ReceiptLog,
#         base_pool: BasePool,
#         erc20_tokens: list[ERC20Token],
#         transfers_for_transaction: list,
#     ) -> dict | None:
#         logger.debug(f"resolving {receipt_log.transaction_hash}-{receipt_log.log_index}")
#         try:
#             topic = receipt_log.topics[0][0:4]
#         except IndexError:
#             logger.error(f"Cant get receipt_log.topics[0][0:4], index error, log: {receipt_log}")
#             return None
#         event_name = self.abi[POOL_CONTRACT].topic_keccaks.get(topic, None)
#         if event_name.lower() == self.pool_contracts_events_enum.withdrawn.name.lower():
#             block_number = receipt_log.block_number - 1
#         else:
#             block_number = receipt_log.block_number
#
#         if receipt_log.topics and event_name:
#             parsed_event = self.parse_event(self.abi[POOL_CONTRACT], event_name, receipt_log)
#
#             tokens_scalars = []
#             for i, erc20_token in enumerate(erc20_tokens):
#                 if erc20_token.address.lower() == NativeTokenAddresses.eth.value:
#                     parsed_event[f"reserve{i}"] = (
#                         self.w3.eth.get_balance(base_pool.address, block_identifier=block_number)
#                         / tokens_scalars[i]
#                     )
#                 else:
#                     tokens_scalars.append(10**erc20_token.decimals)
#                     parsed_event[f"reserve{i}"] = (
#                         self.get_contract_reserve(
#                             base_pool.address,
#                             erc20_token.address,
#                             block_number,
#                         )
#                         / tokens_scalars[i]
#                     )
#
#             if event_name.lower() == self.pool_contracts_events_enum.swapped.name:
#                 return self._parse_swapped(base_pool, parsed_event, tokens_scalars, receipt_log)
#
#             elif event_name.lower() == self.pool_contracts_events_enum.withdrawn.name:
#                 return self._parse_withdrawn(
#                     base_pool,
#                     parsed_event,
#                     erc20_tokens,
#                     tokens_scalars,
#                     receipt_log,
#                     transfers_for_transaction,
#                 )
#
#             elif event_name.lower() == self.pool_contracts_events_enum.deposited.name:
#                 return self._parse_deposited(
#                     base_pool,
#                     parsed_event,
#                     erc20_tokens,
#                     tokens_scalars,
#                     receipt_log,
#                     transfers_for_transaction,
#                 )
#
#     def _parse_swapped(
#         self,
#         base_pool: BasePool,
#         parsed_event: dict,
#         tokens_scalars: list[float],
#         receipt_log: ReceiptLog,
#     ) -> dict[str, list]:
#         logger.debug("resolving swap from swap event")
#
#         if base_pool.tokens_addresses[1].lower() == parsed_event["dst"]:
#             parsed_event["amount0"] = parsed_event["amount"] / tokens_scalars[0]
#             parsed_event["amount1"] = parsed_event["result"] / tokens_scalars[1]
#         else:
#             parsed_event["amount0"] = parsed_event["result"] / tokens_scalars[0]
#             parsed_event["amount1"] = parsed_event["amount"] / tokens_scalars[1]
#
#         swap = self.get_swap_from_swap_event(base_pool, parsed_event, tokens_scalars)
#         pool = self.get_pool_finances_from_sync_event(base_pool, parsed_event, tokens_scalars)
#
#         logger.debug(f"resolved swap from swap event {swap}")
#         swap.log_index = receipt_log.log_index
#         return {"swaps": [swap], "pools": [pool]}
#
#     def _parse_deposited(
#         self,
#         base_pool: BasePool,
#         parsed_event: dict,
#         erc20_tokens: list[ERC20Token],
#         tokens_scalars: list[float],
#         receipt_log: ReceiptLog,
#         transfers_for_transaction: list,
#     ) -> dict[str, list]:
#         logger.debug("resolving mint from mint event")
#
#         parsed_event["amount1"] = parsed_event["amount"] / tokens_scalars[1]
#         amount0_transfers = list(
#             filter(
#                 lambda x: x.token_address == erc20_tokens[1].address.lower(),
#                 transfers_for_transaction,
#             )
#         ).pop()
#
#         parsed_event["amount0"] = amount0_transfers.value / tokens_scalars[0]
#
#         mint = self.get_mint_burn_from_events(base_pool, parsed_event)
#         pool = self.get_pool_finances_from_sync_event(base_pool, parsed_event)
#         logger.debug("resolving burn from mint event")
#         mint.log_index = receipt_log.log_index
#         return {"mints": [mint], "pools": [pool]}
#
#     def _parse_withdrawn(
#         self,
#         base_pool: BasePool,
#         parsed_event: dict,
#         erc20_tokens: list[ERC20Token],
#         tokens_scalars: list[float],
#         receipt_log: ReceiptLog,
#         transfers_for_transaction: list,
#     ) -> dict[str, list]:
#         logger.debug("resolving burn from burn event")
#
#         parsed_event["amount0"] = parsed_event["amount"] / tokens_scalars[0]
#         amount1_transfers = list(
#             filter(
#                 lambda x: x.token_address == erc20_tokens[1].address.lower(),
#                 transfers_for_transaction,
#             )
#         ).pop()
#
#         parsed_event["amount1"] = amount1_transfers.value / tokens_scalars[1]
#
#         burn = self.get_mint_burn_from_events(base_pool, parsed_event)
#         pool = self.get_pool_finances_from_sync_event(base_pool, parsed_event)
#         logger.debug(f"resolved pool finances from sync event {pool}")
#         logger.debug("resolving burn from burn event")
#         burn.log_index = receipt_log.log_index
#         return {"burns": [burn], "pools": [pool]}
#
#     @staticmethod
#     def get_mint_burn_from_events(base_pool, parsed_event, *args):
#         amount0 = parsed_event["amount0"]
#         amount1 = parsed_event["amount1"]
#         mint_burn = MintBurn(
#             pool_address=base_pool.address,
#             sender=parsed_event["account"],
#             owner=parsed_event["account"],
#             amounts=[amount0, amount1],
#         )
#         return mint_burn
#
#     @staticmethod
#     def get_swap_from_swap_event(base_pool, parsed_event, *args):
#         amount0 = parsed_event["amount0"]
#         amount1 = parsed_event["amount1"]
#
#         swap = Swap(
#             pool_address=base_pool.address,
#             sender=parsed_event["account"],
#             to=parsed_event["account"],
#             amounts=[amount0, amount1],
#         )
#         return swap
#
#     @staticmethod
#     def get_pool_finances_from_sync_event(base_pool, parsed_event, *args):
#         reserve0 = parsed_event["reserve0"]
#         reserve1 = parsed_event["reserve1"]
#         if reserve0:
#             token0_price = float(reserve1 / reserve0)
#         else:
#             logger.error("cant get price, as reserve0 = 0")
#             token0_price = 0
#         if reserve1:
#             token1_price = float(reserve0 / reserve1)
#         else:
#             logger.error("cant get price, as reserve1 = 0")
#             token1_price = 0
#         if token0_price >= INFINITE_PRICE_THRESHOLD:
#             logger.error("cant get price, as it's infinite")
#             token0_price = 0
#         if token1_price >= INFINITE_PRICE_THRESHOLD:
#             logger.error("cant get price, as it's infinite")
#             token1_price = 0
#         pool = PoolFinances(**base_pool.dict())
#         pool.reserves = [reserve0, reserve1]
#         pool.prices = get_prices_for_two_pool(token0_price, token1_price)
#         return pool
