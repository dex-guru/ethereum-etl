# import logging
# from enum import Enum
# from pathlib import Path
#
# from eth_typing import ChecksumAddress
# from web3 import Web3
# from web3.exceptions import BadFunctionCallOutput, ContractLogicError
#
# from ethereumetl.domain.dex_pool import EthDexPool
# from ethereumetl.service.dex.base.enums import DexPoolFeeAmount
# from ethereumetl.service.dex.interface import DexClientInterface
#
# logs = logging.getLogger(__name__)
#
# AMM_TYPE = "canto_dex"
# FACTORY_CONTRACT = "BaseV1Factory"
# POOL_CONTRACT = "BaseV1Pair"
#
#
# class CantoDexAmm(DexClientInterface):
#     pool_contract_names = (POOL_CONTRACT,)
#     pool_contracts_events_enum = Enum(
#         value="CantoDexEvents",
#         names=[
#             ("swap", "Swap"),
#             ("mint", "Mint"),
#             ("burn", "Burn"),
#             ("sync", "Sync"),
#         ],
#     )
#
#     def __init__(self, uri: str, amm_type: str, contracts: dict):
#         super().__init__(uri=uri, amm_type=amm_type, contracts=contracts)
#         pool_abi_path = Path(__file__) / "BaseV1Pair.json"
#         self.abi = {}
#         self.abi[POOL_CONTRACT] = self._initiate_contract(pool_abi_path)
#
#     def _get_event_name(self, topics: list[str]):
#         try:
#             topic = topics[0][0:4]
#         except IndexError:
#             logs.error(f"Cant get receipt_log.topics[0][0:4], index error, topics: {topics}")
#             return None
#         event_name = self.abi[POOL_CONTRACT].topic_keccaks.get(topic, None)
#         return event_name
#
#     def get_num_pairs(self, block_identifier: str | int = "latest") -> int:
#         """
#         Gets the total number of pairs created through the factory so far.
#
#         :param block_identifier: block number(int) or 'latest'
#         :return: Total number of pairs.
#         """
#         try:
#             return (
#                 self.abi[FACTORY_CONTRACT]
#                 .contract.functions.allPairsLength()
#                 .call(block_identifier=block_identifier)
#             )
#         except (BadFunctionCallOutput, ValueError) as e:
#             logs.error(
#                 f"Cant get pairs length on {self.factory_address}:{block_identifier}, trying latest:{e}"
#             )
#             try:
#                 return (
#                     self.abi[FACTORY_CONTRACT]
#                     .contract.functions.allPairsLength()
#                     .call(block_identifier="latest")
#                 )
#             except (BadFunctionCallOutput, ValueError) as e:
#                 logs.error(
#                     f"Cant get pairs length on {self.factory_address}: 'latest', error: {e}"
#                 )
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
#                 .contract.functions.allPairs(pair_index)
#                 .call(block_identifier=block_identifier)
#             )
#         except BadFunctionCallOutput:
#             logs.error(
#                 "BadFunctionCallOutput while requesting address for pair with"
#                 f" pair_index: {pair_index}"
#                 f" in block: {block_identifier}"
#             )
#             return None
#         except ValueError:
#             logs.error(
#                 "Found no address for pair with"
#                 f" pair_index: {pair_index}"
#                 f" in block: {block_identifier}"
#             )
#             return None
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
#         if not pair_address:
#             return None
#
#         pair_address = Web3.toChecksumAddress(pair_address)
#
#         if not pair_address:
#             logs.error(
#                 f"Can get pair by index for AMM:{self.factory_address}, index:{pair_index},"
#                 f" at {block_identifier}"
#             )
#             return None
#
#         tokens = [
#             (
#                 self.abi[POOL_CONTRACT]
#                 .contract.functions.token0()
#                 .call({"to": pair_address}, block_identifier)
#             ).lower(),
#             (
#                 self.abi[POOL_CONTRACT]
#                 .contract.functions.token1()
#                 .call({"to": pair_address}, block_identifier)
#             ).lower(),
#         ]
#         return {"address": pair_address.lower(), "tokens_addresses": tokens}
#
#     def get_base_pool(self, address: ChecksumAddress) -> EthDexPool | None:
#         logs.debug(f"Resolving pool addresses for {address}")
#         tokens_addresses = self.get_tokens_addresses_for_pool(address)
#         if tokens_addresses:
#             return EthDexPool(
#                 address=address,
#                 token_addresses=tokens_addresses,
#                 fee_tier=DexPoolFeeAmount.MEDIUM.value,
#                 lp_token_addresses=[],
#             )
#
#     def is_pool_address_for_amm(self, pool_address: ChecksumAddress) -> bool:
#         try:
#             # Canto's slingshot and other dexes, added isPair method to factory
#             return (
#                 self.abi[FACTORY_CONTRACT]
#                 .contract.functions.isPair(Web3.toChecksumAddress(pool_address))
#                 .call(block_identifier="latest")
#             )
#         except (TypeError, ContractLogicError, ValueError, BadFunctionCallOutput) as e:
#             logs.debug(f"Not found factory, fallback to maintainer. Error: {e}")
#         return False
#
#     def get_tokens_addresses_for_pool(self, pool_address: ChecksumAddress) -> list | None:
#         logs.debug(f"Resolving tokens addresses for {pool_address}")
#         try:
#             tokens_addresses = [
#                 (
#                     self.abi[POOL_CONTRACT]
#                     .contract.functions.token0()
#                     .call({"to": pool_address}, "latest")
#                 ),
#                 (
#                     self.abi[POOL_CONTRACT]
#                     .contract.functions.token1()
#                     .call({"to": pool_address}, "latest")
#                 ),
#             ]
#         except TypeError as e:
#             logs.error(f"Cant resolve tokens_addressese for pair {pool_address}, {e}")
#             return None
#
#         return tokens_addresses
#
#     # def resolve_receipt_log(
#     #     self,
#     #     receipt_log: ReceiptLog,
#     #     base_pool: BasePool,
#     #     erc20_tokens: List[ERC20Token],
#     # ) -> Optional[dict]:
#     #     logs.debug(f"resolving {receipt_log.transaction_hash.hex()}-{receipt_log.log_index}")
#     #     event_name = self._get_event_name(receipt_log.topics)
#     #
#     #     if not all((receipt_log.topics, event_name)):
#     #         return None
#     #
#     #     tokens_scalars = []
#     #     for erc20_token in erc20_tokens:
#     #         tokens_scalars.append(10**erc20_token.decimals)
#     #     parsed_event = self.parse_event(self.abi[POOL_CONTRACT], event_name, receipt_log)
#     #
#     #     if event_name.lower() == self.pool_contracts_events_enum.swap.name:
#     #         logs.debug("resolving swap from swap event")
#     #         swap = self.get_swap_from_swap_event(base_pool, parsed_event, tokens_scalars)
#     #         logs.debug(f"resolved swap from swap event {swap}")
#     #         swap.log_index = receipt_log.log_index
#     #
#     #         pool = self.get_pool_finances_with_amounts(
#     #             base_pool, swap.amounts, tokens_scalars, receipt_log.block_number
#     #         )
#     #         if pool:
#     #             return {"swaps": [swap], "pools": [pool]}
#     #         return {"swaps": [swap]}
#     #
#     #     if event_name.lower() == self.pool_contracts_events_enum.burn.name:
#     #         logs.debug(f"resolving burn from burn event")
#     #         burn = self.get_mint_burn_from_events(base_pool, parsed_event, tokens_scalars)
#     #         pool = self.get_pool_finances_with_amounts(
#     #             base_pool, burn.amounts, tokens_scalars, receipt_log.block_number - 1
#     #         )
#     #         logs.debug(f"resolving burn from burn event")
#     #         burn.log_index = receipt_log.log_index
#     #         return {
#     #             "burns": [burn],
#     #             "pools": [pool],
#     #         }
#     #
#     #     if event_name.lower() == self.pool_contracts_events_enum.mint.name:
#     #         logs.debug(f"resolving burn from mint event")
#     #         mint = self.get_mint_burn_from_events(base_pool, parsed_event, tokens_scalars)
#     #         pool = self.get_pool_finances_with_amounts(
#     #             base_pool, mint.amounts, tokens_scalars, receipt_log.block_number
#     #         )
#     #         logs.debug(f"resolving burn from mint event")
#     #         mint.log_index = receipt_log.log_index
#     #         return {
#     #             "mints": [mint],
#     #             "pools": [pool],
#     #         }
#     #
#     #     # if event_name.lower() == self.pool_contracts_events_enum.sync.name:
#     #     #     logs.debug(f'resolving pool finances from sync event')
#     #     #     pool = self.get_pool_finances_from_sync_event(base_pool, parsed_event, tokens_scalars)
#     #     #     logs.debug(f'resolved pool finances from sync event {pool}')
#     #     #     try:
#     #     #         if self.abi[POOL_CONTRACT].contract.functions.stable().call({'to': base_pool.address}, 'latest'):
#     #     #             return {
#     #     #                 "pools": []
#     #     #             }
#     #     #     except Exception:
#     #     #         pass
#     #     #     return {
#     #     #         "pools": [pool]
#     #     #     }
#
#     @staticmethod
#     def get_mint_burn_from_events(base_pool, parsed_event, tokens_scalars):
#         amount0 = parsed_event["amount0"] / tokens_scalars[0]
#         amount1 = parsed_event["amount1"] / tokens_scalars[1]
#         mint_burn = MintBurn(
#             pool_address=base_pool.address,
#             sender=parsed_event["sender"] if parsed_event.get("sender") else parsed_event["owner"],
#             owner=parsed_event["owner"] if parsed_event.get("owner") else parsed_event["sender"],
#             amounts=[amount0, amount1],
#         )
#         return mint_burn
#
#     @staticmethod
#     def get_swap_from_swap_event(base_pool, parsed_event, tokens_scalars):
#         amount0 = (
#             parsed_event["amount0In"] / tokens_scalars[0]
#             - parsed_event["amount0Out"] / tokens_scalars[0]
#         )
#         amount1 = (
#             parsed_event["amount1In"] / tokens_scalars[1]
#             - parsed_event["amount1Out"] / tokens_scalars[1]
#         )
#         swap = Swap(
#             pool_address=base_pool.address,
#             sender=parsed_event["sender"],
#             to=parsed_event["to"],
#             amounts=[amount0, amount1],
#         )
#         return swap
#
#     def get_pool_finances_with_amounts(self, base_pool, amounts, tokens_scalars, block_identifier):
#         try:
#             reserves = (
#                 self.abi[POOL_CONTRACT]
#                 .contract.functions.getReserves()
#                 .call({"to": base_pool.address}, block_identifier)
#             )
#         except Exception:
#             return None
#         reserve0 = reserves[0] / tokens_scalars[0]
#         reserve1 = reserves[1] / tokens_scalars[1]
#
#         token0_price = abs(amounts[1] / amounts[0])
#         token1_price = abs(amounts[0] / amounts[1])
#
#         if token0_price >= INFINITE_PRICE_THRESHOLD:
#             logs.error("cant get price, as it's infinite")
#             token0_price = 0
#         if token1_price >= INFINITE_PRICE_THRESHOLD:
#             logs.error("cant get price, as it's infinite")
#             token1_price = 0
#
#         pool = PoolFinances(**base_pool.dict())
#         pool.reserves = [reserve0, reserve1]
#         pool.prices = get_prices_for_two_pool(token0_price, token1_price)
#         return pool
#
#     @staticmethod
#     def get_pool_finances_from_sync_event(base_pool, parsed_event, tokens_scalars):
#         reserve0 = parsed_event["reserve0"] / tokens_scalars[0]
#         reserve1 = parsed_event["reserve1"] / tokens_scalars[1]
#         if reserve0:
#             token0_price = float(reserve1 / reserve0)
#         else:
#             logs.error("cant get price, as reserve0 = 0")
#             token0_price = 0
#         if reserve1:
#             token1_price = float(reserve0 / reserve1)
#         else:
#             logs.error("cant get price, as reserve1 = 0")
#             token1_price = 0
#         if token0_price >= INFINITE_PRICE_THRESHOLD:
#             logs.error("cant get price, as it's infinite")
#             token0_price = 0
#         if token1_price >= INFINITE_PRICE_THRESHOLD:
#             logs.error("cant get price, as it's infinite")
#             token1_price = 0
#         pool = PoolFinances(**base_pool.dict())
#         pool.reserves = [reserve0, reserve1]
#         pool.prices = get_prices_for_two_pool(token0_price, token1_price)
#         return pool
