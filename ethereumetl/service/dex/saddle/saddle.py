# from collections import defaultdict
# from contextlib import suppress
# from pathlib import Path
#
# from clients.blockchain import BaseBlockchainClient
# from clients.blockchain.amm.base.base_contract import BaseContract
# from clients.blockchain.interfaces import AmmClientI
# from clients.blockchain.models.curve_transaction import SaddleTransactionType
# from clients.blockchain.models.pool import CurvePool, PoolFinances
# from clients.blockchain.models.protocol_transaction import MintBurn, Swap
# from clients.blockchain.models.tokens import ERC20Token
# from clients.blockchain.models.transaction import ReceiptLog
# from config import config
# from eth_typing import ChecksumAddress
# from utils.logger import get_logger
# from utils.prices import get_default_prices
# from web3.exceptions import BadFunctionCallOutput, ContractLogicError
#
# logger = get_logger(__name__)
#
# # https://github.com/saddle-finance/saddle-contract
# FACTORY_CONTRACT = "PoolRegistry"
# POOL_CONTRACT = "SaddlePool"
# POOL_CONTRACT_V2 = "SaddlePoolV2"
#
#
# class SaddleAmm(BaseContract, BaseBlockchainClient, AmmClientI):
#     pool_contract_names = [POOL_CONTRACT, POOL_CONTRACT_V2]
#     pool_contracts_events_enum = SaddleTransactionType
#
#     def __init__(self, uri: str, amm_type: str, contracts: dict):
#         _path_to_this_file = Path(__file__)
#         _path_to_abi = Path(_path_to_this_file.parent.parent, "abi")
#         self.abi_path = f"{_path_to_abi}/{amm_type}/"
#         super().__init__(uri=uri, amm_type=amm_type, contracts=contracts)
#
#         for name in self.pool_contract_names:
#             pool_abi_path = self.abi_path + name + ".json"
#             self.abi[name] = self._initiate_contract(pool_abi_path)
#
#     def resolve_receipt_log(
#         self,
#         receipt_log: ReceiptLog,
#         base_pool: CurvePool,
#         erc20_tokens: list[ERC20Token],
#     ) -> dict | None:
#         logger.debug(f"resolving {receipt_log.transaction_hash}-{receipt_log.log_index}")
#         try:
#             topic = receipt_log.topics[0][0:4]
#         except IndexError:
#             logger.error(f"Cant get receipt_log.topics[0][0:4], index error, log: {receipt_log}")
#             return
#
#         event_name = None
#         use_contract = None
#         for contract in self.pool_contract_names:
#             event_name = self.abi[contract].topic_keccaks.get(topic)
#             if event_name:
#                 use_contract = contract
#                 break
#
#         if event_name is None:
#             return
#
#         parsed_event = self.parse_event(self.abi[use_contract], event_name, receipt_log)
#
#         if not parsed_event:
#             return
#         curve_pool = CurvePool(
#             address=base_pool.address,
#             tokens_addresses=base_pool.tokens_addresses,
#         )
#         tokens_scalars = []
#         for erc20_token in erc20_tokens:
#             tokens_scalars.append(10**erc20_token.decimals)
#
#         if event_name == SaddleTransactionType.AddLiquidity.name:
#             logger.debug("resolving mint from add_liquidity event")
#             mint = self.get_mint_from_events(curve_pool, parsed_event, tokens_scalars)
#             logger.debug("resolved mint from add_liquidity event")
#             mint.log_index = receipt_log.log_index
#             pool = self.get_pool_finances(
#                 curve_pool, tokens_scalars, use_contract, receipt_log.block_number
#             )
#             return {"mints": [mint], "pools": [pool]}
#
#         if event_name == SaddleTransactionType.TokenSwap.name:
#             logger.debug("resolving swap from swap event")
#             swap = self.get_swap_from_event(curve_pool, parsed_event, tokens_scalars)
#             logger.debug(f"resolved swap from swap event {swap}")
#             swap.log_index = receipt_log.log_index
#             pool = self.get_pool_finances(
#                 curve_pool, tokens_scalars, use_contract, receipt_log.block_number
#             )
#             return {"swaps": [swap], "pools": [pool]}
#
#         if event_name == SaddleTransactionType.TokenSwapUnderlying.name:
#             curve_metapool = curve_pool
#             if len(curve_pool.u_tokens_addresses) == 0:
#                 u_tokens_addresses = self.get_underlying_tokens_by_pool_address(curve_pool.address)
#                 if not u_tokens_addresses:
#                     return None
#                 curve_metapool.u_tokens_addresses = u_tokens_addresses
#
#             underlying_scalars = self.get_scalars_for_tokens(curve_metapool.u_tokens_addresses)
#             logger.debug("resolving swap for underlying coins event")
#             swap = self.get_swap_from_event(curve_pool, parsed_event, underlying_scalars)
#             logger.debug("resolved swap for underlying coins event")
#             swap.log_index = receipt_log.log_index
#             metapool_finances = self.hydrate_pool_with_metapool_finances(
#                 curve_metapool,
#                 underlying_scalars,
#                 POOL_CONTRACT_V2,
#                 receipt_log.block_number,
#             )
#             if not metapool_finances:
#                 return None
#
#             return {
#                 "swaps": [swap],
#                 "pools": [metapool_finances],
#             }
#
#         if event_name in SaddleTransactionType.burn_events():
#             logger.debug("resolving burn from curve_burn events")
#             burn = self.get_burns_from_events(curve_pool, parsed_event, event_name, tokens_scalars)
#             if not burn:
#                 return None
#             logger.debug("resolved burn from curve_burn events")
#             burn.log_index = receipt_log.log_index
#             pool = self.get_pool_finances(
#                 curve_pool, tokens_scalars, use_contract, receipt_log.block_number - 1
#             )
#             return {"burns": [burn], "pools": [pool]}
#
#     @staticmethod
#     def get_mint_from_events(
#         curve_pool: CurvePool, parsed_event: dict, tokens_scalars: list
#     ) -> MintBurn:
#         mint = MintBurn(
#             pool_address=curve_pool.address,
#             sender=parsed_event["provider"],
#             owner=parsed_event["provider"],
#             amounts=[
#                 amount / tokens_scalars[idx]
#                 for idx, amount in enumerate(parsed_event["tokenAmounts"])
#             ],
#         )
#         return mint
#
#     @staticmethod
#     def get_swap_from_event(
#         curve_pool: CurvePool, parsed_event: dict, tokens_scalars: list
#     ) -> Swap:
#         first_coin_index = parsed_event["soldId"]
#         second_coin_index = parsed_event["boughtId"]
#
#         amount_first_coin = parsed_event["tokensSold"] / tokens_scalars[first_coin_index]
#         amount_second_coin = parsed_event["tokensBought"] / tokens_scalars[second_coin_index]
#         amounts = [0 for i in range(len(curve_pool.tokens_addresses))]
#         amounts[first_coin_index] = amount_first_coin
#         amounts[second_coin_index] = amount_second_coin
#
#         swap = Swap(
#             pool_address=curve_pool.address,
#             sender=parsed_event["buyer"],
#             to=parsed_event["buyer"],
#             amounts=amounts,
#         )
#         swap.direction_indexes = [first_coin_index, second_coin_index]
#         return swap
#
#     def get_burns_from_events(
#         self,
#         curve_pool: CurvePool,
#         parsed_event: dict,
#         event_name: str,
#         tokens_scalars: list,
#     ) -> MintBurn | None:
#         if event_name == SaddleTransactionType.RemoveLiquidityOne.name:
#             amounts = [0] * len(curve_pool.tokens_addresses)
#             coin_index = parsed_event["boughtId"]
#             amounts[coin_index] = parsed_event["tokensBought"] / tokens_scalars[coin_index]
#
#             return MintBurn(
#                 pool_address=curve_pool.address,
#                 sender=parsed_event["provider"],
#                 owner=parsed_event["provider"],
#                 amounts=amounts,
#             )
#         else:
#             return MintBurn(
#                 pool_address=curve_pool.address,
#                 sender=parsed_event["provider"],
#                 owner=parsed_event["provider"],
#                 amounts=[
#                     amount / tokens_scalars[idx]
#                     for idx, amount in enumerate(parsed_event["tokenAmounts"])
#                 ],
#             )
#
#     def get_pool_finances(
#         self,
#         curve_pool: CurvePool,
#         tokens_scalars: list[int],
#         contract: str = POOL_CONTRACT,
#         block_number: int | str = "latest",
#     ) -> PoolFinances:
#         pool = PoolFinances(**curve_pool.dict())
#         reserves = []
#         for idx, token in enumerate(curve_pool.tokens_addresses):
#             token_reserve = self.get_contract_reserve(curve_pool.address, token)
#             if token_reserve == 0:
#                 try:
#                     token_reserve = (
#                         self.abi[contract]
#                         .contract.functions.getTokenBalance(idx)
#                         .call(
#                             {"to": curve_pool.address},
#                             block_number,
#                         )
#                     )
#                 except Exception:
#                     pass
#             reserves.append(token_reserve / tokens_scalars[idx])
#         pool.reserves = reserves
#
#         # Calculate price rate for every token in pool (exchange every token with each other and take average)
#         # Another solution was suggested by Curve founder (Michael Egorov): calculate rate for coin i-th
#         # try to get exchange rate for i->j, j->i and take average. On practice results were not great
#         #
#         # Note: Token_scalars should be considered during exchange (example: USDT - 6, DAI - 18)
#         coin_count = len(curve_pool.tokens_addresses)
#         prices = get_default_prices(coin_count)
#         for coin_index in range(0, coin_count):
#             for coin_index_for_swap in range(0, coin_count):
#                 if coin_index == coin_index_for_swap:
#                     continue
#                 dx, exchange_precision = self.get_right_exchange_amount_for_coins(
#                     coin_index, coin_index_for_swap, tokens_scalars
#                 )
#                 exchange_rate = None
#                 try:
#                     exchange_rate = (
#                         self.abi[contract]
#                         .contract.functions.calculateSwapUnderlying(
#                             coin_index, coin_index_for_swap, dx
#                         )
#                         .call({"to": curve_pool.address}, block_number)
#                     )
#                 except Exception:
#                     for contract_name in self.pool_contract_names:
#                         with suppress(Exception):
#                             exchange_rate = (
#                                 self.abi[contract_name]
#                                 .contract.functions.calculateSwap(
#                                     coin_index, coin_index_for_swap, dx
#                                 )
#                                 .call({"to": curve_pool.address}, block_number)
#                             )
#                             break
#                 if exchange_rate:
#                     prices[coin_index_for_swap][coin_index] = exchange_rate / exchange_precision
#                 else:
#                     logger.error(
#                         f"Can not detect price for token in saddle pool: {curve_pool.address} {coin_index} "
#                         f"{coin_index_for_swap}"
#                     )
#                     raise Exception(
#                         f"Failed to detect price for saddle pool: {curve_pool.address}"
#                     )
#         pool.prices = prices
#         return pool
#
#     def hydrate_pool_with_metapool_finances(
#         self,
#         curve_pool: CurvePool,
#         underlying_scalars: list[int],
#         contract: str = POOL_CONTRACT_V2,
#         block_number: int | str = "latest",
#     ) -> PoolFinances | None:
#         pool_finances = PoolFinances(
#             address=curve_pool.address,
#             tokens_addresses=curve_pool.u_tokens_addresses,
#         )
#
#         underlying_balances = self.get_underlying_balances_for_pool(
#             curve_pool.address, block_number
#         )
#         if not underlying_balances:
#             return None
#
#         u_tokens_count = len(curve_pool.u_tokens_addresses)
#         underlying_balances = [
#             underlying_balances[idx] / underlying_scalars[idx] for idx in range(u_tokens_count)
#         ]
#         pool_finances.reserves = underlying_balances
#
#         u_prices = get_default_prices(u_tokens_count)
#         for coin_index in range(0, u_tokens_count):
#             for coin_index_for_swap in range(0, u_tokens_count):
#                 if coin_index == coin_index_for_swap:
#                     continue
#                 dx, exchange_precision = self.get_right_exchange_amount_for_coins(
#                     coin_index, coin_index_for_swap, underlying_scalars
#                 )
#                 exchange_rate = None
#                 with suppress(Exception):
#                     exchange_rate = (
#                         self.abi[contract]
#                         .contract.functions.calculateSwapUnderlying(
#                             coin_index, coin_index_for_swap, dx
#                         )
#                         .call({"to": curve_pool.address}, block_number)
#                     )
#                 if exchange_rate:
#                     u_prices[coin_index_for_swap][coin_index] = exchange_rate / exchange_precision
#                 else:
#                     logger.error(
#                         f"Can not detect price for token in saddle pool: {curve_pool.address} {coin_index} "
#                         f"{coin_index_for_swap}"
#                     )
#
#                     return None
#         pool_finances.prices = u_prices
#         return pool_finances
#
#     @staticmethod
#     def get_right_exchange_amount_for_coins(i, j, tokens_scalars: list) -> tuple[int, int]:
#         if tokens_scalars[i] == tokens_scalars[j]:
#             return tokens_scalars[i], tokens_scalars[j]
#         elif tokens_scalars[i] > tokens_scalars[j]:
#             return max(tokens_scalars[i], tokens_scalars[j]), min(
#                 tokens_scalars[i], tokens_scalars[j]
#             )
#         else:
#             return min(tokens_scalars[i], tokens_scalars[j]), max(
#                 tokens_scalars[i], tokens_scalars[j]
#             )
#
#     def get_num_pairs(self, block_identifier: str | int = "latest") -> int | None:
#         try:
#             return (
#                 self.abi[FACTORY_CONTRACT]
#                 .contract.functions.getPoolsLength()
#                 .call(block_identifier=block_identifier)
#             )
#         except (BadFunctionCallOutput, ValueError) as e:
#             logger.error(f"Cant get pairs length on Saddle amm {self.factory_address}, error: {e}")
#
#     def get_pool_data_by_index(self, pool_index: int) -> tuple | None:
#         try:
#             pool_data = (
#                 self.abi[FACTORY_CONTRACT]
#                 .contract.functions.getPoolDataAtIndex(pool_index)
#                 .call(block_identifier="latest")
#             )
#         except (TypeError, BadFunctionCallOutput, ValueError, ContractLogicError):
#             logger.error(f"Can't get Saddle pool info with pool_index {pool_index}")
#             return None
#
#         return pool_data
#
#     def get_list_of_registered_pools(self, block_identifier: str | int = "latest") -> dict | None:
#         num_pairs = self.get_num_pairs(block_identifier)
#         if not num_pairs:
#             return None
#         pools_dict = defaultdict(list)
#         if self.get_chain_id == 250:
#             # Fix for Fantom pools which were deployed before registry
#             pools_dict[1].extend(
#                 [
#                     item.lower()
#                     for item in [
#                         "0x4E1484607760118ebE2Ab07C0c71f1B4D9671e01",
#                         "0x0E510c9b20a5D136E75f7FD2a5F344BD98f9d875",
#                         "0xd7D1b50c8ef77d9aB410723f81363C8B252C729F",
#                         "0xdb5c5A6162115Ce9a188E7D773C4D011F421BbE5",
#                         "0x4A5208F83A17E030a18830521E4064E80728c4FC",
#                         "0x21EA072844fd4aBEd72539750c054E009D877f72",
#                         "0xBea9F78090bDB9e662d8CB301A00ad09A5b756e9",
#                         "0xc969dD0A7AB0F8a0C5A69C0839dB39b6C928bC08",
#                     ]
#                 ]
#             )
#             return pools_dict
#
#         for pool_index in range(0, num_pairs):
#             pool_address = self.get_pool_address_by_index(pool_index)
#
#             pools_dict[1].append(pool_address)
#
#         return pools_dict
#
#     def get_pool_data_by_address(self, pool_address: str) -> dict | None:
#         pool_address = self.w3.toChecksumAddress(pool_address)
#         try:
#             pool_data = (
#                 self.abi[FACTORY_CONTRACT]
#                 .contract.functions.getPoolData(pool_address)
#                 .call(block_identifier="latest")
#             )
#         except (TypeError, BadFunctionCallOutput, ValueError, ContractLogicError):
#             logger.error(f"Can't get Saddle pool info with pool_address {pool_address}")
#             return None
#
#         return pool_data
#
#     def get_lp_token_address_for_pool(self, pool_address: str) -> list[str] | None:
#         pool_data = self.get_pool_data_by_address(pool_address)
#         if not pool_data:
#             return [pool_address.lower()]
#
#         lp_token_address = pool_data[1]
#         if lp_token_address != config.BLACK_HOLE:
#             return [lp_token_address.lower()]
#
#     def get_pool_address_by_index(
#         self, pool_index: int, block_identifier: str | int = "latest"
#     ) -> str | None:
#         pool_data = self.get_pool_data_by_index(pool_index)
#         if pool_data:
#             return pool_data[0]
#         return None
#
#     def get_underlying_tokens_by_pool_address(self, pool_address: str) -> list[str]:
#         pool_data = self.get_pool_data_by_address(pool_address)
#         if pool_data:
#             return pool_data[6]
#         return []
#
#     def get_scalars_for_tokens(self, tokens: list) -> list[int]:
#         return [10 ** self.get_token_decimals(token) for token in tokens]
#
#     def get_underlying_balances_for_pool(self, pool_address: str, block_number: str | int) -> list:
#         try:
#             return (
#                 self.abi[FACTORY_CONTRACT]
#                 .contract.functions.getUnderlyingTokenBalances(pool_address)
#                 .call(block_identifier=block_number)
#             )
#         except Exception as exc:
#             logger.error(
#                 f"Can't get underlying balances for metapool: {pool_address}, failing with error: {exc}"
#             )
#         return []
#
#     def get_base_pool(self, address: ChecksumAddress) -> CurvePool:
#         logger.debug(f"Resolving pool addresses for {address}")
#         tokens_addresses = self.get_pool_data_by_address(address)
#         tokens_addresses = tokens_addresses[4]
#
#         curve_pool = CurvePool(
#             address=address,
#             tokens_addresses=tokens_addresses,
#         )
#         underlying_addresses = self.get_underlying_tokens_by_pool_address(curve_pool.address)
#         if underlying_addresses:
#             curve_pool.u_tokens_addresses = underlying_addresses
#         return curve_pool
#
#     def get_pair_tokens_by_index(
#         self, pair_index: int, block_identifier: str | int = "latest"
#     ) -> dict | None:
#         pool_data = self.get_pool_data_by_index(pair_index)
#         if not pool_data or pool_data[10]:
#             return None
#         return {
#             "address": pool_data[0].lower(),
#             "tokens_addresses": [x.lower() for x in pool_data[5]],
#             "u_tokens_addresses": [x.lower() for x in pool_data[6]],
#             "lp_token_addresses": [pool_data[1].lower()],
#         }
