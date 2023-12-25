# from collections import defaultdict
# from pathlib import Path
#
# from clients.blockchain import BaseBlockchainClient
# from clients.blockchain.amm.base.base_contract import BaseContract
# from clients.blockchain.interfaces import AmmClientI
# from clients.blockchain.models.curve_transaction import CurveTransactionType
# from clients.blockchain.models.pool import (
#     BasePool,
#     CurvePool,
#     PoolFinances,
#     RegistryInfo,
#     RegistryInfoList,
# )
# from clients.blockchain.models.protocol_transaction import MintBurn, Swap
# from clients.blockchain.models.tokens import ERC20Token
# from clients.blockchain.models.transaction import ReceiptLog
# from clients.blockchain.models.transfer import TransferBase
# from config import config
# from eth_typing import ChecksumAddress
# from utils.logger import get_logger
# from utils.prices import get_default_prices
# from web3 import Web3
# from web3.exceptions import BadFunctionCallOutput, ContractLogicError
#
# logger = get_logger(__name__)
#
# ADDRESS_PROVIDER_CONTRACT = "AddressProvider"
# FACTORY_CONTRACT = "Registry"
# POOL_CONTRACT_V1 = "CurvePoolv1"
# EPSILON = 10**-8  # 0.00000001
#
#
# class CurveAmm(BaseContract, BaseBlockchainClient, AmmClientI):
#     pool_contract_names = [f"CurvePoolv{number}" for number in range(1, 8)]
#     _pool_contract_abi_mapper = {
#         f"CurvePoolv{number}": f"CurvePoolv{number}.json" for number in range(1, 8)
#     }
#     pool_contracts_events_enum = CurveTransactionType
#
#     def __init__(self, uri: str, amm_type: str, contracts: dict):
#         super().__init__(uri=uri, amm_type=amm_type, contracts=contracts)
#         _path_to_this_file = Path(__file__)
#         _path_to_abi = Path(_path_to_this_file.parent.parent, "abi")
#         self.abi_path = f"{_path_to_abi}/{amm_type}/"
#
#         for name, file in self._pool_contract_abi_mapper.items():
#             pool_abi_path = self.abi_path + file
#             self.abi[name] = self._initiate_contract(pool_abi_path)
#
#         self._setup_registry_contract(contracts, self.abi_path)
#
#     def _setup_registry_contract(self, contracts: dict, abi_path: str):
#         address_provider = self.w3.toChecksumAddress(contracts["AddressProvider"])
#         self.registry_count = (
#             self.abi[ADDRESS_PROVIDER_CONTRACT]
#             .contract.functions.max_id()
#             .call({"to": address_provider}, "latest")
#         )
#
#         for registry_id in range(0, self.registry_count + 1):
#             registry_address = (
#                 self.abi[ADDRESS_PROVIDER_CONTRACT]
#                 .contract.functions.get_address(
#                     registry_id,
#                 )
#                 .call({"to": address_provider}, "latest")
#             )
#             if (
#                 not registry_address
#                 or registry_address == "0x0000000000000000000000000000000000000000"
#             ):
#                 continue
#
#             registry_abi_path = abi_path + "CurveRegistry.json"
#             self.abi[FACTORY_CONTRACT + f"v_{registry_id}"] = self._initiate_contract(
#                 registry_abi_path, registry_address
#             )
#
#     def resolve_receipt_log(
#         self,
#         receipt_log: ReceiptLog,
#         base_pool: BasePool,
#         erc20_tokens: list[ERC20Token],
#         transfers_for_transaction: list[TransferBase],
#     ) -> dict | None:
#         logger.debug(f"resolving {receipt_log.transaction_hash.hex()}-{receipt_log.log_index}")
#         try:
#             topic = receipt_log.topics[0][0:4]
#         except IndexError:
#             logger.error(f"Cant get receipt_log.topics[0][0:4], index error, log: {receipt_log}")
#             return None
#         event_name = None
#         selected_pool_contract = None
#         for name in self.pool_contract_names:
#             current_event_name = self.abi[name].topic_keccaks.get(topic, None)
#             if current_event_name:
#                 event_name = current_event_name
#                 selected_pool_contract = name
#                 break
#         curve_pool = CurvePool(
#             address=base_pool.address,
#             tokens_addresses=base_pool.tokens_addresses,
#             fee=base_pool.fee,
#         )
#         if receipt_log.topics and event_name:
#             tokens_scalars = []
#             for erc20_token in erc20_tokens:
#                 tokens_scalars.append(10**erc20_token.decimals)
#
#             parsed_event = None
#             try:
#                 parsed_event = self.parse_event(
#                     self.abi[selected_pool_contract], event_name, receipt_log
#                 )
#             except Exception:
#                 pass
#
#             parsed_event = self.convert_burn_event_to_correct_event(
#                 event_name, parsed_event, receipt_log
#             )
#             if not parsed_event:
#                 return None
#
#             self.pool_contracts_events_enum = CurveTransactionType
#             if event_name == self.pool_contracts_events_enum.TokenExchange.name:
#                 logger.debug("resolving swap from swap event")
#                 swap = self.get_swap_from_exchange_event(curve_pool, parsed_event, tokens_scalars)
#                 logger.debug(f"resolved swap from swap event {swap}")
#                 swap.log_index = receipt_log.log_index
#                 pool_finances = self.get_pool_finances(
#                     curve_pool,
#                     tokens_scalars,
#                     block_identifier=receipt_log.block_number,
#                 )
#                 if not pool_finances:
#                     return None
#                 return {"swaps": [swap], "pools": [pool_finances]}
#
#             if event_name == self.pool_contracts_events_enum.TokenExchangeUnderlying.name:
#                 curve_metapool = curve_pool
#                 if len(curve_pool.u_tokens_addresses) == 0:
#                     curve_metapool = self.enrich_pool_with_metapool_addresses(curve_pool)
#                 if not curve_metapool:
#                     return None
#
#                 underlying_scalars = self.get_underlying_scalars_for_metapool(curve_metapool)
#                 if not underlying_scalars:
#                     return None
#                 logger.debug("resolving swap for underlying coins event")
#                 swap = self.get_swap_from_underlying_event(
#                     curve_pool, parsed_event, underlying_scalars
#                 )
#                 logger.debug("resolved swap for underlying coins event")
#                 swap.log_index = receipt_log.log_index
#                 metapool_finances = self.hydrate_pool_with_metapool_finances(
#                     curve_metapool,
#                     underlying_scalars,
#                     block_identifier=receipt_log.block_number,
#                 )
#                 if not metapool_finances:
#                     return None
#
#                 return {
#                     "swaps": [swap],
#                     "pools": [metapool_finances],
#                 }
#
#             if event_name == self.pool_contracts_events_enum.AddLiquidity.name:
#                 logger.debug("resolving mint from add_liquidity event")
#                 mint = self.get_mint_from_events(curve_pool, parsed_event, tokens_scalars)
#                 logger.debug("resolved mint from add_liquidity event")
#                 mint.log_index = receipt_log.log_index
#                 pool_finances = self.get_pool_finances(
#                     curve_pool,
#                     tokens_scalars,
#                     block_identifier=receipt_log.block_number,
#                 )
#                 if not pool_finances:
#                     return None
#                 return {"mints": [mint], "pools": [pool_finances]}
#
#             if event_name in self.pool_contracts_events_enum.burn_events():
#                 logger.debug("resolving burn from curve_burn events")
#                 burn = self.get_burns_from_events(
#                     curve_pool,
#                     parsed_event,
#                     event_name,
#                     tokens_scalars,
#                     receipt_log.block_number,
#                     transfers_for_transaction,
#                 )
#                 if not burn:
#                     return None
#                 logger.debug("resolved burn from curve_burn events")
#                 burn.log_index = receipt_log.log_index
#                 pool_finances = self.get_pool_finances(
#                     curve_pool,
#                     tokens_scalars,
#                     block_identifier=receipt_log.block_number - 1,
#                 )
#                 if not pool_finances:
#                     return None
#                 return {"burns": [burn], "pools": [pool_finances]}
#
#     def get_pool_finances(
#         self,
#         curve_pool: CurvePool,
#         tokens_scalars: list[int],
#         contract: str = POOL_CONTRACT_V1,
#         block_identifier: int = "latest",
#     ) -> PoolFinances | None:
#         pool = PoolFinances(**curve_pool.dict())
#         reserves = []
#         for idx, token in enumerate(curve_pool.tokens_addresses):
#             token_reserve = self.get_contract_reserve(curve_pool.address, token, block_identifier)
#             if token_reserve == 0:
#                 try:
#                     token_reserve = (
#                         self.abi[contract]
#                         .contract.functions.balances(idx)
#                         .call(
#                             {"to": curve_pool.address},
#                             block_identifier,
#                         )
#                     )
#                 except:
#                     token_reserve = 0
#             reserves.append(token_reserve / tokens_scalars[idx])
#         pool.reserves = reserves
#
#         # Solution was suggested by Curve founder (Michael Egorov): calculate rate for coin i-th
#         # try to get exchange rate for i->j, j->i and take average. On practice results were not great
#         #
#         # Note: Token_scalars should be considered during exchange (example: USDT 6, DAI - 18)
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
#                         .contract.functions.get_dy_underlying(coin_index, coin_index_for_swap, dx)
#                         .call({"to": curve_pool.address}, "latest")
#                     )
#                 except Exception:
#                     for contract_name in ["CurvePoolv1", "CurvePoolv3"]:
#                         try:
#                             exchange_rate = (
#                                 self.abi[contract_name]
#                                 .contract.functions.get_dy(coin_index, coin_index_for_swap, dx)
#                                 .call({"to": curve_pool.address}, "latest")
#                             )
#                             break
#                         except Exception:
#                             pass
#                 if exchange_rate:
#                     prices[coin_index_for_swap][coin_index] = exchange_rate / exchange_precision
#                 else:
#                     logger.error(
#                         f"Can not detect price for token in curve pool: {curve_pool.address} {coin_index} "
#                         f"{coin_index_for_swap}"
#                     )
#                     return None
#         pool.prices = prices
#         return pool
#
#     def hydrate_pool_with_metapool_finances(
#         self,
#         curve_pool: CurvePool,
#         underlying_scalars: list[int],
#         contract: str = POOL_CONTRACT_V1,
#         block_identifier: int = "latest",
#     ) -> PoolFinances | None:
#         pool_finances = PoolFinances(
#             address=curve_pool.address,
#             tokens_addresses=curve_pool.u_tokens_addresses,
#         )
#         underlying_balances = []
#         start_id = self.current_registry_id if self.current_registry_id else 0
#         end_id = start_id if start_id else self.registry_count
#         for registry_id in range(start_id, end_id + 1):
#             try:
#                 if not self.abi.get(FACTORY_CONTRACT + "v_" + str(registry_id)):
#                     continue
#                 underlying_balances = (
#                     self.abi[FACTORY_CONTRACT + "v_" + str(registry_id)]
#                     .contract.functions.get_underlying_balances(curve_pool.address)
#                     .call(block_identifier=block_identifier)
#                 )
#             except Exception as exc:
#                 logger.info(
#                     f"Can't get underlying balances for metapool: {curve_pool.address}, "
#                     f"from contract v_{registry_id}, failing with error: {exc}"
#                 )
#                 continue
#         if not underlying_balances:
#             return None
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
#                 try:
#                     exchange_rate = (
#                         self.abi[contract]
#                         .contract.functions.get_dy_underlying(coin_index, coin_index_for_swap, dx)
#                         .call({"to": curve_pool.address}, block_identifier)
#                     )
#                 except Exception:
#                     pass
#                 if exchange_rate:
#                     u_prices[coin_index_for_swap][coin_index] = exchange_rate / exchange_precision
#                 else:
#                     logger.error(
#                         f"Can not detect price for token in curve pool: {curve_pool.address}"
#                     )
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
#     def get_burns_from_events(
#         self,
#         curve_pool: CurvePool,
#         parsed_event,
#         event_name,
#         tokens_scalars,
#         block_identifier,
#         transfers_for_transaction: list[TransferBase],
#         contract: str = POOL_CONTRACT_V1,
#     ) -> MintBurn | None:
#         if event_name == CurveTransactionType.RemoveLiquidityOne.name:
#             amounts = [0] * len(curve_pool.tokens_addresses)
#             coin_index = self.detect_coin_index_for_remove_liquidity_one_event(
#                 parsed_event, curve_pool, transfers_for_transaction
#             )
#
#             if coin_index is None:
#                 logger.error(
#                     f"Can not detect coin_index while resolving event remove_liquidity_one event"
#                     f" pool_index: {curve_pool.address}"
#                     f" in block: {block_identifier}"
#                 )
#                 return None
#             amounts[coin_index] = parsed_event["coin_amount"] / tokens_scalars[coin_index]
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
#                     for idx, amount in enumerate(parsed_event["token_amounts"])
#                 ],
#             )
#
#     @staticmethod
#     def detect_coin_index_for_remove_liquidity_one_event(
#         parsed_event: dict,
#         curve_pool: CurvePool,
#         transfers_for_transaction: list[TransferBase],
#     ) -> int | None:
#         coin_index, transfers_with_coin = None, []
#         for transfer in transfers_for_transaction:
#             if abs(transfer.value - parsed_event["coin_amount"]) < EPSILON:
#                 transfers_with_coin.append(transfer)
#
#         if not transfers_with_coin:
#             return None
#
#         for idx, token_address in enumerate(curve_pool.tokens_addresses):
#             for transfer in transfers_with_coin:
#                 if token_address.lower() == transfer.token_address:
#                     coin_index = idx
#                     break
#         return coin_index
#
#     @staticmethod
#     def get_mint_from_events(curve_pool: CurvePool, parsed_event, tokens_scalars) -> MintBurn:
#         mint = MintBurn(
#             pool_address=curve_pool.address,
#             sender=parsed_event["provider"],
#             owner=parsed_event["provider"],
#             amounts=[
#                 amount / tokens_scalars[idx]
#                 for idx, amount in enumerate(parsed_event["token_amounts"])
#             ],
#         )
#         return mint
#
#     @staticmethod
#     def get_swap_from_exchange_event(curve_pool: CurvePool, parsed_event, tokens_scalars) -> Swap:
#         first_coin_index = parsed_event["sold_id"]
#         second_coin_index = parsed_event["bought_id"]
#
#         amount_first_coin = parsed_event["tokens_sold"] / tokens_scalars[first_coin_index]
#         amount_second_coin = parsed_event["tokens_bought"] / tokens_scalars[second_coin_index]
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
#     @staticmethod
#     def get_swap_from_underlying_event(
#         curve_pool: CurvePool, parsed_event, underlying_tokens_scalars
#     ) -> Swap:
#         first_coin_index = parsed_event["sold_id"]
#         second_coin_index = parsed_event["bought_id"]
#
#         amount_first_coin = (
#             parsed_event["tokens_sold"] / underlying_tokens_scalars[first_coin_index]
#         )
#         amount_second_coin = (
#             parsed_event["tokens_bought"] / underlying_tokens_scalars[second_coin_index]
#         )
#         amounts = [0 for i in range(len(curve_pool.u_tokens_addresses))]
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
#     def enrich_pool_with_metapool_addresses(self, curve_pool: CurvePool):
#         for registry_id in range(0, self.registry_count + 1):
#             try:
#                 if not self.abi.get(FACTORY_CONTRACT + "v_" + str(registry_id)):
#                     continue
#                 tokens_addresses = (
#                     self.abi[FACTORY_CONTRACT + "v_" + str(registry_id)]
#                     .contract.functions.get_underlying_coins(curve_pool.address)
#                     .call(block_identifier="latest")
#                 )
#             except Exception:
#                 logger.error(
#                     f"Can't get token_addresses for metapool: {curve_pool.address} from "
#                     f"contract v_{registry_id}"
#                 )
#                 continue
#             tokens_addresses = [
#                 address for address in tokens_addresses if address != config.BLACK_HOLE
#             ]
#             if tokens_addresses:
#                 curve_pool.u_tokens_addresses = tokens_addresses
#                 self.current_registry_id = registry_id
#                 return curve_pool
#         return curve_pool
#
#     def get_underlying_scalars_for_metapool(self, curve_pool: CurvePool):
#         start_id = self.current_registry_id if self.current_registry_id else 0
#         end_id = start_id if start_id else self.registry_count
#         for registry_id in range(start_id, end_id + 1):
#             try:
#                 if not self.abi.get(FACTORY_CONTRACT + "v_" + str(registry_id)):
#                     continue
#                 underlying_decimals = (
#                     self.abi[FACTORY_CONTRACT + "v_" + str(registry_id)]
#                     .contract.functions.get_underlying_decimals(curve_pool.address)
#                     .call(block_identifier="latest")
#                 )
#             except Exception:
#                 logger.error(
#                     f"Can't get decimals for coins in metapool: {curve_pool.address} from"
#                     f"contract v_{registry_id}"
#                 )
#                 continue
#             underlying_decimals = [scalar for scalar in underlying_decimals if scalar != 0]
#             if len(underlying_decimals) == len(curve_pool.u_tokens_addresses):
#                 return [10**scalar for scalar in underlying_decimals]
#         return None
#
#     def get_base_pool(self, address: ChecksumAddress) -> CurvePool | None:
#         logger.debug(f"Resolving pool addresses for {address}")
#         tokens_addresses = self.get_tokens_addresses_for_pool(address)
#
#         if tokens_addresses:
#             fee = (
#                 self.abi[POOL_CONTRACT_V1].contract.functions.fee().call({"to": address}, "latest")
#             )
#             curve_pool = CurvePool(
#                 address=address,
#                 tokens_addresses=tokens_addresses,
#                 fee=fee,
#             )
#             curve_pool = self.enrich_pool_with_metapool_addresses(curve_pool)
#             return curve_pool
#
#     def get_tokens_balances_for_pool(
#         self,
#         curve_pool: CurvePool,
#         block_identifier: str | int = "latest",
#         **kwargs,
#     ) -> list | None:
#         balances = []
#         for i in range(len(curve_pool.tokens_addresses)):
#             for pool_contract in self._pool_contract_abi_mapper.keys():
#                 try:
#                     balance = (
#                         self.abi[pool_contract]
#                         .contract.functions.balances(i)
#                         .call(
#                             {"to": curve_pool.address},
#                             block_identifier,
#                         )
#                     )
#                     balances.append(balance)
#                     break
#                 except Exception:
#                     pass
#         return balances if len(balances) == len(curve_pool.tokens_addresses) else None
#
#     def get_tokens_addresses_for_pool(
#         self, pool_address: str, block_identifier: str | int = "latest"
#     ) -> list | None:
#         token_addresses = []
#         coin_index = 0
#         while True:
#             current_token_address = None
#             for pool_contract in self._pool_contract_abi_mapper.keys():
#                 try:
#                     current_token_address = (
#                         self.abi[pool_contract]
#                         .contract.functions.coins(coin_index)
#                         .call(
#                             {"to": pool_address},
#                             block_identifier,
#                         )
#                     )
#                     break
#                 except (TypeError, ContractLogicError, ValueError):
#                     continue
#             if not current_token_address:
#                 break
#             else:
#                 token_addresses.append(current_token_address)
#                 coin_index += 1
#         return token_addresses
#
#     def get_num_pairs(
#         self, block_identifier: str | int = "latest"
#     ) -> tuple[int, RegistryInfoList]:
#         """
#         In Curve we have 8 registries and only 4 of them have .pool_count() function so that
#         we iterate over each of registry and check, which has pool info.
#         """
#         sum_of_pools = 0
#         pool_contract_info: list[RegistryInfo] = []
#         for registry_id in range(0, self.registry_count + 1):
#             try:
#                 if not self.abi.get(FACTORY_CONTRACT + "v_" + str(registry_id)):
#                     continue
#                 count = (
#                     self.abi[FACTORY_CONTRACT + f"v_{registry_id}"]
#                     .contract.functions.pool_count()
#                     .call(block_identifier="latest")
#                 )
#                 pool_contract_info.append(
#                     RegistryInfo(registry_id=registry_id, number_of_pools=count)
#                 )
#                 sum_of_pools += count
#             except (
#                 BadFunctionCallOutput,
#                 ValueError,
#                 Exception,
#             ) as e:
#                 logger.info(
#                     f"Cant get pairs length on {self.factory_address} v_{registry_id}: 'latest', error: {e}"
#                 )
#         return sum_of_pools, RegistryInfoList(data=pool_contract_info)
#
#     def get_pair_tokens_by_index(
#         self,
#         pair_index: int,
#         block_identifier: str | int = "latest",
#         registry_index: int = 0,
#     ) -> dict | None:
#         """
#         Gets pool_address and tokens addresses for pool_index
#         :param pool_index: Index of the pair in the factory.
#         :param block_identifier: block number(int) or 'latest'
#         :param registry_index: version of contract to call
#         :return: Pair dict pair address, tokens addresses.
#         """
#         pool_address = self.get_pool_address_by_index(pair_index, block_identifier, registry_index)
#         pool_address = Web3.toChecksumAddress(pool_address)
#
#         if not pool_address:
#             logger.error(
#                 f"Can get pair by index for AMM:{self.factory_address}, index:{pair_index},"
#                 f" at {block_identifier}"
#             )
#             return None
#         tokens_addresses = self.get_tokens_addresses_for_pool(pool_address, block_identifier)
#         curve_pool = CurvePool(
#             address=pool_address.lower(),
#             tokens_addresses=tokens_addresses,
#         )
#         curve_pool = self.enrich_pool_with_metapool_addresses(curve_pool)
#         return {
#             "address": pool_address.lower(),
#             "tokens_addresses": [x.lower() for x in tokens_addresses],
#             "u_tokens_addresses": [x.lower() for x in curve_pool.u_tokens_addresses],
#         }
#
#     def get_pool_address_by_index(
#         self,
#         pool_index: int,
#         block_identifier: str | int = "latest",
#         registry_index: int = 0,
#     ) -> str | None:
#         """
#         Gets the address of the nth pool (0-indexed) created through
#         the factory, or 0x0 if not enough pairs have been created yet.
#         :param pool_index: Index of the simple pool (not metapool) in the factory.
#         :param block_identifier: block number(int) or 'latest'
#         :param registry_index: version of contract to call
#         :return: Address of the indexed pool.
#         """
#         try:
#             return Web3.toHex(
#                 hexstr=self.abi[FACTORY_CONTRACT + f"v_{registry_index}"]
#                 .contract.functions.pool_list(pool_index)
#                 .call(block_identifier=block_identifier)
#             )
#         except BadFunctionCallOutput:
#             logger.error(
#                 "BadFunctionCallOutput while requesting address for pool with"
#                 f" pool_index: {pool_index}"
#                 f" in block: {block_identifier}"
#                 f" from contract v_{registry_index}"
#             )
#             return None
#         except ValueError:
#             logger.error(
#                 "Found no address for pool with"
#                 f" pool_index: {pool_index}"
#                 f" in block: {block_identifier}"
#                 f" from contract v_{registry_index}"
#             )
#             return None
#
#     def get_lp_token_address_for_pool(self, pool_address: str) -> list[str]:
#         pool_address = self.w3.toChecksumAddress(pool_address)
#         for registry_id in range(0, self.registry_count + 1):
#             if not self.abi.get(FACTORY_CONTRACT + "v_" + str(registry_id)):
#                 continue
#             if not hasattr(
#                 self.abi[FACTORY_CONTRACT + f"v_{registry_id}"].contract.functions,
#                 "get_lp_token",
#             ):
#                 continue
#             try:
#                 lp_address = (
#                     self.abi[FACTORY_CONTRACT + f"v_{registry_id}"]
#                     .contract.functions.get_lp_token(pool_address)
#                     .call({"block_identifier": "latest"})
#                 )
#                 if lp_address != config.BLACK_HOLE:
#                     return [lp_address.lower()]
#             except (ContractLogicError, BadFunctionCallOutput, ValueError) as e:
#                 logger.error(
#                     f"Can not get lp address for: {pool_address} from contract v_{registry_id}",
#                     extra={"amm": "curve", "exception": e},
#                 )
#         return [pool_address.lower()]
#
#     def get_list_of_registered_pools(
#         self, block_identifier: str | int = "latest"
#     ) -> dict | None:
#         pools_regestries_info = self.get_num_pairs(block_identifier)
#         if not pools_regestries_info:
#             return None
#
#         pools_dict = defaultdict(list)
#         for registry_info in pools_regestries_info[1].data:
#             for pool_index in range(0, registry_info.number_of_pools):
#                 pool_address = self.get_pool_address_by_index(
#                     pool_index,
#                     block_identifier,
#                     registry_index=registry_info.registry_id,
#                 )
#
#                 pools_dict[registry_info.registry_id].append(pool_address)
#
#         if self.get_chain_id == 42220:
#             # Fix for CELO pools which were deployed before registry
#             pools_dict[0].extend(
#                 [
#                     item.lower()
#                     for item in [
#                         "0x32fD7e563c6521Ab4D59CE3277bcfBe3317CFd63",
#                         "0x998395fed908d33cf27115a1d9ab6555def6cd45",
#                     ]
#                 ]
#             )
#
#         return pools_dict
#
#     def check_for_new_pairs(
#         self, num_of_pairs: int, pool_count_extra_info: RegistryInfoList, amm
#     ) -> list:
#         if not num_of_pairs:
#             return []
#         logger.info(f"Pair Index from amm {amm.id}, last indexed {amm.last_pair_index}")
#
#         new_pairs = []
#         if pool_count_extra_info:
#             start_index = 0 if amm.last_pair_index == 0 else amm.last_pair_index + 1
#             amm_extra_dict = {}
#             if amm.extra:
#                 amm_extra_dict = {
#                     registry_info["registry_id"]: registry_info["number_of_pools"]
#                     for registry_info in amm.extra["data"]
#                 }
#             for registry_info in pool_count_extra_info.data:
#                 registry_id, number_of_pools = (
#                     registry_info.registry_id,
#                     registry_info.number_of_pools,
#                 )
#
#                 start_pool_index, number_of_new_pools = 0, number_of_pools
#                 if (
#                     registry_id in amm_extra_dict
#                     and number_of_pools >= amm_extra_dict[registry_id]
#                 ):
#                     start_pool_index = amm_extra_dict[registry_id] + 1
#                     number_of_new_pools = number_of_pools - amm_extra_dict[registry_id]
#
#                 for pool_index_in_pool in range(start_pool_index, number_of_new_pools):
#                     pair = {
#                         amm.id: start_index,
#                         "amm_pool_info": pool_count_extra_info.dict(),
#                         "extra": {
#                             "registry_id": registry_id,
#                             "pool_index_in_pool": pool_index_in_pool,
#                         },
#                     }
#                     new_pairs.append(pair)
#                     start_index += 1
#         return new_pairs
#
#     def convert_burn_event_to_correct_event(self, event_name, parsed_event, receipt_log):
#         # because MAX number of coins in Curve is 8
#         if (
#             event_name == CurveTransactionType.RemoveLiquidityOne.name
#             and "coin_index" in parsed_event
#             and parsed_event["coin_index"] > 7
#         ):
#             try:
#                 return self.parse_event(self.abi["CurvePoolv5"], event_name, receipt_log)
#             except Exception:
#                 return None
#         return parsed_event
