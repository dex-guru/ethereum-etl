# from collections import defaultdict
# from pathlib import Path
#
# from clients.blockchain.amm.curve import CurveAmm
# from clients.blockchain.models.curve_transaction import CurveTransactionType
# from clients.blockchain.models.pool import CurvePool, PoolFinances
# from clients.blockchain.models.tokens import ERC20Token
# from clients.blockchain.models.transaction import ReceiptLog
# from clients.blockchain.models.transfer import TransferBase
# from config import config
# from eth_typing import ChecksumAddress
# from utils.logger import get_logger
# from web3 import Web3
# from web3.exceptions import BadFunctionCallOutput, ContractLogicError
#
# logger = get_logger(__name__)
#
# # https://github.com/ellipsis-finance/registry
# ADDRESS_PROVIDER_CONTARCT = "AddressProvider"
# FACTORY_CONTRACT = "Registry"
# POOL_CONTRACT = "EllipsisPool"
# POOL_CONTRACT_V2 = "EllipsisPoolv2"
#
#
# class EllipsisAmm(CurveAmm):
#     pool_contract_names = [POOL_CONTRACT, POOL_CONTRACT_V2]
#     _pool_contract_abi_mapper = {
#         POOL_CONTRACT: "EllipsisPool.json",
#         POOL_CONTRACT_V2: "EllipsisPoolv2.json",
#     }
#     pool_contracts_events_enum = CurveTransactionType
#
#     def __init__(self, uri: str, amm_type: str, contracts: dict):
#         _path_to_this_file = Path(__file__)
#         _path_to_abi = Path(_path_to_this_file.parent.parent, "abi")
#         self.abi_path = f"{_path_to_abi}/{amm_type}/"
#         super().__init__(uri=uri, amm_type=amm_type, contracts=contracts)
#         self._setup_registry_contract(contracts, self.abi_path)
#
#     def _setup_registry_contract(self, contracts: dict, abi_path: str):
#         address_provider = contracts["AddressProvider"]
#         registry_address = (
#             self.abi[ADDRESS_PROVIDER_CONTARCT]
#             .contract.functions.get_registry()
#             .call({"to": address_provider}, "latest")
#         )
#         registry_abi_path = abi_path + "Registry.json"
#         self.abi[FACTORY_CONTRACT] = self._initiate_contract(registry_abi_path, registry_address)
#
#     def get_base_pool(self, address: ChecksumAddress) -> CurvePool | None:
#         logger.debug(f"Resolving pool addresses for {address}")
#         tokens_addresses = self.get_tokens_addresses_for_pool(address)
#
#         if tokens_addresses:
#             fee = self.abi[POOL_CONTRACT].contract.functions.fee().call({"to": address}, "latest")
#             return CurvePool(
#                 address=address,
#                 tokens_addresses=tokens_addresses,
#                 fee=fee,
#             )
#
#     def get_tokens_addresses_for_pool(
#         self, pool_address: str, block_identifier: str | int = "latest"
#     ) -> list | None:
#         token_addresses = []
#         coin_index = 0
#         while True:
#             current_token_address = None
#             for pool_contract in self.pool_contract_names:
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
#                 .contract.functions.pool_count()
#                 .call(block_identifier=block_identifier)
#             )
#         except (BadFunctionCallOutput, ValueError) as e:
#             logger.error(
#                 f"Cant get pairs length on {self.factory_address}:{block_identifier}, trying latest: {e}"
#             )
#             try:
#                 return (
#                     self.abi[FACTORY_CONTRACT]
#                     .contract.functions.pool_count()
#                     .call(block_identifier="latest")
#                 )
#             except (BadFunctionCallOutput, ValueError) as e:
#                 logger.error(
#                     f"Cant get pairs length on {self.factory_address}: 'latest', error: {e}"
#                 )
#                 return 0
#
#     def get_list_of_registered_pools(
#         self, block_identifier: str | int = "latest"
#     ) -> dict | None:
#         num_pairs = self.get_num_pairs(block_identifier)
#         if not num_pairs:
#             return None
#         pools_dict = defaultdict(list)
#         for pool_index in range(0, num_pairs):
#             pool_address = self.get_pair_address_by_index(pool_index, block_identifier)
#
#             pools_dict[1].append(pool_address)
#         return pools_dict
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
#                 .contract.functions.pool_list(pair_index)
#                 .call(block_identifier=block_identifier)
#             )
#         except BadFunctionCallOutput:
#             logger.error(
#                 "BadFunctionCallOutput while requesting address for pair with"
#                 f" pair_index: {pair_index}"
#                 f" in block: {block_identifier}"
#             )
#             return
#         except ValueError:
#             logger.error(
#                 "Found no address for pair with"
#                 f" pair_index: {pair_index}"
#                 f" in block: {block_identifier}"
#             )
#             return
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
#                 f"Can't get pair by index for AMM:{self.factory_address}, index: {pair_index},"
#                 f" at {block_identifier}"
#             )
#             return
#         tokens = self.get_tokens_addresses_for_pool(pair_address, block_identifier)
#
#         return {"address": pair_address.lower(), "tokens_addresses": tokens}
#
#     def resolve_receipt_log(
#         self,
#         receipt_log: ReceiptLog,
#         curve_pool: CurvePool,
#         erc20_tokens: list[ERC20Token],
#         transfers_for_transaction: list[TransferBase],
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
#
#         tokens_scalars = []
#         for erc20_token in erc20_tokens:
#             tokens_scalars.append(10**erc20_token.decimals)
#
#         if event_name == self.pool_contracts_events_enum.AddLiquidity.name:
#             logger.debug("resolving mint from add_liquidity event")
#             mint = self.get_mint_from_events(curve_pool, parsed_event, tokens_scalars)
#             logger.debug("resolved mint from add_liquidity event")
#             mint.log_index = receipt_log.log_index
#             pool = self.get_pool_finances(
#                 curve_pool, tokens_scalars, use_contract, receipt_log.block_number
#             )
#             return {"mints": [mint], "pools": [pool]}
#
#         if event_name == self.pool_contracts_events_enum.TokenExchange.name:
#             logger.debug("resolving swap from swap event")
#             swap = self.get_swap_from_exchange_event(curve_pool, parsed_event, tokens_scalars)
#             logger.debug(f"resolved swap from swap event {swap}")
#             swap.log_index = receipt_log.log_index
#             pool = self.get_pool_finances(
#                 curve_pool, tokens_scalars, use_contract, receipt_log.block_number
#             )
#             return {"swaps": [swap], "pools": [pool]}
#
#         if event_name in self.pool_contracts_events_enum.burn_events():
#             logger.debug("resolving burn from curve_burn events")
#             burn = self.get_burns_from_events(
#                 curve_pool,
#                 parsed_event,
#                 event_name,
#                 tokens_scalars,
#                 receipt_log.block_number,
#                 transfers_for_transaction,
#             )
#             logger.debug("resolved burn from curve_burn events")
#             burn.log_index = receipt_log.log_index
#             pool = self.get_pool_finances(
#                 curve_pool, tokens_scalars, use_contract, receipt_log.block_number - 1
#             )
#             return {"burns": [burn], "pools": [pool]}
#
#     def get_pool_finances(
#         self,
#         curve_pool: CurvePool,
#         tokens_scalars: list[int],
#         contract: str = POOL_CONTRACT,
#         block_number: str | int = "latest",
#     ) -> PoolFinances:
#         pool = PoolFinances(**curve_pool.dict())
#         reserves = []
#         for idx, token in enumerate(curve_pool.tokens_addresses):
#             token_reserve = self.get_contract_reserve(curve_pool.address, token)
#             if token_reserve == 0:
#                 try:
#                     token_reserve = (
#                         self.abi[contract]
#                         .contract.functions.balances(idx)
#                         .call({"to": curve_pool.address}, block_identifier=block_number)
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
#         from utils.prices import get_default_prices
#
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
#                         .call({"to": curve_pool.address}, block_number)
#                     )
#                 except Exception:
#                     for contract_name in [POOL_CONTRACT, POOL_CONTRACT_V2]:
#                         try:
#                             exchange_rate = (
#                                 self.abi[contract_name]
#                                 .contract.functions.get_dy(coin_index, coin_index_for_swap, dx)
#                                 .call({"to": curve_pool.address}, block_number)
#                             )
#                             break
#                         except Exception:
#                             pass
#                 if exchange_rate:
#                     prices[coin_index_for_swap][coin_index] = exchange_rate / exchange_precision
#                 else:
#                     logger.error(
#                         f"Can not detect price for token in ellipsis pool: {curve_pool.address} {coin_index} "
#                         f"{coin_index_for_swap}"
#                     )
#                     raise Exception
#         pool.prices = prices
#         return pool
#
#     def get_tokens_balances_for_pool(
#         self,
#         curve_pool: CurvePool,
#         block_identifier: str | int,
#         *,
#         contract,
#     ) -> list | None:
#         balances = []
#         for i in range(len(curve_pool.tokens_addresses)):
#             try:
#                 balance = (
#                     self.abi[contract]
#                     .contract.functions.balances(i)
#                     .call(
#                         {"to": curve_pool.address},
#                         block_identifier,
#                     )
#                 )
#                 balances.append(balance)
#
#             except ValueError:
#                 balance = (
#                     self.abi[contract]
#                     .contract.functions.balances(i)
#                     .call({"to": curve_pool.address})
#                 )
#                 balances.append(balance)
#
#             except Exception:
#                 pass
#
#         return balances if len(balances) == len(curve_pool.tokens_addresses) else None
#
#     def get_lp_token_address_for_pool(self, pool_address: str) -> list[str]:
#         pool_address = self.w3.toChecksumAddress(pool_address)
#         try:
#             lp_address = (
#                 self.abi[FACTORY_CONTRACT]
#                 .contract.functions.get_lp_token(pool_address)
#                 .call({"block_identifier": "latest"})
#             )
#             if lp_address != config.BLACK_HOLE:
#                 return [lp_address.lower()]
#         except (ContractLogicError, BadFunctionCallOutput, ValueError) as e:
#             logger.error(
#                 f"Can not get lp address for: {pool_address} from contract",
#                 extra={"amm": "ellipsis", "exception": e},
#             )
#         return [pool_address.lower()]
