# from pathlib import Path
#
# from clients.blockchain import BaseBlockchainClient
# from clients.blockchain.amm.base.base_contract import BaseContract
# from clients.blockchain.interfaces import AmmClientI
# from clients.blockchain.models.platypus import PlatypusTransactionType
# from clients.blockchain.models.pool import BasePool, PoolFinances
# from clients.blockchain.models.protocol_transaction import MintBurn, Swap
# from clients.blockchain.models.tokens import ERC20Token
# from clients.blockchain.models.transaction import ReceiptLog
# from config import config
# from eth_typing import ChecksumAddress
# from utils.logger import get_logger
# from utils.prices import get_default_prices
# from web3 import Web3
# from web3.exceptions import BadFunctionCallOutput, ContractLogicError
#
# logs = get_logger(__name__)
#
# # https://github.com/platypus-finance/core/tree/master/contracts/pool
# FACTORY_CONTRACT = ""
# POOL_CONTRACT = "PoolSecondary"
# ASSET_CONTRACT = "Asset"
#
#
# class PlatypusAmm(BaseContract, BaseBlockchainClient, AmmClientI):
#     pool_contract_names = [POOL_CONTRACT]
#     pool_contracts_events_enum = PlatypusTransactionType
#
#     _pool_contract_abi_mapper = {
#         POOL_CONTRACT: f"{POOL_CONTRACT}.json",
#         ASSET_CONTRACT: f"{ASSET_CONTRACT}.json",
#     }
#
#     def __init__(self, uri: str, amm_type: str, contracts: dict):
#         super().__init__(uri=uri, amm_type=amm_type, contracts=contracts)
#         _path_to_this_file = Path(__file__)
#         _path_to_abi = Path(_path_to_this_file.parent.parent, "abi")
#         self.abi_path = f"{_path_to_abi}/{amm_type}/"
#
#         for name, file_ in self._pool_contract_abi_mapper.items():
#             pool_abi_path = self.abi_path + file_
#             self.abi[name] = self._initiate_contract(pool_abi_path)
#
#     def _get_event_name(self, topics: list[str]) -> str | None:
#         try:
#             topic = topics[0][0:4]
#         except IndexError:
#             logs.error(f"Cant get receipt_log.topics[0][0:4], index error, topics: {topics}")
#             return None
#         event_name = self.abi[POOL_CONTRACT].topic_keccaks.get(topic, None)
#         return event_name
#
#     def resolve_receipt_log(
#         self,
#         receipt_log: ReceiptLog,
#         base_pool: BasePool,
#         erc20_tokens: list[ERC20Token],
#     ) -> dict | None:
#         event_name = self._get_event_name(receipt_log.topics)
#         if not all((receipt_log.topics, event_name)):
#             return None
#
#         parsed_event = self.parse_event(self.abi[POOL_CONTRACT], event_name, receipt_log)
#         if not parsed_event:
#             return
#
#         base_pool = BasePool(
#             address=base_pool.address,
#             tokens_addresses=base_pool.tokens_addresses,
#         )
#         tokens_scalars = {}
#         for erc20_token in erc20_tokens:
#             tokens_scalars[erc20_token.address.lower()] = 10**erc20_token.decimals
#
#         if event_name == PlatypusTransactionType.Deposit.name:
#             burn = self.get_mint_burn_from_events(base_pool, parsed_event, tokens_scalars)
#             if not burn:
#                 return None
#             burn.log_index = receipt_log.log_index
#             pool = self.get_pool_finances(base_pool, tokens_scalars, receipt_log.block_number)
#             if not pool:
#                 return None
#             return {
#                 "burns": [burn],
#                 "pools": [pool],
#             }
#         elif event_name == PlatypusTransactionType.Withdraw.name:
#             mint = self.get_mint_burn_from_events(base_pool, parsed_event, tokens_scalars)
#             if not mint:
#                 return None
#             mint.log_index = receipt_log.log_index
#             pool = self.get_pool_finances(base_pool, tokens_scalars, receipt_log.block_number - 1)
#             if not pool:
#                 return None
#             return {
#                 "mints": [mint],
#                 "pools": [pool],
#             }
#         elif event_name == PlatypusTransactionType.Swap.name:
#             swap = self.get_swap_from_event(base_pool, parsed_event, tokens_scalars)
#             if not swap:
#                 return None
#             swap.log_index = receipt_log.log_index
#             pool = self.get_pool_finances(base_pool, tokens_scalars, receipt_log.block_number)
#             if not pool:
#                 return None
#             return {
#                 "swaps": [swap],
#                 "pools": [pool],
#             }
#
#     @staticmethod
#     def get_mint_burn_from_events(
#         base_pool: BasePool, parsed_event: dict, tokens_scalars: dict
#     ) -> MintBurn:
#         amount = parsed_event["amount"] / tokens_scalars[parsed_event["token"]]
#         mint_burn = MintBurn(
#             pool_address=base_pool.address,
#             sender=parsed_event["sender"],
#             owner=parsed_event["to"],
#             amounts=[
#                 0 if parsed_event["token"] != token.lower() else amount
#                 for token in base_pool.tokens_addresses
#             ],
#         )
#         return mint_burn
#
#     def get_swap_from_event(
#         self, base_pool: BasePool, parsed_event: dict, tokens_scalars: dict
#     ) -> Swap:
#         amount0 = parsed_event["fromAmount"] / tokens_scalars[parsed_event["fromToken"]]
#         amount1 = parsed_event["toAmount"] / tokens_scalars[parsed_event["toToken"]]
#         amounts = []
#         for token in base_pool.tokens_addresses:
#             if token.lower() == parsed_event["fromToken"]:
#                 amounts.append(amount0)
#             elif token.lower() == parsed_event["toToken"]:
#                 amounts.append(amount1)
#             else:
#                 amounts.append(0)
#         swap = Swap(
#             pool_address=base_pool.address,
#             sender=parsed_event["sender"],
#             to=parsed_event["to"],
#             amounts=amounts,
#         )
#         return swap
#
#     def get_pool_finances(
#         self,
#         base_pool: BasePool,
#         tokens_scalars: dict,
#         block_number: int | str,
#     ) -> PoolFinances | None:
#         pool_finances = PoolFinances(**base_pool.dict())
#         assets_addresses = []
#         for address in base_pool.tokens_addresses:
#             try:
#                 asset_address = (
#                     self.abi[POOL_CONTRACT]
#                     .contract.functions.assetOf(address)
#                     .call(
#                         {"to": base_pool.address},
#                         block_identifier=block_number,
#                     )
#                 )
#                 assets_addresses.append(asset_address)
#             except (
#                 TypeError,
#                 BadFunctionCallOutput,
#                 ValueError,
#                 ContractLogicError,
#             ) as e:
#                 logs.error(f"Cant resolve asset for Platypus pool {base_pool.address}, {e}")
#                 continue
#
#         reserves = [
#             self.get_reserve_for_asset(base_pool.address, asset, block_number)
#             for asset in assets_addresses
#         ]
#         if not all(reserves):
#             return None
#         pool_finances.reserves = reserves
#
#         tokens_count = len(base_pool.tokens_addresses)
#         prices = get_default_prices(tokens_count)
#         for coin_index in range(0, tokens_count):
#             for coin_index_for_swap in range(0, tokens_count):
#                 coin_address = base_pool.tokens_addresses[coin_index]
#                 coin_address_for_swap = base_pool.tokens_addresses[coin_index_for_swap]
#                 if coin_index == coin_index_for_swap:
#                     continue
#                 dx, exchange_precision = self.get_right_exchange_amount_for_coins(
#                     coin_address, coin_address_for_swap, tokens_scalars
#                 )
#                 exchange_rate = None
#                 try:
#                     exchange_rate, _ = (
#                         self.abi[POOL_CONTRACT]
#                         .contract.functions.quotePotentialSwap(
#                             coin_address, coin_address_for_swap, dx
#                         )
#                         .call({"to": base_pool.address}, block_number)
#                     )
#                 except Exception:
#                     logs.error(
#                         f"Can not call quotePotentialSwap function for pool {base_pool.address}"
#                     )
#                     pass
#                 if exchange_rate:
#                     prices[coin_index_for_swap][coin_index] = exchange_rate / exchange_precision
#                 else:
#                     logs.error(
#                         f"Can not detect price for token in platypus pool: {base_pool.address}"
#                     )
#                     return None
#         pool_finances.prices = prices
#         return pool_finances
#
#     def get_base_pool(self, address: ChecksumAddress) -> BasePool | None:
#         tokens_addresses = self.get_tokens_addresses_for_pool(address)
#         if tokens_addresses:
#             return BasePool(
#                 address=address,
#                 tokens_addresses=tokens_addresses,
#             )
#
#     def get_tokens_addresses_for_pool(self, pool_address: ChecksumAddress) -> list | None:
#         try:
#             return (
#                 self.abi[POOL_CONTRACT]
#                 .contract.functions.getTokenAddresses()
#                 .call({"to": pool_address}, "latest")
#             )
#         except (TypeError, BadFunctionCallOutput, ValueError, ContractLogicError) as e:
#             logs.error(f"Cant resolve tokens_addresses for Platypus pair {pool_address}, {e}")
#             return None
#
#     def get_lp_token_address_for_pool(self, pool_address: str) -> list[str] | None:
#         converted_pool_address = Web3.toChecksumAddress(pool_address)
#         tokens_addresses = self.get_tokens_addresses_for_pool(converted_pool_address)
#
#         lp_token_addreses = []
#         for address in tokens_addresses:
#             try:
#                 address = (
#                     self.abi[POOL_CONTRACT]
#                     .contract.functions.assetOf(address)
#                     .call({"to": pool_address}, "latest")
#                 )
#                 lp_token_addreses.append(address)
#             except (
#                 TypeError,
#                 BadFunctionCallOutput,
#                 ValueError,
#                 ContractLogicError,
#             ) as e:
#                 logs.error(f"Cant resolve asset for Platypus pool {pool_address}, {e}")
#                 continue
#         return [
#             lp_token_address.lower()
#             for lp_token_address in lp_token_addreses
#             if lp_token_address not in config.NULL_ADDRESSES_LIST
#         ]
#
#     def get_reserve_for_asset(
#         self,
#         pool_address: str,
#         asset: str,
#         block_number: str | int,
#     ) -> float | None:
#         decimals, cash = None, None
#         try:
#             decimals = (
#                 self.abi[ASSET_CONTRACT]
#                 .contract.functions.decimals()
#                 .call({"to": asset}, block_number)
#             )
#         except (TypeError, BadFunctionCallOutput, ValueError, ContractLogicError) as e:
#             logs.error(f"Cant resolve decimal asset for Platypus pool {pool_address}, {e}")
#
#         try:
#             cash = (
#                 self.abi[ASSET_CONTRACT]
#                 .contract.functions.cash()
#                 .call({"to": asset}, block_number)
#             )
#         except (TypeError, BadFunctionCallOutput, ValueError, ContractLogicError) as e:
#             logs.error(f"Cant resolve cash for Platypus pool {pool_address}, {e}")
#
#         if decimals is None or cash is None:
#             return None
#         return cash / (10**decimals)
#
#     @staticmethod
#     def get_right_exchange_amount_for_coins(
#         addr0: str, addr1: str, tokens_scalars: dict
#     ) -> tuple[int, int]:
#         addr0 = addr0.lower()
#         addr1 = addr1.lower()
#         if tokens_scalars[addr0] == tokens_scalars[addr1]:
#             return tokens_scalars[addr0], tokens_scalars[addr1]
#         elif tokens_scalars[addr0] > tokens_scalars[addr1]:
#             return max(tokens_scalars[addr0], tokens_scalars[addr1]), min(
#                 tokens_scalars[addr0], tokens_scalars[addr1]
#             )
#         else:
#             return min(tokens_scalars[addr0], tokens_scalars[addr1]), max(
#                 tokens_scalars[addr0], tokens_scalars[addr1]
#             )
