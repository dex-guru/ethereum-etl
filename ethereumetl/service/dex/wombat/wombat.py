# from enum import Enum
# from pathlib import Path
#
# from clients.blockchain import BaseBlockchainClient
# from clients.blockchain.amm.base.base_contract import BaseContract
# from clients.blockchain.interfaces import AmmClientI
# from clients.blockchain.models.pool import BasePool, PoolFinances
# from clients.blockchain.models.protocol_transaction import MintBurn, Swap
# from clients.blockchain.models.tokens import ERC20Token
# from clients.blockchain.models.transaction import ReceiptLog
# from clients.blockchain.models.transfer import TransferBase, TransferType
# from config import config
# from dexguru_utils.enums import TransactionChoices
# from utils.logger import get_logger
# from utils.prices import get_default_prices
# from web3 import Web3
#
# POOL_CONTRACT = "Pool"
# LP_CONTRACT = "LPToken"
# FACTORY_CONTRACT = "MasterWombat"
# logger = get_logger(__name__)
#
#
# class WombatTransactionsTypes(Enum):
#     swap = "swap"
#     transfer = "transfer"
#     sync = "sync"
#     deposit = "deposit"
#     withdraw = "withdraw"
#
#
# class WombatAmm(BaseContract, BaseBlockchainClient, AmmClientI):
#     pool_contract_names = (POOL_CONTRACT,)
#     pool_contracts_events_enum = WombatTransactionsTypes
#
#     def __init__(self, uri: str, amm_type: str, contracts: dict):
#         super().__init__(uri=uri, amm_type=amm_type, contracts=contracts)
#         _path_to_this_file = Path(__file__)
#         _path_to_abi = Path(_path_to_this_file.parent.parent, "abi")
#         abi_path = f"{_path_to_abi}/{amm_type}/"
#         pool_abi_path = abi_path + "Pool.json"
#         lp_abi_path = abi_path + "LPToken.json"
#         self.abi[POOL_CONTRACT] = self._initiate_contract(pool_abi_path)
#         self.abi[LP_CONTRACT] = self._initiate_contract(lp_abi_path)
#
#     def get_base_pool(self, pool_address: str) -> BasePool | None:
#         pool_address = Web3.toChecksumAddress(pool_address)
#         try:
#             tokens = (
#                 self.abi[POOL_CONTRACT]
#                 .contract.functions.getTokens()
#                 .call({"to": pool_address}, "latest")
#             )
#         except Exception as e:
#             logger.error(f"Error while getting tokens for pool {pool_address}: {e}")
#             return None
#         lp_tokens = []
#         for token_address in tokens:
#             try:
#                 lp_tokens.append(
#                     self.abi[POOL_CONTRACT]
#                     .contract.functions.addressOfAsset(Web3.toChecksumAddress(token_address))
#                     .call({"to": pool_address}, "latest")
#                 )
#             except Exception as e:
#                 logger.error(f"Error while getting lp token for pool {pool_address}: {e}")
#                 break
#         return BasePool(
#             address=pool_address.lower(),
#             tokens_addresses=[token.lower() for token in tokens],
#             u_tokens_addresses=[lp_token.lower() for lp_token in lp_tokens],
#         )
#
#     def get_lp_token_address_for_pool(self, pool_address: str) -> list[str] | None:
#         """Wombat has LP token for each token in pool."""
#         pool = self.get_base_pool(pool_address)
#         return [token.lower() for token in pool.u_tokens_addresses]
#
#     def resolve_receipt_log(
#         self,
#         receipt_log: ReceiptLog,
#         base_pool: BasePool,
#         erc20_tokens: list[ERC20Token],
#         transfers: list[TransferBase],
#     ) -> dict | None:
#         logger.debug(f"resolving {receipt_log.transaction_hash.hex()}-{receipt_log.log_index}")
#         try:
#             topic = receipt_log.topics[0][0:4]
#         except IndexError:
#             logger.error(f"Cant get receipt_log.topics[0][0:4], index error, log: {receipt_log}")
#             return None
#         event_name = self.abi[POOL_CONTRACT].topic_keccaks.get(topic, None)
#         if not (receipt_log.topics and event_name):
#             return None
#         block_number = receipt_log.block_number
#         tokens_scalars = {}
#         for erc20_token in erc20_tokens:
#             tokens_scalars[erc20_token.address.lower()] = 10**erc20_token.decimals
#         parsed_event = self.parse_event(self.abi[POOL_CONTRACT], event_name, receipt_log)
#
#         if event_name.lower() == self.pool_contracts_events_enum.swap.name:
#             logger.debug("resolving swap from swap event")
#             swap = self.get_swap_from_swap_event(base_pool, parsed_event, tokens_scalars)
#             logger.debug(f"resolved swap from swap event {swap}")
#             swap.log_index = receipt_log.log_index
#             return {
#                 "swaps": [swap],
#                 "pools": [
#                     self.get_pool_finances(base_pool, parsed_event, tokens_scalars, block_number)
#                 ],
#             }
#
#         if event_name.lower() == self.pool_contracts_events_enum.withdraw.name:
#             logger.debug("resolving burn from burn event")
#             parsed_event["event_type"] = "burn"
#             burn = self.get_mint_burn_from_events(
#                 base_pool, parsed_event, tokens_scalars, transfers
#             )
#             logger.debug("resolving burn from burn event")
#             burn.log_index = receipt_log.log_index
#             return {
#                 "burns": [burn],
#                 "pools": [
#                     self.get_pool_finances(
#                         base_pool, parsed_event, tokens_scalars, block_number - 1
#                     )
#                 ],
#             }
#
#         if event_name.lower() == self.pool_contracts_events_enum.deposit.name:
#             logger.debug("resolving burn from mint event")
#             parsed_event["event_type"] = "mint"
#             mint = self.get_mint_burn_from_events(
#                 base_pool, parsed_event, tokens_scalars, transfers
#             )
#             logger.debug("resolving burn from mint event")
#             mint.log_index = receipt_log.log_index
#             return {
#                 "mints": [mint],
#                 "pools": [
#                     self.get_pool_finances(base_pool, parsed_event, tokens_scalars, block_number)
#                 ],
#             }
#
#     def get_pool_finances(
#         self,
#         base_pool: BasePool,
#         parsed_event: dict,
#         tokens_scalars: dict,
#         block_number: int = "latest",
#     ) -> PoolFinances | None:
#         # get liability for each lp token in pool
#         reserves = []
#         liabilities = {}
#         cashes = {}
#         # all lp decimals should be same for one pool.
#         decimals = (
#             self.abi[LP_CONTRACT]
#             .contract.functions.decimals()
#             .call(
#                 {"to": Web3.toChecksumAddress(base_pool.u_tokens_addresses[0])},
#                 block_number,
#             )
#         )
#         for i, token in enumerate(base_pool.u_tokens_addresses):
#             token = Web3.toChecksumAddress(token)
#             try:
#                 # liability is liquidity for wombat
#                 liability = (
#                     self.abi[LP_CONTRACT]
#                     .contract.functions.liability()
#                     .call({"to": token}, block_number)
#                 )
#                 cash = (
#                     self.abi[LP_CONTRACT]
#                     .contract.functions.cash()
#                     .call({"to": token}, block_number)
#                 )
#                 reserves.append(liability / (10**decimals))
#                 liabilities[token.lower()] = liability / (10**decimals)
#                 cashes[token.lower()] = (
#                     cash / tokens_scalars[base_pool.tokens_addresses[i].lower()]
#                 )
#             except Exception as e:
#                 logger.error(f"Error while getting liability for pool {base_pool.address}: {e}")
#                 return None
#         prices = self._get_pool_prices(
#             base_pool.address, decimals, liabilities, cashes, block_number
#         )
#         pool_finances = PoolFinances(
#             address=base_pool.address,
#             tokens_addresses=base_pool.tokens_addresses,
#             u_tokens_addresses=base_pool.u_tokens_addresses,
#             reserves=reserves,
#             prices=prices,
#             lp_token_addresses=base_pool.u_tokens_addresses,
#             transaction_type=TransactionChoices.swap,
#         )
#         return pool_finances
#
#     def _get_pool_prices(
#         self,
#         pool_address: str,
#         decimals: int,
#         liabilities: dict,
#         cashes: dict,
#         block_identifier: str | int = "latest",
#     ) -> list:
#         """
#         Get pool prices.
#
#         Prices calculates from exchange rate:
#
#         1 + (A / rx ** 2)
#         exchange_rate = -------------------
#         1 + (A / ry ** 2)
#
#         A is amplification coefficient for pool
#         rx|ry is coverage ratio
#
#         r = Cash / Liability
#         """
#         prices = get_default_prices(len(liabilities))
#         amp = (
#             self.abi[POOL_CONTRACT]
#             .contract.functions.ampFactor()
#             .call({"to": pool_address}, block_identifier)
#         )
#         amp = amp / 10**decimals
#         ratios = []
#         for cash, liability in zip(cashes.values(), liabilities.values()):
#             ratios.append(cash / liability)
#         for i in range(len(ratios)):
#             for j in range(len(ratios)):
#                 if i != j:
#                     prices[i][j] = (1 + (amp / ratios[j] ** 2)) / (1 + (amp / ratios[i] ** 2))
#         return prices
#
#     @staticmethod
#     def get_swap_from_swap_event(base_pool, parsed_event, tokens_scalars: dict):
#         amounts = [0 for _ in range(len(base_pool.tokens_addresses))]
#         tokens = [token.lower() for token in base_pool.tokens_addresses]
#         coin0_index = tokens.index(parsed_event["fromToken"].lower())
#         coin1_index = tokens.index(parsed_event["toToken"].lower())
#         amount0 = parsed_event["fromAmount"] / tokens_scalars[parsed_event["fromToken"]]
#         amount1 = parsed_event["toAmount"] / tokens_scalars[parsed_event["toToken"]]
#         amounts[coin0_index] = amount0
#         amounts[coin1_index] = amount1
#         swap = Swap(
#             pool_address=base_pool.address.lower(),
#             sender=parsed_event["sender"],
#             to=parsed_event["to"],
#             amounts=amounts,
#             direction_indexes=[coin0_index, coin1_index],
#         )
#         return swap
#
#     @staticmethod
#     def get_mint_burn_from_events(base_pool, parsed_event, tokens_scalars: dict, transfers: list):
#         lp_address = None
#         # user can burn BUSD LP token and get HAY token
#         if parsed_event["event_type"] == "burn":
#             burned_tokens = set()
#             u_tokens = [token.lower() for token in base_pool.u_tokens_addresses]
#             for transfer in transfers:
#                 if transfer.type != TransferType.ERC20:
#                     continue
#                 if (
#                     transfer.token_address.lower() in u_tokens
#                     and transfer.to_address in config.NULL_ADDRESSES_LIST
#                 ):
#                     burned_tokens.add(transfer.token_address.lower())
#             if len(burned_tokens) == 1:
#                 lp_address = burned_tokens.pop()
#         tokens = [token.lower() for token in base_pool.tokens_addresses]
#         amounts = [0 for _ in range(len(base_pool.tokens_addresses))]
#         coin_index = tokens.index(parsed_event["token"].lower())
#         # amount should be calculated in token decimals
#         amount = parsed_event["amount"] / tokens_scalars[parsed_event["token"].lower()]
#         amounts[coin_index] = amount
#         mint_burn = MintBurn(
#             pool_address=base_pool.address.lower(),
#             sender=parsed_event["sender"].lower(),
#             owner=parsed_event["to"].lower(),
#             amounts=amounts,
#             lp_token_address=lp_address or base_pool.u_tokens_addresses[coin_index].lower(),
#         )
#         return mint_burn
