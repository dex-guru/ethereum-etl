# from pathlib import Path
#
# from clients.blockchain import BaseBlockchainClient
# from clients.blockchain.amm.base.base_contract import BaseContract
# from clients.blockchain.interfaces import AmmClientI
# from clients.blockchain.models.pool import BasePool, PoolFinances
# from clients.blockchain.models.protocol_transaction import MintBurn, Swap
# from clients.blockchain.models.tokens import ERC20Token
# from clients.blockchain.models.transaction import ReceiptLog
# from utils.logger import get_logger
# from utils.prices import get_prices_for_two_pool
# from web3 import Web3
#
# POOL_CONTRACT = "LBPair"
#
# logger = get_logger(__name__)
#
#
# class TraderJoeV21Amm(BaseContract, BaseBlockchainClient, AmmClientI):
#     pool_contract_names = (POOL_CONTRACT,)
#     swaps_event_name = "swap"
#     mints_event_names = ("mint", "depositedtobins")
#     burns_event_names = ("burn", "withdrawnfrombins")
#
#     def __init__(self, uri: str, amm_type: str, contracts: dict):
#         super().__init__(uri=uri, amm_type=amm_type, contracts=contracts)
#         _path_to_this_file = Path(__file__)
#         _path_to_abi = Path(_path_to_this_file.parent.parent, "abi")
#         abi_path = f"{_path_to_abi}/{amm_type}/"
#         pool_abi_path = abi_path + POOL_CONTRACT + ".json"
#         self.abi[POOL_CONTRACT] = self._initiate_contract(pool_abi_path)
#
#     def get_base_pool(self, pool_address: str) -> BasePool | None:
#         pool_address = Web3.toChecksumAddress(pool_address)
#         try:
#             token_0 = (
#                 self.abi[POOL_CONTRACT]
#                 .contract.functions.getTokenX()
#                 .call({"to": pool_address}, "latest")
#             )
#             token_1 = (
#                 self.abi[POOL_CONTRACT]
#                 .contract.functions.getTokenY()
#                 .call({"to": pool_address}, "latest")
#             )
#         except Exception as e:
#             logger.error(f"Error while getting tokens for pool {pool_address}: {e}")
#             return None
#         return BasePool(
#             address=pool_address.lower(),
#             tokens_addresses=[token_0.lower(), token_1.lower()],
#         )
#
#     def _get_event_name(self, topics: list[str]):
#         try:
#             topic = topics[0][0:4]
#         except IndexError:
#             logger.error(f"Cant get receipt_log.topics[0][0:4], index error, topics: {topics}")
#             return None
#         event_name = self.abi[POOL_CONTRACT].topic_keccaks.get(topic, None)
#         return event_name
#
#     @staticmethod
#     def decode_amounts(amounts: bytes) -> tuple:
#         """
#         Decodes the amounts bytes input as 2 integers.
#
#         :param amounts: amounts to decode.
#         :return: tuple of ints with the values decoded.
#         """
#         # Read the right 128 bits of the 256 bits
#         amounts_x = int.from_bytes(amounts, byteorder="big") & (2**128 - 1)
#
#         # Read the left 128 bits of the 256 bits
#         amounts_y = int.from_bytes(amounts, byteorder="big") >> 128
#
#         return amounts_x, amounts_y
#
#     def resolve_receipt_log(
#         self,
#         receipt_log: ReceiptLog,
#         base_pool: BasePool,
#         erc20_tokens: list[ERC20Token],
#     ) -> dict | None:
#         logger.debug(f"resolving {receipt_log.transaction_hash.hex()}-{receipt_log.log_index}")
#         event_name = self._get_event_name(receipt_log.topics)
#
#         if not all((receipt_log.topics, event_name)):
#             return None
#
#         tokens_scalars = []
#         for erc20_token in erc20_tokens:
#             tokens_scalars.append(10**erc20_token.decimals)
#         parsed_event = self.parse_event(self.abi[POOL_CONTRACT], event_name, receipt_log)
#         if event_name.lower() == self.swaps_event_name:
#             logger.debug("resolving swap from swap event")
#             swap = self.get_swap_from_swap_event(base_pool, parsed_event, tokens_scalars)
#             pool = self.get_pool_finances(
#                 base_pool, receipt_log.block_number, tokens_scalars, parsed_event
#             )
#             logger.debug(f"resolved swap from swap event {swap}")
#             swap.log_index = receipt_log.log_index
#             return {
#                 "swaps": [swap],
#                 "pools": [pool] if pool else [],
#             }
#
#         if event_name.lower() in self.burns_event_names:
#             logger.debug("resolving burn from burn event")
#             burn = self.get_mint_burn_from_events(base_pool, parsed_event, tokens_scalars)
#             pool = self.get_pool_finances(
#                 base_pool, receipt_log.block_number - 1, tokens_scalars, parsed_event
#             )
#             logger.debug("resolving burn from burn event")
#             burn.log_index = receipt_log.log_index
#             return {
#                 "burns": [burn],
#                 "pools": [pool] if pool else [],
#             }
#
#         if event_name.lower() in self.mints_event_names:
#             logger.debug("resolving burn from mint event")
#             mint = self.get_mint_burn_from_events(base_pool, parsed_event, tokens_scalars)
#             pool = self.get_pool_finances(
#                 base_pool, receipt_log.block_number, tokens_scalars, parsed_event
#             )
#             logger.debug("resolving burn from mint event")
#             mint.log_index = receipt_log.log_index
#             return {
#                 "mints": [mint],
#                 "pools": [pool] if pool else [],
#             }
#
#     def get_swap_from_swap_event(
#         self, base_pool: BasePool, parsed_event: dict, tokens_scalars: list[int]
#     ):
#         amounts_in = self.decode_amounts(parsed_event["amountsIn"])
#         amounts_out = self.decode_amounts(parsed_event["amountsOut"])
#
#         amount0 = amounts_in[0] / tokens_scalars[0] - amounts_out[0] / tokens_scalars[0]
#         amount1 = amounts_in[1] / tokens_scalars[1] - amounts_out[1] / tokens_scalars[1]
#         swap = Swap(
#             pool_address=base_pool.address,
#             sender=parsed_event["sender"],
#             to=parsed_event["to"],
#             amounts=[amount0, amount1],
#         )
#         return swap
#
#     def get_pool_finances(
#         self,
#         base_pool: BasePool,
#         block_number: int,
#         tokens_scalars: list[int],
#         parsed_event: dict,
#     ):
#         try:
#             reserves = (
#                 self.abi[POOL_CONTRACT]
#                 .contract.functions.getReserves()
#                 .call({"to": base_pool.address, "block_identifier": block_number})
#             )
#         except Exception as e:
#             logger.error(f"Error while getting reserves for pool {base_pool.address}: {e}")
#             return None
#         try:
#             bin_step = (
#                 self.abi[POOL_CONTRACT]
#                 .contract.functions.getBinStep()
#                 .call({"to": base_pool.address})
#             )
#         except Exception as e:
#             logger.error(f"Error while getting bin step for pool {base_pool.address}: {e}")
#             return None
#
#         if parsed_event.get("id"):
#             active_bin = parsed_event["id"]
#         else:
#             try:
#                 active_bin = (
#                     self.abi[POOL_CONTRACT]
#                     .contract.functions.getActiveId()
#                     .call({"to": base_pool.address, "block_identifier": block_number})
#                 )
#             except Exception as e:
#                 logger.error(f"Error while getting active bin for pool {base_pool.address}: {e}")
#                 return None
#
#         token_scalars_diff = tokens_scalars[1] / tokens_scalars[0]
#
#         # https://docs.traderjoexyz.com/guides/price-from-id
#         price0 = ((1 + bin_step / 10_000) ** (active_bin - 8388608)) / token_scalars_diff
#         price1 = 1 / price0
#         prices = get_prices_for_two_pool(price0, price1)
#
#         pool = PoolFinances(
#             prices=prices,
#             reserves=[reserves[0] / tokens_scalars[0], reserves[1] / tokens_scalars[1]],
#             **base_pool.dict(),
#         )
#         return pool
#
#     def get_mint_burn_from_events(
#         self, base_pool: BasePool, parsed_event: dict, tokens_scalars: list[int]
#     ):
#         amounts = parsed_event["amounts"]
#         amount0, amount1 = 0, 0
#         for amount in amounts:
#             amount0_, amount1_ = self.decode_amounts(amount)
#             amount0 += amount0_
#             amount1 += amount1_
#         amount0 /= tokens_scalars[0]
#         amount1 /= tokens_scalars[1]
#         mint_burn = MintBurn(
#             pool_address=base_pool.address,
#             sender=parsed_event["sender"],
#             amounts=[amount0, amount1],
#             owner=parsed_event["to"],
#         )
#         return mint_burn
