# from pathlib import Path
#
# from clients.blockchain import BaseBlockchainClient
# from clients.blockchain.amm.base.base_contract import BaseContract
# from clients.blockchain.interfaces import AmmClientI
# from clients.blockchain.models.pool import BasePool, FeeAmount, Pool, PoolFinances
# from clients.blockchain.models.protocol_transaction import (
#     BaseMintBurn,
#     BaseProtocolTransaction,
#     BaseSwap,
#     MintBurn,
#     ProtocolTransaction,
#     ProtocolType,
#     Swap,
#     TransactionType,
# )
# from clients.blockchain.models.tokens import ERC20Token
# from clients.blockchain.models.transaction import Receipt, ReceiptLog, Transaction
# from config import config
# from eth_typing import ChecksumAddress
# from utils.common import INFINITE_PRICE_THRESHOLD
# from utils.logger import get_logger
# from utils.prices import get_prices_for_two_pool
# from web3 import Web3
# from web3.exceptions import BadFunctionCallOutput, ContractLogicError
#
# logs = get_logger(__name__)
#
# FACTORY_CONTRACT = "IDMMFactory"
# POOL_CONTRACT = "IDMMPool"
#
#
# class DMMAmm(BaseContract, BaseBlockchainClient, AmmClientI):
#     pool_contract_names = (POOL_CONTRACT,)
#     pool_contracts_events_enum = TransactionType
#
#     def __init__(self, uri: str, amm_type: str, contracts: dict):
#         super().__init__(uri=uri, amm_type=amm_type, contracts=contracts)
#         _path_to_this_file = Path(__file__)
#         _path_to_abi = Path(_path_to_this_file.parent.parent, "abi")
#         abi_path = f"{_path_to_abi}/{amm_type}/"
#         pool_abi_path = abi_path + "IDMMPool.json"
#         self.abi[POOL_CONTRACT] = self._initiate_contract(pool_abi_path)
#
#     def resolve_receipt_log(
#         self,
#         receipt_log: ReceiptLog,
#         base_pool: BasePool,
#         erc20_tokens: list[ERC20Token],
#     ) -> dict | None:
#         logs.debug(f"resolving {receipt_log.transaction_hash.hex()}-{receipt_log.log_index}")
#         try:
#             topic = receipt_log.topics[0][0:4]
#         except IndexError:
#             logs.error(f"Cant get receipt_log.topics[0][0:4], index error, log: {receipt_log}")
#             return None
#         event_name = self.abi[POOL_CONTRACT].topic_keccaks.get(topic, None)
#         if receipt_log.topics and event_name:
#             tokens_scalars = []
#             for erc20_token in erc20_tokens:
#                 tokens_scalars.append(10**erc20_token.decimals)
#             parsed_event = self.parse_event(self.abi[POOL_CONTRACT], event_name, receipt_log)
#
#             if event_name.lower() == self.pool_contracts_events_enum.swap.name:
#                 logs.debug("resolving swap from swap event")
#                 swap = self.get_swap_from_swap_event(base_pool, parsed_event, tokens_scalars)
#                 pool = self.get_pool_finances(base_pool, receipt_log.block_number, tokens_scalars)
#                 logs.debug(f"resolved swap from swap event {swap}")
#                 swap.log_index = receipt_log.log_index
#                 return {
#                     "swaps": [swap],
#                     "pools": [pool] if pool else [],
#                 }
#
#             if event_name.lower() == self.pool_contracts_events_enum.burn.name:
#                 logs.debug("resolving burn from burn event")
#                 burn = self.get_mint_burn_from_events(base_pool, parsed_event, tokens_scalars)
#                 pool = self.get_pool_finances(
#                     base_pool, receipt_log.block_number - 1, tokens_scalars
#                 )
#                 logs.debug("resolving burn from burn event")
#                 burn.log_index = receipt_log.log_index
#                 return {
#                     "burns": [burn],
#                     "pools": [pool] if pool else [],
#                 }
#
#             if event_name.lower() == self.pool_contracts_events_enum.mint.name:
#                 logs.debug("resolving burn from mint event")
#                 mint = self.get_mint_burn_from_events(base_pool, parsed_event, tokens_scalars)
#                 pool = self.get_pool_finances(base_pool, receipt_log.block_number, tokens_scalars)
#                 logs.debug("resolving burn from mint event")
#                 mint.log_index = receipt_log.log_index
#                 return {
#                     "mints": [mint],
#                     "pools": [pool] if pool else [],
#                 }
#
#             # TODO: resolve sync event
#             # if event_name.lower() == self.pool_contracts_events_enum.sync.name:
#             #     logs.debug(f'resolving pool finances from sync event')
#             #     pool = self.get_pool_finances_from_sync_event(base_pool, parsed_event, tokens_scalars)
#             #     logs.debug(f'resolved pool finances from sync event {pool}')
#             #     return {
#             #         "pools": [pool]
#             #     }
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
#                 .contract.functions.allPoolsLength()
#                 .call(block_identifier=block_identifier)
#             )
#         except (BadFunctionCallOutput, ValueError) as e:
#             logs.error(
#                 f"Cant get pairs length on {self.factory_address}:{block_identifier}, trying latest:{e}"
#             )
#             try:
#                 return (
#                     self.abi[FACTORY_CONTRACT]
#                     .contract.functions.allPoolsLength()
#                     .call(block_identifier="latest")
#                 )
#             except (BadFunctionCallOutput, ValueError) as e:
#                 logs.error(f"Cant get pairs length on {self.factory_address}:'latest", {e})
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
#
#         pair_address = Web3.toChecksumAddress(pair_address)
#
#         if not pair_address:
#             logs.error(
#                 f"Cant get pair by index for AMM:{self.factory_address}, index:{pair_index},"
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
#     def get_base_pool(self, address: ChecksumAddress) -> BasePool | None:
#         logs.debug(f"Resolving pool addresses for {address}")
#         tokens_addresses = self.get_tokens_addresses_for_pool(address)
#         if tokens_addresses:
#             return BasePool(
#                 address=address,
#                 tokens_addresses=tokens_addresses,
#                 fee=FeeAmount.MEDIUM.value,
#             )
#
#     def get_tokens_addresses_for_pool(self, pool_address: str) -> list | None:
#         try:
#             tokens_addresses = [
#                 (
#                     self.abi[POOL_CONTRACT]
#                     .contract.functions.token0()
#                     .call({"to": pool_address}, "latest")
#                 ).lower(),
#                 (
#                     self.abi[POOL_CONTRACT]
#                     .contract.functions.token1()
#                     .call({"to": pool_address}, "latest")
#                 ).lower(),
#             ]
#         except TypeError as e:
#             logs.error(f"Cant resolve tokens_addressese for pair {pool_address}, {e}")
#             return None
#         return tokens_addresses
#
#     def resolve_base_transaction(
#         self, event_name, receipt_log, transaction
#     ) -> BaseProtocolTransaction | None:
#         tokens_addresses = []
#         pool_address = None
#         if event_name.lower() in [
#             self.pool_contracts_events_enum.mint.name,
#             self.pool_contracts_events_enum.burn.name,
#             self.pool_contracts_events_enum.swap.name,
#             self.pool_contracts_events_enum.sync.name,
#         ]:
#             tokens_addresses = self.get_tokens_addresses_for_pool(
#                 Web3.toChecksumAddress(receipt_log.address)
#             )
#             if not tokens_addresses:
#                 return None
#             pool_address = receipt_log.address
#         elif event_name.lower() == self.pool_contracts_events_enum.transfer.name:
#             tokens_addresses = [receipt_log.address]
#             pool_address = None
#         base_transaction = BaseProtocolTransaction(
#             id=f"{transaction.address}-{receipt_log.log_index}",
#             pool_address=pool_address,
#             log_index=receipt_log.log_index,
#             transaction_type=event_name.lower(),
#             transaction_address=transaction.address,
#             timestamp=transaction.timestamp,
#             protocol_type=ProtocolType.dmm,
#             tokens_addresses=tokens_addresses,
#             wallet_address=transaction.from_address,
#         )
#         return base_transaction
#
#     def resolve_transaction(
#         self, transaction: Transaction, transaction_receipt: Receipt
#     ) -> ProtocolTransaction | None:
#         protocol_transaction = ProtocolTransaction()
#         transaction_pools = {}
#         for receipt_log in transaction_receipt.logs:
#             receipt_log: ReceiptLog
#             try:
#                 topic = receipt_log.topics[0][0:4]
#             except IndexError:
#                 logs.error(f"Cant get receipt_log.topics[0][0:4], index error, log: {receipt_log}")
#                 continue
#             event_name = self.abi[POOL_CONTRACT].topic_keccaks.get(topic, None)
#             if receipt_log.topics and event_name:
#                 base_transaction: BaseProtocolTransaction = self.resolve_base_transaction(
#                     event_name, receipt_log, transaction
#                 )
#
#                 if not base_transaction:
#                     continue
#
#                 tokens_info: list[ERC20Token] = []
#                 for token_address in base_transaction.tokens_addresses:
#                     token = self.get_token(token_address)
#                     if token:
#                         tokens_info.append(self.get_token(token_address))
#
#                 if len(tokens_info) < len(base_transaction.tokens_addresses):
#                     continue
#
#                 protocol_transaction.tokens.extend(tokens_info)
#                 tokens_scalars = []
#                 for token_info in tokens_info:
#                     tokens_scalars.append(10**token_info.decimals)
#
#                 parsed_event = self.parse_event(self.abi[POOL_CONTRACT], event_name, receipt_log)
#
#                 if (
#                     base_transaction.transaction_type
#                     == self.pool_contracts_events_enum.transfer.name
#                 ):
#                     pass
#
#                 elif (
#                     base_transaction.transaction_type == self.pool_contracts_events_enum.sync.name
#                 ):
#                     reserve0 = parsed_event["reserve0"] / tokens_scalars[0]
#                     reserve1 = parsed_event["reserve1"] / tokens_scalars[1]
#
#                     if reserve0:
#                         token0_price = float(reserve1 / reserve0)
#                     else:
#                         token0_price = 0
#                     if reserve1:
#                         token1_price = float(reserve0 / reserve1)
#                     else:
#                         token1_price = 0
#
#                     if token0_price >= INFINITE_PRICE_THRESHOLD:
#                         token0_price = 0
#
#                     if token1_price >= INFINITE_PRICE_THRESHOLD:
#                         token1_price = 0
#
#                     transaction_pools[base_transaction.pool_address] = Pool(
#                         address=base_transaction.pool_address,
#                         tokens_addresses=base_transaction.tokens_addresses,
#                         reserves=[
#                             reserve0,
#                             reserve1,
#                         ],
#                         prices=[token0_price, token1_price],
#                         transaction_type=base_transaction.transaction_type,
#                         wallet_address=base_transaction.wallet_address,
#                     )
#                 elif (
#                     base_transaction.transaction_type == self.pool_contracts_events_enum.swap.name
#                 ):
#                     swap = self.resolve_swap(
#                         base_transaction,
#                         parsed_event,
#                         tokens_scalars,
#                         transaction_pools,
#                     )
#                     if not swap:
#                         continue
#                     protocol_transaction.swaps.append(swap)
#                     protocol_transaction.pools.append(
#                         transaction_pools[base_transaction.pool_address]
#                     )
#                 elif base_transaction.transaction_type in [
#                     self.pool_contracts_events_enum.mint.name,
#                     self.pool_contracts_events_enum.burn.name,
#                 ]:
#                     mint_burn = self.resolve_mint_burn(
#                         base_transaction,
#                         parsed_event,
#                         tokens_scalars,
#                         transaction_pools,
#                     )
#                     if not mint_burn:
#                         continue
#                     protocol_transaction.pools.append(
#                         transaction_pools[base_transaction.pool_address]
#                     )
#                     if (
#                         base_transaction.transaction_type
#                         == self.pool_contracts_events_enum.mint.name
#                     ):
#                         protocol_transaction.mints.append(mint_burn)
#                     if (
#                         base_transaction.transaction_type
#                         == self.pool_contracts_events_enum.burn.name
#                     ):
#                         protocol_transaction.burns.append(mint_burn)
#                 else:
#                     logs.debug(f"Event type {event_name} is not handled by indexation")
#         return protocol_transaction
#
#     def resolve_mint_burn(
#         self,
#         base_transaction: BaseProtocolTransaction,
#         parsed_event: dict,
#         tokens_decimals_scalars: list[int],
#         transaction_pools: dict,
#     ) -> BaseMintBurn | None:
#         amount0 = parsed_event["amount0"] / tokens_decimals_scalars[0]
#         amount1 = parsed_event["amount1"] / tokens_decimals_scalars[1]
#
#         return BaseMintBurn(
#             **base_transaction.dict(),
#             sender=parsed_event["sender"] if parsed_event.get("sender") else parsed_event["owner"],
#             owner=parsed_event["owner"] if parsed_event.get("owner") else parsed_event["sender"],
#             prices=transaction_pools[base_transaction.pool_address].prices,
#             amounts=[amount0, amount1],
#             reserves=transaction_pools[base_transaction.pool_address].reserves,
#         )
#
#     def resolve_swap(
#         self,
#         base_transaction: BaseProtocolTransaction,
#         parsed_event: dict,
#         tokens_decimals_scalars: list[int],
#         transaction_pools: dict,
#     ) -> BaseSwap | None:
#         amount0 = (
#             parsed_event["amount0In"] / tokens_decimals_scalars[0]
#             - parsed_event["amount0Out"] / tokens_decimals_scalars[0]
#         )
#         amount1 = (
#             parsed_event["amount1In"] / tokens_decimals_scalars[1]
#             - parsed_event["amount1Out"] / tokens_decimals_scalars[1]
#         )
#
#         if not (amount1 and amount0):
#             return None
#
#         token0_price = abs(amount1 / amount0)
#
#         return BaseSwap(
#             **base_transaction.dict(),
#             sender=parsed_event["sender"],
#             recipient=parsed_event["to"],
#             prices=[token0_price, 1 / token0_price],
#             amounts=[amount0, amount1],
#             reserves=transaction_pools[base_transaction.pool_address].reserves,
#         )
#
#     def get_pool_finances(
#         self,
#         base_pool: BasePool,
#         block_number: str | int,
#         tokens_scalars: list[int],
#     ):
#         try:
#             reserves = (
#                 self.abi[POOL_CONTRACT]
#                 .contract.functions.getReserves()
#                 .call({"to": base_pool.address}, block_number)
#             )
#         except (TypeError, ContractLogicError, ValueError, BadFunctionCallOutput) as e:
#             logs.debug("Not found reserves for %s. Error: %s", base_pool.address, e)
#             if "backsync" in config.PIPELINE:
#                 return None
#             reserves = (
#                 self.abi[POOL_CONTRACT]
#                 .contract.functions.getReserves()
#                 .call({"to": base_pool.address}, "latest")
#             )
#         pool = PoolFinances(**base_pool.dict())
#         reserves = [reserves[0] / tokens_scalars[0], reserves[1] / tokens_scalars[1]]
#         reserve0 = reserves[0]
#         reserve1 = reserves[1]
#         if reserve0:
#             token0_price = float(reserve1 / reserve0)
#         else:
#             logs.warning(
#                 "cant get price, as reserve0 = 0",
#                 extra={
#                     "pool_address": base_pool.address,
#                     "block_number": block_number,
#                 },
#             )
#             token0_price = 0
#         if reserve1:
#             token1_price = float(reserve0 / reserve1)
#         else:
#             logs.warning(
#                 "cant get price, as reserve0 = 0",
#                 extra={
#                     "pool_address": base_pool.address,
#                     "block_number": block_number,
#                 },
#             )
#             token1_price = 0
#         if token0_price >= INFINITE_PRICE_THRESHOLD:
#             logs.warning(
#                 "cant get price, as it's infinite",
#                 extra={
#                     "pool_address": base_pool.address,
#                     "block_number": block_number,
#                 },
#             )
#             token0_price = 0
#         if token1_price >= INFINITE_PRICE_THRESHOLD:
#             logs.warning(
#                 "cant get price, as it's infinite",
#                 extra={
#                     "pool_address": base_pool.address,
#                     "block_number": block_number,
#                 },
#             )
#             token1_price = 0
#         pool.prices = get_prices_for_two_pool(token0_price, token1_price)
#         pool.reserves = reserves
#         return pool
