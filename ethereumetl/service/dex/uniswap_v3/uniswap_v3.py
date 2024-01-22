# import json
# import logging
# from pathlib import Path
# from typing import Literal
#
# from web3 import Web3
# from web3.exceptions import BadFunctionCallOutput, ContractLogicError
#
# from ethereumetl.domain.dex_pool import EthDexPool
# from ethereumetl.domain.dex_trade import EthDexTrade
# from ethereumetl.domain.receipt_log import ParsedReceiptLog
# from ethereumetl.domain.token import EthToken
# from ethereumetl.domain.token_transfer import EthTokenTransfer
# from ethereumetl.service.dex.base.interface import DexClientInterface
# from ethereumetl.service.dex.enums import DexPoolFeeAmount, TransactionType
#
# logs = logging.getLogger(__name__)
# to_checksum = Web3.toChecksumAddress
#
# POOL_CONTRACT = "Pool"
# TICK_BASE = 1.0001
# MIN_TICK = -887272
# MAX_TICK = -MIN_TICK
# MIN_SQRT_RATIO = 4295128739
# MAX_SQRT_RATIO = 1461446703485210103287273052203988822378723970342
# POOL_INIT_CODE_HASH = "0xe34f199b19b2b4f47f68442619d555527d244f78a3297ea89325f843f87b8b54"
#
#
# class UniswapV3Amm(DexClientInterface):
#     pool_contract_names = (POOL_CONTRACT,)
#     pool_contracts_events_enum = TransactionType
#
#     def __init__(self, web3: Web3):
#         self.web3 = web3
#         pool_abi_path = Path(__file__).parent / "Pool.json"
#         self.pool_contract_abi = self.web3.eth.contract(abi=json.loads(pool_abi_path.read_text()))
#
#     def resolve_asset_from_log(self, parsed_log: ParsedReceiptLog) -> EthDexPool | None:
#         pool_address = parsed_log.address
#         checksum_address = to_checksum(pool_address)
#         logs.debug(f"Resolving pool addresses for {pool_address}")
#         factory_address = self.get_factory_address(checksum_address)
#         if not factory_address:
#             return None
#         tokens_addresses = self.get_tokens_addresses_for_pool(checksum_address)
#         if not tokens_addresses:
#             return None
#         ticks_spacing = self.get_ticks_spacing(checksum_address)
#         return EthDexPool(
#             address=pool_address,
#             token_addresses=[token.lower() for token in tokens_addresses],
#             fee=self.tick_spacing_to_fee(ticks_spacing),
#             factory_address=factory_address.lower(),
#             lp_token_addresses=[pool_address],
#         )
#
#     def resolve_receipt_log(
#         self,
#         parsed_receipt_log: ParsedReceiptLog,
#         dex_pool: EthDexPool | None = None,
#         tokens_for_pool: list[EthToken] | None = None,
#         transfers_for_transaction: list[EthTokenTransfer] | None = None,
#     ) -> EthDexTrade | None:
#         pass
#
#     # def resolve_receipt_log(
#     #     self,
#     #     parsed_receipt_log: ParsedReceiptLog,
#     #     dex_pool: EthDexPool | None = None,
#     #     tokens_for_pool: list[EthToken] | None = None,
#     #     transfers_for_transaction: list[EthTokenTransfer] | None = None,
#     # ) -> EthDexTrade | None:
#     #     event_name = parsed_receipt_log.event_name
#     #     if not parsed_receipt_log.parsed_event or not event_name:
#     #         return None
#     #     tokens_scalars = []
#     #     for erc20_token in tokens_for_pool:
#     #         tokens_scalars.append(10**erc20_token.decimals)
#     #
#     #     if event_name.lower() == self.pool_contracts_events_enum.swap.name:
#     #         logs.debug("resolving swap from swap event")
#     #         swap = self.get_swap_from_swap_event(dex_pool, parsed_receipt_log.parsed_event, tokens_scalars)
#     #         logs.debug(f"resolved swap from swap event {swap}")
#     #         swap.log_index = parsed_receipt_log.log_index
#     #         return {
#     #             "swaps": [swap],
#     #             "pools": [
#     #                 self.get_pool_finances(
#     #                     base_pool,
#     #                     parsed_event,
#     #                     tokens_scalars,
#     #                     receipt_log.block_number,
#     #                 )
#     #             ],
#     #         }
#     #
#     #     if event_name.lower() == self.pool_contracts_events_enum.burn.name:
#     #         logs.debug(f"resolving burn from burn event")
#     #         burn = self.get_mint_burn_from_events(base_pool, parsed_event, tokens_scalars)
#     #         logs.debug(f"resolving burn from burn event")
#     #         burn.log_index = receipt_log.log_index
#     #         return {
#     #             "burns": [burn],
#     #             "pools": [
#     #                 self.get_pool_finances(
#     #                     base_pool,
#     #                     parsed_event,
#     #                     tokens_scalars,
#     #                     receipt_log.block_number - 1,
#     #                 )
#     #             ],
#     #         }
#     #
#     #     if event_name.lower() == self.pool_contracts_events_enum.mint.name:
#     #         logs.debug(f"resolving burn from mint event")
#     #         mint = self.get_mint_burn_from_events(base_pool, parsed_event, tokens_scalars)
#     #         logs.debug(f"resolving burn from mint event")
#     #         mint.log_index = receipt_log.log_index
#     #         return {
#     #             "mints": [mint],
#     #             "pools": [
#     #                 self.get_pool_finances(
#     #                     base_pool,
#     #                     parsed_event,
#     #                     tokens_scalars,
#     #                     receipt_log.block_number,
#     #                 )
#     #             ],
#     #         }
#
#     # def get_pool_finances(
#     #     self,
#     #     base_pool,
#     #     parsed_event,
#     #     tokens_scalars,
#     #     block_number: Union[int, str] = "latest",
#     # ):
#     #     sqrt_price_x96 = parsed_event.get("sqrtPriceX96")
#     #     if not sqrt_price_x96:
#     #         slot0 = self.get_slot0(base_pool.address)
#     #         sqrt_price_x96 = slot0["sqrtPriceX96"]
#     #
#     #     token0_price = self.calculate_token0_price_from_sqrt_price_x96(
#     #         sqrt_price_x96, tokens_scalars
#     #     )
#     #     token1_price = 1 / token0_price
#     #
#     #     reserves = []
#     #     for idx, token in enumerate(base_pool.tokens_addresses):
#     #         reserves.append(
#     #             self.get_contract_reserve(base_pool.address, token, block_number)
#     #             / tokens_scalars[idx]
#     #         )
#     #
#     #     pool = PoolFinances(**base_pool.dict())
#     #     pool.reserves = [reserves[0], reserves[1]]
#     #     pool.prices = get_prices_for_two_pool(token0_price, token1_price)
#     #     return pool
#     #
#     # @staticmethod
#     # def get_mint_burn_from_events(base_pool, parsed_event, tokens_scalars):
#     #     mint_burn = MintBurn(
#     #         pool_address=base_pool.address,
#     #         sender=parsed_event["sender"] if parsed_event.get("sender") else parsed_event["owner"],
#     #         owner=parsed_event["owner"] if parsed_event.get("owner") else parsed_event["sender"],
#     #         amounts=[
#     #             parsed_event["amount0"] / tokens_scalars[0],
#     #             parsed_event["amount1"] / tokens_scalars[1],
#     #         ],
#     #     )
#     #     return mint_burn
#     #
#     @staticmethod
#     def get_swap_from_swap_event(base_pool, parsed_event, tokens_scalars):
#         amount0 = parsed_event["amount0"] / tokens_scalars[0]
#         amount1 = parsed_event["amount1"] / tokens_scalars[1]
#
#         swap = EthDexTrade(
#             pool_address=base_pool.address,
#             sender=parsed_event["sender"],
#             to=parsed_event["recipient"],
#             amounts=[amount0, amount1],
#         )
#         return swap
#
#     #
#     # def resolve_transaction(
#     #     self,
#     #     transaction: Transaction,
#     #     transaction_receipt: Receipt,
#     # ) -> Optional[ProtocolTransaction]:
#     #     protocol_transaction = ProtocolTransaction()
#     #     for receipt_log in transaction_receipt.logs:
#     #         receipt_log: ReceiptLog
#     #         topic = receipt_log.topics[0][0:4]
#     #         event_name = self.abi[POOL_CONTRACT].topic_keccaks.get(topic, None)
#     #         if receipt_log.topics and event_name:
#     #             base_transaction: BaseProtocolTransaction = self.resolve_base_transaction(
#     #                 event_name, receipt_log, transaction
#     #             )
#     #
#     #             # TODO: Get from storage instead, after
#     #             tokens_info: List[ERC20Token] = []
#     #             for token_address in base_transaction.tokens_addresses:
#     #                 token = self.get_token(token_address)
#     #                 if token:
#     #                     tokens_info.append(token)
#     #
#     #             if len(tokens_info) < len(base_transaction.tokens_addresses):
#     #                 continue
#     #
#     #             protocol_transaction.tokens.extend(tokens_info)
#     #             tokens_scalars = []
#     #             for token_info in tokens_info:
#     #                 tokens_scalars.append(10**token_info.decimals)
#     #
#     #             parsed_event = self.parse_event(self.abi[POOL_CONTRACT], event_name, receipt_log)
#     #
#     #             ticks_spacing = self.get_ticks_spacing(receipt_log.address)
#     #
#     #             if base_transaction.transaction_type == self.pool_contracts_events_enum.swap.name:
#     #                 swap = self.resolve_swap(
#     #                     base_transaction, parsed_event, tokens_scalars, ticks_spacing
#     #                 )
#     #                 if not swap:
#     #                     continue
#     #                 protocol_transaction.swaps.append(swap)
#     #                 protocol_transaction.pools.append(
#     #                     Pool(
#     #                         address=base_transaction.pool_address,
#     #                         tokens_addresses=base_transaction.tokens_addresses,
#     #                         fee=V3Pool.tick_spacing_to_fee(ticks_spacing),
#     #                         reserves=swap.reserves,
#     #                         prices=swap.prices,
#     #                         transaction_type=self.pool_contracts_events_enum.swap.name,
#     #                         wallet_address=swap.wallet_address,
#     #                     )
#     #                 )
#     #             elif base_transaction.transaction_type in [
#     #                 self.pool_contracts_events_enum.mint.name,
#     #                 self.pool_contracts_events_enum.burn.name,
#     #             ]:
#     #                 mint_burn = self.resolve_mint_burn(
#     #                     base_transaction,
#     #                     parsed_event,
#     #                     tokens_scalars,
#     #                     ticks_spacing,
#     #                     receipt_log.block_number,
#     #                 )
#     #                 if not mint_burn:
#     #                     continue
#     #                 protocol_transaction.pools.append(
#     #                     Pool(
#     #                         address=base_transaction.pool_address,
#     #                         tokens_addresses=base_transaction.tokens_addresses,
#     #                         fee=V3Pool.tick_spacing_to_fee(ticks_spacing),
#     #                         reserves=mint_burn.reserves,
#     #                         prices=mint_burn.prices,
#     #                         transaction_type=self.pool_contracts_events_enum.swap.name,
#     #                         wallet_address=mint_burn.wallet_address,
#     #                     )
#     #                 )
#     #                 if event_name.lower() == self.pool_contracts_events_enum.mint.name:
#     #                     protocol_transaction.mints.append(mint_burn)
#     #                 if event_name.lower() == self.pool_contracts_events_enum.burn.name:
#     #                     protocol_transaction.burns.append(mint_burn)
#     #             else:
#     #                 logs.debug(f"Not existing event type yet {event_name}")
#     #
#     #     return protocol_transaction
#
#     def get_slot0(self, pool_address, block_identifier: Literal['latest'] | int = "latest"):
#         values = self.pool_contract_abi.functions.slot0().call(
#             {"to": pool_address}, block_identifier
#         )
#         names = [
#             "sqrtPriceX96",
#             "tick",
#             "observationIndex",
#             "observationCardinality",
#             "observationCardinalityNext",
#             "feeProtocol",
#             "unlocked",
#         ]
#         return dict(zip(names, values))
#
#     def get_liquidity(self, pool_address, block_identifier: Literal['latest'] | int = "latest"):
#         return self.pool_contract_abi.functions.liquidity().call(
#             {"to": pool_address}, block_identifier
#         )
#
#     def get_factory_address(self, pool_address: str) -> str | None:
#         try:
#             factory_address = self.pool_contract_abi.functions.factory().call(
#                 {"to": pool_address}, "latest"
#             )
#         except (ValueError, TypeError, BadFunctionCallOutput, ContractLogicError):
#             return None
#         return factory_address
#
#     @staticmethod
#     def tick_spacing_to_fee(tick_spacing: int) -> DexPoolFeeAmount:
#         fees_spacings = {
#             1: DexPoolFeeAmount.VERY_LOW,
#             10: DexPoolFeeAmount.LOW,
#             60: DexPoolFeeAmount.MEDIUM,
#             200: DexPoolFeeAmount.HIGH,
#         }
#         try:
#             return fees_spacings[tick_spacing]
#         except KeyError:
#             return DexPoolFeeAmount.UNDEFINED
#
#     def get_tokens_addresses_for_pool(self, pool_address: str) -> list | None:
#         try:
#             tokens_addresses = [
#                 (
#                     self.pool_contract_abi.functions.token0().call({"to": pool_address}, "latest")
#                 ).lower(),
#                 (
#                     self.pool_contract_abi.functions.token1().call({"to": pool_address}, "latest")
#                 ).lower(),
#             ]
#         except (ValueError, TypeError, BadFunctionCallOutput, ContractLogicError):
#             return None
#         return tokens_addresses
#
#     # TODO: Need to add from SDK
#     # def compute_pool_address(self, token0_address: str, token1_address: str, fee: FeeAmount):
#     #     Web3().eth.codec()
#     #     keccac = Web3.keccak(text='bytes')
#
#     @staticmethod
#     def calculate_token0_price_from_sqrt_price_x96(
#         sqrt_price_x96: int, token_decimals_scalars: list[int]
#     ) -> float:
#         # TODO: failing with point in some cases, check
#         token_scalars_diff = token_decimals_scalars[1] / token_decimals_scalars[0]
#         price = ((sqrt_price_x96**2) / (2**192)) / token_scalars_diff
#         return price
#
#     # def resolve_mint_burn(
#     #     self,
#     #     base_transaction: BaseProtocolTransaction,
#     #     parsed_event: dict,
#     #     tokens_decimals_scalars: List[int],
#     #     ticks_spacing: int,
#     #     block_number: Union[int, str] = "latest",
#     # ) -> Optional[BaseMintBurn]:
#     #     slot0 = self.get_slot0(base_transaction.pool_address)
#     #
#     #     token0_price = self.calculate_token0_price_from_sqrt_price_x96(
#     #         slot0["sqrtPriceX96"], tokens_decimals_scalars
#     #     )
#     #
#     #     if not token0_price:
#     #         return None
#     #
#     #     liquidity = self.get_liquidity(
#     #         base_transaction.pool_address,
#     #         block_number - 1
#     #         if base_transaction.transaction_type == self.pool_contracts_events_enum.burn.name
#     #         else block_number,
#     #     )
#     #
#     #     reserves = self.amounts_locked_at_tick(parsed_event["tickLower"], liquidity, ticks_spacing)
#     #
#     #     return BaseMintBurn(
#     #         **base_transaction.dict(),
#     #         sender=parsed_event["sender"] if parsed_event.get("sender") else parsed_event["owner"],
#     #         owner=parsed_event["owner"],
#     #         prices=[token0_price, 1 / token0_price],
#     #         amounts=[
#     #             parsed_event["amount0"] / tokens_decimals_scalars[0],
#     #             parsed_event["amount1"] / tokens_decimals_scalars[1],
#     #         ],
#     #         reserves=[
#     #             reserves[0] / tokens_decimals_scalars[0],
#     #             reserves[1] / tokens_decimals_scalars[1],
#     #         ],
#     #     )
#     #
#     # def resolve_swap(
#     #     self,
#     #     base_transaction: BaseProtocolTransaction,
#     #     parsed_event: dict,
#     #     tokens_decimals_scalars: List[int],
#     #     ticks_spacing: int,
#     # ) -> Optional[BaseSwap]:
#     #     # token0_price = self.calculate_token0_price_from_sqrt_price_x96(parsed_event['sqrtPriceX96'],
#     #     #                                                                tokens_decimals_scalars[1])
#     #
#     #     amount0 = parsed_event["amount0"] / tokens_decimals_scalars[0]
#     #     amount1 = parsed_event["amount1"] / tokens_decimals_scalars[1]
#     #
#     #     if amount1 and amount0:
#     #         token0_price = abs(amount1 / amount0)
#     #     else:
#     #         return None
#     #
#     #     # TODO: fix to have TVL there instead of tick reserves
#     #     reserves = self.amounts_locked_at_tick(
#     #         parsed_event["tick"], parsed_event["liquidity"], ticks_spacing
#     #     )
#     #
#     #     return BaseSwap(
#     #         **base_transaction.dict(),
#     #         sender=parsed_event["sender"],
#     #         recipient=parsed_event["recipient"],
#     #         prices=[token0_price, 1 / token0_price],
#     #         amounts=[
#     #             parsed_event["amount0"] / tokens_decimals_scalars[0],
#     #             parsed_event["amount1"] / tokens_decimals_scalars[1],
#     #         ],
#     #         reserves=[
#     #             reserves[0] / tokens_decimals_scalars[0],
#     #             reserves[1] / tokens_decimals_scalars[1],
#     #         ],
#     #     )
#     #
#     # def resolve_base_transaction(
#     #     self, event_name, receipt_log, transaction
#     # ) -> BaseProtocolTransaction:
#     #     if event_name.lower() in [
#     #         self.pool_contracts_events_enum.mint.name,
#     #         self.pool_contracts_events_enum.burn.name,
#     #         self.pool_contracts_events_enum.swap.name,
#     #     ]:
#     #         tokens_addresses = self.get_tokens_addresses_for_pool(
#     #             Web3.toChecksumAddress(receipt_log.address)
#     #         )
#     #     elif event_name.lower() == self.pool_contracts_events_enum.transfer.name:
#     #         tokens_addresses = [receipt_log.address]
#     #     else:
#     #         tokens_addresses = []
#     #     base_transaction = BaseProtocolTransaction(
#     #         id=f"{transaction.address}-{receipt_log.log_index}",
#     #         pool_address=receipt_log.address,
#     #         log_index=receipt_log.log_index,
#     #         transaction_type=event_name.lower(),
#     #         transaction_address=transaction.address,
#     #         timestamp=transaction.timestamp,
#     #         protocol_type=ProtocolType.uniswap_v3,
#     #         tokens_addresses=tokens_addresses,
#     #         wallet_address=transaction.from_address,
#     #     )
#     #     return base_transaction
#     #
#     # @staticmethod
#     # def amounts_locked_at_tick(tick: int, liquidity: int, ticks_spacing: int) -> Tuple[int, int]:
#     #     bottom_tick = math.floor(tick / ticks_spacing) * ticks_spacing
#     #     top_tick = bottom_tick + ticks_spacing
#     #
#     #     def tick_to_price(tick_idx):
#     #         return TICK_BASE**tick_idx
#     #
#     #     sa = tick_to_price(bottom_tick // 2)
#     #     sb = tick_to_price(top_tick // 2)
#     #
#     #     # Compute real amounts of the two assets
#     #     amount0 = liquidity * (sb - sa) / (sa * sb)
#     #     amount1 = liquidity * (sb - sa)
#     #
#     #     return amount0, amount1
#
#     def get_ticks_spacing(
#         self,
#         pool_address,
#         block_identifier: Literal['latest', 'earliest', 'pending', 'safe', 'finalized']
#         | int = "latest",
#     ):
#         return self.pool_contract_abi.functions.tickSpacing().call(
#             {"to": pool_address}, block_identifier
#         )
