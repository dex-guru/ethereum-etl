# import json
# import logging
# from collections import OrderedDict
# from enum import Enum
# from pathlib import Path
#
# from web3 import Web3
#
# from ethereumetl.domain.dex_pool import EthDexPool
# from ethereumetl.misc.info import NULL_ADDRESSES
# from ethereumetl.service.dex import DexClientInterface
# from ethereumetl.service.dex.base.base_dex_client import BaseDexClient
#
# LOGGER = logging.getLogger(__name__)
#
#
# class BancorV2Amm(BaseDexClient, DexClientInterface):
#     CONTRACT_REGISTRY = "ContractRegistry"
#     BANCOR_NETWORK = "BancorNetwork"
#     BANCOR_CONVERTER = "BancorConverter"
#     BANCOR_CONVERTER_REGISTRY = "BancorConverterRegistry"
#     STANDARD_POOL_CONVERTER = "StandardPoolConverter"
#     LIQUIDITY_POOL_V1_CONVERTER = "LiquidityPoolV1Converter"
#     DYNAMIC_CONTRACTS = [
#         BANCOR_NETWORK,
#         BANCOR_CONVERTER_REGISTRY,
#         BANCOR_CONVERTER,
#         STANDARD_POOL_CONVERTER,
#         LIQUIDITY_POOL_V1_CONVERTER,
#     ]
#     DEFAULT_SCALAR = 10**18
#
#     pool_contract_names = [
#         BANCOR_CONVERTER,
#         LIQUIDITY_POOL_V1_CONVERTER,
#         STANDARD_POOL_CONVERTER,
#     ]
#
#     class BancorTransactionEnum(str, Enum):
#         Conversion = "Conversion"  # swap
#         LiquidityAdded = "LiquidityAdded"  # mint
#         LiquidityRemoved = "LiquidityRemoved"  # burn
#         TokenRateUpdate = "TokenRateUpdate"  # sync
#
#     pool_contracts_events_enum = BancorTransactionEnum
#
#     def __init__(self, uri: str, amm_type: str, contracts: dict):
#         super().__init__(uri=uri, amm_type=amm_type, contracts=contracts)
#
#         abi_path = Path(__file__).parent.parent / "abi" / amm_type
#
#         self.pool_contract_get_tokens = OrderedDict(
#             (
#                 (self.STANDARD_POOL_CONVERTER, self.standard_pool_converter_get_tokens),
#                 (self.BANCOR_CONVERTER, self.bancor_converter_get_tokens),
#                 (
#                     self.LIQUIDITY_POOL_V1_CONVERTER,
#                     self.liquidity_pool_v1_converter_get_tokens,
#                 ),
#             )
#         )
#
#         self.converter_abis = {}
#         for converter_name in self.pool_contract_get_tokens:
#             converter_abi_path = abi_path / f"{converter_name}.json"
#             with open(converter_abi_path) as fh:
#                 self.converter_abis[converter_name] = json.load(fh)
#
#         address_of = self.abi[self.CONTRACT_REGISTRY].contract.functions.addressOf
#         for contract_name in self.DYNAMIC_CONTRACTS:
#             pool_abi_path = abi_path / f"{contract_name}.json"
#             contract_address = address_of(contract_name.encode()).call()
#             if contract_address == NULL_ADDRESSES:
#                 contract_address = None
#             self.abi[contract_name] = self._initiate_contract(
#                 filename=pool_abi_path,
#                 address=contract_address,
#             )
#
#     def get_num_pairs(self, block_identifier: str | int = "latest") -> int:
#         function = self.abi[self.BANCOR_CONVERTER_REGISTRY].contract.functions.getAnchorCount
#         return function().call(block_identifier=block_identifier)
#
#     def get_base_pool(self, pool_address: str) -> EthDexPool | None:
#         self.get_pair_tokens_by_index(0)["tokens_addresses"]
#         base_pool = EthDexPool(address=pool_address, amm=self.amm_type)
#         return base_pool
#
#     def get_pair_tokens_by_index(
#         self, pair_index: int, block_identifier: str | int = "latest"
#     ) -> dict[str, str]:
#         function = self.abi[self.BANCOR_CONVERTER_REGISTRY].contract.functions.getAnchor
#         address = function(pair_index).call(block_identifier=block_identifier)
#         function = self.abi[
#             self.BANCOR_CONVERTER_REGISTRY
#         ].contract.functions.getConvertersByAnchors
#         converter_addresses = function([address]).call(block_identifier=block_identifier)
#         converter_address = converter_addresses[0]
#
#         tokens = None
#         for contract_name, function in self.pool_contract_get_tokens.items():
#             contract = self.get_converter_contract(name=contract_name, address=converter_address)
#             try:
#                 tokens = function(contract=contract)
#                 break
#             except Exception as exception:
#                 LOGGER.warning(
#                     "[Bancor V2] Skipped contract '%s' because cannot get pair tokens for '%s' (%s)",
#                     contract_name,
#                     converter_address,
#                     exception,
#                 )
#                 continue
#         tokens = tokens or []
#         tokens = [
#             self.convert_zero_addresses_to_wrapped_ethereum(token.lower()) for token in tokens
#         ]
#
#         return {"address": converter_address, "tokens_addresses": tokens}
#
#     # def resolve_receipt_log(
#     #     self,
#     #     receipt_log: ReceiptLog,
#     #     base_pool: BasePool,
#     #     erc20_tokens: List[ERC20Token],
#     # ) -> Optional[Dict[str, Any]]:
#     #     try:
#     #         topic = receipt_log.topics[0][0:4]
#     #     except IndexError:
#     #         LOGGER.error(f"Cant get receipt_log.topics[0][0:4], index error, log: {receipt_log}")
#     #         return None
#     #
#     #     event_name = None
#     #     contract = None
#     #     for contract in self.get_pool_contracts()[0]:
#     #         event_name = contract.topic_keccaks.get(topic, None)
#     #         if event_name:
#     #             break
#     #     if not event_name:
#     #         return
#     #
#     #     tokens_decimals = OrderedDict()
#     #     tokens_indexes = OrderedDict()
#     #     for index, erc20_token in enumerate(erc20_tokens):
#     #         address = erc20_token.address.lower()
#     #         tokens_decimals[address] = 10**erc20_token.decimals
#     #         tokens_indexes[address] = index
#     #     parsed_event = self.parse_event(contract, event_name, receipt_log)
#     #
#     #     function = {
#     #         self.pool_contracts_events_enum.Conversion: self.get_data_from_swap_event,
#     #         self.pool_contracts_events_enum.LiquidityAdded: partial(
#     #             self.get_data_from_mint_burn_event, key="mints"
#     #         ),
#     #         self.pool_contracts_events_enum.LiquidityRemoved: partial(
#     #             self.get_data_from_mint_burn_event, key="burns"
#     #         ),
#     #         # self.pool_contracts_events_enum.TokenRateUpdate: self.get_data_from_sync_event,
#     #     }.get(event_name)
#     #     if not function:
#     #         return
#     #
#     #     try:
#     #         return function(
#     #             base_pool=base_pool,
#     #             parsed_event=parsed_event,
#     #             tokens_decimals=tokens_decimals,
#     #             tokens_indexes=tokens_indexes,
#     #             receipt_log=receipt_log,
#     #         )
#     #     except Exception as exc:
#     #         LOGGER.error(
#     #             "Exception when handling Bancor V2 event '%s' (%s) %s",
#     #             event_name,
#     #             exc.__class__.__name__,
#     #             exc,
#     #         )
#
#     def get_converter_contract(self, name: str, address: str) -> ABIContract:
#         abi = self.converter_abis[name]
#         params = {"abi": abi, "address": Web3.toChecksumAddress(address)}
#         return ABIContract(
#             contract=self.w3.eth.contract(**params),
#             address=address,
#             abi_contract_dict=abi,
#         )
#
#     # def get_data_from_swap_event(
#     #     self,
#     #     base_pool: BasePool,
#     #     parsed_event: Dict[str, Any],
#     #     tokens_decimals: Dict[str, int],
#     #     tokens_indexes: Dict[str, int],
#     #     receipt_log: ReceiptLog,
#     # ) -> Dict[str, List[BaseModel]]:
#     #     from_token = self.convert_zero_addresses_to_wrapped_ethereum(parsed_event["_fromToken"])
#     #     to_token = self.convert_zero_addresses_to_wrapped_ethereum(parsed_event["_toToken"])
#     #
#     #     from_scalar = tokens_decimals.get(from_token, self.DEFAULT_SCALAR)
#     #     from_amount = parsed_event["_amount"] / from_scalar
#     #
#     #     to_scalar = tokens_decimals.get(to_token, self.DEFAULT_SCALAR)
#     #     to_amount = parsed_event["_return"] / to_scalar
#     #
#     #     amounts = [0, 0]
#     #     amounts[tokens_indexes[from_token]] = -from_amount
#     #     amounts[tokens_indexes[to_token]] = to_amount
#     #
#     #     swap = Swap(
#     #         pool_address=base_pool.address,
#     #         sender=parsed_event["_trader"],
#     #         to=parsed_event["_trader"],
#     #         log_index=receipt_log.log_index,
#     #         amounts=amounts,
#     #     )
#     #     pool = self.get_data_from_sync_event(
#     #         base_pool, parsed_event, tokens_decimals, tokens_indexes, receipt_log
#     #     )
#     #     return {
#     #         "swaps": [swap],
#     #         "pools": pool["pools"],
#     #     }
#     #
#     # def get_data_from_mint_burn_event(
#     #     self,
#     #     key: str,
#     #     base_pool: BasePool,
#     #     parsed_event: Dict[str, Any],
#     #     tokens_decimals: Dict[str, int],
#     #     tokens_indexes: Dict[str, int],
#     #     receipt_log: ReceiptLog,
#     # ) -> Dict[str, List[BaseModel]]:
#     #     reserve_token = self.convert_zero_addresses_to_wrapped_ethereum(
#     #         parsed_event["_reserveToken"]
#     #     )
#     #     reserve_token_index = tokens_indexes[reserve_token]
#     #     reserve_scalar = tokens_decimals.get(reserve_token, self.DEFAULT_SCALAR)
#     #
#     #     amounts = [0, 0]
#     #     amounts[reserve_token_index] = parsed_event["_amount"] / reserve_scalar
#     #     mint_burn = MintBurn(
#     #         pool_address=base_pool.address,
#     #         sender=parsed_event["_provider"],
#     #         owner=parsed_event["_provider"],
#     #         log_index=receipt_log.log_index,
#     #         amounts=amounts,
#     #     )
#     #     if key == "burns":
#     #         receipt_log.block_number = receipt_log.block_number - 1
#     #     pool = self.get_data_from_sync_event(
#     #         base_pool, parsed_event, tokens_decimals, tokens_indexes, receipt_log
#     #     )
#     #     return {
#     #         key: [mint_burn],
#     #         "pools": pool["pools"],
#     #     }
#     #
#     # def get_data_from_sync_event(
#     #     self,
#     #     base_pool: BasePool,
#     #     parsed_event: Dict[str, Any],
#     #     tokens_decimals: Dict[str, int],
#     #     tokens_indexes: Dict[str, int],
#     #     receipt_log: ReceiptLog,
#     # ) -> Dict[str, List[BaseModel]]:
#     #     reserves = []
#     #     for token, decimal in tokens_decimals.items():
#     #         token = self.convert_wrapped_addresses_to_zero_ethereum(token)
#     #         try:
#     #             reserve = self.get_contract_reserve(
#     #                 account=receipt_log.address,
#     #                 token_address=token,
#     #                 block_identifier=receipt_log.block_number,
#     #             )
#     #         except Exception:
#     #             contract = self.get_converter_contract(
#     #                 name=self.STANDARD_POOL_CONVERTER,
#     #                 address=receipt_log.address,
#     #             )
#     #             reserve = contract.contract.functions.reserveBalance(
#     #                 Web3.toChecksumAddress(token),
#     #             ).call({}, receipt_log.block_number)
#     #         reserves.append(reserve / decimal)
#     #
#     #     prices = get_prices_for_two_pool(
#     #         reserves[1] / reserves[0],
#     #         reserves[0] / reserves[1],
#     #     )
#     #
#     #     pool = PoolFinances(
#     #         **base_pool.dict(),
#     #         reserves=reserves,
#     #         prices=prices,
#     #         amm=self.amm_type,
#     #     )
#     #     return {"pools": [pool]}
#
#     @staticmethod
#     def standard_pool_converter_get_tokens(
#         contract: ABIContract,
#         block_identifier: str | int = "latest",
#     ) -> list[str]:
#         return contract.contract.functions.reserveTokens().call(block_identifier=block_identifier)
#
#     @staticmethod
#     def bancor_converter_get_tokens(
#         contract: ABIContract,
#         block_identifier: str | int = "latest",
#     ) -> list[str]:
#         return contract.contract.connectorTokens.call(block_identifier=block_identifier)
#
#     @staticmethod
#     def liquidity_pool_v1_converter_get_tokens(
#         contract: ABIContract,
#         block_identifier: str | int = "latest",
#     ):
#         function = contract.contract.functions.connectorTokenCount
#         tokens_count = function().call(block_identifier=block_identifier)
#
#         function = contract.contract.functions.connectorTokens
#         return [
#             function(index).call(block_identifier=block_identifier)
#             for index in range(tokens_count)
#         ]
#
#     @staticmethod
#     def convert_zero_addresses_to_wrapped_ethereum(address: str) -> str:
#         return ETHEREUM_ZERO_ADDRESSES_MAPPING.get(address, address)
#
#     @staticmethod
#     def convert_wrapped_addresses_to_zero_ethereum(address: str) -> str:
#         return ETHEREUM_ZERO_ADDRESSES_MAPPING_REVERT.get(address, address)
