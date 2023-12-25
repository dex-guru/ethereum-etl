# import logging
#
# from web3 import Web3
# from web3.exceptions import BadFunctionCallOutput, ContractLogicError
#
# from ethereumetl.service.dex.dodo.base import BaseDODOAmmClient
# from ethereumetl.service.dex.interface import DexClientInterface
#
# logs = logging.getLogger(__name__)
#
#
# class DODOv1Amm(BaseDODOAmmClient, DexClientInterface):
#     """
#     DODO V1 AMM client.
#
#     Docs: <https://docs.dodoex.io/english/developers/contract-data/contract-event>.
#     Other docs: <https://dodoex.github.io/docs/docs/>.
#     Contracts: <https://docs.dodoex.io/english/developers/contracts-address/ethereum#dodo-v1>.
#
#     On each v1 pool, DODO has 2 LP tokens (base and quote).
#
#
#     # Example transactions
#
#     Contract DODORouteProxy using for proxying calling v1 contract from v2 contract.
#
#     Example V2 swap for V1 pool:
#         <https://bscscan.com/tx/0x0cc63056baa5ef57c3fc0b4390ffb1d69c91a7b59e2a3e2778bfc8f1ec569ee1>.
#     Example V2 add liquidity for V1 pool:
#         <https://bscscan.com/tx/0xf2e4965825979d284fb889ef2b3a60f8813075a53075edc5e43d66246a91d871>.
#
#
#     # Factory for contracts
#
#     GnosisSafeProxy (proxy for GnosisSafe) is a maintainer contract used as a factory for DODO contracts.
#     GnosisSafeProxy it is a Multisig wallet, and can have different names and realizations on other chains.
#
#     Examples
#     --------
#         - BNB: 0xcaa42F09AF66A8BAE3A7445a7f63DAD97c11638b
#         - ETH: 0x95C4F5b83aA70810D4f142d58e5F7242Bd891CB0
#         - Polygon: 0x3CD6D7F5fF977bf8069548eA1F9441b061162b42
#     See `clients.blockchain.base.evm_base.EVMBase.get_pool_factory` where using maintainer as factory.
#
#
#     # Topics of methods
#
#     Method `Dodo Swap V1`: topic0 = 0xe93ad76094f247c0dafc1c61adc2187de1ac2738f7a3b49cb20b2263420251a3.
#     Method `Add Liquidity To V1`: topic0 = 0x18081cde2fa64894914e1080b98cca17bb6d1acf633e57f6e26ebdb945ad830b
#         -- mint (Deposit).
#     """
#
#     pool_contract_name = "DODO"
#     amm_type = "dodo"
#     pool_contract_names = [pool_contract_name]
#     pool_contracts_events_enum = DODOTransactionType
#
#     _get_lp_token_methods = ["_BASE_CAPITAL_TOKEN_", "_QUOTE_CAPITAL_TOKEN_"]
#
#     def is_pool_address_for_amm(self, pool_address: str) -> bool:
#         pool_address = Web3.toChecksumAddress(pool_address)
#         try:
#             fee_rate = (
#                 self.abi[self.pool_contract_name]
#                 .contract.functions._MT_FEE_RATE_()
#                 .call({"to": pool_address}, "latest")
#             )
#             print(fee_rate)
#         except (TypeError, ContractLogicError, ValueError, BadFunctionCallOutput):
#             return False
#         return isinstance(fee_rate, int)
#
#     def resolve_receipt_log(
#         self,
#         receipt_log: ReceiptLog,
#         base_pool: BasePool,
#         erc20_tokens: list[ERC20Token],
#         transfers: list[TransferBase],
#     ) -> dict | None:
#         logs.debug(
#             f"resolving {receipt_log.transaction_hash.hex()}-{receipt_log.log_index} by DODO AMM client"
#         )
#         logs.debug(f"receipt_log: {receipt_log}, {base_pool}, {erc20_tokens}, {transfers}")
#         tokens_scalars = [10**t.decimals for t in erc20_tokens]
#
#         parsed_event, event_name = self._get_parsed_event_from_receipt_log(receipt_log)
#         if not (parsed_event and event_name):
#             return None
#         block_number = receipt_log.block_number
#         if event_name == self.pool_contracts_events_enum.burn.value:
#             block_number = receipt_log.block_number - 1
#
#         reserve0 = (
#             self.get_contract_reserve(base_pool.address, erc20_tokens[0].address, block_number)
#             / tokens_scalars[0]
#         )
#         reserve1 = (
#             self.get_contract_reserve(base_pool.address, erc20_tokens[1].address, block_number)
#             / tokens_scalars[1]
#         )
#
#         parsed_event["reserve0"] = reserve0
#         parsed_event["reserve1"] = reserve1
#
#         pool: PoolFinances = self.get_pool_finances_from_event(base_pool, parsed_event)
#         pool.amm = self.amm_type
#         logs.debug(f"resolved pool from event: {pool}")
#
#         if event_name in self.pool_contracts_events_enum.swap_events():
#             logs.debug(f"resolving swap from swap event ({event_name})")
#             if event_name == self.pool_contracts_events_enum.buy_base_token.value:
#                 swap: Swap = self._get_swap_from_buy_base_token_event(
#                     base_pool, parsed_event, tokens_scalars
#                 )
#             else:
#                 swap: Swap = self._get_swap_from_sell_base_token_event(
#                     base_pool, parsed_event, tokens_scalars
#                 )
#             logs.debug(f"resolved swap from swap event: {swap}")
#             swap.log_index = receipt_log.log_index
#             return {"swaps": [swap], "pools": [pool]}
#
#         if event_name == self.pool_contracts_events_enum.mint.value:
#             logs.debug("resolving mint from mint event")
#             mint: MintBurn = self.get_mint_burn_from_events(
#                 base_pool, parsed_event, tokens_scalars
#             )
#             logs.debug(f"resolved mint from mint event: {mint}")
#             mint.log_index = receipt_log.log_index
#             return {"mints": [mint], "pools": [pool]}
#
#         if event_name == self.pool_contracts_events_enum.burn.value:
#             logs.debug("resolving burn from burn event")
#             burn: MintBurn = self.get_mint_burn_from_events(
#                 base_pool, parsed_event, tokens_scalars
#             )
#             logs.debug(f"resolved burn from burn event: {burn}")
#             burn.log_index = receipt_log.log_index
#             return {"burns": [burn], "pools": [pool]}
#
#     def get_mint_burn_from_events(
#         self, base_pool: BasePool, parsed_event: dict, tokens_scalars: list[int]
#     ) -> MintBurn:
#         """
#         receiver -- user wallet
#         payer -- DODO contract.
#         """
#         amounts = [0, 0]
#
#         is_base_token = parsed_event["isBaseToken"]
#         position = 0 if is_base_token else 1
#
#         amounts[position] = parsed_event["amount"] / tokens_scalars[position]
#
#         call_kwargs = {"to": base_pool.address}
#         contract = self.abi[self.pool_contract_name].contract
#         if is_base_token:
#             lp_token_address = contract.functions._BASE_CAPITAL_TOKEN_().call(call_kwargs)
#         else:
#             lp_token_address = contract.functions._QUOTE_CAPITAL_TOKEN_().call(call_kwargs)
#
#         mint_burn = MintBurn(
#             pool_address=base_pool.address,
#             sender=parsed_event["payer"],
#             owner=parsed_event["receiver"],
#             amounts=amounts,
#             lp_token_address=lp_token_address,
#         )
#         return mint_burn
#
#     @staticmethod
#     def _get_swap_from_buy_base_token_event(
#         base_pool: BasePool, parsed_event: dict, tokens_scalars: list[int]
#     ) -> Swap:
#         amount0 = parsed_event["receiveBase"] / tokens_scalars[0]
#         amount1 = parsed_event["payQuote"] / tokens_scalars[1]
#
#         swap = Swap(
#             pool_address=base_pool.address,
#             # Note: buyer -- contract, not wallet. But it is magically fixed in blockchain service.
#             sender=parsed_event["buyer"],
#             to=parsed_event["buyer"],
#             amounts=[-amount0, amount1],
#         )
#         return swap
#
#     @staticmethod
#     def _get_swap_from_sell_base_token_event(
#         base_pool: BasePool, parsed_event: dict, tokens_scalars: list[int]
#     ) -> Swap:
#         amount0 = parsed_event["payBase"] / tokens_scalars[0]
#         amount1 = parsed_event["receiveQuote"] / tokens_scalars[1]
#
#         swap = Swap(
#             pool_address=base_pool.address,
#             # Note: seller -- contract, not wallet. But it is magically fixed in blockchain service.
#             sender=parsed_event["seller"],
#             to=parsed_event["seller"],
#             amounts=[amount0, -amount1],
#         )
#         return swap
#
#     def get_lp_token_address_for_pool(self, pool_address: str) -> list[str]:
#         lp_tokens = []
#         pool_address = self.w3.toChecksumAddress(pool_address)
#         for method in self._get_lp_token_methods:
#             lp_tokens.append(
#                 getattr(self.abi[self.pool_contract_name].contract.functions, method)().call(
#                     {"to": pool_address},
#                 )
#             )
#         lp_tokens = [_.lower() for _ in lp_tokens]
#         return lp_tokens
