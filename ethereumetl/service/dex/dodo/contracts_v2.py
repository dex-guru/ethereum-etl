#
# from clients.blockchain.amm.dodo.base import BaseDODOAmmClient
# from clients.blockchain.interfaces import AmmClientI
# from clients.blockchain.models.dodo_transaction import DODOv2TransactionType
# from clients.blockchain.models.pool import BasePool, PoolFinances
# from clients.blockchain.models.protocol_transaction import MintBurn, Swap
# from clients.blockchain.models.tokens import ERC20Token
# from clients.blockchain.models.transaction import ReceiptLog
# from clients.blockchain.models.transfer import TransferBase, TransferType
# from eth_utils import is_address
# from utils.logger import get_logger
# from web3 import Web3
# from web3.exceptions import BadFunctionCallOutput, ContractLogicError
#
# logs = get_logger(__name__)
#
#
# class DODOv2Amm(BaseDODOAmmClient, AmmClientI):
#     """
#     DODO V2 AMM client.
#
#     Docs: <https://docs.dodoex.io/english/developers/contract-data/contract-event>.
#     Other docs: <https://dodoex.github.io/docs/docs/>.
#     Contracts: <https://docs.dodoex.io/english/developers/contracts-address/ethereum#dodo-v2>.
#
#     This client parse DVM compatible (by ABI and events) contracts:
#         - DVM (one token and standard pools, DODO Vending Machine),
#         - DPP (DODO Private Pool),
#         - DPPAdvanced,
#         - DSP (fixed pool, DODO Stable Pool)
#
#     # BSC examples:
#
#     Tx with swap with 0 values: 0xe82e7b80ea4f668d3b1ac275163590f6956bab1e11515c26bcfb59efc39ce1c5.
#
#     Event with one token (base) mint: 0x12153133e9d2e8a031208b6d714f80c1c62599c12f4850bad12a193715b0979d-62
#         (DVM pool: 0xfb8c399f1187bdb5adf2007b32d5d76651b77ff4).
#
#     Event with both tokens mint: 0x82dda2f78a888b48b6437bac478f668c25adf6c71c21bb1b27800fc7a3ca6f6d-76
#         (DVM pool: 0x566ab0fd046c27c4ffaf2f87eda6bd15f4f4b3bd).
#
#     DVM mint: topic0 = 0x0f6798a560793a54c3bcfe86a93cde1e73087d944c0ea20544137d4121396885.
#     DSP swap: topic0 = 0xc2c0245e056d5fb095f04cd6373bc770802ebd1e6c918eb78fdef843cdb37b0f.
#
#     Todo:
#     ----
#         - fix parsing https://bscscan.com/tx/0x9c5af9df97c9bcd3d4271267b4cbf0e55ade9a43c1d3f133a981e870e8f0ac6e
#             - transfer from 0x0 to wallet, but we search transfer from wallet to pool
#     """
#
#     pool_contract_name = "DVM"
#     amm_type = "dodo_v2"
#     pool_contract_names = [pool_contract_name]
#     pool_contracts_events_enum = DODOv2TransactionType
#
#     def is_pool_address_for_amm(self, pool_address: str) -> bool:
#         # TODO: do optimize
#         pool_address = Web3.toChecksumAddress(pool_address)
#         for contract_name in self.pool_contract_names:
#             try:
#                 fee_rate_model_address = (
#                     self.abi[contract_name]
#                     .contract.functions._MT_FEE_RATE_MODEL_()
#                     .call({"to": pool_address}, "latest")
#                 )
#             except (
#                 TypeError,
#                 ContractLogicError,
#                 ValueError,
#                 BadFunctionCallOutput,
#             ):
#                 fee_rate_model_address = False
#             return True if is_address(fee_rate_model_address) else False
#
#     def resolve_receipt_log(
#         self,
#         receipt_log: ReceiptLog,
#         base_pool: BasePool,
#         erc20_tokens: list[ERC20Token],
#         transfers: list[TransferBase],
#     ) -> dict | None:
#         logs.debug(
#             f"resolving {receipt_log.transaction_hash.hex()}-{receipt_log.log_index} by DODO v2 AMM client"
#         )
#         tokens_scalars = [10**t.decimals for t in erc20_tokens]
#
#         parsed_event, event_name = self._get_parsed_event_from_receipt_log(receipt_log)
#         if not (parsed_event and event_name):
#             return None
#         block_number = receipt_log.block_number
#         if event_name in self.pool_contracts_events_enum.burn.value:
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
#         if event_name in self.pool_contracts_events_enum.swap.value:
#             logs.debug(f"resolving swap from swap event ({event_name})")
#             swap: Swap = self.get_swap_from_event(base_pool, parsed_event, tokens_scalars)
#             logs.debug(f"resolved swap from swap event: {swap}")
#             swap.log_index = receipt_log.log_index
#             return {"swaps": [swap], "pools": [pool]}
#
#         if event_name == self.pool_contracts_events_enum.mint.value:
#             logs.debug("resolving mint from mint event")
#             mint: MintBurn | None = self.get_mint_burn_from_events(
#                 base_pool, parsed_event, tokens_scalars, transfers
#             )
#             logs.debug(f"resolved mint from mint event: {mint}")
#             if not (mint and pool):
#                 return None
#
#             mint.log_index = receipt_log.log_index
#             return {"mints": [mint], "pools": [pool]}
#
#         if event_name == self.pool_contracts_events_enum.burn.value:
#             logs.debug("resolving burn from burn event")
#             burn: MintBurn | None = self.get_mint_burn_from_events(
#                 base_pool, parsed_event, tokens_scalars, transfers
#             )
#             logs.debug(f"resolved burn from burn event: {burn}")
#             if not (burn and pool):
#                 return None
#
#             burn.log_index = receipt_log.log_index
#             return {"burns": [burn], "pools": [pool]}
#
#     def _get_base_token_address(self, pool_address: str) -> str:
#         for contract_name in self.pool_contract_names:
#             contract = self.abi[contract_name].contract
#             base_token_address = contract.functions._BASE_TOKEN_().call(
#                 {"to": pool_address}, "latest"
#             )
#             if base_token_address:
#                 return base_token_address
#
#     def get_swap_from_event(self, base_pool: BasePool, parsed_event, tokens_scalars) -> Swap:
#         amounts = [0, 0]
#
#         is_from_base_token = (
#             self._get_base_token_address(base_pool.address).lower() == parsed_event["fromToken"]
#         )
#         from_token_position = 0 if is_from_base_token else 1
#
#         for position in range(len(amounts)):
#             value = (
#                 parsed_event["fromAmount"]
#                 if position == from_token_position
#                 else parsed_event["toAmount"]
#             )
#             amounts[position] = value / tokens_scalars[position]
#
#         swap = Swap(
#             pool_address=base_pool.address,
#             sender=parsed_event["trader"],
#             to=parsed_event["trader"],
#             amounts=amounts,
#         )
#         return swap
#
#     def get_mint_burn_from_events(
#         self, base_pool, parsed_event, tokens_scalars, transfers: list[TransferBase]
#     ) -> MintBurn | None:
#         amounts = [0, 0]
#
#         tokens_addresses = [t.lower() for t in base_pool.tokens_addresses]
#
#         try:
#             target_transfers = self._filter_target_transfers_for_burn_mint(
#                 base_pool, parsed_event, tokens_addresses, transfers
#             )
#         except AssertionError as e:
#             logs.error(f"Failed to get target transfers for burn/mint: {e}")
#             return None
#
#         target_transfers_by_token = {t.token_address.lower(): t for t in target_transfers}
#
#         for idx, token_address in enumerate(tokens_addresses):
#             if token_address in target_transfers_by_token:
#                 amounts[idx] = parsed_event["value"] / tokens_scalars[idx]
#
#         mint_burn = MintBurn(
#             pool_address=base_pool.address,
#             sender=base_pool.address,
#             owner=parsed_event["user"],
#             amounts=amounts,
#             lp_token_address=base_pool.address,
#         )
#         return mint_burn
#
#     @staticmethod
#     def _filter_target_transfers_for_burn_mint(
#         base_pool,
#         parsed_event,
#         tokens_addresses: list[str],
#         transfers: list[TransferBase],
#     ) -> list[TransferBase]:
#         erc20_transfers = [t for t in transfers if t.type == TransferType.ERC20]
#         target_transfers = [
#             t
#             for t in erc20_transfers
#             if t.to_address.lower() == base_pool.address.lower()
#             and t.from_address.lower() == parsed_event["user"].lower()
#             and t.value == parsed_event["value"]
#             and t.token_address.lower() in tokens_addresses
#         ]
#         assert len(target_transfers) <= len(tokens_addresses), "Too many transfers found"
#         assert len(target_transfers) > 0, "No transfers found"
#         return target_transfers
