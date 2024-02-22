import logging
from enum import Enum

from eth_utils import is_address
from web3 import Web3
from web3.exceptions import BadFunctionCallOutput, ContractLogicError

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.domain.dex_trade import EthDexTrade
from ethereumetl.domain.receipt_log import ParsedReceiptLog
from ethereumetl.domain.token import EthToken
from ethereumetl.domain.token_transfer import EthTokenTransfer
from ethereumetl.service.dex.dodo.base import BaseDODOAmmClient
from ethereumetl.utils import get_prices_for_two_pool

logs = logging.getLogger(__name__)
to_checksum = Web3.to_checksum_address


class DODOv2TransactionType(Enum):
    swap = "DODOSwap"
    burn = "Burn"
    mint = "Mint"


class DODOv2Amm(BaseDODOAmmClient):
    """
    DODO V2 AMM client.

    Docs: <https://docs.dodoex.io/english/developers/contract-data/contract-event>.
    Other docs: <https://dodoex.github.io/docs/docs/>.
    Contracts: <https://docs.dodoex.io/english/developers/contracts-address/ethereum#dodo-v2>.

    This client parse DVM compatible (by ABI and events) contracts:
        - DVM (one token and standard pools, DODO Vending Machine),
        - DPP (DODO Private Pool),
        - DPPAdvanced,
        - DSP (fixed pool, DODO Stable Pool)

    # BSC examples:

    Tx with swap with 0 values: 0xe82e7b80ea4f668d3b1ac275163590f6956bab1e11515c26bcfb59efc39ce1c5.

    Event with one token (base) mint: 0x12153133e9d2e8a031208b6d714f80c1c62599c12f4850bad12a193715b0979d-62
        (DVM pool: 0xfb8c399f1187bdb5adf2007b32d5d76651b77ff4).

    Event with both tokens mint: 0x82dda2f78a888b48b6437bac478f668c25adf6c71c21bb1b27800fc7a3ca6f6d-76
        (DVM pool: 0x566ab0fd046c27c4ffaf2f87eda6bd15f4f4b3bd).

    DVM mint: topic0 = 0x0f6798a560793a54c3bcfe86a93cde1e73087d944c0ea20544137d4121396885.
    DSP swap: topic0 = 0xc2c0245e056d5fb095f04cd6373bc770802ebd1e6c918eb78fdef843cdb37b0f.

    Todo:
    ----
        - fix parsing https://bscscan.com/tx/0x9c5af9df97c9bcd3d4271267b4cbf0e55ade9a43c1d3f133a981e870e8f0ac6e
            - transfer from 0x0 to wallet, but we search transfer from wallet to pool

    """

    pool_contract_name = "DVM.json"
    amm_type = "dodo_v2"
    pool_contracts_events_enum = DODOv2TransactionType

    def is_pool_address_for_amm(self, pool_address: str) -> bool:
        try:
            fee_rate_model_address = self.pool_contract.functions._MT_FEE_RATE_MODEL_().call(
                {"to": to_checksum(pool_address)}, "latest"
            )
        except (
            TypeError,
            ContractLogicError,
            ValueError,
            BadFunctionCallOutput,
        ):
            fee_rate_model_address = False
        return True if is_address(fee_rate_model_address) else False

    def resolve_receipt_log(
        self,
        parsed_receipt_log: ParsedReceiptLog,
        dex_pool: EthDexPool,
        tokens_for_pool: list[EthToken],
        transfers_for_transaction: list[EthTokenTransfer],
    ) -> EthDexTrade | None:
        tokens_scalars = self._get_scalars_for_tokens(tokens_for_pool, dex_pool)
        parsed_receipt_log.parsed_event = self._get_normalized_event(
            parsed_receipt_log.event_name, parsed_receipt_log.parsed_event
        )
        parsed_event, event_name = parsed_receipt_log.parsed_event, parsed_receipt_log.event_name
        if not (parsed_event and event_name):
            return None
        finance_info = self.get_pool_finances_from_event(
            dex_pool, parsed_receipt_log, tokens_scalars
        )

        if event_name in self.pool_contracts_events_enum.swap.value:
            logs.debug(f"resolving swap from swap event ({event_name})")
            amounts = self.get_swap_from_event(dex_pool, parsed_event, tokens_scalars)
            return EthDexTrade(
                pool_address=dex_pool.address,
                token_amounts=amounts,
                transaction_hash=parsed_receipt_log.transaction_hash,
                log_index=parsed_receipt_log.log_index,
                block_number=parsed_receipt_log.block_number,
                event_type='swap',
                token_reserves=[finance_info['reserve_0'], finance_info['reserve_1']],
                token_prices=get_prices_for_two_pool(
                    finance_info['price_0'], finance_info['price_1']
                ),
                token_addresses=dex_pool.token_addresses,
            )

        if event_name == self.pool_contracts_events_enum.mint.value:
            logs.debug("resolving mint from mint event")
            mint = self.get_mint_burn_from_events(
                dex_pool,
                parsed_receipt_log,
                tokens_scalars,
                transfers_for_transaction,
                finance_info,
            )
            logs.debug(f"resolved mint from mint event: {mint}")
            return mint

        if event_name == self.pool_contracts_events_enum.burn.value:
            logs.debug("resolving burn from burn event")
            burn = self.get_mint_burn_from_events(
                dex_pool,
                parsed_receipt_log,
                tokens_scalars,
                transfers_for_transaction,
                finance_info,
            )
            logs.debug(f"resolved burn from burn event: {burn}")
            return burn

    def _get_base_token_address(self, pool_address: str) -> str:
        base_token_address = self.pool_contract.functions._BASE_TOKEN_().call(
            {"to": to_checksum(pool_address)}, "latest"
        )
        return base_token_address

    @staticmethod
    def get_swap_from_event(
        base_pool: EthDexPool, parsed_event: dict, tokens_scalars: list[int]
    ) -> list[float]:
        amounts = [0.0, 0.0]
        token_0_index = int(base_pool.token_addresses[0] == parsed_event["fromToken"])
        token_1_index = 1 - token_0_index
        amounts[token_0_index] = parsed_event["fromAmount"] / tokens_scalars[token_0_index]
        amounts[token_1_index] = -parsed_event["toAmount"] / tokens_scalars[token_1_index]

        return amounts

    def get_mint_burn_from_events(
        self,
        base_pool: EthDexPool,
        parsed_receipt_log: ParsedReceiptLog,
        tokens_scalars: list[int],
        transfers: list[EthTokenTransfer],
        finance_info: dict,
    ) -> EthDexTrade | None:
        amounts = [0.0, 0.0]
        parsed_event = parsed_receipt_log.parsed_event
        tokens_addresses = [t.lower() for t in base_pool.token_addresses]

        try:
            target_transfers = self._filter_target_transfers_for_burn_mint(
                base_pool, parsed_event, tokens_addresses, transfers
            )
        except AssertionError as e:
            logs.error(f"Failed to get target transfers for burn/mint: {e}")
            return None

        target_transfers_by_token = {t.token_address.lower(): t for t in target_transfers}

        for idx, token_address in enumerate(tokens_addresses):
            amounts[idx] = target_transfers_by_token[token_address].value / tokens_scalars[idx]

        lp_token_address = self.get_lp_token_address_for_pool(base_pool.address)

        mint_burn = EthDexTrade(
            pool_address=base_pool.address,
            token_amounts=amounts,
            transaction_hash=parsed_receipt_log.transaction_hash,
            log_index=parsed_receipt_log.log_index,
            block_number=parsed_receipt_log.block_number,
            event_type='mint' if parsed_receipt_log.event_name == "Mint" else 'burn',
            token_reserves=[finance_info['reserve_0'], finance_info['reserve_1']],
            token_prices=get_prices_for_two_pool(finance_info['price_0'], finance_info['price_1']),
            token_addresses=base_pool.token_addresses,
            lp_token_address=lp_token_address,
        )
        return mint_burn

    @staticmethod
    def _filter_target_transfers_for_burn_mint(
        base_pool,
        parsed_event,
        tokens_addresses: list[str],
        transfers: list[EthTokenTransfer],
    ) -> list[EthTokenTransfer]:
        target_transfers = [
            t
            for t in transfers
            if (
                t.to_address.lower() == base_pool.address.lower()
                and t.from_address.lower() == parsed_event["user"].lower()
                and t.value == parsed_event["value"]
                and t.token_address.lower() in tokens_addresses
            )
            or (
                t.from_address.lower() == base_pool.address.lower()
                and t.to_address.lower() == parsed_event["user"].lower()
                and t.token_address.lower() in tokens_addresses
            )
        ]
        assert len(target_transfers) <= len(tokens_addresses) + 1, "Too many transfers found"
        assert len(target_transfers) > 0, "No transfers found"
        return target_transfers
