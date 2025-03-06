from enum import Enum
import json
import logging
from pathlib import Path
from functools import lru_cache

from eth_typing import ChecksumAddress
from web3 import Web3
from web3.exceptions import BadFunctionCallOutput, ContractLogicError

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.domain.dex_trade import EthDexTrade
from ethereumetl.domain.receipt_log import ParsedReceiptLog
from ethereumetl.domain.token import EthToken
from ethereumetl.domain.token_transfer import EthTokenTransfer
from ethereumetl.service.dex.base.base_dex_client import BaseDexClient
from ethereumetl.utils import get_prices_for_two_pool


to_checksum = Web3.to_checksum_address
logs = logging.getLogger(__name__)


class CarbonTransactionType(Enum):
    tokenstraded = "TokensTraded"


class CarbonDeFiAmm(BaseDexClient):
    def __init__(self, web3: Web3, chain_id: int | None = None, file_path: str = __file__):
        super().__init__(web3, chain_id, file_path)
        carbon_controller_abi_path = Path(file_path).parent / "CarbonController.json"
        self.carbon_controller_abi = self._initiate_contract(abi_path=carbon_controller_abi_path)

        with open(Path(__file__).parent / "deploys" / str(chain_id) / "metadata.json") as f:
            metadata = json.load(f)
            self.CARBON_CONTROLLER = metadata[0]["contracts"]["CarbonController"]

    @lru_cache(maxsize=128)
    def _get_pair_fee_ppm(self, token0: ChecksumAddress, token1: ChecksumAddress) -> int | None:
        try:
            pair_fee_ppm = self.carbon_controller_abi.functions.pairTradingFeePPM(
                to_checksum(token0), to_checksum(token1)
            ).call({"to": self.CARBON_CONTROLLER}, "latest")
        except (ValueError, TypeError, BadFunctionCallOutput, ContractLogicError):
            return None
        return int(pair_fee_ppm)

    @lru_cache(maxsize=128)
    def _get_token_reserves(
        self, token0: ChecksumAddress, token1: ChecksumAddress
    ) -> list[float] | None:
        try:
            token0_reserve = self._get_balance_of(token0, self.CARBON_CONTROLLER, 'latest')
            token1_reserve = self._get_balance_of(token1, self.CARBON_CONTROLLER, 'latest')
        except (ValueError, TypeError, BadFunctionCallOutput, ContractLogicError):
            return None
        return [token0_reserve, token1_reserve]

    def resolve_asset_from_log(self, parsed_log: ParsedReceiptLog) -> EthDexPool | None:
        event_name = parsed_log.event_name
        parsed_event = parsed_log.parsed_event

        if event_name.lower() == CarbonTransactionType.tokenstraded.name:
            try:
                token_in = parsed_event.get('sourceToken')
                token_out = parsed_event.get('targetToken')
                fee = self._get_pair_fee_ppm(
                    parsed_log.parsed_event.token0,
                    parsed_log.parsed_event.token1,
                )

                if not token_in or not token_out or not fee:
                    return None

                return EthDexPool(
                    address=self.CARBON_CONTROLLER,
                    token_addresses=[token_in, token_out],
                    fee=fee,
                )
            except (ValueError, TypeError, KeyError) as e:
                logs.debug(f"Resolved asset not found for ${parsed_event}: {e}")
            return None

    def resolve_receipt_log(
        self,
        parsed_receipt_log: ParsedReceiptLog,
        dex_pool: EthDexPool | None = None,
        tokens_for_pool: list[EthToken] | None = None,
        transfers_for_transaction: list[EthTokenTransfer] | None = None,
    ) -> EthDexTrade | None:
        event_name = parsed_receipt_log.event_name
        parsed_event = parsed_receipt_log.parsed_event

        if event_name.lower() == CarbonTransactionType.tokenstraded.name:
            try:
                source_token = parsed_event.get('sourceToken')
                target_token = parsed_event.get('targetToken')
                if not source_token or not target_token:
                    return None

                source_amount = parsed_event.get('sourceAmount')
                target_amount = parsed_event.get('targetAmount')
                if not source_amount or not target_amount:
                    return None

                token_scalars = self._get_scalars_for_tokens(tokens_for_pool, dex_pool)

                token_reserves = self._get_token_reserves(source_token, target_token)
                if not token_reserves:
                    return None

                amount_in = source_amount / token_scalars[0]
                amount_out = target_amount / token_scalars[1]
                prices = [
                    abs(amount_out / amount_in),
                    abs(amount_in / amount_out),
                ]

                return EthDexTrade(
                    pool_address=self.CARBON_CONTROLLER,
                    token_amounts=[
                        amount_in,
                        amount_out,
                    ],
                    transaction_hash=parsed_receipt_log.transaction_hash,
                    log_index=parsed_receipt_log.log_index,
                    block_number=parsed_receipt_log.block_number,
                    event_type='swap',
                    token_reserves=token_reserves,
                    token_prices=get_prices_for_two_pool(prices[0], prices[1]),
                    token_addresses=dex_pool.token_addresses,
                )
            except (ValueError, TypeError, KeyError) as e:
                logs.debug(f"Resolved log not found for ${parsed_receipt_log}: {e}")

            return None
