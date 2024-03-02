import json
import logging
from pathlib import Path

from eth_typing import ChecksumAddress
from web3 import Web3

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.domain.dex_trade import EthDexTrade
from ethereumetl.domain.receipt_log import ParsedReceiptLog
from ethereumetl.domain.token import EthToken
from ethereumetl.domain.token_transfer import EthTokenTransfer
from ethereumetl.service.dex.base.base_dex_client import BaseDexClient
from ethereumetl.utils import get_prices_for_two_pool

logger = logging.getLogger(__name__)
to_checksum = Web3.to_checksum_address


class TraderJoeV21Amm(BaseDexClient):
    swaps_event_name = "swap"
    mints_event_names = ("mint", "depositedtobins")
    burns_event_names = ("burn", "withdrawnfrombins")
    POOL_ABI_PATH = "LBPair.json"

    def __init__(self, web3: Web3, chain_id: int, path_to_file: str = __file__):
        super().__init__(web3, chain_id, path_to_file)
        self.pool_contract = self._w3.eth.contract(
            abi=json.loads((Path(__file__).parent / self.POOL_ABI_PATH).read_text())
        )

    def _get_factory_address(self, address: ChecksumAddress) -> ChecksumAddress | None:
        try:
            factory = self.pool_contract.functions.getFactory().call(
                {"to": to_checksum(address)}, "latest"
            )
        except Exception as e:
            logger.error(f"Error while getting factory for pool {address}: {e}")
            return None
        return factory

    def resolve_asset_from_log(self, parsed_log: ParsedReceiptLog) -> EthDexPool | None:
        checksum_address = to_checksum(parsed_log.address)
        factory = self._get_factory_address(checksum_address)
        if not factory:
            return None
        token_addresses = self._get_token_addresses(checksum_address)
        if not token_addresses:
            return None
        return EthDexPool(
            address=parsed_log.address,
            factory_address=factory.lower(),
            token_addresses=[token.lower() for token in token_addresses],
            fee=0,
            lp_token_addresses=[parsed_log.address],
        )

    def _get_token_addresses(self, pool_address: ChecksumAddress) -> list[ChecksumAddress] | None:
        try:
            token_0 = self.pool_contract.functions.getTokenX().call({"to": pool_address}, "latest")
            token_1 = self.pool_contract.functions.getTokenY().call({"to": pool_address}, "latest")
        except Exception as e:
            logger.error(f"Error while getting tokens for pool {pool_address}: {e}")
            return None
        return [token_0, token_1]

    @staticmethod
    def decode_amounts(amounts: bytes) -> tuple:
        """
        Decodes the amounts bytes input as 2 integers.

        :param amounts: amounts to decode.
        :return: tuple of ints with the values decoded.
        """
        # Read the right 128 bits of the 256 bits
        amounts_x = int.from_bytes(amounts, byteorder="big") & (2**128 - 1)

        # Read the left 128 bits of the 256 bits
        amounts_y = int.from_bytes(amounts, byteorder="big") >> 128

        return amounts_x, amounts_y

    def resolve_receipt_log(
        self,
        parsed_receipt_log: ParsedReceiptLog,
        dex_pool: EthDexPool,
        tokens_for_pool: list[EthToken],
        transfers_for_transaction: list[EthTokenTransfer],
    ) -> EthDexTrade | None:
        event_name = parsed_receipt_log.event_name

        tokens_scalars = self._get_scalars_for_tokens(tokens_for_pool, dex_pool)
        parsed_event = self.normalize_event(
            self._get_events_abi(self.pool_contract, event_name)['inputs'],
            parsed_receipt_log.parsed_event,
        )
        finance_info = self._get_pool_finances(
            dex_pool, parsed_receipt_log.block_number - 1, tokens_scalars, parsed_event
        )
        if event_name.lower() == self.swaps_event_name:
            logger.debug("resolving swap from swap event")
            amounts = self._get_amounts_from_swap_event(parsed_event, tokens_scalars)
            return EthDexTrade(
                token_amounts=amounts,
                pool_address=dex_pool.address,
                transaction_hash=parsed_receipt_log.transaction_hash,
                log_index=parsed_receipt_log.log_index,
                block_number=parsed_receipt_log.block_number,
                event_type="swap",
                token_reserves=[finance_info["reserve_0"], finance_info["reserve_1"]],
                token_prices=get_prices_for_two_pool(
                    finance_info["price_0"], finance_info["price_1"]
                ),
                token_addresses=dex_pool.token_addresses,
            )

        if event_name.lower() in self.burns_event_names:
            logger.debug("resolving burn from burn event")
            amounts = self._get_amounts_from_mint_burn_events(parsed_event, tokens_scalars)
            return EthDexTrade(
                token_amounts=amounts,
                pool_address=dex_pool.address,
                transaction_hash=parsed_receipt_log.transaction_hash,
                log_index=parsed_receipt_log.log_index,
                block_number=parsed_receipt_log.block_number,
                event_type="burn",
                token_reserves=[finance_info["reserve_0"], finance_info["reserve_1"]],
                token_prices=get_prices_for_two_pool(
                    finance_info["price_0"], finance_info["price_1"]
                ),
                token_addresses=dex_pool.token_addresses,
                lp_token_address=dex_pool.lp_token_addresses[0],
            )

        if event_name.lower() in self.mints_event_names:
            logger.debug("resolving burn from mint event")
            amounts = self._get_amounts_from_mint_burn_events(parsed_event, tokens_scalars)
            return EthDexTrade(
                token_amounts=amounts,
                pool_address=dex_pool.address,
                transaction_hash=parsed_receipt_log.transaction_hash,
                log_index=parsed_receipt_log.log_index,
                block_number=parsed_receipt_log.block_number,
                event_type="mint",
                token_reserves=[finance_info["reserve_0"], finance_info["reserve_1"]],
                token_prices=get_prices_for_two_pool(
                    finance_info["price_0"], finance_info["price_1"]
                ),
                token_addresses=dex_pool.token_addresses,
                lp_token_address=dex_pool.lp_token_addresses[0],
            )

    def _get_amounts_from_swap_event(self, parsed_event: dict, tokens_scalars: list[int]):
        amounts_in = self.decode_amounts(parsed_event["amountsIn"])
        amounts_out = self.decode_amounts(parsed_event["amountsOut"])

        amount0 = amounts_in[0] / tokens_scalars[0] - amounts_out[0] / tokens_scalars[0]
        amount1 = amounts_in[1] / tokens_scalars[1] - amounts_out[1] / tokens_scalars[1]
        return [amount0, amount1]

    def _get_pool_finances(
        self,
        base_pool: EthDexPool,
        block_number: int,
        tokens_scalars: list[int],
        parsed_event: dict,
    ) -> dict:
        checksum_address = to_checksum(base_pool.address)
        default_finance_info = {
            "reserve_0": 0,
            "reserve_1": 0,
            "price_0": 0,
            "price_1": 0,
        }
        try:
            reserves = self.pool_contract.functions.getReserves().call(
                {"to": checksum_address, "block_identifier": block_number}  # type: ignore
            )
        except Exception as e:
            logger.error(f"Error while getting reserves for pool {base_pool.address}: {e}")
            return default_finance_info
        try:
            bin_step = self.pool_contract.functions.getBinStep().call({"to": checksum_address})
        except Exception as e:
            logger.error(f"Error while getting bin step for pool {base_pool.address}: {e}")
            return default_finance_info

        if parsed_event.get("id"):
            active_bin = parsed_event["id"]
        else:
            try:
                active_bin = self.pool_contract.functions.getActiveId().call(
                    {"to": checksum_address, "block_identifier": block_number}  # type: ignore
                )
            except Exception as e:
                logger.error(f"Error while getting active bin for pool {base_pool.address}: {e}")
                return default_finance_info

        token_scalars_diff = tokens_scalars[1] / tokens_scalars[0]

        # https://docs.traderjoexyz.com/guides/price-from-id
        price0 = ((1 + bin_step / 10_000) ** (active_bin - 8388608)) / token_scalars_diff
        price1 = 1 / price0
        finance_info = {
            "reserve_0": reserves[0] / tokens_scalars[0],
            "reserve_1": reserves[1] / tokens_scalars[1],
            "price_0": price0,
            "price_1": price1,
        }
        return finance_info

    def _get_amounts_from_mint_burn_events(self, parsed_event: dict, tokens_scalars: list[int]):
        amounts = parsed_event["amounts"]
        amount0, amount1 = 0.0, 0.0
        for amount in amounts:
            amount0_, amount1_ = self.decode_amounts(amount)
            amount0 += amount0_
            amount1 += amount1_
        amount0 /= tokens_scalars[0]
        amount1 /= tokens_scalars[1]
        return [amount0, amount1]
