import logging
from enum import Enum
from functools import cache
from pathlib import Path
from typing import Literal

from eth_typing import ChecksumAddress
from web3 import Web3
from web3.exceptions import BadFunctionCallOutput, ContractLogicError

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.domain.dex_trade import EthDexTrade
from ethereumetl.domain.receipt_log import ParsedReceiptLog
from ethereumetl.domain.token import EthToken
from ethereumetl.domain.token_transfer import EthTokenTransfer, TokenStandard
from ethereumetl.misc.info import NULL_ADDRESSES
from ethereumetl.service.dex.base.base_dex_client import BaseDexClient
from ethereumetl.utils import get_default_zero_prices, get_prices_for_two_pool

POOL_CONTRACT = "Pool"
LP_CONTRACT = "LPToken"
FACTORY_CONTRACT = "MasterWombat"
logger = logging.getLogger(__name__)
to_checksum = Web3.to_checksum_address


class WombatTransactionsTypes(Enum):
    swap = "swap"
    transfer = "transfer"
    sync = "sync"
    deposit = "deposit"
    withdraw = "withdraw"


class WombatAmm(BaseDexClient):
    pool_contract_names = (POOL_CONTRACT,)
    pool_contracts_events_enum = WombatTransactionsTypes

    def __init__(self, web3: Web3, chain_id: int, file_path: str = __file__):
        super().__init__(web3, chain_id, file_path)
        _path_to_abi = Path(file_path).parent
        pool_abi_path = _path_to_abi / "Pool.json"
        lp_abi_path = _path_to_abi / "LPToken.json"
        self._pool_contract = self._initiate_contract(pool_abi_path)
        self._lp_contract = self._initiate_contract(lp_abi_path)

    def resolve_asset_from_log(self, parsed_log: ParsedReceiptLog) -> EthDexPool | None:
        checksum_address = to_checksum(parsed_log.address)
        factory_address = self._get_factory_address(checksum_address)
        if not factory_address:
            return None
        token_addresses = self._get_token_addresses(checksum_address)
        if not token_addresses:
            return None
        lp_tokens = self._get_lp_tokens(checksum_address, token_addresses)
        if not lp_tokens:
            return None
        return EthDexPool(
            address=parsed_log.address,
            factory_address=factory_address.lower(),
            token_addresses=[token.lower() for token in token_addresses],
            lp_token_addresses=[lp_token.lower() for lp_token in lp_tokens],
            fee=0,
            underlying_token_addresses=[lp_token.lower() for lp_token in lp_tokens],
        )

    def _get_factory_address(self, pool_address: ChecksumAddress) -> ChecksumAddress | None:
        try:
            return self._pool_contract.functions.masterWombat().call({"to": pool_address})
        except Exception as e:
            logger.debug(f"Error while getting factory address for pool {pool_address}: {e}")
            return None

    def _get_token_addresses(self, pool_address: ChecksumAddress) -> list[ChecksumAddress] | None:
        try:
            return self._pool_contract.functions.getTokens().call({"to": pool_address})
        except Exception as e:
            logger.debug(f"Error while getting tokens for pool {pool_address}: {e}")
            return None

    def _get_lp_tokens(
        self, pool_address: ChecksumAddress, token_addresses: list[ChecksumAddress]
    ) -> list[ChecksumAddress] | None:
        """Wombat has LP token for each token in pool."""
        lp_tokens = []
        for token_address in token_addresses:
            try:
                lp_tokens.append(
                    self._pool_contract.functions.addressOfAsset(token_address).call(
                        {"to": pool_address}, "latest"
                    )
                )
            except Exception as e:
                logger.debug(f"Error while getting lp token for pool {pool_address}: {e}")
                return None
        return lp_tokens

    def resolve_receipt_log(
        self,
        parsed_receipt_log: ParsedReceiptLog,
        dex_pool: EthDexPool,
        tokens_for_pool: list[EthToken],
        transfers_for_transaction: list[EthTokenTransfer],
    ) -> EthDexTrade | None:
        event_name = parsed_receipt_log.event_name

        tokens_scalars = {}
        for erc20_token in tokens_for_pool:
            tokens_scalars[erc20_token.address.lower()] = 10**erc20_token.decimals
        if event_name.lower() == self.pool_contracts_events_enum.swap.name:
            logger.debug("resolving swap from swap event")
            swap = self._get_trade_from_swap_event(dex_pool, parsed_receipt_log, tokens_scalars)
            return swap

        if event_name.lower() == self.pool_contracts_events_enum.withdraw.name:
            logger.debug("resolving burn from burn event")
            burn = self._get_mint_burn_from_events(
                dex_pool, parsed_receipt_log, tokens_scalars, transfers_for_transaction
            )
            logger.debug("resolving burn from burn event")
            return burn

        if event_name.lower() == self.pool_contracts_events_enum.deposit.name:
            logger.debug("resolving burn from mint event")
            mint = self._get_mint_burn_from_events(
                dex_pool, parsed_receipt_log, tokens_scalars, transfers_for_transaction
            )
            logger.debug("resolving burn from mint event")
            return mint

    def _get_finance_info(
        self,
        base_pool: EthDexPool,
        tokens_scalars: dict,
        block_number: int | Literal['latest'] = "latest",
    ) -> dict:
        # get liability for each lp token in pool
        reserves = []
        liabilities = {}
        cashes = {}
        # all lp decimals should be same for one pool.
        decimals = self._get_decimals(base_pool.underlying_token_addresses[0])
        for i, token in enumerate(base_pool.underlying_token_addresses):
            token = Web3.toChecksumAddress(token)
            try:
                # liability is liquidity for wombat
                liability = self._lp_contract.functions.liability().call(
                    {"to": token}, block_number
                )
                cash = self._lp_contract.functions.cash().call({"to": token}, block_number)
                reserves.append(liability / (10**decimals))
                liabilities[token.lower()] = liability / (10**decimals)
                cashes[token.lower()] = cash / tokens_scalars[base_pool.token_addresses[i].lower()]
            except Exception as e:
                logger.debug(f"Error while getting liability for pool {base_pool.address}: {e}")
                return {
                    'reserves': [0 for _ in range(len(base_pool.token_addresses))],
                    'prices': get_default_zero_prices(len(base_pool.token_addresses)),
                }
        prices = self._get_pool_prices(
            base_pool.address, decimals, liabilities, cashes, block_number
        )
        return {
            'reserves': reserves,
            'prices': prices,
        }

    @cache
    def _get_decimals(self, token_address: str) -> int:
        try:
            return self.erc20_contract_abi.functions.decimals().call(
                {"to": to_checksum(token_address)},
                block_identifier="latest",
            )
        except (ValueError, TypeError, BadFunctionCallOutput, ContractLogicError):
            return 0

    @cache
    def _amp_factor(
        self, pool_address: ChecksumAddress, block_identifier: Literal['latest'] | int = "latest"
    ) -> int | None:
        try:
            return self._pool_contract.functions.ampFactor().call(
                {"to": pool_address}, block_identifier
            )
        except (ValueError, TypeError, BadFunctionCallOutput, ContractLogicError):
            return None

    def _get_pool_prices(
        self,
        pool_address: str,
        decimals: int,
        liabilities: dict,
        cashes: dict,
        block_identifier: str | int = "latest",
    ) -> list:
        """
        Get pool prices.

        Prices calculates from exchange rate:

                        1 + (A / rx ** 2)
        exchange_rate = -------------------
                        1 + (A / ry ** 2)

        A is amplification coefficient for pool
        rx|ry is coverage ratio

        r = Cash / Liability
        """
        prices = get_default_zero_prices(len(liabilities))
        amp = self._amp_factor(to_checksum(pool_address), block_identifier)
        if not amp:
            return prices
        amp = amp / 10**decimals
        ratios = []
        for cash, liability in zip(cashes.values(), liabilities.values()):
            ratios.append(cash / liability)
        for i in range(len(ratios)):
            for j in range(len(ratios)):
                if i != j:
                    prices[i][j] = (1 + (amp / ratios[j] ** 2)) / (1 + (amp / ratios[i] ** 2))
        return prices

    def _get_trade_from_swap_event(
        self, base_pool: EthDexPool, parsed_receipt_log: ParsedReceiptLog, tokens_scalars: dict
    ) -> EthDexTrade | None:
        parsed_event = parsed_receipt_log.parsed_event
        amount0 = parsed_event["fromAmount"] / tokens_scalars[parsed_event["fromToken"].lower()]
        amount1 = -parsed_event["toAmount"] / tokens_scalars[parsed_event["toToken"].lower()]
        finance_info = self._get_finance_info(
            base_pool, tokens_scalars, parsed_receipt_log.block_number - 1
        )
        involved_token_indices = [
            base_pool.token_addresses.index(parsed_event["fromToken"].lower()),
            base_pool.token_addresses.index(parsed_event["toToken"].lower()),
        ]
        reserves = [finance_info["reserves"][i] for i in involved_token_indices]
        prices = [
            finance_info["prices"][involved_token_indices[0]][involved_token_indices[1]],
            finance_info["prices"][involved_token_indices[1]][involved_token_indices[0]],
        ]
        prices = get_prices_for_two_pool(prices[0], prices[1])
        return EthDexTrade(
            token_amounts=[amount0, amount1],
            pool_address=base_pool.address,
            transaction_hash=parsed_receipt_log.transaction_hash,
            log_index=parsed_receipt_log.log_index,
            block_number=parsed_receipt_log.block_number,
            event_type='swap',
            token_addresses=[parsed_event["fromToken"], parsed_event["toToken"]],
            token_reserves=reserves,
            token_prices=prices,
        )

    def _get_mint_burn_from_events(
        self,
        base_pool: EthDexPool,
        parsed_receipt_log: ParsedReceiptLog,
        tokens_scalars: dict,
        transfers: list[EthTokenTransfer],
    ):
        lp_address = None
        parsed_event = parsed_receipt_log.parsed_event
        # user can burn BUSD LP token and get HAY token
        if parsed_event["event_type"] == self.pool_contracts_events_enum.withdraw.name:
            burned_tokens = set()
            u_tokens = [token.lower() for token in base_pool.underlying_token_addresses]
            for transfer in transfers:
                if transfer.token_standard != TokenStandard.ERC20:
                    continue
                if (
                    transfer.token_address.lower() in u_tokens
                    and transfer.to_address in NULL_ADDRESSES
                ):
                    burned_tokens.add(transfer.token_address.lower())
            if len(burned_tokens) == 1:
                lp_address = burned_tokens.pop()
        tokens = [token.lower() for token in base_pool.token_addresses]
        amounts = [0.0 for _ in range(len(base_pool.token_addresses))]
        coin_index = tokens.index(parsed_event["token"].lower())
        # amount should be calculated in token decimals
        amount = parsed_event["amount"] / tokens_scalars[parsed_event["token"].lower()]
        amounts[coin_index] = amount

        finance_info = self._get_finance_info(
            base_pool, tokens_scalars, parsed_receipt_log.block_number - 1
        )
        return EthDexTrade(
            token_amounts=amounts,
            pool_address=base_pool.address,
            transaction_hash=parsed_receipt_log.transaction_hash,
            log_index=parsed_receipt_log.log_index,
            block_number=parsed_receipt_log.block_number,
            event_type=parsed_event["event_type"].lower(),
            token_reserves=finance_info["reserves"],
            token_prices=finance_info["prices"],
            lp_token_address=lp_address
            or base_pool.underlying_token_addresses[coin_index].lower(),
            token_addresses=base_pool.token_addresses,
        )
