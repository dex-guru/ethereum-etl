import json
import logging
from enum import Enum
from functools import cache
from pathlib import Path
from typing import Literal

from eth_typing import ChecksumAddress
from web3 import Web3
from web3.contract import Contract
from web3.exceptions import ABIFunctionNotFound, BadFunctionCallOutput, ContractLogicError

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.domain.dex_trade import EthDexTrade
from ethereumetl.domain.receipt_log import ParsedReceiptLog
from ethereumetl.domain.token import EthToken
from ethereumetl.domain.token_transfer import EthTokenTransfer
from ethereumetl.misc.info import NULL_ADDRESSES, get_chain_config
from ethereumetl.service.dex.base.base_dex_client import BaseDexClient
from ethereumetl.service.dex.enums import DexPoolFeeAmount
from ethereumetl.utils import get_default_prices, get_default_zero_prices, get_prices_for_two_pool

logger = logging.getLogger(__name__)

ADDRESS_PROVIDER_CONTRACT = "AddressProvider"
FACTORY_CONTRACT = "Registry"
POOL_CONTRACT_V1 = "CurvePoolv1"
POOL_CONTRACT_V7 = "CurvePoolv7"
POOL_CONTRACT_V8 = "CurvePoolv8"
EPSILON = 10**-8  # 0.00000001

to_checksum = Web3.to_checksum_address


class CurveTransactionType(Enum):
    TokenExchange = "TokenExchange"
    TokenExchangeUnderlying = "TokenExchangeUnderlying"  # swap for underlying coins
    AddLiquidity = "AddLiquidity"  # mint
    RemoveLiquidity = "RemoveLiquidity"  # burn for many coins
    RemoveLiquidityOne = "RemoveLiquidityOne"
    RemoveLiquidityImbalance = "RemoveLiquidityImbalance"  # burn for many coins

    @classmethod
    def burn_events(cls) -> list:
        return [
            cls.RemoveLiquidityOne.name,
            cls.RemoveLiquidityImbalance.name,
            cls.RemoveLiquidity.name,
        ]


class CurveAmm(BaseDexClient):
    pool_contract_names = [f"CurvePoolv{number}" for number in range(1, 9)]
    _pool_contract_abi_mapper = {
        f"CurvePoolv{number}": f"CurvePoolv{number}.json" for number in range(1, 9)
    }
    pool_contracts_events_enum = CurveTransactionType
    ADDRESS_PROVIDER_FILE_NAME = "AddressProvider.json"

    def __init__(self, web3: Web3, chain_id: int | None = None, file_path: str = __file__):
        super().__init__(web3, chain_id, file_path)
        self.abi: dict[str, type[Contract]] = {}
        self.chain_id = chain_id
        self.native_token: dict = get_chain_config(chain_id)['native_token']
        self.eee = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
        metadata = self._load_json(
            Path(file_path).parent / 'deploys' / str(self.chain_id) / 'metadata.json'
        )
        self._setup_info_contracts(file_path, metadata)
        self._setup_registry_contracts(file_path, metadata)

    def _setup_info_contracts(self, file_path: str, metadata: list[dict]):
        _path_to_abi = Path(file_path).parent

        for name, file in self._pool_contract_abi_mapper.items():
            pool_abi_path = _path_to_abi / file
            self.abi[name] = self._w3.eth.contract(abi=self._load_json(pool_abi_path))

        address_provider_address = metadata[0]['contracts']['AddressProvider']
        self.address_provider_contract = self._w3.eth.contract(
            address=address_provider_address,
            abi=self._load_json(_path_to_abi / self.ADDRESS_PROVIDER_FILE_NAME),
        )

        pool_info_abi = self._load_json(_path_to_abi / "PoolInfo.json")
        # pool info contract is always on first position
        pool_info_address = self.address_provider_contract.functions.get_id_info(1).call()[0]
        self.pool_info_contract = self._w3.eth.contract(
            address=pool_info_address, abi=pool_info_abi
        )

    @staticmethod
    def _load_json(path: Path) -> list[dict]:
        with open(path) as f:
            return json.load(f)

    def _setup_registry_contracts(self, file_path: str, metadata: list[dict]):
        path = Path(file_path).parent
        registry_abi_path = path / "CurveRegistry.json"
        self.registry_count = self.address_provider_contract.functions.max_id().call()
        self.registry_contract_names = []
        registry_id = 0
        for registry_id in range(1, self.registry_count + 1):
            registry_address = self.address_provider_contract.functions.get_address(
                registry_id,
            ).call()
            if (
                not registry_address
                or registry_address == "0x0000000000000000000000000000000000000000"
            ):
                continue

            self.abi[FACTORY_CONTRACT + f"v_{registry_id}"] = self._w3.eth.contract(
                address=registry_address, abi=self._load_json(registry_abi_path)
            )
            self.registry_contract_names.append(FACTORY_CONTRACT + f"v_{registry_id}")

        for dex in metadata:
            for name, address in dex['contracts'].items():
                if name == 'AddressProvider':
                    continue
                if name not in self.abi:
                    registry_id += 1
                    address = to_checksum(address)
                    self.abi[FACTORY_CONTRACT + f'v_{registry_id}'] = self._w3.eth.contract(
                        address=address, abi=self._load_json(registry_abi_path)
                    )
                    self.registry_contract_names.append(FACTORY_CONTRACT + f'v_{registry_id}')

    def resolve_asset_from_log(self, parsed_log: ParsedReceiptLog) -> EthDexPool | None:
        address = parsed_log.address
        checksum_address = to_checksum(address)
        factory_address = self._get_factory_address(checksum_address)
        if not factory_address:
            return None
        token_addresses, underlying_addresses = self._get_tokens_addresses_for_pool(
            checksum_address
        )
        if not token_addresses:
            return None
        lp_token_addresses = self._get_lp_token_address_for_pool(checksum_address)
        if not lp_token_addresses:
            lp_token_addresses = [address]

        return EthDexPool(
            address=address,
            factory_address=factory_address.lower(),
            token_addresses=token_addresses,
            underlying_token_addresses=underlying_addresses,
            lp_token_addresses=[lp.lower() for lp in lp_token_addresses],
            fee=DexPoolFeeAmount.UNDEFINED.value,
        )

    def _get_fee_for_pool(self, address: ChecksumAddress) -> int:
        try:
            return self.abi[POOL_CONTRACT_V1].functions.fee().call({"to": address}, "latest")
        except (ValueError, TypeError, BadFunctionCallOutput, ContractLogicError):
            return 0

    @cache
    def _get_factory_address(self, address: ChecksumAddress) -> str | None:
        try:
            return self.abi[POOL_CONTRACT_V7].functions.factory().call({"to": address}, "latest")
        except (ValueError, TypeError, BadFunctionCallOutput, ContractLogicError):
            pass
        for contract_name in self.registry_contract_names:
            contract = self.abi[contract_name]
            try:
                coins = contract.functions.get_coins(address).call()
            except (
                ValueError,
                TypeError,
                BadFunctionCallOutput,
                ContractLogicError,
                ABIFunctionNotFound,
            ):
                continue
            coins = [coin for coin in coins if coin not in NULL_ADDRESSES]
            if coins:
                return contract.address
        try:
            admin = self.abi[POOL_CONTRACT_V8].functions.admin().call({"to": address}, "latest")
            if admin:
                return self.abi[POOL_CONTRACT_V7].functions.factory().call({"to": admin}, "latest")
        except (ValueError, TypeError, BadFunctionCallOutput, ContractLogicError):
            return None

    def resolve_receipt_log(
        self,
        parsed_receipt_log: ParsedReceiptLog,
        dex_pool: EthDexPool,
        tokens_for_pool: list[EthToken],
        transfers_for_transaction: list[EthTokenTransfer],
    ) -> EthDexTrade | None:
        event_name = parsed_receipt_log.event_name

        if not event_name:
            return None
        tokens_scalars = self._get_scalars_for_tokens(tokens_for_pool, dex_pool)

        parsed_event = parsed_receipt_log.parsed_event

        parsed_event = self.convert_burn_event_to_correct_event(event_name, parsed_event)
        if not parsed_event:
            return None

        if event_name == self.pool_contracts_events_enum.TokenExchange.name:
            logger.debug("resolving swap from swap event")
            swap = self.get_swap_from_exchange_event(dex_pool, parsed_receipt_log, tokens_scalars)
            logger.debug(f"resolved swap from swap event {swap}")
            return swap

        if event_name == self.pool_contracts_events_enum.TokenExchangeUnderlying.name:
            curve_metapool = dex_pool
            if len(dex_pool.underlying_token_addresses) == 0:
                curve_metapool = self.enrich_pool_with_metapool_addresses(dex_pool)
            if not curve_metapool:
                return None

            underlying_scalars = self.get_underlying_scalars_for_metapool(curve_metapool)
            if not underlying_scalars:
                return None
            logger.debug("resolving swap for underlying coins event")
            swap = self.get_swap_from_underlying_event(
                curve_metapool, parsed_receipt_log, underlying_scalars
            )
            logger.debug("resolved swap for underlying coins event")
            return swap

        if event_name == self.pool_contracts_events_enum.AddLiquidity.name:
            logger.debug("resolving mint from add_liquidity event")
            mint = self.get_mint_from_events(dex_pool, parsed_receipt_log, tokens_scalars)
            logger.debug("resolved mint from add_liquidity event")
            return mint

        if event_name in self.pool_contracts_events_enum.burn_events():
            logger.debug("resolving burn from curve_burn events")
            burn = self.get_burns_from_events(
                dex_pool,
                parsed_receipt_log,
                tokens_scalars,
                transfers_for_transaction,
            )
            return burn

    def get_pool_finances(
        self,
        curve_pool: EthDexPool,
        tokens_scalars: list[int],
        contract: str = POOL_CONTRACT_V1,
        block_identifier: int | Literal['latest'] = "latest",
    ) -> dict:
        reserves = []
        checksum_pool_address = to_checksum(curve_pool.address)
        coin_count = len(curve_pool.token_addresses)

        default_finance = {
            'reserves': [0.0] * coin_count,
            'prices': get_default_zero_prices(coin_count),
        }
        for idx, token in enumerate(curve_pool.token_addresses):
            token_reserve = self._get_balance_of(token, curve_pool.address, block_identifier)
            if token_reserve == 0:
                try:
                    token_reserve = (
                        self.abi[contract]
                        .functions.balances(idx)
                        .call(
                            {"to": checksum_pool_address},
                            block_identifier,
                        )
                    )
                except (
                    ValueError,
                    TypeError,
                    BadFunctionCallOutput,
                    ContractLogicError,
                    ABIFunctionNotFound,
                ):
                    token_reserve = 0
            reserves.append(token_reserve / tokens_scalars[idx])

        # Solution was suggested by Curve founder (Michael Egorov): calculate rate for coin i-th
        # try to get exchange rate for i->j, j->i and take average. On practice results were not great
        #
        # Note: Token_scalars should be considered during exchange (example: USDT 6, DAI - 18)
        prices = get_default_prices(coin_count)
        for coin_index in range(0, coin_count):
            for coin_index_for_swap in range(0, coin_count):
                if coin_index == coin_index_for_swap:
                    continue
                dx, exchange_precision = self.get_right_exchange_amount_for_coins(
                    coin_index, coin_index_for_swap, tokens_scalars
                )
                exchange_rate = None
                try:
                    exchange_rate = (
                        self.abi[contract]
                        .functions.get_dy_underlying(coin_index, coin_index_for_swap, dx)
                        .call({"to": checksum_pool_address}, "latest")
                    )
                except Exception:
                    for contract_name in ["CurvePoolv1", "CurvePoolv3"]:
                        try:
                            exchange_rate = (
                                self.abi[contract_name]
                                .functions.get_dy(coin_index, coin_index_for_swap, dx)
                                .call({"to": checksum_pool_address}, "latest")
                            )
                            break
                        except Exception:
                            pass
                if exchange_rate:
                    prices[coin_index_for_swap][coin_index] = exchange_rate / exchange_precision
                else:
                    logger.error(
                        f"Can not detect price for token in curve pool: {curve_pool.address} {coin_index} "
                        f"{coin_index_for_swap}"
                    )
                    return default_finance
        finance_info = {
            'reserves': reserves,
            'prices': prices,
        }
        return finance_info

    def get_pool_with_metapool_finances(
        self,
        curve_pool: EthDexPool,
        underlying_scalars: list[int],
        contract: str = POOL_CONTRACT_V1,
        block_identifier: int | Literal['latest'] = "latest",
    ) -> dict:
        checksum_pool_address = to_checksum(curve_pool.address)
        underlying_balances = []
        start_id = self.current_registry_id if self.current_registry_id else 0
        end_id = start_id if start_id else self.registry_count
        u_tokens_count = len(curve_pool.underlying_token_addresses)
        default_finance = {
            'reserves': [0.0] * u_tokens_count,
            'prices': get_default_zero_prices(u_tokens_count),
        }

        for registry_id in range(start_id, end_id + 1):
            try:
                if not self.abi.get(FACTORY_CONTRACT + "v_" + str(registry_id)):
                    continue
                underlying_balances = (
                    self.abi[FACTORY_CONTRACT + "v_" + str(registry_id)]
                    .functions.get_underlying_balances(checksum_pool_address)
                    .call(block_identifier=block_identifier)
                )
            except Exception as exc:
                logger.info(
                    f"Can't get underlying balances for metapool: {curve_pool.address}, "
                    f"from contract v_{registry_id}, failing with error: {exc}"
                )
                continue
        if not underlying_balances:
            return default_finance
        underlying_balances = [
            underlying_balances[idx] / underlying_scalars[idx] for idx in range(u_tokens_count)
        ]

        u_prices = get_default_prices(u_tokens_count)
        for coin_index in range(0, u_tokens_count):
            for coin_index_for_swap in range(0, u_tokens_count):
                if coin_index == coin_index_for_swap:
                    continue
                dx, exchange_precision = self.get_right_exchange_amount_for_coins(
                    coin_index, coin_index_for_swap, underlying_scalars
                )
                exchange_rate = None
                try:
                    exchange_rate = (
                        self.abi[contract]
                        .functions.get_dy_underlying(coin_index, coin_index_for_swap, dx)
                        .call({"to": checksum_pool_address}, block_identifier)
                    )
                except Exception:
                    pass
                if exchange_rate:
                    u_prices[coin_index_for_swap][coin_index] = exchange_rate / exchange_precision
                else:
                    logger.error(
                        f"Can not detect price for token in curve pool: {curve_pool.address}"
                    )
                    return default_finance
        return {
            'reserves': underlying_balances,
            'prices': u_prices,
        }

    @staticmethod
    def get_right_exchange_amount_for_coins(i, j, tokens_scalars: list) -> tuple[int, int]:
        if tokens_scalars[i] == tokens_scalars[j]:
            return tokens_scalars[i], tokens_scalars[j]
        elif tokens_scalars[i] > tokens_scalars[j]:
            return max(tokens_scalars[i], tokens_scalars[j]), min(
                tokens_scalars[i], tokens_scalars[j]
            )
        else:
            return min(tokens_scalars[i], tokens_scalars[j]), max(
                tokens_scalars[i], tokens_scalars[j]
            )

    def get_burns_from_events(
        self,
        curve_pool: EthDexPool,
        parsed_receipt_log: ParsedReceiptLog,
        tokens_scalars,
        transfers_for_transaction: list[EthTokenTransfer],
        contract: str = POOL_CONTRACT_V1,
    ) -> EthDexTrade | None:
        event_name = parsed_receipt_log.event_name
        parsed_event = parsed_receipt_log.parsed_event
        block_identifier = parsed_receipt_log.block_number
        finance_info = self.get_pool_finances(
            curve_pool, tokens_scalars, block_identifier=block_identifier
        )
        if event_name == CurveTransactionType.RemoveLiquidityOne.name:
            amounts = [0.0] * len(curve_pool.token_addresses)
            coin_index = self.detect_coin_index_for_remove_liquidity_one_event(
                parsed_event, curve_pool, transfers_for_transaction
            )

            if coin_index is None:
                logger.error(
                    f"Can not detect coin_index while resolving event remove_liquidity_one event"
                    f" pool_index: {curve_pool.address}"
                    f" in block: {block_identifier}"
                )
                return None
            amounts[coin_index] = parsed_event["coin_amount"] / tokens_scalars[coin_index]

            return EthDexTrade(
                pool_address=curve_pool.address,
                token_amounts=amounts,
                transaction_hash=parsed_receipt_log.transaction_hash,
                log_index=parsed_receipt_log.log_index,
                block_number=parsed_receipt_log.block_number,
                event_type='burn',
                token_addresses=curve_pool.token_addresses,
                token_reserves=finance_info['reserves'],
                token_prices=finance_info['prices'],
                lp_token_address=curve_pool.lp_token_addresses[0],
            )
        else:
            amounts = [
                amount / tokens_scalars[idx]
                for idx, amount in enumerate(parsed_event["token_amounts"])
            ]
            return EthDexTrade(
                pool_address=curve_pool.address,
                token_amounts=amounts,
                transaction_hash=parsed_receipt_log.transaction_hash,
                log_index=parsed_receipt_log.log_index,
                block_number=parsed_receipt_log.block_number,
                event_type='burn',
                token_addresses=curve_pool.token_addresses,
                token_reserves=finance_info['reserves'],
                token_prices=finance_info['prices'],
                lp_token_address=curve_pool.lp_token_addresses[0],
            )

    @staticmethod
    def detect_coin_index_for_remove_liquidity_one_event(
        parsed_event: dict,
        curve_pool: EthDexPool,
        transfers_for_transaction: list[EthTokenTransfer],
    ) -> int | None:
        coin_index, transfers_with_coin = None, []
        for transfer in transfers_for_transaction:
            if abs(transfer.value - parsed_event["coin_amount"]) < EPSILON:
                transfers_with_coin.append(transfer)

        if not transfers_with_coin:
            return None

        for idx, token_address in enumerate(curve_pool.token_addresses):
            for transfer in transfers_with_coin:
                if token_address.lower() == transfer.token_address:
                    coin_index = idx
                    break
        return coin_index

    def get_mint_from_events(
        self,
        curve_pool: EthDexPool,
        parsed_receipt_log: ParsedReceiptLog,
        tokens_scalars: list[int],
    ) -> EthDexTrade:
        amounts = [
            amount / tokens_scalars[idx]
            for idx, amount in enumerate(parsed_receipt_log.parsed_event["token_amounts"])
        ]
        finance_info = self.get_pool_finances(
            curve_pool, tokens_scalars, block_identifier=parsed_receipt_log.block_number
        )
        return EthDexTrade(
            pool_address=curve_pool.address,
            token_amounts=amounts,
            transaction_hash=parsed_receipt_log.transaction_hash,
            log_index=parsed_receipt_log.log_index,
            block_number=parsed_receipt_log.block_number,
            event_type='mint',
            token_addresses=curve_pool.token_addresses,
            token_reserves=finance_info['reserves'],
            token_prices=finance_info['prices'],
            lp_token_address=curve_pool.lp_token_addresses[0],
        )

    def get_swap_from_exchange_event(
        self,
        curve_pool: EthDexPool,
        parsed_receipt_log: ParsedReceiptLog,
        tokens_scalars: list[int],
    ) -> EthDexTrade:
        parsed_event = parsed_receipt_log.parsed_event
        first_coin_index = parsed_event["sold_id"]
        second_coin_index = parsed_event["bought_id"]

        amount_first_coin = parsed_event["tokens_sold"] / tokens_scalars[first_coin_index]
        amount_second_coin = -parsed_event["tokens_bought"] / tokens_scalars[second_coin_index]

        amounts = [amount_first_coin, amount_second_coin]
        tokens = [
            curve_pool.token_addresses[first_coin_index],
            curve_pool.token_addresses[second_coin_index],
        ]

        finance_info = self.get_pool_finances(
            curve_pool, tokens_scalars, block_identifier=parsed_receipt_log.block_number
        )

        prices_for_two_involved_tokens = [
            finance_info['prices'][first_coin_index][second_coin_index],
            finance_info['prices'][second_coin_index][first_coin_index],
        ]

        return EthDexTrade(
            pool_address=curve_pool.address,
            token_amounts=amounts,
            transaction_hash=parsed_receipt_log.transaction_hash,
            log_index=parsed_receipt_log.log_index,
            block_number=parsed_receipt_log.block_number,
            event_type='swap',
            token_addresses=tokens,
            token_reserves=[
                finance_info['reserves'][first_coin_index],
                finance_info['reserves'][second_coin_index],
            ],
            token_prices=get_prices_for_two_pool(*prices_for_two_involved_tokens),
        )

    def get_swap_from_underlying_event(
        self,
        curve_pool: EthDexPool,
        parsed_receipt_log: ParsedReceiptLog,
        underlying_tokens_scalars,
    ) -> EthDexTrade:
        parsed_event = parsed_receipt_log.parsed_event
        first_coin_index = parsed_event["sold_id"]
        second_coin_index = parsed_event["bought_id"]

        amount_first_coin = (
            parsed_event["tokens_sold"] / underlying_tokens_scalars[first_coin_index]
        )
        amount_second_coin = (
            -parsed_event["tokens_bought"] / underlying_tokens_scalars[second_coin_index]
        )
        tokens = [
            curve_pool.underlying_token_addresses[first_coin_index],
            curve_pool.underlying_token_addresses[second_coin_index],
        ]
        amounts = [amount_first_coin, amount_second_coin]

        finance_info = self.get_pool_with_metapool_finances(
            curve_pool,
            underlying_tokens_scalars,
            block_identifier=parsed_receipt_log.block_number,
        )

        prices_for_two_involved_tokens = [
            finance_info['prices'][first_coin_index][second_coin_index],
            finance_info['prices'][second_coin_index][first_coin_index],
        ]

        return EthDexTrade(
            pool_address=curve_pool.address,
            token_amounts=amounts,
            transaction_hash=parsed_receipt_log.transaction_hash,
            log_index=parsed_receipt_log.log_index,
            block_number=parsed_receipt_log.block_number,
            event_type='swap',
            token_addresses=tokens,
            token_reserves=[
                finance_info['reserves'][first_coin_index],
                finance_info['reserves'][second_coin_index],
            ],
            token_prices=get_prices_for_two_pool(*prices_for_two_involved_tokens),
        )

    def enrich_pool_with_metapool_addresses(self, curve_pool: EthDexPool) -> EthDexPool:
        for registry_id in range(0, self.registry_count + 1):
            try:
                if not self.abi.get(FACTORY_CONTRACT + "v_" + str(registry_id)):
                    continue
                tokens_addresses = (
                    self.abi[FACTORY_CONTRACT + "v_" + str(registry_id)]
                    .functions.get_underlying_coins(curve_pool.address)
                    .call(block_identifier="latest")
                )
            except Exception:
                logger.error(
                    f"Can't get token_addresses for metapool: {curve_pool.address} from "
                    f"contract v_{registry_id}"
                )
                continue
            tokens_addresses = self._normalize_and_clean_addresses(tokens_addresses)
            if tokens_addresses:
                curve_pool.underlying_token_addresses = tokens_addresses
                self.current_registry_id = registry_id
                return curve_pool
        return curve_pool

    def get_underlying_scalars_for_metapool(self, curve_pool: EthDexPool) -> list[int] | None:
        start_id = self.current_registry_id if self.current_registry_id else 0
        end_id = start_id if start_id else self.registry_count
        for registry_id in range(start_id, end_id + 1):
            try:
                if not self.abi.get(FACTORY_CONTRACT + "v_" + str(registry_id)):
                    continue
                underlying_decimals = (
                    self.abi[FACTORY_CONTRACT + "v_" + str(registry_id)]
                    .functions.get_underlying_decimals(curve_pool.address)
                    .call(block_identifier="latest")
                )
            except Exception:
                logger.error(
                    f"Can't get decimals for coins in metapool: {curve_pool.address} from"
                    f"contract v_{registry_id}"
                )
                continue
            underlying_decimals = [scalar for scalar in underlying_decimals if scalar != 0]
            if len(underlying_decimals) == len(curve_pool.underlying_token_addresses):
                return [10**scalar for scalar in underlying_decimals]
        return None

    def _normalize_and_clean_addresses(self, token_addresses: list[str]) -> list[str]:
        token_addresses_cleared = [
            address.lower().replace(self.eee, self.native_token['address'])
            for address in token_addresses
        ]
        token_addresses_cleared = [
            address for address in token_addresses_cleared if address not in NULL_ADDRESSES
        ]
        return token_addresses_cleared

    def _get_tokens_addresses_for_pool(
        self, pool_address: ChecksumAddress
    ) -> tuple[list[str], list[str]]:

        def get_coins_from_pool() -> list[str]:
            coin_addresses = []
            coin_index = 0
            while True:
                current_token_address = None
                for pool_contract in self._pool_contract_abi_mapper.keys():
                    try:
                        current_token_address = (
                            self.abi[pool_contract]
                            .functions.coins(coin_index)
                            .call(
                                {"to": pool_address},
                                'latest',
                            )
                        )
                        break
                    except (
                        TypeError,
                        ContractLogicError,
                        ValueError,
                        BadFunctionCallOutput,
                        ABIFunctionNotFound,
                    ):
                        continue
                if not current_token_address:
                    break
                else:
                    coin_addresses.append(current_token_address)
                    coin_index += 1
            coin_addresses = self._normalize_and_clean_addresses(coin_addresses)
            return coin_addresses

        def get_underlying_tokens_addresses() -> list[str]:
            # Try to get underlying tokens from registry contracts
            for registry_id in range(0, self.registry_count + 1):
                try:
                    if not self.abi.get(FACTORY_CONTRACT + "v_" + str(registry_id)):
                        continue
                    u_tokens_addresses = (
                        self.abi[FACTORY_CONTRACT + "v_" + str(registry_id)]
                        .functions.get_underlying_coins(pool_address)
                        .call(block_identifier="latest")
                    )
                except (
                    ValueError,
                    TypeError,
                    BadFunctionCallOutput,
                    ContractLogicError,
                    ABIFunctionNotFound,
                ):
                    continue
                u_tokens_addresses = self._normalize_and_clean_addresses(u_tokens_addresses)
                if u_tokens_addresses:
                    return u_tokens_addresses

            # Try to get underlying tokens from pool contract
            coin_addresses = []
            coin_index = 0
            while True:
                current_token_address = None
                for pool_contract in self._pool_contract_abi_mapper.keys():
                    try:
                        current_token_address = (
                            self.abi[pool_contract]
                            .functions.underlying_coins(coin_index)
                            .call(
                                {"to": pool_address},
                                'latest',
                            )
                        )
                        break
                    except (
                        TypeError,
                        ContractLogicError,
                        ValueError,
                        BadFunctionCallOutput,
                        ABIFunctionNotFound,
                    ):
                        continue
                if not current_token_address:
                    break
                else:
                    coin_addresses.append(current_token_address)
                    coin_index += 1
            coin_addresses = self._normalize_and_clean_addresses(coin_addresses)
            return coin_addresses

        try:
            pool_coins = self.pool_info_contract.functions.get_pool_coins(pool_address).call()
        except (ValueError, TypeError, BadFunctionCallOutput, ContractLogicError):
            pool_coins = [[], []]

        token_addresses, underlying_addresses = pool_coins[:2]
        token_addresses = self._normalize_and_clean_addresses(token_addresses)
        underlying_addresses = self._normalize_and_clean_addresses(underlying_addresses)

        if not token_addresses:
            token_addresses = get_coins_from_pool()
            underlying_addresses = get_underlying_tokens_addresses()

        return token_addresses, underlying_addresses

    def _get_lp_token_address_for_pool(self, pool_address: ChecksumAddress) -> list[str]:

        def get_lp_from_registry():
            for registry_id in range(0, self.registry_count + 1):
                if not self.abi.get(FACTORY_CONTRACT + "v_" + str(registry_id)):
                    continue
                if not hasattr(
                    self.abi[FACTORY_CONTRACT + f"v_{registry_id}"].functions,
                    "get_lp_token",
                ):
                    continue
                try:
                    lp_address = (
                        self.abi[FACTORY_CONTRACT + f"v_{registry_id}"]
                        .functions.get_lp_token(pool_address)
                        .call()
                    )
                    if lp_address not in NULL_ADDRESSES:
                        return [lp_address.lower()]
                except (ContractLogicError, BadFunctionCallOutput, ValueError) as e:
                    logger.debug(
                        f"Can not get lp address for: {pool_address} from contract v_{registry_id}",
                        extra={"amm": "curve", "exception": e},
                    )
            return [pool_address.lower()]

        try:
            lp_token_address = self.pool_info_contract.functions.get_pool_info(pool_address).call()
        except (ValueError, TypeError, BadFunctionCallOutput, ContractLogicError):
            return get_lp_from_registry()

        return [lp_token_address[5]]

    def convert_burn_event_to_correct_event(self, event_name, parsed_event):
        # because MAX number of coins in Curve is 8
        if (
            event_name == CurveTransactionType.RemoveLiquidityOne.name
            and "coin_index" in parsed_event
            and parsed_event["coin_index"] > 7
        ):
            try:
                return self.normalize_event(
                    self._get_events_abi(self.abi['CurvePoolv5'], event_name), parsed_event
                )
            except Exception:
                return None
        return parsed_event
