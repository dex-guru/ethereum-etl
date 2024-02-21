import logging
from enum import Enum

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
to_checksum = Web3.toChecksumAddress


class DODOTransactionType(Enum):
    """DODO V1 events."""

    buy_base_token = "BuyBaseToken"
    sell_base_token = "SellBaseToken"
    burn = "Withdraw"
    mint = "Deposit"

    @classmethod
    def swap_events(cls) -> list:
        return [cls.buy_base_token.value, cls.sell_base_token.value]


class DODOv1Amm(BaseDODOAmmClient):
    """
    DODO V1 AMM client.

    Docs: <https://docs.dodoex.io/english/developers/contract-data/contract-event>.
    Other docs: <https://dodoex.github.io/docs/docs/>.
    Contracts: <https://docs.dodoex.io/english/developers/contracts-address/ethereum#dodo-v1>.

    On each v1 pool, DODO has 2 LP tokens (base and quote).


    # Example transactions

    Contract DODORouteProxy using for proxying calling v1 contract from v2 contract.

    Example V2 swap for V1 pool:
        <https://bscscan.com/tx/0x0cc63056baa5ef57c3fc0b4390ffb1d69c91a7b59e2a3e2778bfc8f1ec569ee1>.
    Example V2 add liquidity for V1 pool:
        <https://bscscan.com/tx/0xf2e4965825979d284fb889ef2b3a60f8813075a53075edc5e43d66246a91d871>.


    # Factory for contracts

    GnosisSafeProxy (proxy for GnosisSafe) is a maintainer contract used as a factory for DODO contracts.
    GnosisSafeProxy it is a Multisig wallet, and can have different names and realizations on other chains.

    Examples
    --------
        - BNB: 0xcaa42F09AF66A8BAE3A7445a7f63DAD97c11638b
        - ETH: 0x95C4F5b83aA70810D4f142d58e5F7242Bd891CB0
        - Polygon: 0x3CD6D7F5fF977bf8069548eA1F9441b061162b42
    See `clients.blockchain.base.evm_base.EVMBase.get_pool_factory` where using maintainer as factory.


    # Topics of methods

    Method `Dodo Swap V1`: topic0 = 0xe93ad76094f247c0dafc1c61adc2187de1ac2738f7a3b49cb20b2263420251a3.
    Method `Add Liquidity To V1`: topic0 = 0x18081cde2fa64894914e1080b98cca17bb6d1acf633e57f6e26ebdb945ad830b
        -- mint (Deposit).

    """

    pool_contract_name = "DODO.json"
    amm_type = "dodo"
    pool_contracts_events_enum = DODOTransactionType

    _get_lp_token_methods = ["_BASE_CAPITAL_TOKEN_", "_QUOTE_CAPITAL_TOKEN_"]

    def __init__(self, web3: Web3, chain_id: int, path_to_file: str = __file__):
        super().__init__(web3, chain_id, path_to_file)

    def is_pool_address_for_amm(self, pool_address: str) -> bool:
        try:
            fee_rate = self.pool_contract.functions._MT_FEE_RATE_().call(
                {"to": to_checksum(pool_address)}, "latest"
            )
        except (TypeError, ContractLogicError, ValueError, BadFunctionCallOutput):
            return False
        return isinstance(fee_rate, int)

    def resolve_receipt_log(
        self,
        parsed_receipt_log: ParsedReceiptLog,
        dex_pool: EthDexPool,
        tokens_for_pool: list[EthToken],
        transfers_for_transaction: list[EthTokenTransfer],
    ) -> EthDexTrade | None:
        parsed_event = self._get_normalized_event(
            parsed_receipt_log.event_name, parsed_receipt_log.parsed_event
        )
        tokens_scalars = self._get_scalars_for_tokens(tokens_for_pool, dex_pool)

        event_name = parsed_receipt_log.event_name
        if not (parsed_event and event_name):
            return None
        block_number = parsed_receipt_log.block_number
        if event_name == self.pool_contracts_events_enum.burn.value:
            block_number -= 1

        reserves = []
        for idx, token in enumerate(dex_pool.token_addresses):
            reserves.append(
                self.erc20_contract_abi.functions.balanceOf(to_checksum(dex_pool.address)).call(
                    {"to": to_checksum(token)},
                    parsed_receipt_log.block_number,
                )
                / tokens_scalars[idx]
            )

        parsed_event["reserve0"] = reserves[0]
        parsed_event["reserve1"] = reserves[1]

        finance_info = self.get_pool_finances_from_event(
            dex_pool, parsed_receipt_log, tokens_scalars
        )

        if event_name in self.pool_contracts_events_enum.swap_events():
            logs.debug(f"resolving swap from swap event ({event_name})")
            if event_name == self.pool_contracts_events_enum.buy_base_token.value:
                amounts = self._get_swap_from_buy_base_token_event(parsed_event, tokens_scalars)
            else:
                amounts = self._get_swap_from_sell_base_token_event(parsed_event, tokens_scalars)
            return EthDexTrade(
                pool_address=dex_pool.address,
                token_amounts=amounts,
                transaction_hash=parsed_receipt_log.transaction_hash,
                log_index=parsed_receipt_log.log_index,
                block_number=parsed_receipt_log.block_number,
                event_type='swap',
                token_reserves=reserves,
                token_prices=get_prices_for_two_pool(
                    finance_info['price_0'], finance_info['price_1']
                ),
                token_addresses=dex_pool.token_addresses,
            )

        if event_name == self.pool_contracts_events_enum.mint.value:
            logs.debug("resolving mint from mint event")
            mint = self.get_mint_burn_from_events(
                dex_pool, parsed_receipt_log, tokens_scalars, finance_info
            )
            logs.debug(f"resolved mint from mint event: {mint}")
            return mint

        if event_name == self.pool_contracts_events_enum.burn.value:
            logs.debug("resolving burn from burn event")
            burn = self.get_mint_burn_from_events(
                dex_pool, parsed_receipt_log, tokens_scalars, finance_info
            )
            logs.debug(f"resolved burn from burn event: {burn}")
            return burn

    def get_mint_burn_from_events(
        self,
        base_pool: EthDexPool,
        parsed_receipt_log: ParsedReceiptLog,
        tokens_scalars: list[int],
        finance_info: dict,
    ) -> EthDexTrade:
        """
        receiver -- user wallet
        payer -- DODO contract.
        """
        amounts = [0.0, 0.0]
        parsed_event = parsed_receipt_log.parsed_event
        is_base_token = parsed_event["isBaseToken"]
        position = 0 if is_base_token else 1

        amounts[position] = parsed_event["amount"] / tokens_scalars[position]

        call_kwargs = {"to": to_checksum(base_pool.address)}
        if is_base_token:
            lp_token_address = self.pool_contract.functions._BASE_CAPITAL_TOKEN_().call(
                call_kwargs  # type: ignore
            )
        else:
            lp_token_address = self.pool_contract.functions._QUOTE_CAPITAL_TOKEN_().call(
                call_kwargs  # type: ignore
            )
        event_type = (
            'mint'
            if parsed_receipt_log.event_name == self.pool_contracts_events_enum.mint.value
            else 'burn'
        )
        mint_burn = EthDexTrade(
            pool_address=base_pool.address,
            token_amounts=amounts,
            transaction_hash=parsed_receipt_log.transaction_hash,
            log_index=parsed_receipt_log.log_index,
            block_number=parsed_receipt_log.block_number,
            event_type='mint' if event_type == 'mint' else 'burn',
            token_reserves=[finance_info['reserve_0'], finance_info['reserve_1']],
            token_prices=get_prices_for_two_pool(finance_info['price_0'], finance_info['price_1']),
            token_addresses=base_pool.token_addresses,
            lp_token_address=lp_token_address,
        )
        return mint_burn

    @staticmethod
    def _get_swap_from_buy_base_token_event(
        parsed_event: dict, tokens_scalars: list[int]
    ) -> list[float]:
        amount0 = parsed_event["receiveBase"] / tokens_scalars[0]
        amount1 = parsed_event["payQuote"] / tokens_scalars[1]
        return [-amount0, amount1]

    @staticmethod
    def _get_swap_from_sell_base_token_event(
        parsed_event: dict, tokens_scalars: list[int]
    ) -> list[float]:
        amount0 = parsed_event["payBase"] / tokens_scalars[0]
        amount1 = parsed_event["receiveQuote"] / tokens_scalars[1]

        return [amount0, -amount1]

    def get_lp_token_address_for_pool(self, pool_address: str) -> list[str]:
        lp_tokens = []
        pool_address = to_checksum(pool_address)
        for method in self._get_lp_token_methods:
            try:
                lp_tokens.append(
                    getattr(self.pool_contract.functions, method)().call(
                        {"to": pool_address},
                    )
                )
            except (TypeError, ContractLogicError, ValueError, BadFunctionCallOutput):
                logs.debug(f"Cant resolve lp token for method {method}")
                return []
        lp_tokens = [_.lower() for _ in lp_tokens]
        return lp_tokens
