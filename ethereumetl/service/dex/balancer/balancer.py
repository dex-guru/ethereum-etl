import json
import logging
from decimal import Decimal
from enum import Enum
from pathlib import Path

from eth_utils import to_hex
from web3 import Web3
from web3.contract import Contract
from web3.exceptions import ABIFunctionNotFound, ContractLogicError

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.domain.dex_trade import EthDexTrade
from ethereumetl.domain.receipt_log import ParsedReceiptLog
from ethereumetl.domain.token import EthToken
from ethereumetl.domain.token_transfer import EthTokenTransfer
from ethereumetl.service.dex.base.base_dex_client import BaseDexClient
from ethereumetl.utils import get_default_prices, get_default_zero_prices, get_prices_for_two_pool

logs = logging.getLogger(__name__)
to_checksum = Web3.toChecksumAddress

AMM_TYPE = "balancer"
VAULT_CONTRACT = "Vault"
POOL_CONTRACT = "Pool"
POOL_FACTORY_CONTRACT = "PoolFactory"

STABLE_POOL_FACTORIES = [
    "StablePoolFactory",
    "MetaStablePoolFactory",
    "StablePhantomPoolFactory",
    "StablePoolFactoryV2",
    "ComposableStablePoolFactory",
    "AaveLinearPoolFactory",
    "AaveLinearPoolV2Factory",
]

"""
Left there to have addresses for debug.
POOLS_FACTORIES = {
    'StablePoolFactory': '0xc66Ba2B6595D3613CCab350C886aCE23866EDe24', # no weights https://etherscan.io/address/0x9f19a375709baf0e8e35c2c5c65aca676c4c7191#readContract
    'WeightedPoolFactory': '0x8E9aa87E45e92bad84D5F8DD1bff34Fb92637dE9',
    'WeightedPool2TokensFactory': '0xA5bf2ddF098bb0Ef6d120C98217dD6B141c74EE0',
    'MetaStablePoolFactory': '0x67d27634E44793fE63c467035E31ea8635117cd4', #  no weights https://etherscan.io/address/0x1e19cf2d73a72ef1332c882f20534b6519be0276#readContract
    'LiquidityBootstrappingPoolFactory': '0x751A0bC0e3f75b38e01Cf25bFCE7fF36DE1C87DE',
    'InvestmentPoolFactory': '0x48767F9F868a4A7b86A90736632F6E44C2df7fa9',
    'AaveLinearPoolFactory': '0xD7FAD3bd59D6477cbe1BE7f646F7f1BA25b230f8', #  no weights https://etherscan.io/address/0xe6bcc79f328eec93d4ec8f7ed35534d9ab549faa#readContract
    'StablePhantomPoolFactory': '0xb08E16cFc07C684dAA2f93C70323BAdb2A6CBFd2', #  no weights https://etherscan.io/address/0x7b50775383d3d6f0215a8f290f2c9e2eebbeceb2#readContract
    'ERC4626LinearPoolFactory': '0xe061bf85648e9fa7b59394668cfeef980aec4c66', # no weigths https://etherscan.io/address/0x8f4063446f5011bc1c9f79a819efe87776f23704#readContract
    'StablePoolFactoryV2': '0x8df6EfEc5547e31B0eb7d1291B511FF8a2bf987c', # no weights https://etherscan.io/address/0x13f2f70a951fb99d48ede6e25b0bdf06914db33f#readContract
    'NoProtocolFeeLiquidityBootstrappingPoolFactory': '0x0F3e0c4218b7b0108a3643cFe9D3ec0d4F57c54e',
    'ComposableStablePoolFactory': '0xf9ac7B9dF2b3454E841110CcE5550bD5AC6f875F', # no weights https://etherscan.io/address/0xf9ac7B9dF2b3454E841110CcE5550bD5AC6f875F#readContract
}
"""


class BalancerTransactionType(Enum):
    swap = "Swap"
    poolbalancechanged = "PoolBalanceChanged"


class BalancerAmm(BaseDexClient):
    pool_contract_names = [POOL_CONTRACT, VAULT_CONTRACT]
    pool_contracts_events_enum = BalancerTransactionType

    def __init__(self, web3: Web3, chain_id: int, file_name: str = __file__):
        super().__init__(web3, chain_id, file_name)
        self.abi: dict[str, Contract | type[Contract]] = {}
        self._path_to_abi = Path(__file__).parent
        pool_abi_path = Path(file_name).parent / "Pool.json"
        vault_abi_path = Path(file_name).parent / "Vault.json"
        with open(Path(__file__).parent / "deploys" / str(chain_id) / "metadata.json") as f:
            metadata = json.load(f)
            self.VAULT_ADDRESS = metadata[0]["contracts"][VAULT_CONTRACT]
            self._setup_factory_contracts(metadata)
        self.pool_contract_abi = self._initiate_contract(abi_path=pool_abi_path)
        self.vault_contract_abi = self._initiate_contract(
            address=self.VAULT_ADDRESS, abi_path=vault_abi_path
        )

    def _setup_factory_contracts(self, metadatas: list[dict]):
        for metadata in metadatas:
            for name, address in metadata["contracts"].items():
                self.abi[name] = self._initiate_contract(
                    abi_path=self._path_to_abi / f"{name}.json", address=address
                )

    def resolve_asset_from_log(self, parsed_log: ParsedReceiptLog) -> EthDexPool | None:
        pool_id, pool_address = self._get_pool_id_and_address_from_receipt_log(parsed_log)
        if not pool_id or not pool_address:
            return None
        logs.debug(f"Resolving pool addresses for {pool_id}")
        tokens_addresses = self._get_tokens_addresses_for_pool(pool_id)
        if not tokens_addresses:
            return None
        pool_address_lower = pool_address.lower()

        try:
            pool_fee = self.pool_contract_abi.functions.getSwapFeePercentage().call(
                {"to": pool_address}, "latest"
            )
            fee_converted = pool_fee / (10**18)
        except ContractLogicError:
            fee_converted = 0
        return EthDexPool(
            address=pool_address_lower,
            token_addresses=[token_address.lower() for token_address in tokens_addresses],
            fee=int(fee_converted),
            lp_token_addresses=[pool_address_lower],
            factory_address=self.VAULT_ADDRESS.lower(),
        )

    def _get_tokens_addresses_for_pool(self, pool_id: str) -> list | None:
        """
        in balancer we operate pool_id 0x32296969ef14eb0c6d29669c550d4a0449130230000200000000000000000080
        which is smart contract address 0x32296969ef14eb0c6d29669c550d4a0449130230 (42) plus extra.
        """
        logs.debug(f"Resolving tokens addresses for {pool_id}")
        try:
            tokens_addresses = self.vault_contract_abi.functions.getPoolTokens(pool_id).call(
                {"to": self.VAULT_ADDRESS}, "latest"
            )
        except (ContractLogicError, ValueError, TypeError):
            # logs.error(f"Cant resolve tokens_addresses for pool {pool_id}, {e}")
            return None
        return tokens_addresses[0] if tokens_addresses else None

    @staticmethod
    def _pool_id_to_address(pool_id: str) -> str:
        if not pool_id.startswith("0x"):
            pool_id = "0x" + pool_id
        pool_address = Web3.toChecksumAddress(pool_id[:42])
        return pool_address

    def _get_pool_id_and_address_from_receipt_log(
        self, receipt_log: ParsedReceiptLog
    ) -> tuple[str, str] | tuple[None, None]:
        pool_id = to_hex(receipt_log.parsed_event["poolId"])
        pool_address = self._pool_id_to_address(pool_id)
        return pool_id, pool_address

    def resolve_receipt_log(
        self,
        parsed_receipt_log: ParsedReceiptLog,
        dex_pool: EthDexPool,
        tokens_for_pool: list[EthToken],
        transfers_for_transaction: list[EthTokenTransfer],
    ) -> EthDexTrade | None:
        tokens_scalars = {}
        for erc20_token in tokens_for_pool:
            tokens_scalars[erc20_token.address.lower()] = 10**erc20_token.decimals
        parsed_event = parsed_receipt_log.parsed_event
        event_name = parsed_receipt_log.event_name

        if event_name.lower() == BalancerTransactionType.swap.name:
            finance_info = self._get_pool_finances(
                dex_pool, parsed_event, tokens_scalars, parsed_receipt_log.block_number - 1
            )
            if not finance_info:
                return None
            swap = self.get_swap_from_swap_event(
                dex_pool, finance_info, parsed_receipt_log, tokens_scalars
            )
            return swap

        if event_name.lower() == BalancerTransactionType.poolbalancechanged.name:
            finance_info = self._get_pool_finances(
                dex_pool, parsed_event, tokens_scalars, parsed_receipt_log.block_number - 1
            )
            if not finance_info:
                return None
            burn_mint = self.get_mint_burn_from_events(
                dex_pool, finance_info, parsed_receipt_log, tokens_scalars
            )
            return burn_mint

    def get_mint_burn_from_events(
        self,
        base_pool: EthDexPool,
        finance_info: dict,
        parsed_receipt_log: ParsedReceiptLog,
        tokens_scalars: dict,
    ) -> EthDexTrade:
        parsed_event = parsed_receipt_log.parsed_event
        mint_burn_amounts = {}
        for idx, token_address in enumerate(parsed_event["tokens"]):
            tokens_scalar = tokens_scalars[token_address.lower()]
            mint_burn_amounts[token_address.lower()] = parsed_event["deltas"][idx] / tokens_scalar

        amounts = []
        for idx, token_address in enumerate(base_pool.token_addresses):
            if mint_burn_amounts.get(token_address):
                amounts.append(abs(mint_burn_amounts.get(token_address.lower(), 0.0)))
            else:
                amounts.append(0.0)

        is_mint = self._find_if_mint(parsed_event["deltas"])

        mint_burn = EthDexTrade(
            token_amounts=amounts,
            pool_address=base_pool.address.lower(),
            transaction_hash=parsed_receipt_log.transaction_hash,
            log_index=parsed_receipt_log.log_index,
            block_number=parsed_receipt_log.block_number,
            event_type="mint" if is_mint else "burn",
            token_reserves=finance_info['reserves'],
            token_prices=finance_info['prices'],
            token_addresses=base_pool.token_addresses,
            lp_token_address=base_pool.address.lower(),
        )
        return mint_burn

    @staticmethod
    def _find_if_mint(deltas: list[int]) -> bool:
        negative_deltas = [delta for delta in deltas if delta < 0]
        if negative_deltas:
            return False
        else:
            return True

    # https://github.com/TokenEngineeringCommunity/BalancerPools_Model/blob/735ff5742d095419e74c4d9ee37cab54ffc9b802/model/parts/balancer_math.py#L29
    # **********************************************************************************************
    # calcSpotPrice                                                                             //
    # sP = spotPrice                                                                            //
    # bI = token_balance_in                ( bI / wI )         1                                  //
    # bO = token_balance_out         sP =  -----------  *  ----------                             //
    # wI = token_weight_in                 ( bO / wO )     ( 1 - sF )                             //
    # wO = token_weight_out                                                                       //
    # sF = swap_fee                                                                              //
    # **********************************************************************************************/
    @staticmethod
    def calc_spot_price(
        token_balance_in: Decimal,
        token_weight_in: Decimal,
        token_balance_out: Decimal,
        token_weight_out: Decimal,
        swap_fee: Decimal,
    ) -> float:
        numer = token_balance_in / token_weight_in
        denom = token_balance_out / token_weight_out
        ratio = numer / denom
        scale = 1 / (1 - swap_fee)
        return float(ratio * scale)

    def calculate_spot_prices(
        self, pool: EthDexPool, finance_info: dict, ref_token_index: int
    ) -> list[float]:
        swap_fee = pool.fee
        balance_in = finance_info['reserves'][ref_token_index]
        weight_in = finance_info['weights'][ref_token_index] if finance_info['weights'] else 0.5
        spot_prices = [0.0] * len(pool.token_addresses)
        for idx, token in enumerate(pool.token_addresses):
            if idx == ref_token_index:
                spot_prices[idx] = 1.0
                continue
            balance_out = finance_info['reserves'][idx]
            weight_out = finance_info['weights'][idx] if finance_info['weights'] else 0.5

            spot_prices[idx] = self.calc_spot_price(
                token_balance_in=Decimal(balance_in),
                token_weight_in=Decimal(weight_in),
                token_balance_out=Decimal(balance_out),
                token_weight_out=Decimal(weight_out),
                swap_fee=Decimal(swap_fee),
            )
        return spot_prices

    def _resolve_pool_finances_metadata(
        self,
        base_pool: EthDexPool,
        parsed_event: dict,
        tokens_scalars: dict,
        block_number: int,
    ) -> dict:
        finance_info = {
            "reserves": [0.0] * len(base_pool.token_addresses),
            "prices": get_default_prices(len(base_pool.token_addresses)),
            "weights": [],
        }
        pool_id = to_hex(parsed_event["poolId"])
        try:
            pool_tokens = self.vault_contract_abi.functions.getPoolTokens(pool_id).call(
                block_identifier=block_number
            )
        except ValueError as e:
            if "trie" in e.args[0]["message"]:
                logs.error(
                    f"Pool finances are missing on the node for {block_number}, archive node would help"
                )
                return finance_info
            logs.error(f"ValueError requesting getPoolTokens for balancer {pool_id}")
            return finance_info

        if pool_tokens:
            reserves = []
            for idx, pool_token_reserve in enumerate(pool_tokens[1]):
                reserves.append(pool_token_reserve / tokens_scalars[pool_tokens[0][idx].lower()])
            finance_info['reserves'] = reserves
        try:
            weights = self.pool_contract_abi.functions.getNormalizedWeights().call(
                transaction={"to": Web3.toChecksumAddress(base_pool.address)},
                block_identifier=block_number,
            )
            weights = [pool_weight / 10**18 for pool_weight in weights]
            finance_info['weights'] = weights
        except ContractLogicError:
            logs.info("Pool is not one of the weighted once, tokens price from swap")
        return finance_info

    def _get_pool_prices_based_on_stable_factory(
        self, pool: EthDexPool, block_number: int
    ) -> list[list[float]] | None:
        factory_name = None
        for contract_name, contract in self.abi.items():
            if "Factory" in contract_name:
                try:
                    is_factory = contract.functions.isPoolFromFactory(
                        Web3.toChecksumAddress(pool.address)
                    ).call(block_identifier=block_number)
                except ABIFunctionNotFound:
                    continue
                if is_factory:
                    factory_name = contract_name
                    break
        if factory_name in STABLE_POOL_FACTORIES:
            # setting prices to in case it's stable pools [[1, 1, 1], [1, 1, 1], [1, 1, 1]]
            # as in those pools we are assuming that proces are interchangable
            # more complicated math could be implemented, leaving for now
            prices = get_default_prices(len(pool.token_addresses))
        else:
            logs.error(
                "Not supported case for pairs with > 2 tokens and no weights and not stable"
            )
            return get_default_zero_prices(len(pool.token_addresses))
        return prices

    def _hydrate_pool_with_prices_for_burn_mint(
        self,
        pool: EthDexPool,
        finance_info: dict,
        parsed_event: dict,
        tokens_scalars: dict,
        block_number: int,
    ) -> dict:
        coin_count = len(pool.token_addresses)
        finance_info['prices'] = get_default_prices(coin_count)
        if finance_info["weights"]:
            # In case we have pool weights we can calculate spot proces mathematically
            for coin_index in range(0, coin_count):
                finance_info['prices'][coin_index] = self.calculate_spot_prices(
                    pool, finance_info, coin_index
                )
        else:
            # Not working for all cases, assumming stable pools for now
            # if coin_count == 2:
            #     # Calculating based on price rate of one token against another, as we don't have
            #     # amount like in swap to calculate from them
            #     price_rate = self.abi[POOL_CONTRACT].contract.functions. \
            #         getRate().call(transaction={'to': Web3.toChecksumAddress(pool.address)},
            #                        block_identifier=block_number - 1)
            #     token0_price = price_rate / tokens_scalars[pool.tokens_addresses[1].lower()]
            #     token1_price = 1 / token0_price
            #     pool.prices = get_prices_for_two_pool(token0_price, token1_price)
            #     return pool
            # else:
            # if there are no weights and > 2 coins most likely stable pool
            prices = self._get_pool_prices_based_on_stable_factory(pool, block_number)
            if not prices:
                return finance_info
            finance_info["prices"] = prices
        return finance_info

    def _hydrate_pool_with_prices_for_swap(
        self,
        pool: EthDexPool,
        finance_info: dict,
        parsed_event: dict,
        tokens_scalars: dict,
        block_number: int,
    ) -> dict:
        tokens_addresses = [t_a.lower() for t_a in pool.token_addresses.copy()]
        token_in = parsed_event["tokenIn"].lower()
        token_out = parsed_event["tokenOut"].lower()
        token_in_idx = tokens_addresses.index(token_in)
        token_out_idx = tokens_addresses.index(token_out)

        # to figure out new reserves after the swap (sync event simulation)
        finance_info['reserves'][token_in_idx] = (
            finance_info['reserves'][token_in_idx]
            + parsed_event["amountIn"] / tokens_scalars[token_in]
        )
        finance_info['reserves'][token_out_idx] = (
            finance_info['reserves'][token_out_idx]
            + parsed_event["amountOut"] / tokens_scalars[token_out]
        )

        coin_count = len(pool.token_addresses)
        finance_info['prices'] = get_default_zero_prices(coin_count)

        if finance_info['weights']:
            # In case we have pool weights we can calculate spot proces mathematically
            for coin_index in range(0, coin_count):
                finance_info['prices'][coin_index] = self.calculate_spot_prices(
                    pool, finance_info, coin_index
                )
        else:
            if coin_count == 2:
                # Calculating prices using swap itself.
                swap_prices = [1, 1]
                swap_prices[token_in_idx] = parsed_event["amountIn"] / parsed_event["amountOut"]
                swap_prices[token_out_idx] = parsed_event["amountOut"] / parsed_event["amountIn"]
                finance_info['prices'] = get_prices_for_two_pool(swap_prices[0], swap_prices[1])
            else:
                # if there are no weights and > 2 coins most likely stable pool
                finance_info['prices'] = self._get_pool_prices_based_on_stable_factory(
                    pool, block_number
                )
        return finance_info

    def _get_pool_finances(
        self,
        base_pool: EthDexPool,
        parsed_event: dict,
        tokens_scalars: dict,
        block_number: int,
    ) -> dict | None:

        if parsed_event.get("deltas") and not self._find_if_mint(parsed_event["deltas"]):
            # In case it's Burn we need to get previous block reserves
            finance_info = self._resolve_pool_finances_metadata(
                base_pool, parsed_event, tokens_scalars, block_number - 1
            )
        else:
            finance_info = self._resolve_pool_finances_metadata(
                base_pool, parsed_event, tokens_scalars, block_number
            )
        if parsed_event.get("deltas"):
            # In case it's Burn/Mint
            return self._hydrate_pool_with_prices_for_burn_mint(
                base_pool, finance_info, parsed_event, tokens_scalars, block_number
            )
        if parsed_event.get("amountIn"):
            return self._hydrate_pool_with_prices_for_swap(
                base_pool, finance_info, parsed_event, tokens_scalars, block_number
            )

    @staticmethod
    def get_swap_from_swap_event(
        base_pool: EthDexPool,
        finance_info: dict,
        parsed_receipt_log: ParsedReceiptLog,
        tokens_scalars: dict,
    ) -> EthDexTrade:
        parsed_event = parsed_receipt_log.parsed_event
        token_in = parsed_event["tokenIn"].lower()
        token_out = parsed_event["tokenOut"].lower()
        amount_in = parsed_event["amountIn"] / tokens_scalars[token_in]
        amount_out = parsed_event["amountOut"] / tokens_scalars[token_out]

        tokens_addresses = [t_a.lower() for t_a in base_pool.token_addresses.copy()]
        token_in_idx = tokens_addresses.index(token_in.lower())
        token_out_idx = tokens_addresses.index(token_out.lower())
        swap = EthDexTrade(
            token_amounts=[amount_in, -amount_out],
            pool_address=base_pool.address.lower(),
            transaction_hash=parsed_receipt_log.transaction_hash,
            log_index=parsed_receipt_log.log_index,
            block_number=parsed_receipt_log.block_number,
            event_type="swap",
            token_reserves=[
                finance_info['reserves'][token_in_idx],
                finance_info['reserves'][token_out_idx],
            ],
            token_prices=get_prices_for_two_pool(
                finance_info['prices'][token_out_idx][token_in_idx],
                finance_info['prices'][token_in_idx][token_out_idx],
            ),
            token_addresses=[token_in, token_out],
        )
        return swap
