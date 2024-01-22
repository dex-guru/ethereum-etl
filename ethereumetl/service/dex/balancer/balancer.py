import json
import logging
from enum import Enum
from pathlib import Path

from eth_utils import to_hex
from web3 import Web3
from web3.exceptions import ContractLogicError

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.domain.dex_trade import EthDexTrade
from ethereumetl.domain.receipt_log import ParsedReceiptLog
from ethereumetl.domain.token import EthToken
from ethereumetl.domain.token_transfer import EthTokenTransfer
from ethereumetl.service.dex.base.interface import DexClientInterface

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


class BalancerAmm(DexClientInterface):
    pool_contract_names = [POOL_CONTRACT, VAULT_CONTRACT]
    pool_contracts_events_enum = BalancerTransactionType
    VAULT_ADDRESS = '0xBA12222222228d8Ba445958a75a0704d566BF2C8'

    def __init__(self, web3: Web3, chain_id: int | None = None):
        pool_abi_path = Path(__file__).parent / "Pool.json"
        vault_abi_path = Path(__file__).parent / "Vault.json"
        if chain_id:
            with open(Path(__file__).parent / "deploys" / str(chain_id) / "metadata.json") as f:
                metadata = json.load(f)
                self.VAULT_ADDRESS = metadata["contracts"][VAULT_CONTRACT]
        self._w3 = web3
        self.pool_contract_abi = self._w3.eth.contract(abi=json.loads(pool_abi_path.read_text()))
        self.vault_contract_abi = self._w3.eth.contract(abi=json.loads(vault_abi_path.read_text()))

    def resolve_receipt_log(
        self,
        parsed_receipt_log: ParsedReceiptLog,
        base_pool: EthDexPool | None = None,
        erc20_tokens: list[EthToken] | None = None,
        token_transfers: list[EthTokenTransfer] | None = None,
    ) -> EthDexTrade | None:
        pass

    def resolve_asset_from_log(self, parsed_log: ParsedReceiptLog) -> EthDexPool | None:
        pool_id, pool_address = self._get_pool_id_and_address_from_receipt_log(parsed_log)
        if not pool_id:
            return None

        return self.get_base_pool(pool_id)

    def get_base_pool(self, pool_id: str) -> EthDexPool | None:
        """
        Supports resolving base pool by PoolID only, can're solve by smart contract address.
        """
        logs.debug(f"Resolving pool addresses for {pool_id}")
        tokens_addresses = self.get_tokens_addresses_for_pool(pool_id)
        pool_address = self._pool_id_to_address(pool_id)
        pool_address_lower = pool_address.lower()
        if not tokens_addresses:
            return None
        try:
            pool_fee = self.pool_contract_abi.functions.getSwapFeePercentage().call(
                {"to": pool_address}, "latest"
            )
            fee_converted = pool_fee / (10**18)
        except ContractLogicError:
            fee_converted = 0
        # TODO Check if fees are braking something
        return EthDexPool(
            address=pool_address_lower,
            token_addresses=[token_address.lower() for token_address in tokens_addresses],
            fee=int(fee_converted),
            lp_token_addresses=[pool_address_lower],
            factory_address=self.VAULT_ADDRESS.lower(),
        )

    def get_tokens_addresses_for_pool(self, pool_address: str) -> list | None:
        """
        in balancer we operate pool_id 0x32296969ef14eb0c6d29669c550d4a0449130230000200000000000000000080
        which is smart contract address 0x32296969ef14eb0c6d29669c550d4a0449130230 (42) plus extra.
        """
        pool_id = pool_address  #
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
        if "0x" not in pool_id:
            pool_id = "0x" + pool_id
        pool_address = Web3.toChecksumAddress(pool_id[:42])
        return pool_address

    def _get_pool_id_and_address_from_receipt_log(
        self, receipt_log: ParsedReceiptLog
    ) -> tuple[str, str] | tuple[None, None]:
        if receipt_log.event_name.lower() in ["swap", "poolbalancechanged"]:
            pool_id = to_hex(receipt_log.parsed_event["poolId"])
            pool_address = self._pool_id_to_address(pool_id)
            return pool_id, pool_address
        return None, None

    #
    # # def resolve_receipt_log(
    # #     self,
    # #     receipt_log: EthReceiptLog,
    # #     base_pool: Optional[EthDexPool] = None,
    # #     erc20_tokens=None,
    # # ) -> Optional[dict]:
    # #     if erc20_tokens is None:
    # #         erc20_tokens = []
    # #
    # #     logs.debug(f"resolving {receipt_log.transaction_hash.hex()}-{receipt_log.log_index}")
    # #     try:
    # #         topic = receipt_log.topics[0][0:4]
    # #     except IndexError:
    # #         logs.error(f"Cant get receipt_log.topics[0][0:4], index error, log: {receipt_log}")
    # #         return None
    # #     event_name = self.abi[VAULT_CONTRACT].topic_keccaks.get(topic, None)
    # #     if receipt_log.topics and event_name:
    # #         tokens_scalars = {}
    # #         # in case it wasn't called from blockchain service where we are resolving those upfront
    # #         # from redis
    # #         if not base_pool:
    # #             pool_id = self.get_pool_id_and_address_from_receipt_log(receipt_log)
    # #             if not pool_id:
    # #                 return None
    # #             pool_id = pool_id[0]
    # #             base_pool = self.get_base_pool(pool_id)
    # #
    # #         if not erc20_tokens:
    # #             for token_address in base_pool.tokens_addresses:
    # #                 erc20_tokens.append(self.get_token(token_address))
    # #
    # #         for erc20_token in erc20_tokens:
    # #             tokens_scalars[erc20_token.address.lower()] = 10**erc20_token.decimals
    # #         parsed_event = self.parse_event(self.abi[VAULT_CONTRACT], event_name, receipt_log)
    # #
    # #         if event_name.lower() == BalancerTransactionType.swap.name:
    # #             pool = self._get_pool_finances(
    # #                 base_pool, parsed_event, tokens_scalars, receipt_log.block_number
    # #             )
    # #             if not pool:
    # #                 logs.error(f"Cant get pool finances for {base_pool.address}")
    # #                 return None
    # #             pool.transaction_type = "swap"
    # #             logs.debug(f"resolving {pool.transaction_type} from {event_name.lower()} event")
    # #             swap = self.get_swap_from_swap_event(base_pool, parsed_event, tokens_scalars)
    # #             logs.debug(f"resolved {pool.transaction_type} from {event_name.lower()} event")
    # #             swap.log_index = receipt_log.log_index
    # #             return {"swaps": [swap], "pools": [pool]}
    # #
    # #         if event_name.lower() == BalancerTransactionType.poolbalancechanged.name:
    # #             pool = self._get_pool_finances(
    # #                 base_pool, parsed_event, tokens_scalars, receipt_log.block_number
    # #             )
    # #             if not pool:
    # #                 logs.error(f"Cant get pool finances for {base_pool.address}")
    # #                 return None
    # #             if_mint = self._find_if_mint(parsed_event["deltas"])
    # #             pool.transaction_type = "mint" if if_mint else "burn"
    # #             logs.debug(f"resolving {pool.transaction_type} from {event_name.lower()} event")
    # #             burn_mint = self.get_mint_burn_from_events(base_pool, parsed_event, tokens_scalars)
    # #             logs.debug(f"resolved {pool.transaction_type} from {event_name.lower()} event")
    # #             burn_mint.log_index = receipt_log.log_index
    # #             return {"pools": [pool], "mints" if if_mint else "burns": [burn_mint]}
    #
    # def get_mint_burn_from_events(self, base_pool, parsed_event, tokens_scalars) -> MintBurn:
    #     mint_burn_amounts = {}
    #     for idx, token_address in enumerate(parsed_event["tokens"]):
    #         tokens_scalar = tokens_scalars[token_address]
    #         mint_burn_amounts[token_address] = parsed_event["deltas"][idx] / tokens_scalar
    #
    #     amounts = []
    #     for idx, token_address in enumerate(base_pool.tokens_addresses):
    #         if mint_burn_amounts.get(token_address.lower()):
    #             amounts.append(abs(mint_burn_amounts.get(token_address.lower())))
    #         else:
    #             amounts.append(0.0)
    #
    #     if_mint = self._find_if_mint(parsed_event["deltas"])
    #     mint_burn = MintBurn(
    #         pool_address=base_pool.address.lower(),
    #         sender=parsed_event["liquidityProvider"] if if_mint else base_pool.address.lower(),
    #         owner=base_pool.address.lower()
    #         if if_mint
    #         else parsed_event["liquidityProvider"].lower(),
    #         amounts=amounts,
    #         lp_token_address=base_pool.address.lower(),
    #     )
    #     return mint_burn
    #
    # @staticmethod
    # def _find_if_mint(deltas: List[int]) -> bool:
    #     negative_deltas = [delta for delta in deltas if delta < 0]
    #     if negative_deltas:
    #         return False
    #     else:
    #         return True
    #
    # # https://github.com/TokenEngineeringCommunity/BalancerPools_Model/blob/735ff5742d095419e74c4d9ee37cab54ffc9b802/model/parts/balancer_math.py#L29
    # # **********************************************************************************************
    # # calcSpotPrice                                                                             //
    # # sP = spotPrice                                                                            //
    # # bI = token_balance_in                ( bI / wI )         1                                  //
    # # bO = token_balance_out         sP =  -----------  *  ----------                             //
    # # wI = token_weight_in                 ( bO / wO )     ( 1 - sF )                             //
    # # wO = token_weight_out                                                                       //
    # # sF = swap_fee                                                                              //
    # # **********************************************************************************************/
    # @staticmethod
    # def calc_spot_price(
    #     token_balance_in: Decimal,
    #     token_weight_in: Decimal,
    #     token_balance_out: Decimal,
    #     token_weight_out: Decimal,
    #     swap_fee: Decimal,
    # ) -> float:
    #     numer = token_balance_in / token_weight_in
    #     denom = token_balance_out / token_weight_out
    #     ratio = numer / denom
    #     scale = 1 / (1 - swap_fee)
    #     return float(ratio * scale)
    #
    # def calculate_spot_prices(self, pool: PoolFinances, ref_token_index: int) -> List[float]:
    #     swap_fee = pool.fee
    #     balance_in = pool.reserves[ref_token_index]
    #     weight_in = pool.weights[ref_token_index] if pool.weights else 0.5
    #     spot_prices = [0.0] * len(pool.tokens_addresses)
    #     for idx, token in enumerate(pool.tokens_addresses):
    #         if idx == ref_token_index:
    #             spot_prices[idx] = 1.0
    #             continue
    #         balance_out = pool.reserves[idx]
    #         weight_out = pool.weights[idx] if pool.weights else 0.5
    #
    #         spot_prices[idx] = self.calc_spot_price(
    #             token_balance_in=Decimal(balance_in),
    #             token_weight_in=Decimal(weight_in),
    #             token_balance_out=Decimal(balance_out),
    #             token_weight_out=Decimal(weight_out),
    #             swap_fee=Decimal(swap_fee),
    #         )
    #     return spot_prices
    #
    # def _resolve_pool_finances_metadata(
    #     self,
    #     base_pool: BasePool,
    #     parsed_event: dict,
    #     tokens_scalars: dict,
    #     block_number: int,
    # ) -> Optional[PoolFinances]:
    #     pool = PoolFinances(**base_pool.dict())
    #     pool.lp_token_addresses = self.get_lp_token_address_for_pool(pool.address)
    #     pool_id = to_hex(parsed_event["poolId"])
    #     try:
    #         pool_tokens = (
    #             self.abi[VAULT_CONTRACT]
    #             .contract.functions.getPoolTokens(pool_id)
    #             .call(block_identifier=block_number)
    #         )
    #     except ValueError as e:
    #         if "trie" in e.args[0]["message"]:
    #             logs.error(
    #                 f"Pool finances are missing on the node for {block_number}, archive node would help"
    #             )
    #             return None
    #         logs.error(f"ValueError requesting getPoolTokens for balancer {pool_id}")
    #         return None
    #     if pool_tokens:
    #         for idx, pool_token_reserve in enumerate(pool_tokens[1]):
    #             pool.reserves.append(
    #                 pool_token_reserve / tokens_scalars[pool_tokens[0][idx].lower()]
    #             )
    #     try:
    #         pool.weights = (
    #             self.abi[POOL_CONTRACT]
    #             .contract.functions.getNormalizedWeights()
    #             .call(
    #                 transaction={"to": Web3.toChecksumAddress(base_pool.address)},
    #                 block_identifier=block_number,
    #             )
    #         )
    #     except ContractLogicError as e:
    #         logs.info("Pool is not one of the weighted once, tokens price from swap")
    #
    #     for idx, pool_weight in enumerate(pool.weights):
    #         pool.weights[idx] = pool_weight / (10**18)
    #     return pool
    #
    # def _get_pool_prices_based_on_stable_factory(
    #     self, pool: PoolFinances, block_number: int
    # ) -> Optional[List[List[float]]]:
    #     factory_name = None
    #     for contract_name, contract in self.abi.items():
    #         if "Factory" in contract_name:
    #             try:
    #                 is_factory = contract.contract.functions.isPoolFromFactory(
    #                     Web3.toChecksumAddress(pool.address)
    #                 ).call(block_identifier=block_number)
    #             except ABIFunctionNotFound:
    #                 continue
    #             if is_factory:
    #                 factory_name = contract_name
    #                 break
    #     if factory_name in STABLE_POOL_FACTORIES:
    #         # setting prices to in case it's stable pools [[1, 1, 1], [1, 1, 1], [1, 1, 1]]
    #         # as in those pools we are assuming that proces are interchangable
    #         # more complicated math could be implemented, leaving for now
    #         prices = get_default_prices(len(pool.tokens_addresses))
    #     else:
    #         logs.error(
    #             "Not supported case for pairs with > 2 tokens and no weights and not stable"
    #         )
    #         return None
    #     return prices
    #
    # def _hydrate_pool_with_prices_for_burn_mint(
    #     self,
    #     pool: PoolFinances,
    #     parsed_event: dict,
    #     tokens_scalars: dict,
    #     block_number: int,
    # ) -> Optional[PoolFinances]:
    #     if_mint = self._find_if_mint(parsed_event["deltas"])
    #     pool.transaction_type = "mint" if if_mint else "burn"
    #     coin_count = len(pool.tokens_addresses)
    #     pool.prices = get_default_prices(coin_count)
    #     if pool.weights:
    #         # In case we have pool weights we can calculate spot proces mathematically
    #         for coin_index in range(0, coin_count):
    #             pool.prices[coin_index] = self.calculate_spot_prices(pool, coin_index)
    #     else:
    #         # Not working for all cases, assumming stable pools for now
    #         # if coin_count == 2:
    #         #     # Calculating based on price rate of one token against another, as we don't have
    #         #     # amount like in swap to calculate from them
    #         #     price_rate = self.abi[POOL_CONTRACT].contract.functions. \
    #         #         getRate().call(transaction={'to': Web3.toChecksumAddress(pool.address)},
    #         #                        block_identifier=block_number - 1)
    #         #     token0_price = price_rate / tokens_scalars[pool.tokens_addresses[1].lower()]
    #         #     token1_price = 1 / token0_price
    #         #     pool.prices = get_prices_for_two_pool(token0_price, token1_price)
    #         #     return pool
    #         # else:
    #         # if there are no weights and > 2 coins most likely stable pool
    #         pool.prices = self._get_pool_prices_based_on_stable_factory(pool, block_number)
    #         if not pool.prices:
    #             return None
    #     return pool
    #
    # def _hydrate_pool_with_prices_for_swap(
    #     self,
    #     pool: PoolFinances,
    #     parsed_event: dict,
    #     tokens_scalars: dict,
    #     block_number: int,
    # ) -> Optional[PoolFinances]:
    #     pool.transaction_type = "swap"
    #     tokens_addresses = [t_a.lower() for t_a in pool.tokens_addresses.copy()]
    #     token_in_idx = tokens_addresses.index(parsed_event["tokenIn"])
    #     token_out_idx = tokens_addresses.index(parsed_event["tokenOut"])
    #
    #     # to figure out new reserves after the swap (sync event simulation)
    #     pool.reserves[token_in_idx] = (
    #         pool.reserves[token_in_idx]
    #         + parsed_event["amountIn"] / tokens_scalars[parsed_event["tokenIn"]]
    #     )
    #     pool.reserves[token_out_idx] = (
    #         pool.reserves[token_out_idx]
    #         + parsed_event["amountOut"] / tokens_scalars[parsed_event["tokenOut"]]
    #     )
    #
    #     coin_count = len(pool.tokens_addresses)
    #     pool.prices = get_default_prices(coin_count)
    #
    #     if pool.weights:
    #         # In case we have pool weights we can calculate spot proces mathematically
    #         for coin_index in range(0, coin_count):
    #             pool.prices[coin_index] = self.calculate_spot_prices(pool, coin_index)
    #     else:
    #         if coin_count == 2:
    #             # Calculating prices using swap itself.
    #             swap_prices = [1, 1]
    #             swap_prices[token_in_idx] = parsed_event["amountIn"] / parsed_event["amountOut"]
    #             swap_prices[token_out_idx] = parsed_event["amountOut"] / parsed_event["amountIn"]
    #             pool.prices = get_prices_for_two_pool(swap_prices[0], swap_prices[1])
    #         else:
    #             # if there are no weights and > 2 coins most likely stable pool
    #             pool.prices = self._get_pool_prices_based_on_stable_factory(pool, block_number)
    #             if not pool.prices:
    #                 return None
    #     return pool
    #
    # def _get_pool_finances(
    #     self,
    #     base_pool: BasePool,
    #     parsed_event: dict,
    #     tokens_scalars: dict,
    #     block_number: int,
    # ) -> Optional[PoolFinances]:
    #     if parsed_event.get("deltas") and not self._find_if_mint(parsed_event["deltas"]):
    #         # In case it's Burn we need to get previous block reserves
    #         pool = self._resolve_pool_finances_metadata(
    #             base_pool, parsed_event, tokens_scalars, block_number - 1
    #         )
    #     else:
    #         pool = self._resolve_pool_finances_metadata(
    #             base_pool, parsed_event, tokens_scalars, block_number
    #         )
    #     if not pool:
    #         return None
    #     if parsed_event.get("deltas"):
    #         # In case it's Burn/Mint
    #         return self._hydrate_pool_with_prices_for_burn_mint(
    #             pool, parsed_event, tokens_scalars, block_number
    #         )
    #     if parsed_event.get("amountIn"):
    #         return self._hydrate_pool_with_prices_for_swap(
    #             pool, parsed_event, tokens_scalars, block_number
    #         )
    #
    # @staticmethod
    # def get_swap_from_swap_event(base_pool, parsed_event, tokens_scalars) -> Swap:
    #     amount_in = parsed_event["amountIn"] / tokens_scalars[parsed_event["tokenIn"]]
    #     amount_out = parsed_event["amountOut"] / tokens_scalars[parsed_event["tokenOut"]]
    #
    #     tokens_addresses = [t_a.lower() for t_a in base_pool.tokens_addresses.copy()]
    #     token_in_idx = tokens_addresses.index(parsed_event["tokenIn"])
    #     token_out_idx = tokens_addresses.index(parsed_event["tokenOut"])
    #     amounts = [0.0] * len(base_pool.tokens_addresses)
    #     amounts[token_in_idx] = amount_in
    #     amounts[token_out_idx] = amount_out
    #     swap = Swap(
    #         pool_address=base_pool.address,
    #         sender=base_pool.address,
    #         to=base_pool.address,
    #         amounts=amounts,
    #     )
    #     swap.direction_indexes = [token_in_idx, token_out_idx]
    #     return swap
