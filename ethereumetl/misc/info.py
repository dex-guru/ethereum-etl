import json
from functools import cache
from pathlib import Path

NULL_ADDRESSES = {
    '0x0000000000000000000000000000000000000000',
    '0x0000000000000000000000000000000000000001',
    '0x0000000000000000000000000000000000000002',
    '0x0000000000000000000000000000000000000003',
    '0x0000000000000000000000000000000000000004',
    '0x0000000000000000000000000000000000000005',
    '0x0000000000000000000000000000000000000006',
    '0x0000000000000000000000000000000000000007',
    '0x0000000000000000000000000000000000000008',
    '0x0000000000000000000000000000000000000009',
    '0x000000000000000000000000000000000000dead',
    '0x1111111111111111111111111111111111111111',
    '0x2222222222222222222222222222222222222222',
    '0x3333333333333333333333333333333333333333',
    '0x4444444444444444444444444444444444444444',
    '0x6666666666666666666666666666666666666666',
    '0x8888888888888888888888888888888888888888',
    '0x1234567890123456789012345678901234567890',
    '0xdead000000000000000042069420694206942069',
    '0x0123456789012345678901234567890123456789',
    '0x00000000000000000000045261d4ee77acdb3286',
    '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
    '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
    '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
    '0xffffffffffffffffffffffffffffffffffffffff',
}

PARSABLE_TRADE_EVENTS = [
    "Swap",
    "Burn",
    "Mint",
    "Sync",  # uniswap v2, v3, kyber
    "RemoveLiquidityImbalance",
    "Deposit",
    "Withdraw",
    "BuyBaseToken",
    "SellBaseToken",  # DODO v1
    "DODOSwap",  # DODO v2
    "Deposited",
    "Withdrawn",
    "Swapped",  # 1inch mooniswap v1,
    "Conversion",
    "LiquidityAdded",
    "LiquidityRemoved",
    "TokenRateUpdate",  # Bancor v2
    "ExchangeNeg",
    "ExchangePos",  # Meshswap
    "TokenSwap",
    "TokenSwapUnderlying",  # Saddle
    "PoolBalanceChanged",  # Balancer
    "Collect",  # QuickSwap v3
    "DepositedToBins",
    "WithdrawnFromBins",  # TraderJoe v2.1
    "TokenSwap",
    "TokenSwapUnderlying",
    "TokenExchange",
    "TokenExchangeUnderlying",
    "AddLiquidity",
    "RemoveLiquidity",
    "RemoveLiquidityOne",
    "RemoveLiquidityImbalance",
    "Buy",
    "Sell",
]

INFINITE_PRICE_THRESHOLD = 9.999999999999999e45


@cache
def get_chain_config(chain_id) -> dict:
    def all_addresses_to_lower(item: dict):
        for key in item:
            if isinstance(item[key], str) and item[key].startswith('0x'):
                item[key] = item[key].lower()
            elif isinstance(item[key], dict):
                all_addresses_to_lower(item[key])

        return item

    with open(Path(__file__).parent.parent / 'chains_config.json') as file:
        data = json.load(file)
        # Search for the dictionary with the matching chain_id
    for chain in data:
        if chain.get("id") == chain_id:
            all_addresses_to_lower(chain)
            return chain
    raise ValueError(f'Chain id {chain_id} not found in chains_config.json')
