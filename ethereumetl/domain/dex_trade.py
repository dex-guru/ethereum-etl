from dataclasses import dataclass
from typing import Literal


@dataclass(slots=True)
class EthDexTrade:
    token_amounts_raw: list[int]
    pool_address: str
    transaction_hash: str
    log_index: int
    block_number: int
    event_type: Literal["swap", "burn", "mint"]
    token_reserves: list[float | int]
    token_prices: list[float]
    lp_token_address: str | None = None


@dataclass(slots=True)
class EnrichedDexTrade:
    block_number: int
    block_hash: str
    block_timestamp: int
    transaction_hash: str
    log_index: int
    transaction_type: Literal["swap", "burn", "mint"]
    token_addresses: list[str]
    symbols: list[str]
    amounts: list[float]
    amount_stable: float
    amount_native: float
    prices_stable: list[float]
    prices_native: list[float]
    pool_address: str
    wallet_address: str
    reserves: list[float]
    reserves_stable: list[float]
    reserves_native: list[float]
    lp_token_address: str = ''
