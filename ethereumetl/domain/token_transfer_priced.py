from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(slots=True)
class Direction:
    from_address: str
    to_address: str


@dataclass(slots=True)
class TokenTransferPriced:
    token_addresses: list[str]
    wallets: list[str]
    direction: Direction
    transaction_address: str
    block_number: int
    id: str
    transfer_type: Literal['erc20', 'erc721', 'erc1155']
    chain_id: int
    timestamp: int | None = None
    transaction_type: Literal['transfer'] = 'transfer'
    symbols: list[str] | None = None
    prices_stable: list[float] | None = None
    amount_stable: float | None = None
    amounts: list[float] | None = None
