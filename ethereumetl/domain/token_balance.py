from dataclasses import dataclass
from typing import Optional, TypeVar

UInt256 = TypeVar('UInt256', bound=int)
UInt64 = TypeVar('UInt64', bound=int)
HexStr = TypeVar('HexStr', bound=str)


@dataclass
class TokenBalance:
    """
    Represents an ERC20 or ERC721 or ERC1155 token balance of an address after the block is
    processed.

    fields:
        token_address: str (HexStr) - The address of the token contract. Should start with "0x".
        holder_address: str (HexStr) - The address of the token holder. Should start with "0x".
        block_number: int (UInt64) - The block number where the token balance was updated.
        value: int (UInt256) - The token balance.
        token_id: int (UInt256) | None - The token id for ERC721 and ERC1155 tokens. None for
            ERC20 tokens.
    """

    token_address: HexStr
    holder_address: HexStr
    block_number: UInt64
    value: UInt256
    token_id: Optional[UInt256] = None
