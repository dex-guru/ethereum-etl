from dataclasses import dataclass

from ethereumetl.domain.token_transfer import TokenStandard


@dataclass
class EthTokenBalance:
    """
    Represents an ERC20 or ERC721 or ERC1155 token balance of an address after the block is
    processed.

    fields:
        token_address: str (HexStr) - The address of the token contract. Should start with "0x".
        holder_address: str (HexStr) - The address of the token holder. Should start with "0x".
        block_number: int (UInt64) - The block number where the token balance was updated.
        value: int (UInt256) - The token balance.
        token_id: int (UInt256) The token id for ERC721 and ERC1155 tokens. 0 for ERC20 tokens.
    """

    token_address: str
    token_standard: TokenStandard
    holder_address: str
    block_number: int
    token_id: int
    value: int
