from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class InternalTransfer:
    from_address: str
    to_address: Optional[str]
    value: int
    transaction_hash: Optional[str]
    block_number: Optional[int]
    block_timestamp: Optional[datetime]
    id: Optional[int]
    gas_limit: Optional[int]
