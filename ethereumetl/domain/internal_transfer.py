from dataclasses import dataclass
from typing import Optional


@dataclass
class InternalTransfer:
    from_address: str
    to_address: Optional[str]
    value: int
    transaction_hash: str
    id: int
    gas_limit: int
