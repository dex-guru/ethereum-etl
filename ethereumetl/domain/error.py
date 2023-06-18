from dataclasses import dataclass
from typing import Any


@dataclass
class EthError:
    block_number: int
    timestamp: int
    kind: str
    data: dict[str, Any]
