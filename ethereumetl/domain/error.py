from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class EthError:
    block_number: int
    timestamp: int
    kind: str
    data: dict[str, Any]
