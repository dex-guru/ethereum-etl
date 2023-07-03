from dataclasses import dataclass


@dataclass(slots=True)
class InternalTransfer:
    from_address: str
    to_address: str | None
    value: int
    transaction_hash: str
    id: int
    gas_limit: int
