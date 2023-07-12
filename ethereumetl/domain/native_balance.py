from dataclasses import dataclass


@dataclass(slots=True)
class EthNativeBalance:
    block_number: int
    address: str
    value: int
