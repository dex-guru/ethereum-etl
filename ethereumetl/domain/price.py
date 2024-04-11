from dataclasses import dataclass


@dataclass(slots=True)
class Price:
    token_address: str
    price_stable: float | int
    price_native: float | int
    score: int
    type = 'base_token_price'
