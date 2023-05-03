import dataclasses

from ethereumetl.domain.token_balance import EthTokenBalance
from ethereumetl.enumeration.entity_type import EntityType


class EthTokenBalanceMapper:
    @staticmethod
    def token_balance_to_dict(balance: EthTokenBalance) -> dict[str, int | str | None]:
        res = dataclasses.asdict(balance)
        res['type'] = EntityType.TOKEN_BALANCE
        return res
