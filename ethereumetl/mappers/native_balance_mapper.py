from typing import TypedDict

from ethereumetl.domain.native_balance import EthNativeBalance
from ethereumetl.enumeration.entity_type import EntityType


class EthNativeBalanceItem(TypedDict):
    type: str
    block_number: int
    address: str
    value: int


class NativeBalanceMapper:
    @staticmethod
    def native_balance_to_dict(native_balance: EthNativeBalance) -> EthNativeBalanceItem:
        return {
            'type': EntityType.NATIVE_BALANCE.value,
            'block_number': native_balance.block_number,
            'address': native_balance.address,
            'value': native_balance.value,
        }

    @staticmethod
    def native_balance_from_dict(native_balance_dict: EthNativeBalanceItem) -> EthNativeBalance:
        return EthNativeBalance(
            block_number=native_balance_dict['block_number'],
            address=native_balance_dict['address'],
            value=native_balance_dict['value'],
        )
