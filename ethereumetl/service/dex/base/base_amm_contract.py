from clients.blockchain.amm.base.base_contract import BaseContract
from clients.blockchain.models.abi_contract import ABIContract


class BaseAmm(BaseContract):
    pool_contract_name: str | list[str] = ""

    def get_pool_contract(self) -> ABIContract | list[ABIContract] | None:
        if isinstance(self.pool_contract_name, list):
            return [self.abi.get(x) for x in self.pool_contract_name]
        return self.abi.get(self.pool_contract_name)
