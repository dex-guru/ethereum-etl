# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from typing import Any

from ethereumetl.domain.contract import EthContract


class EthContractMapper:
    @staticmethod
    def rpc_result_to_contract(contract_address, rpc_result) -> EthContract:
        contract = EthContract(
            address=contract_address,
            bytecode=rpc_result,
        )

        return contract

    @staticmethod
    def contract_to_dict(contract: EthContract) -> dict[str, Any]:
        return {
            'type': 'contract',
            'address': contract.address,
            'bytecode': contract.bytecode,
            'function_sighashes': contract.function_sighashes,
            'is_erc20': contract.is_erc20,
            'is_erc721': contract.is_erc721,
            'block_number': contract.block_number,
        }
