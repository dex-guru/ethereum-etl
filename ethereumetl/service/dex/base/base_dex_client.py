import json
from pathlib import Path
from typing import Literal

from web3 import Web3
from web3.contract import Contract
from web3.exceptions import BadFunctionCallOutput, ContractLogicError

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.domain.dex_trade import EthDexTrade
from ethereumetl.domain.receipt_log import ParsedReceiptLog
from ethereumetl.domain.token import EthToken
from ethereumetl.domain.token_transfer import EthTokenTransfer
from ethereumetl.service.dex.base.interface import DexClientInterface

to_checksum = Web3.to_checksum_address


class BaseDexClient(DexClientInterface):
    def __init__(self, web3: Web3, chain_id: int | None = None, file_path: str | None = __file__):
        self._w3 = web3
        self.chain_id = chain_id
        erc20_abi_path = Path(__file__).parent / "ERC20.json"
        self.erc20_contract_abi = self._w3.eth.contract(abi=json.loads(erc20_abi_path.read_text()))

    def resolve_asset_from_log(self, parsed_log: ParsedReceiptLog) -> EthDexPool | None:
        raise NotImplementedError()

    @staticmethod
    def _get_contract_topics_types_names(abi_contract: dict):
        topic_keccaks = {}
        topic_types = {}
        topic_names = {}
        topic_indexed_types = {}
        topic_indexed_names = {}
        for element in abi_contract:
            if element.get("name") and element.get("type") == "event":
                all_types = []
                indexed_types = []
                indexed_names = []
                types = []
                names = []

                for input in element["inputs"]:
                    all_types.append(input["type"])
                    if input.get("indexed"):
                        indexed_types.append(input["type"])
                        indexed_names.append(input["name"])
                    else:
                        types.append(input["type"])
                        names.append(input["name"])

                joined_input_types = ",".join(input for input in all_types)
                topic_keccaks[
                    Web3.keccak(text=f"{element['name']}({joined_input_types})")[0:4]
                ] = element["name"]
                topic_types[element["name"]] = types
                topic_names[element["name"]] = names
                topic_indexed_types[element["name"]] = indexed_types
                topic_indexed_names[element["name"]] = indexed_names

        return (
            topic_keccaks,
            topic_indexed_types,
            topic_indexed_names,
            topic_names,
            topic_types,
        )

    def _get_balance_of(
        self,
        token_address: str,
        address_to_check_balance,
        block_number: int | Literal["latest"] = "latest",
    ) -> int:
        try:
            return self.erc20_contract_abi.functions.balanceOf(
                to_checksum(address_to_check_balance)
            ).call(
                {"to": to_checksum(token_address)},
                block_number,
            )
        except (ValueError, TypeError, BadFunctionCallOutput, ContractLogicError):
            return 0

    @staticmethod
    def _get_scalars_for_tokens(tokens: list[EthToken], dex_pool: EthDexPool) -> list[int]:
        token_scalars = []
        for token_address in dex_pool.token_addresses:
            token = next((token for token in tokens if token.address == token_address), None)
            if not token:
                return []
            token_scalars.append(10**token.decimals)
        return token_scalars

    @staticmethod
    def _get_events_abi(contract_abi: type[Contract], event_name: str):
        return contract_abi.events[event_name]().abi

    def resolve_receipt_log(
        self,
        parsed_receipt_log: ParsedReceiptLog,
        dex_pool: EthDexPool,
        tokens_for_pool: list[EthToken],
        transfers_for_transaction: list[EthTokenTransfer],
    ) -> EthDexTrade | None:
        raise NotImplementedError()
