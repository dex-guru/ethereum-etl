from web3 import Web3

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.domain.dex_trade import EthDexTrade
from ethereumetl.domain.receipt_log import ParsedReceiptLog
from ethereumetl.domain.token import EthToken
from ethereumetl.domain.token_transfer import EthTokenTransfer
from ethereumetl.service.dex.base.interface import DexClientInterface


class BaseDexClient(DexClientInterface):
    def __init__(self, web3: Web3, chain_id: int | None = None):
        self._w3 = web3

    def resolve_asset_from_log(self, parsed_log: ParsedReceiptLog) -> EthDexPool | None:
        return None

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

    def get_base_pool(self, address: str) -> EthDexPool | None:
        raise NotImplementedError()

    def resolve_receipt_log(
        self,
        parsed_receipt_log: ParsedReceiptLog,
        dex_pool: EthDexPool | None = None,
        tokens_for_pool: list[EthToken] | None = None,
        transfers_for_transaction: list[EthTokenTransfer] | None = None,
    ) -> EthDexTrade | None:
        return None
