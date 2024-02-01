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
import json
import logging
from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, cast

from eth_utils import event_abi_to_log_topic, to_hex
from eth_utils.abi import _abi_to_signature
from hexbytes import HexBytes
from web3 import Web3
from web3.exceptions import LogTopicError, MismatchedABI

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.domain.dex_trade import EthDexTrade
from ethereumetl.domain.receipt_log import EthReceiptLog, ParsedReceiptLog
from ethereumetl.domain.token import EthToken
from ethereumetl.domain.token_transfer import EthTokenTransfer
from ethereumetl.service.dex.dex_client_factory import ContractAdaptersFactory

logger = logging.getLogger('eth_pool_service')

ABI = Sequence[Mapping[str, Any]]
EventABI = dict[str, Any]
FilePath = str
to_checksum = Web3.to_checksum_address


class EthResolveLogService:
    def __init__(self, web3, chain_id=None):
        self._web3: Web3 = web3
        self._chain_id = chain_id
        self._events_inventory = {}
        self._dex_client_factory = ContractAdaptersFactory(self._web3, self._chain_id)
        self._possible_dex_types = self._dex_client_factory.get_all_supported_dex_types()
        self.__initiate_events_inventory()

    def __initiate_events_inventory(self):
        events_inventory: dict[tuple[str, int], dict[str, Any]] = defaultdict()

        def read_event_abis():
            abi_dir = Path(__file__).parent / 'dex'
            assert abi_dir.is_dir()

            for file_path in abi_dir.rglob('*.json'):
                with file_path.open() as f:
                    data = json.load(f)

                assert isinstance(data, list)

                rel_path = file_path.relative_to(abi_dir)
                yield str(rel_path), data

        def get_event_abis_with_file_paths(abis):
            for file_path_, abi_objects in abis:
                for abi_object in abi_objects:
                    match abi_object:
                        case {
                            "anonymous": False,
                            "type": "event",
                            "name": str(),
                            "inputs": list(),
                        }:
                            yield file_path_, cast(dict, abi_object)

        event_abis = get_event_abis_with_file_paths(read_event_abis())
        for file_path, event_abi in event_abis:
            parent_path, file_name = Path(file_path).parent, Path(file_path).name
            event_signature_hash = to_hex(event_abi_to_log_topic(event_abi)).casefold()
            event_topic_count = sum(1 for i in event_abi['inputs'] if i['indexed']) + 1
            sighash_topic_count = (event_signature_hash, event_topic_count)
            if events_inventory.get(sighash_topic_count):
                events_inventory[sighash_topic_count]['namespace'].add(str(parent_path))
                events_inventory[sighash_topic_count]['contract_name'].add(str(file_name[:-5]))
                events_inventory[sighash_topic_count]['event_abi_json_list'].append(event_abi)
                continue

            events_inventory[(event_signature_hash, event_topic_count)] = {
                'event_signature': _abi_to_signature(event_abi),
                'event_name': event_abi['name'],
                'namespace': {str(parent_path)},
                'contract_name': {file_name[:-5]},
                'event_abi_json': event_abi,
                'event_abi_json_list': [event_abi],
            }
        self.events_inventory = events_inventory

    def _get_event_inventory_for_log(self, log: EthReceiptLog) -> dict | None:
        if not log.topics:
            return None
        sighash_with_topics_count = (log.topics[0], len(log.topics))
        event: dict | None = self.events_inventory.get(sighash_with_topics_count, None)
        return event

    def parse_log(self, log: EthReceiptLog) -> ParsedReceiptLog | None:
        event = self._get_event_inventory_for_log(log)
        if not event:
            return None
        for event_abi in event['event_abi_json_list']:
            contract = self._web3.eth.contract(abi=[event_abi])
            event_abi = getattr(contract.events, event['event_name'], None)
            if not event_abi:
                logging.debug(f"Event method not found: {event['event_name']}")
                continue
            try:
                parsed_event = event_abi().process_log(self._to_hex_log(log))
                break
            except (MismatchedABI, LogTopicError, TypeError) as e:
                logging.debug(f"Failed to parse event: {e}")
        else:
            logger.warning(f"Could not parse log {log}")
            return None

        return ParsedReceiptLog(
            transaction_hash=log.transaction_hash,
            block_number=log.block_number,
            log_index=log.log_index,
            event_name=event['event_name'],
            namespace=event['namespace'],
            address=log.address,
            parsed_event={**parsed_event.args},
        )

    @staticmethod
    def _to_hex_log(receipt_log: EthReceiptLog):
        return {
            "address": to_checksum(receipt_log.address),
            "topics": [HexBytes(topic) for topic in receipt_log.topics],
            "data": HexBytes(receipt_log.data),
            "blockNumber": receipt_log.block_number,
            "transactionHash": HexBytes(receipt_log.transaction_hash),
            "transactionIndex": receipt_log.transaction_index,
            "blockHash": HexBytes(receipt_log.block_hash) if receipt_log.block_hash else None,
            "logIndex": receipt_log.log_index,
        }

    def get_dex_pool(self, log: ParsedReceiptLog) -> EthDexPool | None:
        dex_pool = self._dex_client_factory.resolve_asset_from_log(log)
        return dex_pool

    def resolve_log(
        self,
        parsed_log: ParsedReceiptLog,
        dex_pool: EthDexPool | None = None,
        tokens_for_pool: list[EthToken] | None = None,
        transfers_for_transaction: list[EthTokenTransfer] | None = None,
    ) -> EthDexTrade | None:
        return self._dex_client_factory.resolve_log(
            parsed_log, dex_pool, tokens_for_pool, transfers_for_transaction
        )
