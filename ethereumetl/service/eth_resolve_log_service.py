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
from collections import OrderedDict, defaultdict
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
from ethereumetl.utils import Singleton

logger = logging.getLogger('eth_pool_service')

ABI = Sequence[Mapping[str, Any]]
EventABI = dict[str, Any]
FilePath = str
to_checksum = Web3.to_checksum_address


class EthResolveLogService(metaclass=Singleton):
    def __init__(self, web3, chain_id):
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
                events_inventory[sighash_topic_count]['namespaces'].add(str(parent_path))
                events_inventory[sighash_topic_count]['contract_name'].add(str(file_name[:-5]))
                events_inventory[sighash_topic_count]['event_abi_by_namespace'][
                    str(parent_path)
                ] = event_abi
                continue

            events_inventory[(event_signature_hash, event_topic_count)] = {
                'event_signature': _abi_to_signature(event_abi),
                'event_name': event_abi['name'],
                'namespaces': {str(parent_path)},
                'contract_name': {file_name[:-5]},
                'event_abi_json': event_abi,
                'event_abi_by_namespace': {str(parent_path): event_abi},
                'contract': self._web3.eth.contract(abi=[event_abi]),
            }
        self.events_inventory = events_inventory

    def _get_event_inventory_for_log(self, log: EthReceiptLog) -> dict | None:
        if not log.topics:
            return None
        sighash_with_topics_count = (log.topics[0], len(log.topics))
        event: dict | None = self.events_inventory.get(sighash_with_topics_count, None)
        return event

    def parse_log(
        self, log: EthReceiptLog, filter_for_events: list[str] | None = None
    ) -> ParsedReceiptLog | None:
        event = self._get_event_inventory_for_log(log)
        if not event:
            return None
        if filter_for_events and event['event_name'] not in filter_for_events:
            return None
        contract = event['contract']
        event_abi = getattr(contract.events, event['event_name'], None)
        if not event_abi:
            return None
        try:
            parsed_event = event_abi().process_log(self._to_hex_log(log))
            input_names = [i['name'] for i in event['event_abi_json']['inputs']]
            ordered_args = OrderedDict(
                (input_name, parsed_event.args[input_name]) for input_name in input_names
            )
        except (MismatchedABI, LogTopicError, TypeError) as e:
            logging.warning(f"Failed to parse event: {e}")
            return None

        return ParsedReceiptLog(
            transaction_hash=log.transaction_hash,
            block_number=log.block_number,
            log_index=log.log_index,
            event_name=event['event_name'],
            namespaces=event['namespaces'],
            address=log.address,
            parsed_event=ordered_args,
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

    def resolve_log(
        self,
        parsed_log: ParsedReceiptLog,
        dex_pool: EthDexPool,
        tokens_for_pool: list[EthToken],
        transfers_for_transaction: list[EthTokenTransfer],
    ) -> EthDexTrade | None:
        namespace = self._dex_client_factory.get_namespace_by_factory(dex_pool.factory_address)
        if namespace and self._dex_client_factory.get(namespace):
            namespaces = (namespace,)
        else:
            namespaces = parsed_log.namespaces

        for namespace in namespaces:
            dex_client = self._dex_client_factory.get(namespace)
            if not dex_client:
                logging.debug(f"Failed to get dex client for namespace: {namespace}")
                continue
            try:
                resolved_log = dex_client.resolve_receipt_log(
                    parsed_receipt_log=parsed_log,
                    dex_pool=dex_pool,
                    tokens_for_pool=tokens_for_pool,
                    transfers_for_transaction=transfers_for_transaction,
                )
            except Exception as e:
                logging.info(f"Failed to resolve log: {e}", exc_info=True)
                continue
            if resolved_log:
                resolved_log.amm = self._dex_client_factory.get_dex_name_by_factory_address(
                    dex_pool.factory_address
                )
                return resolved_log

    def resolve_asset_from_log(self, parsed_log: ParsedReceiptLog) -> EthDexPool | None:
        for namespace in parsed_log.namespaces:
            dex_client = self._dex_client_factory.initiated_adapters.get(namespace)
            if not dex_client:
                logging.debug(f"Failed to get dex client for namespace: {namespace}")
                continue
            try:
                asset = dex_client.resolve_asset_from_log(parsed_log)
            except Exception as e:
                logging.error(f"Failed to resolve asset from log: {e}", exc_info=True)
                continue
            if asset:
                return asset
        return None
