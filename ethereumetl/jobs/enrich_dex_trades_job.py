import logging
from collections import defaultdict
from copy import copy
from typing import Literal

from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.mappers.dex_trade_mapper import EnrichedDexTradeMapper
from ethereumetl.misc.info import NULL_ADDRESSES
from ethereumetl.service.detect_swap_owner import SwapOwnerDetectionService
from ethereumetl.service.price_service import PriceService


class EnrichDexTradeJob(BaseJob):
    def __init__(
        self,
        dex_pools,
        base_tokens_prices,
        tokens,
        token_transfers,
        internal_transfers,
        transactions,
        dex_trades,
        stablecoin_addresses,
        native_token,
        item_exporter,
    ):
        self._detect_owner_service = SwapOwnerDetectionService()
        self._enriched_dex_trade_mapper = EnrichedDexTradeMapper()
        self._price_service = PriceService(
            base_tokens_prices=base_tokens_prices,
            stablecoin_addresses=stablecoin_addresses,
            native_token=native_token,
        )

        self.item_exporter = item_exporter
        self._native_token_address = native_token['address']
        self._dex_pools_by_address = {dex_pool['address']: dex_pool for dex_pool in dex_pools}
        self._lp_addresses_by_pool_address = {
            dex_pool['address']: dex_pool['lp_token_addresses'] for dex_pool in dex_pools
        }
        self._tokens_by_address = {token['address']: token for token in tokens}
        self._token_transfers_by_hash = defaultdict(list)
        self.enriched_dex_trades: list[dict] = []
        self._dex_trades_by_hash = defaultdict(list)

        self._build_token_transfers_by_hash(token_transfers)
        self._build_token_transfers_from_internal_transfers(internal_transfers)
        self._build_token_transfers_from_transactions(transactions)

        for dex_trade in dex_trades:
            self._dex_trades_by_hash[dex_trade['transaction_hash']].append(dex_trade)

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self._enrich_all()

    def _end(self):
        self.item_exporter.close()

    def _enrich_all(self):
        for _dex_trades_by_hash in self._dex_trades_by_hash.values():
            self._enrich(_dex_trades_by_hash)

    def _build_token_transfers_by_hash(self, token_transfers):
        for transfer in token_transfers:
            self._token_transfers_by_hash[transfer['transaction_hash']].append(transfer)

    def _build_token_transfers_from_internal_transfers(self, internal_transfers):
        for transfer in internal_transfers:
            if transfer['value'] == 0:
                continue
            transfer['token_address'] = copy(self._native_token_address)
            self._token_transfers_by_hash[transfer['transaction_hash']].append(transfer)

    def _build_token_transfers_from_transactions(self, transactions):
        for tx in transactions:
            if tx['value'] == 0:
                continue
            transfer = {
                'value': tx['value'],
                'from_address': tx['from_address'],
                'to_address': tx['to_address'],
                'transaction_hash': tx['hash'],
                'block_number': tx['block_number'],
                'token_address': '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
            }
            self._token_transfers_by_hash[transfer['transaction_hash']].append(transfer)

    def _enrich(self, dex_trades):
        liquidity_events_by_event_type: dict[str, list[dict]] = {
            'burn': [],
            'mint': [],
        }

        for dex_trade in dex_trades:
            dex_pool = self._dex_pools_by_address.get(dex_trade['pool_address'])
            if not dex_pool:
                logging.warning(f'Could not find dex pool: {dex_trade["pool_address"]}')
                continue
            dex_trade['amounts'] = copy(dex_trade['token_amounts'])
            dex_trade['factory_address'] = dex_pool['factory_address']
            dex_trade['log_index'] = int(dex_trade['log_index'])
            dex_trade = self._price_service.resolve_price_for_trade(dex_trade)
            if not dex_trade:
                continue

            if dex_trade['event_type'] in ('burn', 'mint'):
                liquidity_events_by_event_type[dex_trade['event_type']].append(dex_trade)
            else:
                self._enrich_swap_event(dex_trade)

        # liquidity events needs to enrich as whole list because we need to merge some mints/burns
        self._enrich_liquidity_events(liquidity_events_by_event_type['burn'], event_type='burn')
        self._enrich_liquidity_events(liquidity_events_by_event_type['mint'], event_type='mint')

    def _enrich_liquidity_events(self, dex_trades, event_type: Literal['burn', 'mint']):
        def merge_events(_events_by_lp_token_address):
            _merged_events = []
            for event_list in _events_by_lp_token_address.values():
                if not event_list:
                    continue
                merged_event = event_list[0]
                merged_event['amounts'] = [
                    sum([i['amounts'][token_idx] for i in event_list])
                    for token_idx in range(len(event_list[0]['amounts']))
                ]
                merged_event['token_amounts'] = [
                    sum([i['token_amounts'][token_idx] for i in event_list])
                    for token_idx in range(len(event_list[0]['token_amounts']))
                ]

                _merged_events.append(merged_event)
            return _merged_events

        if not dex_trades:
            return

        lp_token_addresses = list({dex_trade['lp_token_address'] for dex_trade in dex_trades})
        merged_transfers = self._get_transfers_for_liquidity_events(
            self._token_transfers_by_hash.get(dex_trades[0]['transaction_hash'], []),
            event_type,
            lp_token_addresses,
        )
        events_by_lp_token_address = defaultdict(list)
        for dex_trade in dex_trades:
            events_by_lp_token_address[dex_trade['lp_token_address']].append(dex_trade)

        merged_events = merge_events(events_by_lp_token_address)

        for merged_event in merged_events:
            pool_address = merged_event['pool_address']
            pool = self._dex_pools_by_address[pool_address]
            transfers = merged_transfers.get(merged_event['lp_token_address'], [])
            transfers_amount_sum = sum([i['value'] for i in transfers])

            for transfer in transfers:
                amounts = copy(merged_event['amounts'])
                if len(events_by_lp_token_address.get(merged_event['lp_token_address'], [])) > 1:
                    amounts = [
                        (amount / transfers_amount_sum) * transfer['value'] for amount in amounts
                    ]
                sum_amount_stable = sum(
                    [
                        amount * price_stable
                        for amount, price_stable in zip(amounts, merged_event['prices_stable'])
                    ]
                )
                sum_amount_native = sum(
                    [
                        amount * price_native
                        for amount, price_native in zip(amounts, merged_event['prices_native'])
                    ]
                )
                reserves_stable = [
                    r * p
                    for r, p in zip(
                        merged_event['token_reserves'],
                        [
                            self._price_service.base_tokens_prices.get(
                                token_address.lower(), {}
                            ).get('price_stable', 0)
                            for token_address in pool['token_addresses']
                        ],
                    )
                ]
                reserves_native = [
                    r * p
                    for r, p in zip(
                        merged_event['token_reserves'],
                        [
                            self._price_service.base_tokens_prices.get(
                                token_address.lower(), {}
                            ).get('price_native', 0)
                            for token_address in pool['token_addresses']
                        ],
                    )
                ]

                event = {
                    'block_number': transfer['block_number'],
                    'log_index': merged_event['log_index'],
                    'transaction_hash': transfer['transaction_hash'],
                    'transaction_type': merged_event['event_type'],
                    'token_addresses': merged_event['token_addresses'],
                    'symbols': [
                        self._tokens_by_address[token_address]['symbol']
                        for token_address in merged_event['token_addresses']
                    ],
                    'wallet_address': transfer['to_address'],
                    'amounts': amounts,
                    'prices': merged_event['token_prices'],
                    'amount_stable': sum_amount_stable,
                    'amount_native': sum_amount_native,
                    'prices_stable': merged_event['prices_stable'],
                    'prices_native': merged_event['prices_native'],
                    'pool_address': pool_address,
                    'type': 'enriched_dex_trade',
                    'lp_token_address': merged_event['lp_token_address'],
                    'reserves': merged_event['token_reserves'],
                    'reserves_stable': reserves_stable,
                    'reserves_native': reserves_native,
                    'factory_address': merged_event['factory_address'],
                }

                lp_token = self._tokens_by_address[merged_event['lp_token_address']]
                if lp_token['decimals']:
                    event['amounts'].append(transfer['value'] / 10 ** lp_token['decimals'])
                    event['token_addresses'].append(merged_event['lp_token_address'])
                    event['symbols'].append(lp_token['symbol'])
                    event['amounts'].append(transfer['value'] / 10 ** lp_token['decimals'])
                    event['reserves'].append(lp_token['total_supply'])
                    event['prices_stable'].append(
                        sum(reserves_stable)
                        / (lp_token['total_supply'] / 10 ** lp_token['decimals'])
                        if lp_token['total_supply']
                        else 0
                    )
                    event['prices_native'].append(
                        sum(reserves_native)
                        / (lp_token['total_supply'] / 10 ** lp_token['decimals'])
                        if lp_token['total_supply']
                        else 0
                    )

                self.item_exporter.export_item(event)

    def _enrich_swap_event(self, swap_event):
        pool_address = swap_event['pool_address']
        swap_owner = self._detect_owner_service.get_swap_owner(
            self._token_transfers_by_hash[swap_event['transaction_hash']],
            pool=self._dex_pools_by_address[pool_address],
            all_pool_addresses=list(self._dex_pools_by_address.keys()),
        )
        reserves_stable = [
            r * p for r, p in zip(swap_event['token_reserves'], swap_event['prices_stable'])
        ]
        reserves_native = [
            r * p for r, p in zip(swap_event['token_reserves'], swap_event['prices_native'])
        ]
        event = {
            'block_number': swap_event['block_number'],
            'log_index': swap_event['log_index'],
            'transaction_hash': swap_event['transaction_hash'],
            'transaction_type': swap_event['event_type'],
            'token_addresses': swap_event['token_addresses'],
            'symbols': [
                self._tokens_by_address[token_address]['symbol']
                for token_address in swap_event['token_addresses']
            ],
            'wallet_address': swap_owner,
            'amounts': swap_event['amounts'],
            'prices': swap_event['token_prices'],
            'amount_stable': swap_event['amount_stable'],
            'amount_native': swap_event['amount_native'],
            'prices_stable': swap_event['prices_stable'],
            'prices_native': swap_event['prices_native'],
            'pool_address': pool_address,
            'lp_token_address': '',
            'reserves': swap_event['token_reserves'],
            'reserves_stable': reserves_stable,
            'reserves_native': reserves_native,
            'type': 'enriched_dex_trade',
            'factory_address': swap_event['factory_address'],
        }

        self.item_exporter.export_item(event)

    def _get_target_transfer(
        self,
        current_transfer: dict,
        _tokens: list[str],
        transfers: list[dict],
        lp_token_addresses: list[str],
        is_reverse: bool = False,
    ) -> dict:
        __transfers = {
            i['log_index']: i
            for i in transfers
            if (i['token_address'] == current_transfer['token_address'])
        }

        to_address, from_address = "to_address", "from_address"
        if is_reverse:
            to_address, from_address = from_address, to_address

        if (
            current_transfer['token_address'] in lp_token_addresses
            and current_transfer[from_address] in NULL_ADDRESSES
            and current_transfer[to_address] in NULL_ADDRESSES
        ):
            current_transfer[to_address] = transfers[0][from_address]
            return current_transfer

        if current_transfer[to_address] not in [i[from_address] for i in __transfers.values()]:
            return current_transfer

        for _transfer in __transfers.values():
            if (
                _transfer[from_address] == current_transfer[to_address]
                and _transfer['token_address'] in _tokens
            ):
                current_transfer = __transfers.pop(_transfer['log_index'])
                try:
                    transfers = [t for t in transfers if t['log_index'] != _transfer['log_index']]
                except KeyError:
                    print(_transfer)
                return self._get_target_transfer(
                    current_transfer,
                    _tokens,
                    transfers,
                    lp_token_addresses,
                    is_reverse,
                )

        return current_transfer

    def _get_transfers_for_liquidity_events(
        self,
        transfers: list[dict],
        event_type: Literal['burn', 'mint'],
        lp_token_addresses: list[str],
    ) -> dict[str, list[dict]]:
        if not transfers:
            return {}
        is_reverse = event_type == "burn"

        to_address, from_address = "to_address", "from_address"
        if is_reverse:
            to_address, from_address = from_address, to_address

        transfers_with_null_address: list[dict] = [
            _transfer for _transfer in transfers if _transfer[from_address] in NULL_ADDRESSES
        ]

        final_transfers_in_chains = []
        for _transfer in transfers_with_null_address:
            final_transfers_in_chains.append(
                self._get_target_transfer(
                    _transfer,
                    [t['token_address'].lower() for t in transfers_with_null_address],
                    transfers,
                    lp_token_addresses,
                    is_reverse=is_reverse,
                )
            )

        transfers_by_target_address = defaultdict(list)
        for transfer_ in final_transfers_in_chains:
            transfers_by_target_address[
                f"{transfer_[to_address]}-{transfer_['token_address']}"
            ].append(transfer_)

        merged_transfers: list[dict] = []
        for transfers in transfers_by_target_address.values():
            merged_transfers.append(
                {
                    "token_address": transfers[0]['token_address'],
                    "block_number": transfers[0]['block_number'],
                    "transaction_hash": transfers[0]['transaction_hash'],
                    "log_index": transfers[0]['log_index'],
                    "from_address": transfers[0]['from_address'],
                    "to_address": transfers[0]['to_address'],
                    "value": sum([t['value'] for t in transfers]),
                }
            )

        result_transfers: dict[str, list[dict]] = defaultdict(list)
        for _transfer in merged_transfers:
            result_transfers[_transfer['token_address']].append(_transfer)

        return result_transfers
