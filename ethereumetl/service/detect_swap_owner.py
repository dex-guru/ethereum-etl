import logging
from collections import defaultdict
from copy import deepcopy
from functools import partial

from ethereumetl.misc.info import NULL_ADDRESSES


class SwapOwnerDetectionService:
    def __init__(self):
        self._all_pool_addresses = []

    def get_swap_owner(
        self,
        transfers_for_transaction: list[dict],
        pool: dict,
        all_pool_addresses: list[str] | None = None,
    ) -> str:
        """
        Get swap owner by emitted transfers from transaction.

        We potentially can detect different owners of transaction, but priority of methods:
        bot, regular swap, some other methods and only if we can't detect anything then owner of tx
        """
        if all_pool_addresses:
            self._all_pool_addresses = all_pool_addresses
        else:
            self._all_pool_addresses = [pool['address']]

        potential_arbitrage = self.get_arbitrage_bot_owner(transfers_for_transaction)
        if potential_arbitrage in self._all_pool_addresses:
            potential_arbitrage = None

        potential_regular_swap_owner = self.get_regular_swap_owner(transfers_for_transaction, pool)
        if potential_regular_swap_owner:
            return potential_regular_swap_owner
        elif potential_arbitrage:
            return potential_arbitrage
        else:
            return pool['address'].lower()

    @staticmethod
    def get_arbitrage_bot_owner(
        transfers_for_transaction: list[dict],
    ) -> str | None:
        """
        Detect arbitrage bot by transfers.

        BOT → A → B → C → … → A → BOT. Arbitrage bot always sends and gets only one asset.
        https://www.notion.so/dexguru/Wrong-definition-of-initiator-of-TX-434e714f4d36453ebba9c39080fb953c
        """
        erc_20_transfers = deepcopy(transfers_for_transaction)
        # filter self address send transfers
        erc_20_transfers = [i for i in erc_20_transfers if i['from_address'] != i['to_address']]
        if not erc_20_transfers:
            return None

        # build a tokens moving dict
        trade_dict: dict[str, dict[str, dict[str, int]]] = defaultdict(
            lambda: defaultdict(partial(defaultdict, int))
        )
        for transfer in erc_20_transfers:
            trade_dict[transfer['from_address'].lower()][transfer['token_address'].lower()][
                "val"
            ] -= transfer['value']
            trade_dict[transfer['from_address'].lower()][transfer['token_address'].lower()][
                "cnt"
            ] += 1
            trade_dict[transfer['to_address'].lower()][transfer['token_address'].lower()][
                "val"
            ] += transfer['value']
            trade_dict[transfer['to_address'].lower()][transfer['token_address'].lower()][
                "cnt"
            ] += 1

        for wallet, tokens in trade_dict.items():
            traded_tokens = [
                token for token, value in tokens.items() if value["val"] > 0 and value["cnt"] > 1
            ]
            if traded_tokens and wallet not in NULL_ADDRESSES:
                # Bot should SEND and RECEIVE asset.
                if wallet in [_['from_address'] for _ in erc_20_transfers] and wallet in [
                    _['to_address'] for _ in erc_20_transfers
                ]:
                    logging.info(
                        f"Arbitrage bot detected: {wallet}",
                        extra={"transaction_hash": erc_20_transfers[-1]['transaction_hash']},
                    )
                    return wallet
        return None

    @staticmethod
    def get_proxy_like_nodes(
        transfers_for_transaction: list[dict],
    ) -> list[str]:
        """
        A → PROXY → A. Proxies receive and send same asset with same amount.
        """
        erc_20_transfers = deepcopy(transfers_for_transaction)
        if not erc_20_transfers:
            return []

        # build a tokens moving dict
        trade_dict: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        for transfer in erc_20_transfers:
            trade_dict[transfer['from_address'].lower()][
                transfer['token_address'].lower()
            ] -= transfer['value']
            trade_dict[transfer['to_address'].lower()][
                transfer['token_address'].lower()
            ] += transfer['value']

        result = []
        for wallet, tokens in trade_dict.items():
            sum_value = sum([value for token, value in tokens.items()])
            if sum_value == 0:
                result.append(wallet)
        return result

    @staticmethod
    def transform_data_to_edges(transfers_for_transaction: list[dict]) -> list[tuple]:
        edges_from_transfers = [
            (transfer['from_address'].lower(), transfer['to_address'].lower())
            for transfer in transfers_for_transaction
        ]
        return edges_from_transfers

    @staticmethod
    def transform_edges_to_dict(edges: list[tuple[str, str]]) -> dict:
        d = defaultdict(set)
        for edge in edges:
            d[edge[0]].add(edge[1])
        return d

    def dfs(
        self,
        v: str,
        edges_dict: dict,
        used: dict[str, bool],
        potential_owners: set[str],
    ):
        used[v] = True
        for edge in edges_dict[v]:
            if not used[edge]:
                self.dfs(edge, edges_dict, used, potential_owners)
            elif edge not in NULL_ADDRESSES:
                potential_owners.add(edge)

    @staticmethod
    def fill_used(edges: list[tuple[str, str]]) -> dict[str, bool]:
        used: dict[str, bool] = {}
        for edge in edges:
            used[edge[0]] = False
            used[edge[1]] = False
        return used

    def get_regular_swap_owner(
        self,
        transfers_for_transaction: list[dict],
        pool: dict | None = None,
    ) -> str | None:
        if not pool:
            return None

        edges = self.transform_data_to_edges(transfers_for_transaction)

        potential_owners = list(set([i[0] for i in edges] + [i[1] for i in edges]))

        # filter pools from owners
        potential_owners = [i for i in potential_owners if i not in self._all_pool_addresses]

        # filter routers and proxy
        proxies = self.get_proxy_like_nodes(transfers_for_transaction)
        potential_owners = [i for i in potential_owners if i not in proxies]

        # filter null address
        potential_owners = [i for i in potential_owners if i not in NULL_ADDRESSES]

        detected_owners = []

        for address in potential_owners:
            transfers_for_address = [
                t
                for t in transfers_for_transaction
                if address == t['to_address'] or address == t['from_address']
            ]

            if transfers_for_address:
                tokens = {i['token_address'] for i in transfers_for_address}
                if (
                    len(tokens) > 1
                    and set(tokens).intersection(set(pool['token_addresses']))
                    and address not in self._all_pool_addresses
                ):
                    detected_owners.append(address)

        if len(detected_owners) == 1:
            return detected_owners[0]
        return None
