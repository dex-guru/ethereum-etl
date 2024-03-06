from copy import copy


class PriceService:
    def __init__(
        self,
        base_tokens_prices: list[dict],
        stablecoin_addresses: list[str],
        native_token: dict,
    ):
        self.stablecoin_addresses = [address.lower() for address in stablecoin_addresses]
        self.native_token = native_token
        self.native_token['address'] = self.native_token['address'].lower()
        self.base_tokens_prices = {
            base_token_price['token_address'].lower(): {
                'price_stable': base_token_price['price_stable'],
                'price_native': base_token_price['price_native'],
                'score': base_token_price.get('score', 0),
            }
            for base_token_price in base_tokens_prices
        }

    def set_base_prices_for_trade(self, dex_trade: dict):
        prices = {}
        self._ensure_base_prices(dex_trade)
        base_token = self._get_base_token(dex_trade)
        for token_address in dex_trade['token_addresses']:
            prices[token_address] = self.base_tokens_prices.get(token_address, {})
        dex_trade['prices_stable'] = [
            prices[token_address]['price_stable'] for token_address in dex_trade['token_addresses']
        ]
        dex_trade['prices_native'] = [
            prices[token_address]['price_native'] for token_address in dex_trade['token_addresses']
        ]
        dex_trade['amount_stable'] = prices[base_token]['price_stable'] * abs(
            dex_trade['amounts'][dex_trade['token_addresses'].index(base_token)]
        )
        dex_trade['amount_native'] = prices[base_token]['price_native'] * abs(
            dex_trade['amounts'][dex_trade['token_addresses'].index(base_token)]
        )
        return dex_trade

    def resolve_price_for_trade(self, dex_trade: dict):
        """Resolves the prices of tokens in a DEX trade."""
        self._initialize_prices(dex_trade)
        self._ensure_base_prices(dex_trade)
        if self._trade_involves_stablecoin(dex_trade):
            dex_trade = self._resolve_prices_for_pools_with_stablecoin(dex_trade)
        if self._trade_involves_native_token(dex_trade):
            dex_trade = self._resolve_prices_for_pools_with_native_token(dex_trade)
        if all(dex_trade['prices_stable']) and all(dex_trade['prices_native']):
            self._update_base_prices(dex_trade)
            return dex_trade

        dex_trade = self._resolve_prices_for_generic_trade(dex_trade)
        self._update_base_prices(dex_trade)
        return dex_trade

    def _update_base_prices(self, dex_trade):
        """Updates the base token prices based on the trade."""
        for idx, token_address in enumerate(dex_trade['token_addresses']):
            price_stable = dex_trade['prices_stable'][idx]
            price_native = dex_trade['prices_native'][idx]
            if token_address not in self.base_tokens_prices.keys():
                self.base_tokens_prices[token_address] = {
                    'price_stable': dex_trade['prices_stable'][idx],
                    'price_native': dex_trade['prices_native'][idx],
                    'score': 1,
                }
                continue
            if price_stable:
                self.base_tokens_prices[token_address]['price_stable'] = copy(price_stable)
            if price_native:
                self.base_tokens_prices[token_address]['price_native'] = copy(price_native)

    @staticmethod
    def _initialize_prices(dex_trade):
        """Initializes price arrays in the dex_trade dictionary."""
        token_count = len(dex_trade['token_addresses'])
        dex_trade['prices_stable'] = [0.0] * token_count
        dex_trade['prices_native'] = [0.0] * token_count
        dex_trade['amount_stable'] = 0.0
        dex_trade['amount_native'] = 0.0

    def _ensure_base_prices(self, dex_trade):
        """Ensures that the base token prices are present in the trade."""
        for token_address in dex_trade['token_addresses']:
            if token_address not in self.base_tokens_prices.keys():
                self.base_tokens_prices[token_address] = {
                    'price_stable': 0.0,
                    'price_native': 0.0,
                    'score': 0,
                }

    def _trade_involves_stablecoin(self, dex_trade):
        """Checks if the trade involves a stablecoin."""
        return any(
            token_address in self.stablecoin_addresses
            for token_address in dex_trade['token_addresses']
        )

    def _trade_involves_native_token(self, dex_trade):
        """Checks if the trade involves a native token."""
        return self.native_token['address'] in dex_trade['token_addresses']

    def _get_base_token(self, dex_trade):
        """Gets the base token for the trade."""
        _score = -1
        _token_address = None
        for token_address in dex_trade['token_addresses']:
            if self.base_tokens_prices[token_address]['score'] > _score:
                _score = self.base_tokens_prices[token_address]['score']
                _token_address = token_address
        return _token_address

    def _resolve_prices_for_generic_trade(self, dex_trade):
        """Resolves prices for trades without stablecoins."""
        if len(dex_trade['token_addresses']) != 2:
            # Handling for trades with more than two tokens can be added here
            return dex_trade

        base_token_address = self._get_base_token(dex_trade)

        for idx, token_address in enumerate(dex_trade['token_addresses']):
            if token_address == base_token_address:
                base_price = self.base_tokens_prices.get(token_address, {})
                self._calculate_token_prices(dex_trade, idx, base_price)
                break

        return dex_trade

    def _calculate_token_prices(self, dex_trade, idx, base_price):
        """Calculates and updates the token prices in the trade."""
        opposite_idx = 1 - idx
        opposite_token_ratio = dex_trade['token_prices'][opposite_idx][idx]

        self._update_trade_prices(
            dex_trade,
            idx,
            opposite_idx,
            base_price,
            opposite_token_ratio,
        )

    def _update_trade_prices(
        self,
        dex_trade,
        idx_base,
        opposite_idx,
        base_price,
        opposite_token_ratio,
    ):
        """Updates the trade prices based on the base and opposite token prices."""
        if (
            not dex_trade['prices_stable'][idx_base]
            or not dex_trade['prices_stable'][opposite_idx]
        ):
            if base_price['price_stable']:
                dex_trade['prices_stable'][idx_base] = copy(base_price['price_stable'])
                try:
                    dex_trade['prices_stable'][opposite_idx] = (
                        base_price['price_stable'] / opposite_token_ratio
                    )
                except ZeroDivisionError:
                    dex_trade['prices_stable'][opposite_idx] = copy(
                        self.base_tokens_prices[dex_trade['token_addresses'][opposite_idx]][
                            'price_stable'
                        ]
                    )
                dex_trade['amount_stable'] = base_price['price_stable'] * abs(
                    dex_trade['amounts'][idx_base]
                )

        if (
            not dex_trade['prices_native'][idx_base]
            or not dex_trade['prices_native'][opposite_idx]
        ):
            if base_price['price_native']:
                dex_trade['prices_native'][idx_base] = copy(base_price['price_native'])
                try:
                    dex_trade['prices_native'][opposite_idx] = (
                        base_price['price_native'] / opposite_token_ratio
                    )
                except ZeroDivisionError:
                    dex_trade['prices_native'][opposite_idx] = copy(
                        self.base_tokens_prices[dex_trade['token_addresses'][opposite_idx]][
                            'price_native'
                        ]
                    )
                dex_trade['amount_native'] = base_price['price_native'] * abs(
                    dex_trade['amounts'][idx_base]
                )

    def _resolve_prices_for_pools_with_stablecoin(self, dex_trade):
        if all(
            token_address in self.stablecoin_addresses
            for token_address in dex_trade['token_addresses']
        ):
            return self._resolve_prices_for_pools_with_only_stablecoin(dex_trade)

        stablecoin_index = [
            i
            for i, token_address in enumerate(dex_trade['token_addresses'])
            if token_address in self.stablecoin_addresses
        ][0]
        dex_trade['amount_stable'] = abs(dex_trade['amounts'][stablecoin_index])
        dex_trade['prices_stable'][stablecoin_index] = 1.0
        if not all(dex_trade['amounts']):
            return dex_trade

        dex_trade['prices_stable'][1 - stablecoin_index] = dex_trade['amounts'][
            1 - stablecoin_index
        ] / abs(dex_trade['amounts'][stablecoin_index])

        dex_trade['token_prices'][stablecoin_index] = [1.0, 1.0]
        dex_trade['token_prices'][1 - stablecoin_index] = [1.0, 1.0]

        dex_trade['token_prices'][stablecoin_index][1 - stablecoin_index] = 1.0 / abs(
            dex_trade['prices_stable'][1 - stablecoin_index]
        )
        dex_trade['token_prices'][1 - stablecoin_index][stablecoin_index] = abs(
            dex_trade['prices_stable'][stablecoin_index - 1]
        )

        return dex_trade

    def _resolve_prices_for_pools_with_native_token(self, dex_trade: dict):
        native_token_index = dex_trade['token_addresses'].index(self.native_token['address'])
        dex_trade['amount_native'] = abs(dex_trade['amounts'][native_token_index])
        dex_trade['prices_native'][native_token_index] = 1.0
        if not all(dex_trade['amounts']):
            return dex_trade
        dex_trade['prices_native'][1 - native_token_index] = dex_trade['amounts'][
            1 - native_token_index
        ] / abs(dex_trade['amounts'][native_token_index])

        dex_trade['token_prices'][native_token_index] = [1.0, 1.0]
        dex_trade['token_prices'][1 - native_token_index] = [1.0, 1.0]

        dex_trade['token_prices'][native_token_index][1 - native_token_index] = 1.0 / abs(
            dex_trade['prices_native'][1 - native_token_index]
        )
        dex_trade['token_prices'][1 - native_token_index][native_token_index] = abs(
            dex_trade['prices_native'][native_token_index]
        )

        return dex_trade

    @staticmethod
    def _resolve_prices_for_pools_with_only_stablecoin(dex_trade: dict):
        dex_trade['prices_stable'] = [1.0 for _ in dex_trade['prices_stable']]
        dex_trade['amount_stable'] = sum(abs(amount) for amount in dex_trade['amounts']) / len(
            dex_trade['amounts']
        )
        return dex_trade
