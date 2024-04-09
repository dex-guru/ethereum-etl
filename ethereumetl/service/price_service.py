import logging


class PriceService:
    def __init__(
        self,
        base_tokens_prices: list[dict],
        stablecoin_addresses: list[str],
        native_token: dict,
    ):
        self.stablecoin_addresses = {address.lower() for address in stablecoin_addresses}
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
        self.logger = logging.getLogger('PriceService')

    def set_base_prices_for_trade(self, dex_trade: dict):
        base_token = self._get_base_token(dex_trade)
        prices = {
            token_address: self.base_tokens_prices.get(token_address, {})
            for token_address in dex_trade['token_addresses']
        }
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
        self._validate_prices(dex_trade)
        return dex_trade

    def resolve_price_for_trade(self, dex_trade: dict):
        """Resolves the prices of tokens in a DEX trade."""
        self._initialize_prices(dex_trade)
        self._ensure_base_prices(dex_trade)
        if len(dex_trade['token_addresses']) > 2:
            return self.set_base_prices_for_trade(dex_trade)
        if self._trade_involves_stablecoin(dex_trade):
            dex_trade = self._resolve_prices_for_pools_with_stablecoin(dex_trade)
        if self._trade_involves_native_token(dex_trade):
            dex_trade = self._resolve_prices_for_pools_with_native_token(dex_trade)
        if all(dex_trade['prices_stable']) and all(dex_trade['prices_native']):
            self._validate_prices(dex_trade)
            self._update_base_prices(dex_trade)
            return dex_trade

        dex_trade = self._resolve_prices_for_generic_trade(dex_trade)
        self._validate_prices(dex_trade)
        self._update_base_prices(dex_trade)
        return dex_trade

    def _validate_prices(self, dex_trade):

        def is_valid_with_previous_price(_token_address, price, _price_type):
            previous_price = self.base_tokens_prices[_token_address][f'price_{_price_type}']
            if (
                previous_price and abs(price - previous_price) / previous_price > 1000
            ):  # 100000% change
                return False
            return True

        def reset_prices():
            self.logger.warning(
                f'Prices are invalid for trade. Resetting. "transaction_hash": {dex_trade["transaction_hash"]}'
            )
            token_count = len(dex_trade['token_addresses'])
            dex_trade.update(
                {
                    'prices_stable': [0.0] * token_count,
                    'prices_native': [0.0] * token_count,
                    'amount_stable': 0.0,
                    'amount_native': 0.0,
                }
            )
            return dex_trade

        for price_type in ('stable', 'native'):
            for token_address, current_price in zip(
                dex_trade['token_addresses'], dex_trade[f'prices_{price_type}']
            ):
                if not is_valid_with_previous_price(token_address, current_price, price_type):
                    reset_prices()
                    return dex_trade

        try:
            price_ratio = abs(
                (dex_trade['prices_stable'][0] * dex_trade['amounts'][0])
                / (dex_trade['prices_stable'][1] * dex_trade['amounts'][1])
            )
        except ZeroDivisionError:
            price_ratio = 0.0

        if not price_ratio or 0.8 > price_ratio > 1.2:
            dex_trade = reset_prices()

        return dex_trade

    def _update_base_prices(self, dex_trade):
        """Updates the base token prices based on the trade."""
        for idx, token_address in enumerate(dex_trade['token_addresses']):
            price_stable = dex_trade['prices_stable'][idx]
            price_native = dex_trade['prices_native'][idx]
            self.base_tokens_prices.setdefault(
                token_address,
                {'price_stable': price_stable, 'price_native': price_native, 'score': 1},
            )
            if price_stable:
                self.base_tokens_prices[token_address]['price_stable'] = price_stable
            if price_native:
                self.base_tokens_prices[token_address]['price_native'] = price_native

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
            self.base_tokens_prices.setdefault(
                token_address, {'price_stable': 0.0, 'price_native': 0.0, 'score': 0}
            )

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
        return max(
            dex_trade['token_addresses'],
            key=lambda token_address: self.base_tokens_prices[token_address]['score'],
        )

    def _resolve_prices_for_generic_trade(self, dex_trade):
        if len(dex_trade['token_addresses']) == 2:
            base_token_address = self._get_base_token(dex_trade)
            idx = dex_trade['token_addresses'].index(base_token_address)
            base_price = self.base_tokens_prices.get(base_token_address, {})
            self._calculate_token_prices(dex_trade, idx, base_price)
        return dex_trade

    def _calculate_token_prices(self, dex_trade, idx, base_price):
        """Calculates and updates the token prices in the trade."""
        opposite_idx = 1 - idx
        opposite_token_ratio = dex_trade['token_prices'][opposite_idx][idx]
        self._update_trade_prices(dex_trade, idx, opposite_idx, base_price, opposite_token_ratio)

    def _update_trade_prices(
        self, dex_trade, idx_base, opposite_idx, base_prices_dict, opposite_token_ratio
    ):
        """
        Updates the trade prices based on the base and opposite token prices.

        This method iterates over two price types: 'stable' and 'native'. For each price type, it checks if the prices
        for the base and opposite tokens are not set in the dex_trade dictionary. If they are not set and the base price
        for the current price type is available, it sets the base price and calculates the opposite price based on the
        opposite token ratio. If a ZeroDivisionError occurs during this calculation, it tries to calculate the opposite
        price based on the amounts of the base and opposite tokens. If another ZeroDivisionError occurs, it sets the
        opposite price to the base token price from the base_tokens_prices dictionary. Finally, it calculates the amount
        for the current price type based on the base price and the absolute value of the base token amount.

        Args:
        ----
            dex_trade (dict): The dictionary containing the trade data.
            idx_base (int): The index of the base token in the dex_trade dictionary.
            opposite_idx (int): The index of the opposite token in the dex_trade dictionary.
            base_prices_dict (dict): The dictionary containing the base prices for the 'stable' and 'native' price types.
            opposite_token_ratio (float): The ratio of the opposite token to the base token.


        Returns:
        -------
            None

        """
        for price_type in ('stable', 'native'):
            if all(dex_trade[f'prices_{price_type}']):
                continue
            if not base_prices_dict[f'price_{price_type}']:
                continue
            base_price = base_prices_dict[f'price_{price_type}']
            dex_trade[f'prices_{price_type}'][idx_base] = base_price
            try:
                dex_trade[f'prices_{price_type}'][opposite_idx] = base_price / opposite_token_ratio
            except ZeroDivisionError:
                try:
                    dex_trade[f'prices_{price_type}'][opposite_idx] = abs(
                        dex_trade['amounts'][idx_base]
                        / dex_trade['amounts'][opposite_idx]
                        * base_price
                    )
                except ZeroDivisionError:
                    dex_trade[f'prices_{price_type}'][opposite_idx] = self.base_tokens_prices[
                        dex_trade['token_addresses'][opposite_idx]
                    ][f'price_{price_type}']
            dex_trade[f'amount_{price_type}'] = base_price * abs(dex_trade['amounts'][idx_base])

    def _resolve_prices_for_pools_with_stablecoin(self, dex_trade):
        if all(token in self.stablecoin_addresses for token in dex_trade['token_addresses']):
            return self._resolve_prices_for_pools_with_only_stablecoin(dex_trade)

        stablecoin_index = next(
            (
                i
                for i, token in enumerate(dex_trade['token_addresses'])
                if token in self.stablecoin_addresses
            ),
            None,
        )
        if stablecoin_index is None:
            raise ValueError('No stablecoin found in token addresses')

        dex_trade['amount_stable'] = abs(dex_trade['amounts'][stablecoin_index])
        dex_trade['prices_stable'][stablecoin_index] = 1.0

        opposite_index = 1 - stablecoin_index
        opposite_price = dex_trade['token_prices'][stablecoin_index][opposite_index] or (
            dex_trade['amounts'][stablecoin_index] / dex_trade['amounts'][opposite_index]
            if dex_trade['amounts'][opposite_index]
            else 0.0
        )

        dex_trade['prices_stable'][opposite_index] = abs(opposite_price)
        return dex_trade

    def _resolve_prices_for_pools_with_native_token(self, dex_trade: dict):
        native_token_index = dex_trade['token_addresses'].index(self.native_token['address'])
        dex_trade['amount_native'] = abs(dex_trade['amounts'][native_token_index])
        dex_trade['prices_native'][native_token_index] = 1.0

        opposite_index = 1 - native_token_index
        opposite_price = dex_trade['token_prices'][native_token_index][opposite_index] or (
            dex_trade['amounts'][native_token_index] / dex_trade['amounts'][opposite_index]
            if dex_trade['amounts'][opposite_index]
            else 0.0
        )

        dex_trade['prices_native'][opposite_index] = abs(opposite_price)
        return dex_trade

    @staticmethod
    def _resolve_prices_for_pools_with_only_stablecoin(dex_trade: dict):
        dex_trade['prices_stable'] = [1.0 for _ in dex_trade['prices_stable']]
        dex_trade['amount_stable'] = sum(abs(amount) for amount in dex_trade['amounts']) / len(
            dex_trade['amounts']
        )
        return dex_trade
