from collections.abc import Collection

from clickhouse_connect.driver.client import Client

from blockchainetl.jobs.importers.price_importers.base_price_importer import BasePriceImporter
from ethereumetl.domain.price import Price
from ethereumetl.utils import clickhouse_client_from_url


class ClickhousePriceImporter(BasePriceImporter):

    def __init__(self, chain_id: int, clickhouse_url: str):
        super().__init__(chain_id)
        self.clickhouse_url = clickhouse_url
        self.eth_ch_url = (
            'clickhouse+http://testuser3:testplpassword@stage-ch-eth-01.dexguru.biz/ethereum'
        )
        self.clickhouse: Client = None
        self.eth_ch: Client = None

    def open(self):
        super().open()
        self.clickhouse = clickhouse_client_from_url(self.clickhouse_url)
        if self.chain_id == 614:
            self.eth_ch = clickhouse_client_from_url(self.eth_ch_url)

    def close(self):
        assert self.clickhouse, 'Clickhouse client is not initialized'
        self.clickhouse.close()
        if self.eth_ch:
            self.eth_ch.close()
        super().close()

    def _get_price_from_eth_chain(
        self,
        src_address: str,
        dst_address: str,
        block_number: int | None = None,
    ) -> dict:
        assert self.eth_ch, 'Clickhouse client is not initialized'
        block_number_condition = f'block_number <= {block_number} AND' if block_number else ''

        query = f"""
                    SELECT max(c_s).2 AS latest_price_stable,
                           max(c_n).2 AS latest_price_native
                    FROM candles_5m
                    WHERE {block_number_condition} token_address = '{src_address}'
                    GROUP BY token_address
                """
        return {
            dst_address: {
                'token_address': dst_address,
                'price_stable': d['latest_price_stable'],
                'price_native': d['latest_price_native'],
            }
            for d in self.eth_ch.query(query).named_results()
        }

    def get_prices_for_tokens(
        self,
        token_addresses: Collection[str],
        timestamp: int | None = None,
        block_number: int | None = None,
    ) -> Collection[Price]:
        crosschain_price = {}
        if (
            '0xeb567ec41738c2bab2599a1070fc5b727721b3b6' in token_addresses
            and self.chain_id == 614
        ):
            crosschain_price = self._get_price_from_eth_chain(
                '0x9f9c8ec3534c3ce16f928381372bfbfbfb9f4d24',
                '0xeb567ec41738c2bab2599a1070fc5b727721b3b6',
                block_number,
            )
        elif (
            '0xa3e9bf36ff51ce14a25a2cf4b4086cbcf1df781b' in token_addresses
            and self.chain_id == 261
        ):
            crosschain_price = self._get_price_from_eth_chain(
                '0x525574c899a7c877a11865339e57376092168258',
                '0xa3e9bf36ff51ce14a25a2cf4b4086cbcf1df781b',
                block_number,
            )
        all_prices = []
        tokens_score = self._calculate_pools_count_for_tokens(token_addresses)
        tokens_score[self.native_token_address] = 9e10
        prices = self._get_prices_from_candles_for_tokens(
            base_tokens_addresses=token_addresses, block_number=block_number
        )
        prices.update(crosschain_price)
        for token_address in token_addresses:
            prices_ = prices.get(
                token_address,
                {'price_stable': 0, 'price_native': 0, 'token_address': token_address},
            )
            if token_address in self.stablecoin_addresses:
                # Assigning all stablecoins to be equal to 1 for now, we can complicate the logic here in future
                # not setting the $1 price only to stable which is most trustable at that point
                # (most liquidity/volume) or resolving the prices against CEXes here for stables
                prices_['price_stable'] = 1
            if token_address == self.native_token_address:
                prices_['price_native'] = 1
            prices_['score'] = tokens_score.get(token_address, 0)
            all_prices.append(Price(**prices_))
        return all_prices

    def _get_prices_from_candles_for_tokens(
        self, base_tokens_addresses: Collection[str], block_number: int | None = None
    ):
        # Here we are recieving prices for base tokens from last trades on those tokens
        # logic needs to be improved in future os we either have trust index for prices saved
        # along dex_trade or we would be able to calculate closest path to stable based on
        # pools route
        assert self.clickhouse, 'Clickhouse client is not initialized'
        block_number_condition = f'block_number <= {block_number} AND' if block_number else ''

        query = f"""
                    SELECT max(c_s).2 AS latest_price_stable,
                           max(c_n).2 AS latest_price_native,
                           token_address
                    FROM candles_5m
                    WHERE {block_number_condition} token_address IN {tuple(base_tokens_addresses)}
                    GROUP BY token_address
                """
        return {
            d['token_address']: {
                'token_address': d['token_address'],
                'price_stable': d['latest_price_stable'],
                'price_native': d['latest_price_native'],
            }
            for d in self.clickhouse.query(query).named_results()
        }

    def _calculate_pools_count_for_tokens(self, tokens) -> dict:
        # here we calculating token score based on it's relationships ration
        # more pools existing with token - bigger score,
        # could be explored further using liquidity/volume as weights in
        # links between pools (nodes) in vector representaion of it.
        assert self.clickhouse, 'Clickhouse client is not initialized'
        if not tokens:
            return {}
        query = f"""
            SELECT 
                token_address, 
                uniqMerge(pools_count) as pool_count
            FROM pools_counts
            WHERE token_address IN {tuple(tokens)}
            GROUP BY token_address
        """
        return {
            d['token_address']: d['pool_count']
            for d in self.clickhouse.query(query).named_results()
        }
