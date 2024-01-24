from dataclasses import asdict

from ethereumetl.domain.dex_trade import EnrichedDexTrade, EthDexTrade
from ethereumetl.enumeration.entity_type import EntityType


class EthDexTradeMapper:
    @staticmethod
    def dict_from_dex_trade(dex_trade: EthDexTrade):
        trade = asdict(dex_trade)
        trade['type'] = EntityType.DEX_TRADE.value
        return trade

    @staticmethod
    def dex_trade_from_dict(trade_dict: dict):
        return EthDexTrade(
            token_amounts_raw=trade_dict['token_amounts_raw'],
            pool_address=trade_dict['pool_address'].lower(),
            transaction_hash=trade_dict['transaction_hash'].lower(),
            log_index=trade_dict['log_index'],
            block_number=trade_dict['block_number'],
            event_type=trade_dict['event_type'].lower(),
            token_reserves=trade_dict['token_reserves'],
            token_prices=trade_dict['token_prices'],
            lp_token_address=trade_dict['lp_token_address'].lower(),
            token_addresses=[
                token_address.lower() for token_address in trade_dict['token_addresses']
            ],
        )


class EnrichedDexTradeMapper:
    @staticmethod
    def dict_from_enriched_dex_trade(dex_trade: EnrichedDexTrade) -> dict:
        trade = asdict(dex_trade)
        trade['type'] = EntityType.ENRICHED_DEX_TRADE.value
        return trade

    @staticmethod
    def enriched_dex_trade_from_dict(trade_dict: dict) -> EnrichedDexTrade:
        return EnrichedDexTrade(
            block_number=trade_dict['block_number'],
            log_index=trade_dict['log_index'],
            transaction_hash=trade_dict['transaction_hash'].lower(),
            transaction_type=trade_dict['transaction_type'].lower(),
            token_addresses=trade_dict['token_addresses'],
            symbols=trade_dict['symbols'],
            amounts=trade_dict['amounts'],
            amount_stable=trade_dict['amount_stable'],
            amount_native=trade_dict['amount_native'],
            prices_stable=trade_dict['prices_stable'],
            prices_native=trade_dict['prices_native'],
            pool_address=trade_dict['pool_address'].lower(),
            wallet_address=trade_dict['wallet_address'].lower(),
            block_timestamp=trade_dict['block_timestamp'],
            block_hash=trade_dict['block_hash'].lower(),
            reserves=trade_dict['reserves'],
            reserves_stable=trade_dict['reserves_stable'],
            reserves_native=trade_dict['reserves_native'],
            factory_address=trade_dict['factory_address'].lower(),
        )
