from ethereumetl.service.price_service import PriceService


def test_resolve_price_for_trade_with_two_tokens_including_stablecoin():
    service = PriceService([], ['0x1'], {'address': '0x2'})
    trade = {
        'token_addresses': ['0x1', '0x2'],
        'amounts': [1, 2],
        'token_prices': [[1, 0.5], [2, 1]],
    }
    result = service.resolve_price_for_trade(trade)
    assert result['prices_stable'] == [1.0, 0.5]
    assert result['amount_stable'] == 1.0


def test_resolve_price_for_trade_with_two_tokens_including_native_token():
    service = PriceService([], ['0x1'], {'address': '0x2'})
    trade = {
        'token_addresses': ['0x1', '0x2'],
        'amounts': [1, 2],
        'token_prices': [[1, 0.5], [2, 1]],
    }
    result = service.resolve_price_for_trade(trade)
    assert result['prices_native'] == [2, 1]
    assert result['amount_native'] == 2.0


def test_resolve_price_for_trade_with_two_tokens_neither_stablecoin_nor_native_token():
    service = PriceService([], ['0x1'], {'address': '0x2'})
    trade = {
        'token_addresses': ['0x3', '0x4'],
        'amounts': [1, 2],
        'token_prices': [[1, 0.5], [2, 1]],
    }
    result = service.resolve_price_for_trade(trade)
    assert result['prices_stable'] == [0.0, 0.0]
    assert result['amount_stable'] == 0.0
    assert result['prices_native'] == [0.0, 0.0]
    assert result['amount_native'] == 0.0


def test_resolve_price_for_trade_with_more_than_two_tokens():
    service = PriceService([], ['0x1'], {'address': '0x2'})
    trade = {
        'token_addresses': ['0x1', '0x2', '0x3'],
        'amounts': [1, 2, 3],
        'token_prices': [[1, 2, 3], [0.5, 1, 1.5], [0.33, 0.67, 1]],
    }
    result = service.resolve_price_for_trade(trade)
    assert result['prices_stable'] == [0, 0, 0]
    assert result['amount_stable'] == 0
    assert result['prices_native'] == [0, 0, 0]
    assert result['amount_native'] == 0
