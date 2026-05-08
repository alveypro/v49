import pandas as pd

from src.rules.market_rule_engine import MarketRuleEngine


def _engine() -> MarketRuleEngine:
    return MarketRuleEngine({
        'market_rules': {
            'main_board_limit': 0.10,
            'st_limit': 0.05,
            'chinext_limit': 0.20,
            'star_limit': 0.20,
            't_plus_one': True,
            'filter_st': True,
            'liquidity_min_turnover': 1_000_000,
        }
    })


def test_buy_blocked_when_limit_up():
    engine = _engine()
    row = pd.Series({
        'ts_code': '000001.SZ',
        'pre_close': 10.0,
        'close': 11.0,
        'amount': 20_000_000,
        'is_st': 0,
        'is_suspend': 0,
    })
    ok, reason = engine.check_tradeable(row, direction='buy')
    assert not ok
    assert reason == 'limit_up'


def test_sell_blocked_when_limit_down():
    engine = _engine()
    row = pd.Series({
        'ts_code': '000001.SZ',
        'pre_close': 10.0,
        'close': 9.0,
        'amount': 20_000_000,
        'is_st': 0,
        'is_suspend': 0,
    })
    ok, reason = engine.check_tradeable(row, direction='sell')
    assert not ok
    assert reason == 'limit_down'


def test_t_plus_one_rule():
    engine = _engine()
    assert not engine.check_t_plus_one('2026-03-17', '2026-03-17')
    assert engine.check_t_plus_one('2026-03-17', '2026-03-18')


def test_buy_blocked_when_extreme_low_liquidity():
    engine = MarketRuleEngine({
        'market_rules': {
            'main_board_limit': 0.10,
            'st_limit': 0.05,
            'chinext_limit': 0.20,
            'star_limit': 0.20,
            't_plus_one': True,
            'filter_st': True,
            'execution_liquidity_min_turnover': 300_000,
        }
    })
    row = pd.Series({
        'ts_code': '000001.SZ',
        'pre_close': 10.0,
        'close': 10.1,
        'amount': 120_000,
        'is_st': 0,
        'is_suspend': 0,
    })
    ok, reason = engine.check_tradeable(row, direction='buy')
    assert not ok
    assert reason == 'low_liquidity'
