import pandas as pd

from src.backtest_engine.event_engine import EventDrivenBacktester


def _config() -> dict:
    return {
        'settings': {
            'backtest': {
                'initial_cash': 1_000_000,
                'commission_rate': 0.0003,
                'slippage_rate': 0.0005,
                'stamp_tax_rate': 0.001,
                't_plus_one': True,
                'price_limit_check': True,
            }
        },
        'market_rules': {
            'main_board_limit': 0.10,
            'st_limit': 0.05,
            'chinext_limit': 0.20,
            'star_limit': 0.20,
            't_plus_one': True,
            'filter_st': True,
            'liquidity_min_turnover': 1_000_000,
        },
    }


def test_limit_up_buy_is_blocked_and_counted():
    bt = EventDrivenBacktester(_config())
    data = {
        '000001.SZ': pd.DataFrame([
            {
                'date': '2026-03-17',
                'ts_code': '000001.SZ',
                'open': 11.0,
                'high': 11.0,
                'low': 10.9,
                'close': 11.0,
                'pre_close': 10.0,
                'amount': 50_000_000,
                'is_st': 0,
                'is_suspend': 0,
            }
        ])
    }

    def signal_func(date, daily_market, account):
        return [{'ts_code': '000001.SZ', 'side': 'buy', 'target_pct': 0.5}]

    result = bt.run(['000001.SZ'], '2026-03-17', '2026-03-17', data, signal_func)
    assert result['metrics']['total_trades'] == 0
    assert result['rule_block_stats'].get('limit_up', 0) == 1
    assert result['rule_block_total'] == 1


def test_t_plus_one_sell_is_blocked_and_counted():
    bt = EventDrivenBacktester(_config())
    data = {
        '000001.SZ': pd.DataFrame([
            {
                'date': '2026-03-17',
                'ts_code': '000001.SZ',
                'open': 10.0,
                'high': 10.2,
                'low': 9.9,
                'close': 10.0,
                'pre_close': 10.0,
                'amount': 50_000_000,
                'is_st': 0,
                'is_suspend': 0,
            },
            {
                'date': '2026-03-18',
                'ts_code': '000001.SZ',
                'open': 10.1,
                'high': 10.3,
                'low': 10.0,
                'close': 10.2,
                'pre_close': 10.0,
                'amount': 50_000_000,
                'is_st': 0,
                'is_suspend': 0,
            },
        ])
    }

    def signal_func(date, daily_market, account):
        if date == '2026-03-17':
            return [
                {'ts_code': '000001.SZ', 'side': 'buy', 'target_pct': 0.5},
                {'ts_code': '000001.SZ', 'side': 'sell'},
            ]
        return []

    result = bt.run(['000001.SZ'], '2026-03-17', '2026-03-18', data, signal_func)
    trades = result.get('trades')
    assert len(trades) == 1
    assert trades.iloc[0]['side'] == 'buy'
    assert result['rule_block_stats'].get('t_plus_one', 0) >= 1
