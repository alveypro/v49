from __future__ import annotations

import pandas as pd

from openclaw.runtime.v8_advanced_factor_aggregator import calculate_v8_advanced_factors
from openclaw.runtime.v8_atr_risk import calculate_v8_atr, calculate_v8_dynamic_stops
from openclaw.runtime.v8_core_factors import (
    calculate_capital_flow,
    calculate_chip_concentration,
    calculate_momentum_acceleration,
    calculate_momentum_persistence,
    calculate_obv_energy,
    calculate_profit_quality,
    calculate_relative_strength_momentum,
    calculate_sector_resonance,
    calculate_smart_money,
    calculate_turnover_momentum,
    calculate_valuation_repair,
)
from openclaw.runtime.v8_market_regime import (
    calculate_v8_market_regime,
    calculate_v8_market_sentiment,
    check_v8_volume_confirmation,
    detect_v8_market_trend,
)
from strategies.evaluators.comprehensive_stock_evaluator_v8_ultimate import (
    ATRCalculator,
    AdvancedFactors,
    MarketRegimeFilter,
)


def _stock_frame(rows: int = 70) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "close_price": [10.0 + i * 0.1 for i in range(rows)],
            "high_price": [10.2 + i * 0.1 for i in range(rows)],
            "low_price": [9.8 + i * 0.1 for i in range(rows)],
            "vol": [1000.0 + i for i in range(rows)],
            "float_share": [100000.0 for _ in range(rows)],
        }
    )


def test_atr_calculator_legacy_facade_matches_runtime_payload():
    close = pd.Series([10.0, 11.0, 12.0])
    high = pd.Series([10.5, 11.5, 12.5])
    low = pd.Series([9.5, 10.5, 11.5])

    facade_atr = ATRCalculator.calculate_atr(high, low, close, period=2)
    runtime_atr = calculate_v8_atr(high, low, close, period=2)

    pd.testing.assert_series_equal(facade_atr, runtime_atr)
    assert ATRCalculator.calculate_dynamic_stops(10.0, 0.5) == calculate_v8_dynamic_stops(10.0, 0.5)


def test_market_regime_legacy_facade_matches_runtime_payload():
    index_data = pd.DataFrame({"close": list(range(1, 71)), "volume": [100.0] * 65 + [120.0] * 5})
    returns = index_data["close"].pct_change()

    assert MarketRegimeFilter.detect_market_trend(index_data["close"]) == detect_v8_market_trend(index_data["close"])
    assert MarketRegimeFilter.calculate_market_sentiment(returns) == calculate_v8_market_sentiment(returns)
    assert MarketRegimeFilter.check_volume_confirmation(index_data["volume"]) == check_v8_volume_confirmation(index_data["volume"])
    assert MarketRegimeFilter.comprehensive_filter(index_data) == calculate_v8_market_regime(index_data)


def test_advanced_factors_legacy_facade_matches_runtime_factor_payloads():
    stock_data = _stock_frame()
    close = stock_data["close_price"]
    high = stock_data["high_price"]
    low = stock_data["low_price"]
    volume = stock_data["vol"]
    returns = close.pct_change()
    turnover_rate = volume / stock_data["float_share"]
    index_returns = pd.Series([0.0] * len(stock_data))
    index_data = pd.DataFrame({"close": [3000.0] * len(stock_data), "volume": [100000.0] * len(stock_data)})

    assert AdvancedFactors.relative_strength_momentum(returns, index_returns) == calculate_relative_strength_momentum(returns, index_returns)
    assert AdvancedFactors.momentum_acceleration(returns) == calculate_momentum_acceleration(returns)
    assert AdvancedFactors.momentum_persistence(close) == calculate_momentum_persistence(close)
    assert AdvancedFactors.obv_energy(close, volume) == calculate_obv_energy(close, volume)
    assert AdvancedFactors.chip_concentration(high, low, close, volume) == calculate_chip_concentration(high, low, close, volume)
    assert AdvancedFactors._turnover_momentum(volume, turnover_rate) == calculate_turnover_momentum(volume, turnover_rate)
    assert AdvancedFactors._evaluate_valuation_repair(close, volume) == calculate_valuation_repair(close, volume)
    assert AdvancedFactors._evaluate_profit_quality(close, volume, returns) == calculate_profit_quality(close, volume, returns)
    assert AdvancedFactors._evaluate_capital_flow(close, volume, returns) == calculate_capital_flow(close, volume, returns)
    assert AdvancedFactors._evaluate_sector_resonance(returns, index_data, index_returns) == calculate_sector_resonance(returns, index_data, index_returns)
    assert AdvancedFactors._evaluate_smart_money(close, volume, returns) == calculate_smart_money(close, volume, returns)


def test_advanced_factors_aggregate_legacy_facade_matches_runtime_payload():
    stock_data = _stock_frame()
    index_data = pd.DataFrame({"close": [3000.0] * len(stock_data), "volume": [100000.0] * len(stock_data)})

    assert AdvancedFactors.calculate_all_advanced_factors(stock_data, index_data) == calculate_v8_advanced_factors(
        stock_data,
        index_data=index_data,
    )
