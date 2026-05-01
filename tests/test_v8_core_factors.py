from __future__ import annotations

import pandas as pd

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


def test_calculate_relative_strength_momentum_freezes_thresholds():
    stock_returns = pd.Series([0.01] * 60)
    index_returns = pd.Series([0.0] * 60)

    result = calculate_relative_strength_momentum(stock_returns, index_returns)

    assert result == {"rsm": 1.0, "score": 6, "grade": "略强"}


def test_calculate_momentum_acceleration_freezes_score_band():
    returns = pd.Series([0.01] * 10 + [0.02] * 10)

    result = calculate_momentum_acceleration(returns)

    assert result == {"acceleration": 1.0, "score": 12, "grade": "极速加速"}


def test_calculate_momentum_persistence_counts_recent_breakouts():
    close = pd.Series(list(range(1, 71)))

    result = calculate_momentum_persistence(close, window=60)

    assert result == {"new_highs_count": 11, "score": 12, "grade": "强势突破"}


def test_calculate_obv_energy_freezes_short_data_fallback():
    result = calculate_obv_energy(
        pd.Series([10, 10.1, 10.2]),
        pd.Series([1000, 1100, 1200]),
    )

    assert result == {"obv_slope": 0.0, "divergence": False, "score": 5, "grade": "数据不足"}


def test_calculate_chip_concentration_freezes_big_order_threshold():
    close = pd.Series([10, 10.2] * 15)
    high = close + 0.1
    low = close - 0.1
    volume = pd.Series([1000] * 30)

    result = calculate_chip_concentration(high, low, close, volume, window=20)

    assert result == {"concentration_ratio": 0.55, "score": 15, "grade": "强力控盘"}


def test_calculate_valuation_repair_freezes_discount_band():
    close = pd.Series([10.0] * 59 + [9.0])
    volume = pd.Series([1000] * 60)

    result = calculate_valuation_repair(close, volume)

    assert result == {"score": 9, "grade": "明显折价", "price_ratio": 0.9}


def test_calculate_turnover_momentum_freezes_recent_relative_volume_band():
    volume = pd.Series([100.0] * 20 + [200.0] * 5)
    turnover_rate = pd.Series([1.0] * 20 + [2.0] * 5)

    result = calculate_turnover_momentum(volume, turnover_rate, window=20)

    assert result == {"score": 12, "grade": "放量强换手", "vol_rel": 1.75, "turnover_rel": 1.75}


def test_calculate_profit_quality_freezes_stability_band():
    close = pd.Series([10.0] * 20)
    volume = pd.Series([1000] * 20)
    pct_chg = pd.Series([0.01] * 13 + [-0.005] * 7)

    result = calculate_profit_quality(close, volume, pct_chg)

    assert result == {"score": 10, "grade": "优质上涨", "stability": 0.65}


def test_calculate_capital_flow_freezes_inflow_band():
    close = pd.Series([10.0] * 10)
    volume = pd.Series([1000.0] * 10)
    pct_chg = pd.Series([0.01] * 10)

    result = calculate_capital_flow(close, volume, pct_chg)

    assert result == {"score": 9, "grade": "持续流入", "inflow_score": 10.0, "avg_vol_rel": 1.0}


def test_calculate_sector_resonance_freezes_no_index_fallback():
    result = calculate_sector_resonance(
        pd.Series([0.0] * 20),
        None,
        pd.Series([0.0] * 20),
    )

    assert result == {"score": 6, "grade": "无大盘对比"}


def test_calculate_sector_resonance_freezes_excess_return_band_without_extra():
    result = calculate_sector_resonance(
        pd.Series([0.0] * 19 + [0.1]),
        pd.DataFrame({"close": [100.0] * 20}),
        pd.Series([0.0] * 20),
    )

    assert result == {"score": 8, "grade": "明显领先", "excess_return": 10.0}


def test_calculate_sector_resonance_freezes_context_bonus_and_cap():
    result = calculate_sector_resonance(
        pd.Series([0.0] * 19 + [0.2]),
        pd.DataFrame(
            {
                "close": [100.0] * 20,
                "up_count": [0] * 19 + [60],
                "strong_count": [0] * 19 + [25],
            }
        ),
        pd.Series([0.0] * 20),
    )

    assert result == {"score": 12, "grade": "强势领涨", "excess_return": 20.0}


def test_calculate_smart_money_freezes_three_feature_priority_band():
    result = calculate_smart_money(
        pd.Series([10.0] * 29 + [11.0]),
        pd.Series([100.0] * 15 + [120.0] * 15),
        pd.Series([0.02, -0.02] * 7 + [0.02] + [0.001] * 15),
    )

    assert result == {"score": 15, "grade": "机构重点", "smart_features": 3, "price_trend": 10.0}


def test_calculate_smart_money_freezes_two_feature_attention_band():
    result = calculate_smart_money(
        pd.Series([10.0] * 29 + [10.5]),
        pd.Series([100.0] * 15 + [120.0] * 15),
        pd.Series([0.001] * 15 + [0.02, -0.02] * 7 + [0.02]),
    )

    assert result == {"score": 11, "grade": "机构关注", "smart_features": 2, "price_trend": 5.0}


def test_calculate_smart_money_freezes_one_feature_building_band():
    result = calculate_smart_money(
        pd.Series([10.0] * 29 + [10.5]),
        pd.Series([100.0] * 30),
        pd.Series([0.001] * 15 + [0.02, -0.02] * 7 + [0.02]),
    )

    assert result == {"score": 7, "grade": "有建仓迹象", "smart_features": 1, "price_trend": 5.0}


def test_calculate_smart_money_freezes_zero_feature_default_band():
    result = calculate_smart_money(
        pd.Series([10.0] * 30),
        pd.Series([100.0] * 30),
        pd.Series([0.001] * 15 + [0.02, -0.02] * 7 + [0.02]),
    )

    assert result == {"score": 4, "grade": "普通", "smart_features": 0, "price_trend": 0.0}
