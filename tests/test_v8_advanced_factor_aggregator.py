from __future__ import annotations

import pandas as pd

from openclaw.runtime.v8_advanced_factor_aggregator import calculate_v8_advanced_factors
from openclaw.runtime.v8_core_factors import calculate_momentum_acceleration


class FakeV8Factors:
    @staticmethod
    def relative_strength_momentum(stock_returns, index_returns):
        return {"score": 10, "grade": "relative"}

    @staticmethod
    def momentum_acceleration(returns):
        return {"score": 8, "grade": "acceleration"}

    @staticmethod
    def momentum_persistence(close):
        return {"score": 7, "grade": "persistence"}

    @staticmethod
    def obv_energy(close, volume):
        return {"score": 9, "grade": "obv"}

    @staticmethod
    def chip_concentration(high, low, close, volume):
        return {"score": 11, "grade": "chip"}

    @staticmethod
    def _turnover_momentum(volume, turnover_rate):
        return {"score": 6, "grade": "turnover"}

    @staticmethod
    def _evaluate_valuation_repair(close, volume):
        return {"score": 5, "grade": "valuation"}

    @staticmethod
    def _evaluate_profit_quality(close, volume, returns):
        return {"score": 4, "grade": "roe"}

    @staticmethod
    def _evaluate_capital_flow(close, volume, returns):
        return {"score": 7, "grade": "flow"}

    @staticmethod
    def _evaluate_sector_resonance(returns, index_data, index_returns):
        assert "close" in index_data.columns
        assert "volume" in index_data.columns
        return {"score": 8, "grade": "sector"}

    @staticmethod
    def _evaluate_smart_money(close, volume, returns):
        return {"score": 12, "grade": "smart"}


class RaisingFactors(FakeV8Factors):
    @staticmethod
    def momentum_acceleration(returns):
        raise RuntimeError("boom")


def _stock_frame() -> pd.DataFrame:
    rows = 70
    return pd.DataFrame(
        {
            "close_price": [10 + i * 0.1 for i in range(rows)],
            "high_price": [10.2 + i * 0.1 for i in range(rows)],
            "low_price": [9.8 + i * 0.1 for i in range(rows)],
            "vol": [1000 + i for i in range(rows)],
            "float_share": [100000 for _ in range(rows)],
        }
    )


def test_calculate_v8_advanced_factors_freezes_factor_order_weights_and_total():
    index_data = pd.DataFrame(
        {
            "close": [3000 + i for i in range(70)],
            "volume": [100000 + i for i in range(70)],
        }
    )

    result = calculate_v8_advanced_factors(
        _stock_frame(),
        index_data=index_data,
        factor_api=FakeV8Factors,
    )

    assert list(result["factors"]) == [
        "relative_strength",
        "acceleration",
        "persistence",
        "obv",
        "chip_concentration",
        "turnover_momentum",
        "valuation_repair",
        "roe_trend",
        "capital_flow",
        "sector_resonance",
        "smart_money",
    ]
    assert result["total_score"] == 87
    assert result["max_score"] == 140


def test_calculate_v8_advanced_factors_keeps_no_index_fallback_in_same_payload():
    result = calculate_v8_advanced_factors(
        _stock_frame(),
        index_data=None,
        factor_api=FakeV8Factors,
    )

    assert "relative_strength" not in result["factors"]
    assert result["factors"]["sector_resonance"]["score"] == 6
    assert result["total_score"] == 75
    assert result["max_score"] == 125


def test_calculate_v8_advanced_factors_normalizes_index_aliases():
    index_data = pd.DataFrame(
        {
            "close_price": [3000 + i for i in range(70)],
            "vol": [100000 + i for i in range(70)],
        }
    )

    result = calculate_v8_advanced_factors(
        _stock_frame(),
        index_data=index_data,
        factor_api=FakeV8Factors,
    )

    assert result["factors"]["relative_strength"]["score"] == 10
    assert result["factors"]["sector_resonance"]["score"] == 8


def test_calculate_v8_advanced_factors_returns_structured_failure_payload():
    result = calculate_v8_advanced_factors(
        _stock_frame(),
        index_data=None,
        factor_api=RaisingFactors,
    )

    assert result == {"total_score": 0, "factors": {}, "max_score": 100}


def test_calculate_v8_advanced_factors_accepts_runtime_factor_functions():
    class MixedRuntimeFactors(FakeV8Factors):
        momentum_acceleration = staticmethod(calculate_momentum_acceleration)

    result = calculate_v8_advanced_factors(
        _stock_frame(),
        index_data=None,
        factor_api=MixedRuntimeFactors,
    )

    assert result["factors"]["acceleration"]["score"] in {1, 3, 6, 9, 12}


def test_calculate_v8_advanced_factors_uses_runtime_api_by_default():
    result = calculate_v8_advanced_factors(
        _stock_frame(),
        index_data=pd.DataFrame(
            {
                "close": [3000 + i for i in range(70)],
                "volume": [100000 + i for i in range(70)],
            }
        ),
    )

    assert list(result["factors"]) == [
        "relative_strength",
        "acceleration",
        "persistence",
        "obv",
        "chip_concentration",
        "turnover_momentum",
        "valuation_repair",
        "roe_trend",
        "capital_flow",
        "sector_resonance",
        "smart_money",
    ]
    assert result["max_score"] == 140
    assert "score" in result["factors"]["smart_money"]
