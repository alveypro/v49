from __future__ import annotations

import pandas as pd

from openclaw.runtime.v9_signal_evaluator import calculate_v9_score_from_history


def _history(rows: int = 130) -> pd.DataFrame:
    dates = pd.date_range("2025-01-01", periods=rows, freq="D")
    close = [10 + i * 0.04 for i in range(rows)]
    return pd.DataFrame(
        {
            "trade_date": [d.strftime("%Y%m%d") for d in dates],
            "close": close,
            "vol": [1000 + (i % 5) * 50 for i in range(rows)],
            "amount": [100000 + i * 200 for i in range(rows)],
            "pct_chg": [0.4 if i % 3 else -0.1 for i in range(rows)],
        }
    )


def test_v9_score_returns_zero_for_insufficient_history():
    assert calculate_v9_score_from_history(_history(40)) == {"score": 0.0, "details": {}}


def test_v9_score_uses_price_aliases_and_freezes_details():
    score = calculate_v9_score_from_history(_history(), industry_strength=2.0)

    assert score["score"] > 0
    assert set(score["details"]) == {
        "fund_score",
        "volume_score",
        "momentum_score",
        "sector_score",
        "volatility_score",
        "trend_score",
        "flow_ratio",
        "vol_ratio",
        "momentum_20",
        "momentum_60",
        "vol_20",
    }
    assert score["details"]["sector_score"] > 0


def test_v9_score_increases_with_stronger_industry_context():
    weak = calculate_v9_score_from_history(_history(), industry_strength=-2.0)
    strong = calculate_v9_score_from_history(_history(), industry_strength=4.0)

    assert strong["score"] > weak["score"]
    assert strong["details"]["sector_score"] > weak["details"]["sector_score"]
