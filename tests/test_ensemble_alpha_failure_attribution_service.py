from __future__ import annotations

import sqlite3

from openclaw.services.ensemble_alpha_failure_attribution_service import (
    build_ensemble_alpha_failure_attribution,
)


def test_ensemble_alpha_failure_attribution_identifies_failed_window_drivers():
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE daily_trading_data (
            ts_code TEXT,
            trade_date TEXT,
            pct_chg REAL,
            amount REAL
        );
        """
    )
    for idx in range(10):
        conn.execute("INSERT INTO daily_trading_data VALUES (?, ?, ?, ?)", (f"0000{idx}.SZ", "20260101", 1.0, 1000.0))
        conn.execute("INSERT INTO daily_trading_data VALUES (?, ?, ?, ?)", (f"0001{idx}.SZ", "20260102", -2.0, 1000.0))

    good = {
        "as_of_date": "20260101",
        "sample_facts": [
            _item("000001.SZ", "v4", 80.0, 2.0, 70.0),
            _item("000002.SZ", "v5", 60.0, 1.0, 60.0),
            _item("000003.SZ", "v8", 20.0, -1.0, 45.0),
        ],
    }
    bad = {
        "as_of_date": "20260102",
        "sample_facts": [
            _item("000004.SZ", "v4", 80.0, -3.0, 75.0),
            _item("000005.SZ", "v5", 60.0, -2.0, 72.0),
            _item("000006.SZ", "v8", 20.0, 2.0, 30.0),
        ],
    }

    review = build_ensemble_alpha_failure_attribution(
        conn,
        [good, bad],
        candidate="hard_event_alpha_candidate",
        horizon=5,
    )

    assert review["research_only"] is True
    assert review["focus_failed_window"] == "20260102"
    assert "20260102" in review["failed_windows"]
    assert "market_regime_risk_off" in review["inferred_failure_drivers"]
    assert "risk_off_high_event_flow_seat_capacity_exhaustion_negative" in review["inferred_failure_drivers"]
    focus = next(item for item in review["window_diagnostics"] if item["as_of_date"] == "20260102")
    profile = focus["risk_off_exhaustion_profile"]
    assert profile["active"] is True
    assert profile["exhausted_signal_count"] == 2
    assert profile["exhausted_negative_count"] == 2
    assert profile["exhausted_negative_ratio"] == 1.0
    assert profile["exhausted_avg_forward_return_pct"] < profile["non_exhausted_avg_forward_return_pct"]
    assert {item["ts_code"] for item in profile["exhausted_top_negative_examples"]} == {"000004.SZ", "000005.SZ"}
    assert "do_not_use_failure_attribution_as_promotion_evidence" in review["hard_boundaries"]


def _item(ts_code: str, strategy: str, score: float, forward_return: float, capacity: float) -> dict:
    return {
        "ts_code": ts_code,
        "strategy": strategy,
        "sleeve_scores": {"hard_event_alpha": {"score": score}},
        "tushare_pro_alpha_features": {
            "evidence": {
                "hard_alpha": {
                    "money_flow_persistence": {"score": score},
                    "dragon_tiger_seat_quality": {"score": score},
                    "limit_break_structure": {"score": score},
                    "industry_crowding": {"score": score},
                    "capacity_liquidity": {"score": capacity},
                    "margin_pressure": {"score": score},
                }
            }
        },
        "forward_returns": {"5": {"available": True, "return_pct": forward_return}},
    }
