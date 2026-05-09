from __future__ import annotations

import sqlite3

from openclaw.services.ensemble_alpha_gate_contrast_service import (
    build_ensemble_alpha_gate_contrast,
)


def test_ensemble_alpha_gate_contrast_compares_risk_off_and_source_filters():
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
            _item("000004.SZ", "v6", 80.0, -3.0, 70.0),
            _item("000005.SZ", "v8", 60.0, -2.0, 60.0),
            _item("000006.SZ", "v4", 20.0, 2.0, 45.0),
        ],
    }

    review = build_ensemble_alpha_gate_contrast(
        conn,
        [good, bad],
        candidate="hard_event_alpha_candidate",
        horizon=5,
        blocked_sources=("v6", "v8"),
    )

    assert review["research_only"] is True
    assert set(review["scenario_reviews"]) == {
        "raw",
        "risk_off_gate",
        "source_filter",
        "risk_off_gate_plus_source_filter",
    }
    assert review["scenario_reviews"]["risk_off_gate"]["sample_count"] < review["scenario_reviews"]["raw"]["sample_count"]
    assert "do_not_promote_gate_contrast_to_sleeve_policy_without_fresh_walk_forward" in review["hard_boundaries"]


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
