from __future__ import annotations

import sqlite3

from openclaw.services.ensemble_alpha_predeclared_gate_walk_forward_service import (
    build_ensemble_alpha_predeclared_gate_walk_forward,
)


def test_predeclared_risk_off_gate_walk_forward_passes_only_as_research_only():
    conn = _conn_with_regimes(
        {
            "20260101": 1.0,
            "20260102": 0.8,
            "20260103": 1.2,
            "20260104": 0.6,
            "20260105": -2.0,
        }
    )
    chains = [
        _chain("20260101", 1.0),
        _chain("20260102", 0.8),
        _chain("20260103", 0.6),
        _chain("20260104", 0.4),
        _chain("20260105", -1.0),
    ]

    review = build_ensemble_alpha_predeclared_gate_walk_forward(
        conn,
        chains,
        min_sample_count=12,
        min_retained_windows=4,
    )

    assert review["research_only"] is True
    assert review["not_for_sleeve_policy"] is True
    assert review["predeclared_gate"]["scenario_selection_allowed"] is False
    assert review["predeclared_gate"]["source_strategy_filter_allowed"] is False
    assert review["passed_predeclared_walk_forward_gate"] is True
    assert review["promotion_status"] == "research_only_blocked_from_observation"
    assert review["validation_review"]["retained_window_count"] == 4
    assert review["validation_review"]["excluded_window_count"] == 1
    assert "do_not_promote_ensemble_core_from_gate_validation_without_shadow_portfolio_and_after_cost_benchmark" in review["hard_boundaries"]


def test_predeclared_gate_blocks_when_validation_has_no_risk_off_exclusion():
    conn = _conn_with_regimes(
        {
            "20260101": 1.0,
            "20260102": 0.8,
            "20260103": 1.2,
            "20260104": 0.6,
        }
    )
    chains = [
        _chain("20260101", 1.0),
        _chain("20260102", 0.8),
        _chain("20260103", 0.6),
        _chain("20260104", 0.4),
    ]

    review = build_ensemble_alpha_predeclared_gate_walk_forward(
        conn,
        chains,
        min_sample_count=12,
        min_retained_windows=4,
    )

    assert review["passed_predeclared_walk_forward_gate"] is False
    assert "missing_excluded_risk_off_window_in_validation_set" in review["blocking_reasons"]
    assert review["promotion_status"] == "research_only_blocked_from_observation"


def test_predeclared_gate_blocks_calibration_window_reuse():
    conn = _conn_with_regimes(
        {
            "20260101": 1.0,
            "20260102": 0.8,
            "20260103": 1.2,
            "20260104": 0.6,
            "20260105": -2.0,
        }
    )
    chains = [
        _chain("20260101", 1.0),
        _chain("20260102", 0.8),
        _chain("20260103", 0.6),
        _chain("20260104", 0.4),
        _chain("20260105", -1.0),
    ]

    review = build_ensemble_alpha_predeclared_gate_walk_forward(
        conn,
        chains,
        min_sample_count=12,
        min_retained_windows=4,
        calibration_as_of_dates=["2026-01-05"],
    )

    assert review["passed_predeclared_walk_forward_gate"] is False
    assert "validation_window_overlaps_calibration:20260105" in review["blocking_reasons"]


def _conn_with_regimes(day_pct: dict[str, float]) -> sqlite3.Connection:
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
    for day, pct in day_pct.items():
        for idx in range(10):
            conn.execute("INSERT INTO daily_trading_data VALUES (?, ?, ?, ?)", (f"000{idx:03d}.SZ", day, pct, 1000.0))
    return conn


def _chain(as_of: str, forward_scale: float) -> dict:
    return {
        "as_of_date": as_of,
        "sample_facts": [
            _item(f"{as_of[-2:]}001.SZ", "v4", 80.0, 2.0 * forward_scale),
            _item(f"{as_of[-2:]}002.SZ", "v5", 60.0, 1.0 * forward_scale),
            _item(f"{as_of[-2:]}003.SZ", "v8", 20.0, -1.0 * forward_scale),
        ],
    }


def _item(ts_code: str, strategy: str, score: float, forward_return: float) -> dict:
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
                    "capacity_liquidity": {"score": 70.0},
                    "margin_pressure": {"score": score},
                }
            }
        },
        "forward_returns": {"5": {"available": True, "return_pct": forward_return}},
    }
