from __future__ import annotations

import sqlite3

from openclaw.services.ensemble_execution_cost_service import build_ensemble_execution_cost_replay


def test_ensemble_execution_cost_replay_reports_empty_research_portfolio_blocker():
    conn = sqlite3.connect(":memory:")

    review = build_ensemble_execution_cost_replay(
        conn,
        {
            "research_only": True,
            "not_for_production": True,
            "shadow_weights": [],
        },
        as_of_date="2026-01-01",
    )

    assert review["research_only"] is True
    assert review["not_for_production"] is True
    assert review["gross_return"] == 0.0
    assert review["net_return"] == 0.0
    assert review["turnover"] == 0.0
    assert "missing_shadow_weights" in review["blocking_reasons"]
    assert "do_not_treat_after_cost_replay_as_alpha_evidence" in review["hard_boundaries"]


def test_ensemble_execution_cost_replay_computes_after_cost_return():
    conn = sqlite3.connect(":memory:")
    _create_prices(conn)
    _insert_prices(conn, "000001.SZ", [10.0, 10.2, 10.3, 10.4, 10.5, 11.0])

    review = build_ensemble_execution_cost_replay(
        conn,
        {
            "research_only": True,
            "not_for_production": True,
            "shadow_weights": [
                {
                    "ts_code": "000001.SZ",
                    "weight": 0.2,
                    "latest_amount": 1_000_000.0,
                }
            ],
            "capacity_usage": {"000001.SZ": 0.05},
        },
        as_of_date="20260101",
        holding_days=5,
    )

    assert review["blocking_reasons"] == []
    assert review["gross_return"] > 0.0
    assert review["cost_bps"] > 0.0
    assert review["slippage_bps"] > 0.0
    assert review["net_return"] < review["gross_return"]
    assert review["turnover"] == 0.2
    assert review["trade_replay"][0]["traded"] is True


def test_ensemble_execution_cost_replay_blocks_limit_and_capacity_cases():
    conn = sqlite3.connect(":memory:")
    _create_prices(conn)
    _insert_prices(conn, "000001.SZ", [10.0, 10.2, 10.3, 10.4, 10.5, 11.0], first_pct=10.0)
    _insert_prices(conn, "000002.SZ", [20.0, 20.1, 20.2, 20.3, 20.4, 20.5])

    review = build_ensemble_execution_cost_replay(
        conn,
        {
            "research_only": True,
            "not_for_production": True,
            "shadow_weights": [
                {"ts_code": "000001.SZ", "weight": 0.1, "latest_amount": 1_000_000.0},
                {"ts_code": "000002.SZ", "weight": 0.1, "latest_amount": 1_000_000.0},
            ],
            "capacity_usage": {"000001.SZ": 0.05, "000002.SZ": 0.2},
        },
        as_of_date="20260101",
        holding_days=5,
        max_capacity_usage=0.1,
    )

    assert review["limit_state_blocked_names"] == ["000001.SZ"]
    assert review["capacity_blocked_names"] == ["000002.SZ"]
    assert review["turnover"] == 0.0
    assert {item["blocking_reason"] for item in review["trade_replay"]} == {
        "limit_state_blocked",
        "capacity_usage_exceeds_limit",
    }


def _create_prices(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE daily_trading_data (
            ts_code TEXT,
            trade_date TEXT,
            close_price REAL,
            pct_chg REAL,
            amount REAL
        )
        """
    )


def _insert_prices(conn: sqlite3.Connection, ts_code: str, closes: list[float], first_pct: float = 0.0) -> None:
    rows = []
    for idx, close in enumerate(closes):
        rows.append((ts_code, f"202601{idx + 1:02d}", close, first_pct if idx == 0 else 1.0, 1_000_000.0))
    conn.executemany("INSERT INTO daily_trading_data VALUES (?, ?, ?, ?, ?)", rows)
