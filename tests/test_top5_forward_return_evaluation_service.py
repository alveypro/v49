from __future__ import annotations

import sqlite3

import pytest

from openclaw.services.top5_forward_return_evaluation_service import (
    compact_trade_date,
    evaluate_top5_forward_returns,
    forward_evaluation_gate_failures,
    infer_as_of_trade_date_compact_from_signal_refs,
)


def _seed_daily(conn: sqlite3.Connection, ts_code: str, dates: list[str], closes: list[float]) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_trading_data (
            ts_code TEXT,
            trade_date TEXT,
            open_price REAL,
            high_price REAL,
            low_price REAL,
            close_price REAL,
            vol REAL,
            amount REAL,
            pct_chg REAL,
            turnover_rate REAL
        )
        """
    )
    for d, c in zip(dates, closes, strict=False):
        conn.execute(
            """
            INSERT OR REPLACE INTO daily_trading_data
            (ts_code, trade_date, open_price, high_price, low_price, close_price, vol, amount, pct_chg, turnover_rate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (ts_code, d, c, c, c, c, 1.0, 1.0, 0.0, 0.0),
        )
    conn.commit()


def test_compact_trade_date_normalizes() -> None:
    assert compact_trade_date("2026-05-02") == "20260502"
    assert compact_trade_date("20260502") == "20260502"
    assert compact_trade_date("") == ""


def test_evaluate_top5_forward_returns_weighted_and_equal() -> None:
    conn = sqlite3.connect(":memory:")
    codes = ["AAA.SH", "BBB.SH"]
    dates = ["20260502", "20260503", "20260504", "20260505"]
    _seed_daily(conn, codes[0], dates, [10.0, 11.0, 11.0, 12.0])
    _seed_daily(conn, codes[1], dates, [20.0, 20.0, 22.0, 22.0])

    artifact = {
        "artifact_version": "strategy_competition_portfolio_audit.v1",
        "competition_run_id": "comp_test",
        "ranking_method_hash": "abc",
        "trade_date": "20260502",
        "portfolio_constraints": {"top_stock_count": 2},
        "top5_portfolio_audit": [
            {"ts_code": codes[0], "weight": 0.5},
            {"ts_code": codes[1], "weight": 0.5},
        ],
    }
    out = evaluate_top5_forward_returns(conn, artifact=artifact, horizons=[1, 2])
    conn.close()

    assert out["blocking_reasons"] == []
    # AAA: day1 10 -> 11 => +10%; BBB: day1 20 -> 20 => 0%, day2 20 -> 22 => +10%
    assert out["per_symbol"][0]["forward"]["1"]["return_pct"] == pytest.approx(10.0)
    assert out["per_symbol"][1]["forward"]["1"]["return_pct"] == pytest.approx(0.0)
    assert out["per_symbol"][1]["forward"]["2"]["return_pct"] == pytest.approx(10.0)
    assert out["per_symbol"][0]["forward"]["2"]["return_pct"] == pytest.approx(10.0)

    pw1 = out["portfolio"]["1"]["gross_return_weighted_pct"]
    assert pw1 is not None and abs(float(pw1) - 5.0) < 1e-5  # 0.5*10 + 0.5*0
    ew1 = out["equal_weight_portfolio"]["1"]["gross_return_equal_weight_pct"]
    assert ew1 is not None and abs(float(ew1) - 5.0) < 1e-5

    pw2 = out["portfolio"]["2"]["gross_return_weighted_pct"]
    assert pw2 is not None and abs(float(pw2) - 10.0) < 1e-5


def test_infer_as_of_from_run_ids() -> None:
    artifact = {
        "top5_portfolio_audit": [
            {
                "source": {
                    "signal_refs": [
                        {"run_id": "run_scan_v4_20260506_055433_e11ae2dd"},
                        {"run_id": "run_scan_v7_20260507_080601_5abbdbe8"},
                    ]
                }
            }
        ]
    }
    assert infer_as_of_trade_date_compact_from_signal_refs(artifact) == "20260507"


def test_evaluate_resolves_as_of_via_signal_refs() -> None:
    conn = sqlite3.connect(":memory:")
    _seed_daily(conn, "AAA.SH", ["20260507", "20260508"], [100.0, 101.0])
    artifact = {
        "trade_date": "",
        "top5_portfolio_audit": [
            {
                "ts_code": "AAA.SH",
                "weight": 1.0,
                "source": {"signal_refs": [{"run_id": "run_scan_v4_20260507_055433_x"}]},
            }
        ],
    }
    out = evaluate_top5_forward_returns(conn, artifact=artifact, horizons=[1])
    conn.close()
    assert out["as_of_trade_date_compact"] == "20260507"
    assert "as_of_inferred_from_signal_ref_run_ids" in out["as_of_resolution_notes"]
    assert out["per_symbol"][0]["forward"]["1"]["return_pct"] == pytest.approx(1.0)


def test_forward_evaluation_gate_failures_inferred_as_of() -> None:
    ev = {
        "blocking_reasons": [],
        "as_of_resolution_notes": ["as_of_inferred_from_signal_ref_run_ids"],
        "per_symbol": [{"ts_code": "X.SH", "forward": {"5": {"available": True}}}],
    }
    failures = forward_evaluation_gate_failures(ev, horizons=[5], fail_on_inferred_as_of=True)
    assert "as_of_inferred_from_signal_ref_run_ids" in failures


def test_forward_evaluation_gate_failures_min_symbols() -> None:
    ev = {
        "blocking_reasons": [],
        "as_of_resolution_notes": [],
        "per_symbol": [
            {"ts_code": "X.SH", "forward": {"5": {"available": True}}},
            {"ts_code": "Y.SH", "forward": {"5": {"available": False}}},
        ],
    }
    failures = forward_evaluation_gate_failures(ev, horizons=[5], min_available_symbols_per_horizon=2)
    assert any(x.startswith("coverage_symbols_horizon_5:") for x in failures)


def test_forward_evaluation_gate_failures_ratio() -> None:
    ev = {
        "blocking_reasons": [],
        "as_of_resolution_notes": [],
        "per_symbol": [
            {"ts_code": "X.SH", "forward": {"5": {"available": True}}},
            {"ts_code": "Y.SH", "forward": {"5": {"available": False}}},
        ],
    }
    failures = forward_evaluation_gate_failures(ev, horizons=[5], min_available_ratio_per_horizon=1.0)
    assert any(x.startswith("coverage_ratio_horizon_5:") for x in failures)


def test_forward_evaluation_gate_failures_eval_blocking() -> None:
    ev = {"blocking_reasons": ["missing_as_of_trade_date"], "as_of_resolution_notes": [], "per_symbol": []}
    failures = forward_evaluation_gate_failures(ev, horizons=[5], fail_on_eval_blocking=True)
    assert "eval_blocking_reasons_non_empty" in failures


def test_missing_as_of_blocks_evaluation() -> None:
    conn = sqlite3.connect(":memory:")
    artifact = {
        "trade_date": "",
        "top5_portfolio_audit": [{"ts_code": "AAA.SH", "weight": 1.0}],
    }
    out = evaluate_top5_forward_returns(conn, artifact=artifact, horizons=[5])
    conn.close()
    assert "missing_as_of_trade_date" in out["blocking_reasons"]
