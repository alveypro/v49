from __future__ import annotations

import sqlite3

from openclaw.services.backtest_credibility_service import (
    evaluate_backtest_credibility,
    extract_backtest_credibility_from_signal_run,
)
from openclaw.services.lineage_service import apply_professional_migrations, insert_signal_run, new_run_id


def _credible_audit():
    return {
        "point_in_time_data": True,
        "suspension_and_limit_handling": True,
        "volume_constraint": True,
        "cost_model": True,
        "slippage_model": True,
        "in_sample_out_of_sample_split": True,
        "parameter_sensitivity": True,
        "failed_backtests_recorded": True,
        "sample": {
            "in_sample": {"start": "2025-01-01", "end": "2025-12-31"},
            "out_of_sample": {"start": "2026-01-01", "end": "2026-04-30"},
        },
        "metrics": {"win_rate": 0.56, "max_drawdown": 0.08},
    }


def test_evaluate_backtest_credibility_requires_market_reality_controls():
    result = evaluate_backtest_credibility(
        {
            "point_in_time_data": True,
            "cost_model": True,
            "sample": {"in_sample": {"start": "2025-01-01"}},
            "metrics": {"win_rate": 0.6},
        }
    )

    assert result["passed"] is False
    assert "missing_or_failed:suspension_and_limit_handling" in result["blocking_reasons"]
    assert "missing_or_failed:volume_constraint" in result["blocking_reasons"]
    assert "missing_out_of_sample_window" in result["blocking_reasons"]


def test_extract_backtest_credibility_passes_for_complete_backtest_signal_run(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)
    run_id = new_run_id("backtest", "combo")
    insert_signal_run(
        conn,
        run_id=run_id,
        run_type="backtest",
        strategy="combo",
        trade_date="2026-05-01",
        data_version="trade_date:20260501",
        code_version="git:test:dirty0",
        param_version="param:sha256:test",
        status="success",
        summary={"backtest_credibility": _credible_audit()},
    )

    result = extract_backtest_credibility_from_signal_run(conn, run_id=run_id)
    conn.close()

    assert result["passed"] is True
    assert result["blocking_reasons"] == []
    assert result["strategy"] == "combo"


def test_extract_backtest_credibility_blocks_plain_scan_run(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)
    run_id = new_run_id("scan", "combo")
    insert_signal_run(
        conn,
        run_id=run_id,
        run_type="scan",
        strategy="combo",
        trade_date="2026-05-01",
        data_version="trade_date:20260501",
        code_version="git:test:dirty0",
        param_version="param:sha256:test",
        status="success",
        summary={"backtest_credibility": _credible_audit()},
    )

    result = extract_backtest_credibility_from_signal_run(conn, run_id=run_id)
    conn.close()

    assert result["passed"] is False
    assert result["blocking_reasons"][0] == "not_backtest_or_experiment_run"


def test_extract_backtest_credibility_reads_experiment_backtest_audit(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)
    run_id = new_run_id("experiment", "stable")
    insert_signal_run(
        conn,
        run_id=run_id,
        run_type="experiment",
        strategy="stable",
        trade_date="2026-05-01",
        data_version="trade_date:20260501",
        code_version="git:test:dirty0",
        param_version="param:sha256:test",
        status="success",
        summary={
            "governance_type": "experiment_strategy_evidence",
            "backtest_audit": _credible_audit(),
        },
    )

    result = extract_backtest_credibility_from_signal_run(conn, run_id=run_id)
    conn.close()

    assert result["passed"] is True
    assert result["run_type"] == "experiment"
