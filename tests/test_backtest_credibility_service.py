from __future__ import annotations

import json
import sqlite3

from openclaw.services.backtest_credibility_service import (
    attach_backtest_credibility_to_signal_run,
    build_backtest_credibility_audit,
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
        "metrics": {"win_rate": 0.56, "max_drawdown": 0.08, "signal_density": 0.04, "test_windows": 3},
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


def test_attach_backtest_credibility_to_signal_run_updates_successful_backtest_only(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)
    run_id = new_run_id("backtest", "v5")
    insert_signal_run(
        conn,
        run_id=run_id,
        run_type="backtest",
        strategy="v5",
        trade_date="2026-05-02",
        data_version="trade_date:20260502",
        code_version="git:test:dirty0",
        param_version="param:sha256:test",
        status="success",
        summary={"metrics": {"win_rate": 0.56}},
    )

    result = attach_backtest_credibility_to_signal_run(
        conn,
        run_id=run_id,
        audit=_credible_audit(),
        operator_name="alice",
        evidence_note="candidate DB rehearsal",
    )
    extracted = extract_backtest_credibility_from_signal_run(conn, run_id=run_id)
    row = conn.execute("SELECT summary_json FROM signal_runs WHERE run_id = ?", (run_id,)).fetchone()
    summary = json.loads(row[0])
    conn.close()

    assert result["attached"] is True
    assert result["review"]["passed"] is True
    assert extracted["passed"] is True
    assert summary["backtest_credibility"]["cost_model"] is True
    assert summary["backtest_credibility_attestation"]["operator_name"] == "alice"


def test_attach_backtest_credibility_rejects_incomplete_audit(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)
    run_id = new_run_id("backtest", "v5")
    insert_signal_run(
        conn,
        run_id=run_id,
        run_type="backtest",
        strategy="v5",
        trade_date="2026-05-02",
        data_version="trade_date:20260502",
        code_version="git:test:dirty0",
        param_version="param:sha256:test",
        status="success",
        summary={},
    )

    try:
        attach_backtest_credibility_to_signal_run(
            conn,
            run_id=run_id,
            audit={"point_in_time_data": True},
            operator_name="alice",
        )
    except ValueError as exc:
        message = str(exc)
    else:
        message = ""
    conn.close()

    assert message.startswith("backtest_credibility_not_passed:")
    assert "missing_or_failed:cost_model" in message


def test_build_backtest_credibility_audit_passes_only_with_runtime_evidence():
    result = {
        "status": "success",
        "result": {
            "summary": {
                "win_rate": 0.57,
                "max_drawdown": 0.12,
                "signal_density": 0.08,
                "tradeability_filter_enabled": True,
                "volume_constraint_enabled": True,
                "trading_cost": {"slippage_bp": 10.0, "expected_cost_pct": 0.01},
            },
            "rolling": {
                "train_test_separated": True,
                "train_windows": 3,
                "test_windows": 3,
                "failed_windows": [],
            },
        },
    }

    audit = build_backtest_credibility_audit(result=result, params={"mode": "rolling"}, param_runs=2, failed_runs=[])
    review = evaluate_backtest_credibility(audit)

    assert audit["point_in_time_data"] is True
    assert audit["suspension_and_limit_handling"] is True
    assert audit["volume_constraint"] is True
    assert audit["parameter_sensitivity"] is True
    assert review["passed"] is True


def test_build_backtest_credibility_accepts_round_trip_cost_as_slippage_evidence():
    result = {
        "status": "success",
        "result": {
            "summary": {
                "win_rate": 0.57,
                "max_drawdown": 0.12,
                "signal_density": 0.08,
                "tradeability_filter_enabled": True,
                "volume_constraint_enabled": True,
                "trading_cost": {"base_round_trip_bp": 46.0, "expected_cost_bp": 4.6},
            },
            "rolling": {
                "train_test_separated": True,
                "train_windows": 3,
                "test_windows": 3,
                "failed_windows": [],
            },
        },
    }

    audit = build_backtest_credibility_audit(result=result, params={"mode": "rolling"}, param_runs=2, failed_runs=[])
    review = evaluate_backtest_credibility(audit)

    assert audit["slippage_model"] is True
    assert review["passed"] is True


def test_build_backtest_credibility_preserves_zero_drawdown_metric():
    result = {
        "status": "success",
        "result": {
            "summary": {
                "win_rate": 1.0,
                "max_drawdown": 0.0,
                "signal_density": 0.1,
                "tradeability_filter_enabled": True,
                "volume_constraint_enabled": True,
                "trading_cost": {"base_round_trip_bp": 46.0, "expected_cost_bp": 4.6},
            },
            "rolling": {
                "train_test_separated": True,
                "train_windows": 3,
                "test_windows": 3,
                "failed_windows": [],
            },
        },
    }

    audit = build_backtest_credibility_audit(result=result, params={"mode": "rolling"}, param_runs=2, failed_runs=[])

    assert audit["metrics"]["max_drawdown"] == 0.0


def test_build_backtest_credibility_audit_blocks_single_parameter_zero_density():
    result = {
        "status": "success",
        "result": {
            "summary": {
                "win_rate": 0.0,
                "max_drawdown": 0.3,
                "signal_density": 0.0,
                "tradeability_filter_enabled": True,
                "volume_constraint_enabled": True,
                "trading_cost": {"slippage_bp": 10.0, "expected_cost_pct": 0.01},
            },
            "rolling": {
                "train_test_separated": True,
                "train_windows": 1,
                "test_windows": 1,
                "failed_windows": [],
            },
        },
    }

    audit = build_backtest_credibility_audit(result=result, params={"mode": "rolling"}, param_runs=1, failed_runs=[])
    review = evaluate_backtest_credibility(audit)

    assert review["passed"] is False
    assert "missing_or_failed:parameter_sensitivity" in review["blocking_reasons"]
    assert "missing_positive_signal_density" in review["blocking_reasons"]
