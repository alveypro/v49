from __future__ import annotations

import json
import sqlite3

import pytest

from openclaw.services.data_version_service import build_param_version
from openclaw.services.decision_service import record_decision_event, upsert_decision_snapshot
from openclaw.services.execution_analytics_service import compute_execution_attribution, upsert_execution_attribution
from openclaw.services.execution_fill_service import record_execution_fill
from openclaw.services.execution_order_service import create_execution_order, update_execution_order_status
from openclaw.services.experiment_governance_service import (
    evaluate_experiment_promotion_readiness,
    record_experiment_governance_decision,
    record_experiment_signal_evidence,
    strategy_tier,
)
from openclaw.services.lineage_service import (
    apply_professional_migrations,
    insert_signal_run,
    new_decision_id,
    new_fill_id,
    new_order_id,
    new_run_id,
)


def _complete_evidence():
    return {
        "hypothesis": "stable strategy can improve drawdown control without entering default production",
        "params": {"score_threshold": 60, "holding_days": 10},
        "sample_window": {"start": "2025-01-01", "end": "2025-12-31"},
        "out_of_sample_window": {"start": "2026-01-01", "end": "2026-04-30"},
        "backtest_audit": {
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
        },
        "execution_evidence": {
            "mode": "shadow",
            "sample_count": 7,
            "linked_decision_ids": [],
        },
    }


def _seed_shadow_execution_decision(conn: sqlite3.Connection) -> tuple[str, str]:
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
    )
    decision_id = new_decision_id()
    record_decision_event(
        conn,
        decision_id=decision_id,
        decision_type="approve",
        based_on_run_id=run_id,
        risk_gate_state={"risk_level": "green"},
        release_gate_state={"passed": True},
        approval_reason_codes=["risk_pass"],
        approval_note="shadow execution evidence",
        operator_name="alice",
        decision_payload={"selected_count": 1},
    )
    upsert_decision_snapshot(conn, decision_id=decision_id, decision_status="active", active_flag=True)
    order_id = new_order_id()
    create_execution_order(
        conn,
        order_id=order_id,
        decision_id=decision_id,
        ts_code="000001.SZ",
        target_qty=1000,
        decision_price=10.0,
        submitted_price=10.05,
        status="submitted",
        broker_ref="sim:shadow",
        source_type="shadow",
    )
    update_execution_order_status(conn, order_id=order_id, status="filled")
    record_execution_fill(
        conn,
        fill_id=new_fill_id(),
        order_id=order_id,
        fill_price=10.08,
        fill_qty=1000,
        fill_slippage_bp=80.0,
        venue="SIM",
    )
    upsert_execution_attribution(
        conn,
        order_id=order_id,
        attribution=compute_execution_attribution(
            decision_price=10.0,
            submit_price=10.05,
            avg_fill_price=10.08,
            close_price=10.2,
            target_qty=1000,
            filled_qty=1000,
            delay_sec=18,
        ),
    )
    return run_id, decision_id


def test_strategy_tier_uses_existing_registry():
    assert strategy_tier("v5") == "production"
    assert strategy_tier("stable") == "experimental"
    assert strategy_tier("unknown") == "unknown"


def test_experiment_governance_rejects_production_strategy_as_experiment(tmp_db):
    conn = sqlite3.connect(str(tmp_db))

    with pytest.raises(ValueError, match="strategy_not_experimental"):
        record_experiment_signal_evidence(
            conn,
            run_id=new_run_id("experiment", "v5"),
            strategy="v5",
            trade_date="2026-05-01",
            data_version="trade_date:20260501",
            code_version="git:test:dirty0",
            param_version=build_param_version({"score_threshold": 60}),
            hypothesis="production strategy cannot be registered as experimental evidence",
            params={"score_threshold": 60},
            sample_window={"start": "2025-01-01", "end": "2025-12-31"},
            out_of_sample_window={"start": "2026-01-01", "end": "2026-04-30"},
            backtest_audit={"passed": True},
            execution_evidence={"mode": "shadow", "sample_count": 1},
        )
    conn.close()


def test_incomplete_experiment_evidence_blocks_promotion_and_records_reject_decision(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    run_id = new_run_id("experiment", "v6")
    record_experiment_signal_evidence(
        conn,
        run_id=run_id,
        strategy="v6",
        trade_date="2026-05-01",
        data_version="trade_date:20260501",
        code_version="git:test:dirty0",
        param_version=build_param_version({"score_threshold": 75}),
        hypothesis="v6 short cycle experiment requires out-of-sample proof before production candidacy",
        params={"score_threshold": 75},
        sample_window={"start": "2025-01-01", "end": "2025-06-30"},
        out_of_sample_window={},
        backtest_audit={"passed": False, "reason": "missing_cost_model"},
        execution_evidence={"mode": "shadow", "sample_count": 0},
    )

    readiness = evaluate_experiment_promotion_readiness(conn, run_id=run_id)
    assert readiness["allow_promotion_candidate"] is False
    assert "missing_out_of_sample_window" in readiness["blocking_reasons"]
    assert "backtest_credibility_not_passed" in readiness["blocking_reasons"]
    assert "missing_execution_sample" in readiness["blocking_reasons"]
    assert "missing_execution_decision_links" in readiness["blocking_reasons"]

    with pytest.raises(ValueError, match="experiment_promotion_not_ready"):
        record_experiment_governance_decision(
            conn,
            run_id=run_id,
            decision_type="experiment_promote_candidate",
            operator_name="alice",
        )

    decision = record_experiment_governance_decision(
        conn,
        run_id=run_id,
        decision_type="experiment_reject",
        operator_name="alice",
        approval_reason_codes=["backtest_credibility_not_passed", "missing_execution_sample"],
        approval_note="reject until out-of-sample and execution evidence are available",
    )
    row = conn.execute(
        "SELECT decision_type, based_on_run_id, decision_payload_json FROM decision_events WHERE decision_id = ?",
        (decision["decision_id"],),
    ).fetchone()
    snapshot = conn.execute(
        "SELECT decision_status, active_flag FROM decision_snapshot WHERE decision_id = ?",
        (decision["decision_id"],),
    ).fetchone()
    payload = json.loads(row[2])
    conn.close()
    assert row[0:2] == ("experiment_reject", run_id)
    assert snapshot == ("rejected", 0)
    assert payload["readiness"]["allow_promotion_candidate"] is False


def test_experiment_promotion_requires_full_backtest_credibility_not_just_passed_flag(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    run_id = new_run_id("experiment", "stable")
    record_experiment_signal_evidence(
        conn,
        run_id=run_id,
        strategy="stable",
        trade_date="2026-05-01",
        data_version="trade_date:20260501",
        code_version="git:test:dirty0",
        param_version=build_param_version({"score_threshold": 60}),
        hypothesis="passed flag alone is not enough for production candidacy",
        params={"score_threshold": 60},
        sample_window={"start": "2025-01-01", "end": "2025-12-31"},
        out_of_sample_window={"start": "2026-01-01", "end": "2026-04-30"},
        backtest_audit={"passed": True},
        execution_evidence={"mode": "shadow", "sample_count": 3, "linked_decision_ids": ["dec_shadow_fixture"]},
    )

    readiness = evaluate_experiment_promotion_readiness(conn, run_id=run_id)
    conn.close()

    assert readiness["allow_promotion_candidate"] is False
    assert "backtest_credibility_not_passed" in readiness["blocking_reasons"]
    assert "missing_or_failed:point_in_time_data" in readiness["evidence"]["backtest_credibility"]["blocking_reasons"]


def test_experiment_promotion_requires_execution_decision_links(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    evidence = _complete_evidence()
    evidence["execution_evidence"] = {"mode": "shadow", "sample_count": 3}
    run_id = new_run_id("experiment", "stable")
    record_experiment_signal_evidence(
        conn,
        run_id=run_id,
        strategy="stable",
        trade_date="2026-05-01",
        data_version="trade_date:20260501",
        code_version="git:test:dirty0",
        param_version=build_param_version(evidence["params"]),
        hypothesis=evidence["hypothesis"],
        params=evidence["params"],
        sample_window=evidence["sample_window"],
        out_of_sample_window=evidence["out_of_sample_window"],
        backtest_audit=evidence["backtest_audit"],
        execution_evidence=evidence["execution_evidence"],
    )

    readiness = evaluate_experiment_promotion_readiness(conn, run_id=run_id)
    conn.close()

    assert readiness["allow_promotion_candidate"] is False
    assert "missing_execution_decision_links" in readiness["blocking_reasons"]


def test_complete_experiment_evidence_can_enter_candidate_decision_without_changing_registry(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    _, shadow_decision_id = _seed_shadow_execution_decision(conn)
    evidence = _complete_evidence()
    evidence["execution_evidence"]["linked_decision_ids"] = [shadow_decision_id]
    run_id = new_run_id("experiment", "stable")
    record_experiment_signal_evidence(
        conn,
        run_id=run_id,
        strategy="stable",
        trade_date="2026-05-01",
        data_version="trade_date:20260501",
        code_version="git:test:dirty0",
        param_version=build_param_version(evidence["params"]),
        hypothesis=evidence["hypothesis"],
        params=evidence["params"],
        sample_window=evidence["sample_window"],
        out_of_sample_window=evidence["out_of_sample_window"],
        backtest_audit=evidence["backtest_audit"],
        execution_evidence=evidence["execution_evidence"],
        artifact_path="fixture://experiment/stable_shadow.json",
    )

    readiness = evaluate_experiment_promotion_readiness(conn, run_id=run_id)
    assert readiness["allow_promotion_candidate"] is True
    assert readiness["blocking_reasons"] == []
    assert readiness["evidence"]["execution_review"]["passed"] is True

    decision = record_experiment_governance_decision(
        conn,
        run_id=run_id,
        decision_type="experiment_promote_candidate",
        operator_name="alice",
        approval_reason_codes=["risk_pass", "shadow_execution_observed"],
        approval_note="candidate only; strategy registry remains unchanged",
    )
    row = conn.execute(
        """
        SELECT e.decision_type, e.based_on_run_id, s.strategy, e.release_gate_state, e.decision_payload_json
        FROM decision_events e
        JOIN signal_runs s ON s.run_id = e.based_on_run_id
        WHERE e.decision_id = ?
        """,
        (decision["decision_id"],),
    ).fetchone()
    snapshot = conn.execute(
        "SELECT decision_status, active_flag FROM decision_snapshot WHERE decision_id = ?",
        (decision["decision_id"],),
    ).fetchone()
    release_gate_state = json.loads(row[3])
    payload = json.loads(row[4])
    conn.close()

    assert row[0:3] == ("experiment_promote_candidate", run_id, "stable")
    assert release_gate_state == {"release_gate_required": False, "source": "experiment_governance"}
    assert snapshot == ("candidate", 1)
    assert payload["readiness"]["allow_promotion_candidate"] is True


def test_experiment_promotion_blocks_when_linked_execution_review_fails(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)
    decision_id = new_decision_id()
    record_decision_event(
        conn,
        decision_id=decision_id,
        decision_type="approve",
        based_on_run_id="",
        risk_gate_state={"risk_level": "green"},
        release_gate_state={"passed": True},
        approval_reason_codes=["risk_pass"],
        approval_note="bad shadow execution evidence",
        operator_name="alice",
        decision_payload={"selected_count": 1},
    )
    upsert_decision_snapshot(conn, decision_id=decision_id, decision_status="active", active_flag=True)
    order_id = new_order_id()
    create_execution_order(
        conn,
        order_id=order_id,
        decision_id=decision_id,
        ts_code="000001.SZ",
        target_qty=1000,
        decision_price=10.0,
        submitted_price=10.1,
        status="cancelled",
    )
    upsert_execution_attribution(
        conn,
        order_id=order_id,
        attribution=compute_execution_attribution(
            decision_price=10.0,
            submit_price=10.1,
            avg_fill_price=0.0,
            close_price=10.2,
            target_qty=1000,
            filled_qty=0,
            delay_sec=30,
        ),
    )
    evidence = _complete_evidence()
    evidence["execution_evidence"]["linked_decision_ids"] = [decision_id]
    run_id = new_run_id("experiment", "stable")
    record_experiment_signal_evidence(
        conn,
        run_id=run_id,
        strategy="stable",
        trade_date="2026-05-01",
        data_version="trade_date:20260501",
        code_version="git:test:dirty0",
        param_version=build_param_version(evidence["params"]),
        hypothesis=evidence["hypothesis"],
        params=evidence["params"],
        sample_window=evidence["sample_window"],
        out_of_sample_window=evidence["out_of_sample_window"],
        backtest_audit=evidence["backtest_audit"],
        execution_evidence=evidence["execution_evidence"],
    )

    readiness = evaluate_experiment_promotion_readiness(conn, run_id=run_id)
    conn.close()

    assert readiness["allow_promotion_candidate"] is False
    assert "execution_evidence_review_not_passed" in readiness["blocking_reasons"]
    assert readiness["evidence"]["execution_review"]["passed"] is False
