from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path

from openclaw.services.decision_service import record_decision_event, upsert_decision_snapshot
from openclaw.services.execution_analytics_service import upsert_execution_attribution
from openclaw.services.execution_order_service import create_execution_order
from openclaw.services.lineage_service import (
    apply_professional_migrations,
    insert_signal_run,
    new_decision_id,
    new_order_id,
    new_release_id,
    new_run_id,
    replace_signal_items,
)
from openclaw.services.release_dry_run_service import (
    render_release_dry_run_trend_markdown,
    run_release_dry_run_audit,
    summarize_release_dry_run_trend,
)
from openclaw.services.release_event_service import record_release_event, record_release_validation


def _insert_complete_signal(conn: sqlite3.Connection) -> str:
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
    replace_signal_items(
        conn,
        run_id=run_id,
        items=[{"ts_code": "000001.SZ", "score": 88, "rank_idx": 1, "reason_codes": ["risk_pass"]}],
    )
    return run_id


def _insert_complete_decision(conn: sqlite3.Connection, run_id: str) -> str:
    decision_id = new_decision_id()
    record_decision_event(
        conn,
        decision_id=decision_id,
        decision_type="approve",
        based_on_run_id=run_id,
        risk_gate_state={"risk_level": "green"},
        release_gate_state={"passed": True},
        approval_reason_codes=["risk_pass"],
        approval_note="release approved",
        operator_name="alice",
        decision_payload={"selected_count": 1},
    )
    upsert_decision_snapshot(
        conn,
        decision_id=decision_id,
        decision_status="active",
        effective_trade_date="2026-05-01",
        selected_count=1,
        active_flag=True,
    )
    return decision_id


def _insert_complete_execution(conn: sqlite3.Connection, decision_id: str) -> str:
    order_id = new_order_id()
    create_execution_order(
        conn,
        order_id=order_id,
        decision_id=decision_id,
        ts_code="000001.SZ",
        target_qty=1000,
        decision_price=10.0,
        submitted_price=10.05,
        status="filled",
        source_type="overnight",
    )
    upsert_execution_attribution(
        conn,
        order_id=order_id,
        attribution={
            "fill_ratio": 1.0,
            "slippage_bp": 8.0,
            "opportunity_cost_bp": 0.0,
            "delay_sec": 18,
            "miss_reason_code": "",
            "attribution_json": {"source": "dry_run_fixture"},
        },
    )
    return order_id


def _insert_rollback_reference(conn: sqlite3.Connection) -> str:
    release_id = new_release_id()
    record_release_event(
        conn,
        release_id=release_id,
        release_type="deploy",
        code_version="git:test:dirty0",
        operator_name="alice",
        gate_result={"passed": True},
        payload={"rollback_context": {"previous_release_id": "rel_previous"}},
    )
    record_release_validation(
        conn,
        release_id=release_id,
        validation_type="pytest",
        validation_status="passed",
        validation_output_path="logs/pytest.log",
    )
    return release_id


def _seed_complete_release_ready_fixture(db_path) -> str:
    conn = sqlite3.connect(str(db_path))
    apply_professional_migrations(conn)
    run_id = _insert_complete_signal(conn)
    decision_id = _insert_complete_decision(conn, run_id)
    _insert_complete_execution(conn, decision_id)
    release_id = _insert_rollback_reference(conn)
    conn.close()
    return release_id


def test_release_dry_run_blocks_missing_signal_chain(tmp_db, tmp_path):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)
    _insert_rollback_reference(conn)
    conn.close()

    payload = run_release_dry_run_audit(db_path=tmp_db, code_root=tmp_path, operator_name="alice")

    assert payload["allow_release_gate"] is False
    assert "signal_chain_empty" in payload["blocking_reasons"]
    assert payload["validation_statuses"]["signal_chain_evidence"] == "failed"


def test_release_dry_run_blocks_missing_execution_evidence(tmp_db, tmp_path):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)
    run_id = _insert_complete_signal(conn)
    _insert_complete_decision(conn, run_id)
    _insert_rollback_reference(conn)
    conn.close()

    payload = run_release_dry_run_audit(db_path=tmp_db, code_root=tmp_path, operator_name="alice")

    assert payload["allow_release_gate"] is False
    assert "execution_chain_empty" in payload["blocking_reasons"]
    assert payload["validation_statuses"]["execution_chain_evidence"] == "failed"


def test_release_dry_run_blocks_missing_rollback_reference(tmp_db, tmp_path):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)
    run_id = _insert_complete_signal(conn)
    decision_id = _insert_complete_decision(conn, run_id)
    _insert_complete_execution(conn, decision_id)
    conn.close()

    payload = run_release_dry_run_audit(db_path=tmp_db, code_root=tmp_path, operator_name="alice")

    assert payload["allow_release_gate"] is False
    assert "rollback_reference:no_prior_release_event" in payload["blocking_reasons"]
    assert payload["validation_statuses"]["rollback_reference"] == "failed"


def test_release_dry_run_allows_release_gate_with_complete_fact_chains_and_rollback(tmp_db, tmp_path):
    release_id = _seed_complete_release_ready_fixture(tmp_db)
    before = sqlite3.connect(str(tmp_db)).execute("SELECT COUNT(*) FROM release_events").fetchone()[0]

    payload = run_release_dry_run_audit(db_path=tmp_db, code_root=tmp_path, output_path=tmp_path / "dry_run.json", operator_name="alice")

    after = sqlite3.connect(str(tmp_db)).execute("SELECT COUNT(*) FROM release_events").fetchone()[0]
    assert payload["allow_release_gate"] is True
    assert payload["decision"] == "allow_release_gate"
    assert payload["blocking_reasons"] == []
    assert payload["rollback_context"]["reference"]["release_id"] == release_id
    assert payload["validation_statuses"]["dry_run_no_side_effects"] == "passed"
    assert before == after
    assert "allow_release_gate" in (tmp_path / "dry_run.json").read_text(encoding="utf-8")


def test_release_dry_run_creates_nested_output_parent_for_ci_preflight(tmp_path):
    output_path = tmp_path / "artifacts" / "release_dry_run" / "payload.json"

    payload = run_release_dry_run_audit(
        db_path=tmp_path / "missing.db",
        code_root=tmp_path,
        output_path=output_path,
        operator_name="ci",
    )

    assert payload["allow_release_gate"] is False
    assert output_path.exists()
    assert "database_not_found" in output_path.read_text(encoding="utf-8")


def test_release_dry_run_cli_non_blocking_preserves_blocked_payload(tmp_path):
    output_path = tmp_path / "artifacts" / "release_dry_run" / "payload.json"
    result = subprocess.run(
        [
            sys.executable,
            "tools/release_dry_run_audit.py",
            "--db",
            str(tmp_path / "missing.db"),
            "--code-root",
            str(tmp_path),
            "--output",
            str(output_path),
            "--operator",
            "ci",
            "--non-blocking",
        ],
        cwd=str(Path(__file__).resolve().parents[1]),
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    payload_text = output_path.read_text(encoding="utf-8")
    assert '"allow_release_gate": false' in payload_text
    assert "database_not_found" in payload_text


def test_release_dry_run_trend_identifies_repeated_hard_gate_candidates(tmp_path):
    payload_dir = tmp_path / "payloads"
    payload_dir.mkdir()
    blocked = {
        "allow_release_gate": False,
        "decision": "block_release_gate",
        "blocking_reasons": ["execution_chain_empty"],
        "validation_statuses": {"execution_chain_evidence": "failed", "signal_chain_evidence": "passed"},
        "rollback_context": {"available": True},
    }
    allowed = {
        "allow_release_gate": True,
        "decision": "allow_release_gate",
        "blocking_reasons": [],
        "validation_statuses": {"execution_chain_evidence": "passed", "signal_chain_evidence": "passed"},
        "rollback_context": {"available": True},
    }
    (payload_dir / "run1.json").write_text(json.dumps(allowed), encoding="utf-8")
    (payload_dir / "run2.json").write_text(json.dumps(blocked), encoding="utf-8")
    (payload_dir / "run3.json").write_text(json.dumps(blocked), encoding="utf-8")

    summary = summarize_release_dry_run_trend(
        payload_paths=[payload_dir],
        output_path=tmp_path / "trend" / "summary.json",
        stable_threshold=2,
    )

    assert summary["total_payloads"] == 3
    assert summary["blocked_payloads"] == 2
    assert summary["blocking_reason_counts"]["execution_chain_empty"] == 2
    assert summary["stable_blocking_reasons"] == [{"reason": "execution_chain_empty", "count": 2}]
    assert summary["hard_gate_upgrade_candidates"] == [
        {
            "validation": "execution_chain_evidence",
            "failed_count": 2,
            "consecutive_failures": 2,
            "rollback_reference_available": True,
        }
    ]
    assert summary["recommendation"] == "review_hard_gate_candidates"
    assert (tmp_path / "trend" / "summary.json").exists()


def test_release_dry_run_trend_observes_single_failures_without_candidate(tmp_path):
    payload_path = tmp_path / "single.json"
    payload_path.write_text(
        json.dumps(
            {
                "allow_release_gate": False,
                "decision": "block_release_gate",
                "blocking_reasons": ["rollback_reference:no_prior_release_event"],
                "validation_statuses": {"rollback_reference": "failed"},
            }
        ),
        encoding="utf-8",
    )

    summary = summarize_release_dry_run_trend(payload_paths=[payload_path], stable_threshold=2)

    assert summary["blocked_payloads"] == 1
    assert summary["stable_blocking_reasons"] == []
    assert summary["hard_gate_upgrade_candidates"] == []
    assert summary["recommendation"] == "observe_more_payloads"


def test_release_dry_run_trend_does_not_upgrade_recovered_history(tmp_path):
    payload_dir = tmp_path / "payloads"
    payload_dir.mkdir()
    blocked = {
        "allow_release_gate": False,
        "decision": "block_release_gate",
        "blocking_reasons": ["execution_chain_empty"],
        "validation_statuses": {"execution_chain_evidence": "failed"},
        "rollback_context": {"available": True},
    }
    allowed = {
        "allow_release_gate": True,
        "decision": "allow_release_gate",
        "blocking_reasons": [],
        "validation_statuses": {"execution_chain_evidence": "passed"},
        "rollback_context": {"available": True},
    }
    (payload_dir / "run1.json").write_text(json.dumps(blocked), encoding="utf-8")
    (payload_dir / "run2.json").write_text(json.dumps(blocked), encoding="utf-8")
    (payload_dir / "run3.json").write_text(json.dumps(allowed), encoding="utf-8")

    summary = summarize_release_dry_run_trend(payload_paths=[payload_dir], stable_threshold=2)

    assert summary["validation_failure_counts"]["execution_chain_evidence"] == 2
    assert summary["hard_gate_upgrade_candidates"] == []
    assert summary["recommendation"] == "observe_more_payloads"


def test_release_dry_run_trend_does_not_upgrade_environment_failures(tmp_path):
    payload_dir = tmp_path / "payloads"
    payload_dir.mkdir()
    blocked = {
        "allow_release_gate": False,
        "decision": "block_release_gate",
        "blocking_reasons": ["database_not_found:/tmp/missing.db", "execution_chain_empty"],
        "validation_statuses": {"execution_chain_evidence": "failed"},
        "rollback_context": {"available": True},
    }
    (payload_dir / "run1.json").write_text(json.dumps(blocked), encoding="utf-8")
    (payload_dir / "run2.json").write_text(json.dumps(blocked), encoding="utf-8")

    summary = summarize_release_dry_run_trend(payload_paths=[payload_dir], stable_threshold=2)

    assert summary["stable_blocking_reasons"] == [
        {"reason": "database_not_found:/tmp/missing.db", "count": 2},
        {"reason": "execution_chain_empty", "count": 2},
    ]
    assert summary["hard_gate_upgrade_candidates"] == []


def test_release_dry_run_trend_requires_rollback_reference_for_upgrade(tmp_path):
    payload_dir = tmp_path / "payloads"
    payload_dir.mkdir()
    blocked = {
        "allow_release_gate": False,
        "decision": "block_release_gate",
        "blocking_reasons": ["execution_chain_empty"],
        "validation_statuses": {"execution_chain_evidence": "failed"},
        "rollback_context": {"available": False},
    }
    (payload_dir / "run1.json").write_text(json.dumps(blocked), encoding="utf-8")
    (payload_dir / "run2.json").write_text(json.dumps(blocked), encoding="utf-8")

    summary = summarize_release_dry_run_trend(payload_paths=[payload_dir], stable_threshold=2)

    assert summary["stable_blocking_reasons"] == [{"reason": "execution_chain_empty", "count": 2}]
    assert summary["hard_gate_upgrade_candidates"] == []


def test_release_dry_run_trend_ignores_prior_trend_summary_files(tmp_path):
    payload_dir = tmp_path / "payloads"
    payload_dir.mkdir()
    payload = {
        "allow_release_gate": True,
        "decision": "allow_release_gate",
        "blocking_reasons": [],
        "validation_statuses": {"execution_chain_evidence": "passed"},
        "rollback_context": {"available": True},
    }
    trend_summary = {
        "tool": "tools/release_dry_run_audit.py --trend",
        "decision_counts": {"block_release_gate": 99},
        "validation_failure_counts": {"execution_chain_evidence": 99},
    }
    (payload_dir / "current_readiness_payload.json").write_text(json.dumps(payload), encoding="utf-8")
    (payload_dir / "readiness_trend.json").write_text(json.dumps(trend_summary), encoding="utf-8")

    summary = summarize_release_dry_run_trend(payload_paths=[payload_dir], stable_threshold=2)

    assert summary["total_payload_files"] == 2
    assert summary["total_payloads"] == 1
    assert len(summary["skipped_non_payload_files"]) == 1
    assert summary["decision_counts"] == {"allow_release_gate": 1}
    assert summary["hard_gate_upgrade_candidates"] == []


def test_render_release_dry_run_trend_markdown_is_reviewable():
    summary = {
        "recommendation": "review_hard_gate_candidates",
        "total_payloads": 3,
        "allowed_payloads": 1,
        "blocked_payloads": 2,
        "stable_threshold": 2,
        "hard_gate_upgrade_policy": {
            "candidate_scope": "latest_consecutive_payloads",
            "consecutive_failures_required": 2,
            "requires_rollback_reference": True,
        },
        "hard_gate_upgrade_candidates": [
            {
                "validation": "execution_chain_evidence",
                "failed_count": 2,
                "consecutive_failures": 2,
                "rollback_reference_available": True,
            }
        ],
        "stable_blocking_reasons": [{"reason": "execution_chain_empty", "count": 2}],
        "skipped_non_payload_files": ["artifacts/release_dry_run/readiness_trend.json"],
    }

    markdown = render_release_dry_run_trend_markdown(summary)

    assert "Airivo Release Dry-Run Trend" in markdown
    assert "review_hard_gate_candidates" in markdown
    assert "execution_chain_evidence" in markdown
    assert "execution_chain_empty" in markdown
    assert "readiness_trend.json" in markdown
