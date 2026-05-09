from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from openclaw.services.strategy_competition_production_readiness_service import (
    build_strategy_competition_production_readiness,
)
from tests.test_governance_gate_strategy_optimization import _valid_competition_audit_payload


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _operational_controls() -> dict:
    return {
        "artifact_version": "strategy_competition_production_operational_controls.v1",
        "controls": {
            "kill_switch_enabled": True,
            "rollback_plan_ready": True,
            "live_monitoring_ready": True,
            "incident_owner_assigned": True,
            "max_order_notional_configured": True,
            "max_position_weight_configured": True,
            "max_single_risk_contribution_configured": True,
        },
        "rollback_plan_ref": "runbook://strategy_competition_top5_rollback",
        "incident_owner": "risk_oncall",
        "monitoring_dashboard_ref": "dashboard://strategy_competition_top5_live",
        "max_order_notional": 250000.0,
        "max_position_weight": 0.2,
        "max_single_risk_contribution": 0.4,
        "human_approval_required": True,
    }


def test_production_readiness_blocks_current_blocked_competition_audit(tmp_db: Path, tmp_path: Path):
    payload = _valid_competition_audit_payload()
    payload["result_status"] = "industry_benchmark_competition_blocked"
    payload["passed"] = False
    payload["production_candidate_allowed"] = False
    payload["shadow_execution"] = {"passed": False}
    audit_path = _write_json(tmp_path / "blocked_audit.json", payload)
    controls_path = _write_json(tmp_path / "controls.json", _operational_controls())
    conn = sqlite3.connect(str(tmp_db))

    readiness = build_strategy_competition_production_readiness(
        conn,
        competition_audit_artifact_path=str(audit_path),
        operational_controls_artifact_path=str(controls_path),
        output_dir=tmp_path / "readiness",
    )

    release_count = conn.execute("SELECT COUNT(*) FROM release_events WHERE release_id = ?", (readiness["release_id"],)).fetchone()[0]
    conn.close()
    assert readiness["passed"] is False
    assert readiness["production_release_allowed"] is False
    assert "competition_audit_not_passed_for_production" in readiness["blocking_reasons"]
    assert "shadow_execution_not_passed" in readiness["blocking_reasons"]
    assert release_count == 1


def test_production_readiness_requires_operational_controls(tmp_db: Path, tmp_path: Path):
    payload = _valid_competition_audit_payload()
    audit_path = _write_json(tmp_path / "passed_audit.json", payload)
    conn = sqlite3.connect(str(tmp_db))

    readiness = build_strategy_competition_production_readiness(
        conn,
        competition_audit_artifact_path=str(audit_path),
        output_dir=tmp_path / "readiness",
    )

    conn.close()
    assert readiness["passed"] is False
    assert readiness["production_release_allowed"] is False
    assert "production_operational_control_missing_or_failed:kill_switch_enabled" in readiness["blocking_reasons"]
    assert "rollback_plan_ref_missing" in readiness["blocking_reasons"]


def test_production_readiness_passes_only_with_passed_audit_and_full_controls(tmp_db: Path, tmp_path: Path):
    audit_path = _write_json(tmp_path / "passed_audit.json", _valid_competition_audit_payload())
    controls_path = _write_json(tmp_path / "controls.json", _operational_controls())
    conn = sqlite3.connect(str(tmp_db))

    readiness = build_strategy_competition_production_readiness(
        conn,
        competition_audit_artifact_path=str(audit_path),
        operational_controls_artifact_path=str(controls_path),
        output_dir=tmp_path / "readiness",
    )

    validation = conn.execute(
        "SELECT validation_status FROM release_validations WHERE release_id = ?",
        (readiness["release_id"],),
    ).fetchone()
    conn.close()
    assert readiness["passed"] is True
    assert readiness["production_release_allowed"] is True
    assert readiness["release_contract"]["requires_human_approval_before_live_orders"] is True
    assert readiness["top5_symbols"] == ["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ", "000005.SZ"]
    assert validation[0] == "passed"
