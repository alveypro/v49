from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from openclaw.services.strategy_competition_operational_controls_service import (
    build_strategy_competition_operational_controls,
)
from openclaw.services.strategy_competition_production_readiness_service import (
    build_strategy_competition_production_readiness,
)
from tests.test_governance_gate_strategy_optimization import _valid_competition_audit_payload


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _controls_input(**overrides) -> dict:
    payload = {
        "artifact_version": "strategy_competition_production_operational_controls_input.v1",
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
        "max_position_weight": 0.25,
        "max_single_risk_contribution": 0.4,
        "human_approval_required": True,
    }
    payload.update(overrides)
    return payload


def test_operational_controls_without_input_remains_blocked(tmp_db: Path, tmp_path: Path):
    audit = _valid_competition_audit_payload()
    audit["portfolio_constraints"] = {"max_order_value": 250000.0, "max_single_name_weight": 0.25, "max_single_risk_contribution": 0.4}
    audit_path = _write_json(tmp_path / "audit.json", audit)
    conn = sqlite3.connect(str(tmp_db))

    controls = build_strategy_competition_operational_controls(
        conn,
        competition_audit_artifact_path=str(audit_path),
        output_dir=tmp_path / "controls",
    )

    conn.close()
    assert controls["passed"] is False
    assert "operational_controls_input_missing" in controls["blocking_reasons"]
    assert controls["human_approval_required"] is False


def test_operational_controls_rejects_limits_wider_than_audit(tmp_db: Path, tmp_path: Path):
    audit = _valid_competition_audit_payload()
    audit["portfolio_constraints"] = {"max_order_value": 250000.0, "max_single_name_weight": 0.2, "max_single_risk_contribution": 0.3}
    audit_path = _write_json(tmp_path / "audit.json", audit)
    input_path = _write_json(
        tmp_path / "controls_input.json",
        _controls_input(max_order_notional=300000.0, max_position_weight=0.25, max_single_risk_contribution=0.4),
    )
    conn = sqlite3.connect(str(tmp_db))

    controls = build_strategy_competition_operational_controls(
        conn,
        competition_audit_artifact_path=str(audit_path),
        controls_input_artifact_path=str(input_path),
        output_dir=tmp_path / "controls",
    )

    conn.close()
    assert controls["passed"] is False
    assert "max_order_notional_exceeds_audit_limit:300000.0/250000.0" in controls["blocking_reasons"]
    assert "max_position_weight_exceeds_audit_limit:0.25/0.2" in controls["blocking_reasons"]
    assert "max_single_risk_contribution_exceeds_audit_limit:0.4/0.3" in controls["blocking_reasons"]


def test_operational_controls_passes_and_feeds_readiness(tmp_db: Path, tmp_path: Path):
    audit = _valid_competition_audit_payload()
    audit["portfolio_constraints"] = {"max_order_value": 250000.0, "max_single_name_weight": 0.25, "max_single_risk_contribution": 0.4}
    audit_path = _write_json(tmp_path / "audit.json", audit)
    input_path = _write_json(tmp_path / "controls_input.json", _controls_input())
    conn = sqlite3.connect(str(tmp_db))

    controls = build_strategy_competition_operational_controls(
        conn,
        competition_audit_artifact_path=str(audit_path),
        controls_input_artifact_path=str(input_path),
        output_dir=tmp_path / "controls",
    )
    readiness = build_strategy_competition_production_readiness(
        conn,
        competition_audit_artifact_path=str(audit_path),
        operational_controls_artifact_path=controls["artifact_path"],
        output_dir=tmp_path / "readiness",
    )

    conn.close()
    assert controls["passed"] is True
    assert controls["controls_hash"]
    assert readiness["passed"] is True
    assert readiness["production_release_allowed"] is True
