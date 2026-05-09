from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from openclaw.services.strategy_competition_evidence_intake_service import (
    build_strategy_competition_evidence_intake_packet,
    build_strategy_competition_evidence_submission_review,
)
from tests.test_governance_gate_strategy_optimization import _valid_competition_audit_payload


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _shadow_plan() -> dict:
    return {
        "artifact_version": "strategy_competition_shadow_execution_plan.v1",
        "competition_run_id": "comp_test",
        "orders": [
            {"order_id": f"ord_{idx}", "ts_code": f"00000{idx}.SZ", "target_qty": 100 * idx, "decision_price": 10.0 + idx}
            for idx in range(1, 6)
        ],
    }


def test_evidence_intake_packet_builds_pending_templates_without_approval(tmp_db: Path, tmp_path: Path):
    audit = _valid_competition_audit_payload()
    audit["competition_run_id"] = "comp_test"
    audit["result_status"] = "industry_benchmark_competition_blocked"
    audit["passed"] = False
    audit["production_candidate_allowed"] = False
    audit["shadow_execution"] = {"passed": False, "blocking_reasons": ["missing_attribution"]}
    audit["independent_validation"] = {"passed": False, "blocking_reasons": ["independent_validator_decision_missing"]}
    audit["portfolio_constraints"] = {"max_order_value": 250000.0, "max_single_name_weight": 0.25, "max_single_risk_contribution": 0.4}
    audit_path = _write_json(tmp_path / "audit.json", audit)
    plan_path = _write_json(tmp_path / "shadow_plan.json", _shadow_plan())
    conn = sqlite3.connect(str(tmp_db))

    packet = build_strategy_competition_evidence_intake_packet(
        conn,
        competition_audit_artifact_path=str(audit_path),
        shadow_plan_artifact_path=str(plan_path),
        output_dir=tmp_path / "packet",
    )

    conn.close()
    assert packet["packet_status"] == "evidence_intake_pending"
    assert packet["passed"] is False
    assert packet["production_candidate_allowed"] is False
    assert "shadow_execution" in packet["pending_evidence"]
    assert "independent_validation" in packet["pending_evidence"]
    assert "operational_controls" in packet["pending_evidence"]
    assert len(packet["templates"]["shadow_feedback"]["reports"]) == 5
    assert packet["templates"]["shadow_feedback"]["reports"][0]["status"] == ""
    assert packet["templates"]["independent_validator_decision"]["decision"] == ""
    assert packet["templates"]["operational_controls_input"]["controls"]["kill_switch_enabled"] is False
    assert packet["templates"]["operational_controls_input"]["max_order_notional"] == 250000.0
    assert packet["templates"]["operational_controls_input"]["max_single_risk_contribution"] == 0.4
    assert packet["source_artifact_hashes"]["competition_audit"]
    assert packet["source_artifact_hashes"]["shadow_plan"]
    assert Path(packet["template_files"]["shadow_feedback"]).exists()
    assert Path(packet["template_files"]["independent_validator_decision"]).exists()
    assert Path(packet["template_files"]["operational_controls_input"]).exists()
    assert Path(packet["template_readme"]).exists()
    assert packet["template_file_hashes"]["shadow_feedback"]
    shadow_template = json.loads(Path(packet["template_files"]["shadow_feedback"]).read_text(encoding="utf-8"))
    assert shadow_template["reports"][0]["order_id"] == "ord_1"
    assert packet["packet_hash"]


def test_evidence_intake_packet_marks_all_complete_but_still_not_approval(tmp_db: Path, tmp_path: Path):
    audit = _valid_competition_audit_payload()
    audit["competition_run_id"] = "comp_test"
    audit_path = _write_json(tmp_path / "audit.json", audit)
    shadow_path = _write_json(tmp_path / "shadow.json", {"artifact_path": "shadow.json", "passed": True})
    independent_path = _write_json(tmp_path / "independent.json", {"artifact_path": "independent.json", "passed": True})
    controls_path = _write_json(tmp_path / "controls.json", {"artifact_path": "controls.json", "passed": True})
    readiness_path = _write_json(tmp_path / "readiness.json", {"artifact_path": "readiness.json", "passed": True})
    conn = sqlite3.connect(str(tmp_db))

    packet = build_strategy_competition_evidence_intake_packet(
        conn,
        competition_audit_artifact_path=str(audit_path),
        shadow_evidence_artifact_path=str(shadow_path),
        independent_validation_artifact_path=str(independent_path),
        operational_controls_artifact_path=str(controls_path),
        production_readiness_artifact_path=str(readiness_path),
        output_dir=tmp_path / "packet",
    )

    conn.close()
    assert packet["packet_status"] == "evidence_intake_complete"
    assert packet["pending_evidence"] == []
    assert packet["passed"] is False
    assert packet["production_candidate_allowed"] is False
    assert packet["source_artifact_hashes"]["shadow_evidence"]


def _filled_shadow_feedback_from_packet(packet: dict) -> dict:
    feedback = json.loads(json.dumps(packet["templates"]["shadow_feedback"]))
    for report in feedback["reports"]:
        report["status"] = "filled"
        report["broker_ref"] = f"shadow_feedback:{report['order_id']}"
        report["close_price"] = 11.0
        report["delay_sec"] = 300
        report["fills"][0]["fill_ref"] = "shadow_fill"
        report["fills"][0]["fill_price"] = 10.9
    return feedback


def _filled_validator_from_packet(packet: dict) -> dict:
    decision = json.loads(json.dumps(packet["templates"]["independent_validator_decision"]))
    decision["decision"] = "approved"
    decision["validator_name"] = "external_risk_reviewer"
    decision["conflict_of_interest_attestation"] = True
    decision["validation_summary"] = "Reviewed all current artifacts and found no unaddressed gate exceptions."
    return decision


def _filled_controls_from_packet(packet: dict) -> dict:
    controls = json.loads(json.dumps(packet["templates"]["operational_controls_input"]))
    controls["controls"] = {key: True for key in controls["controls"]}
    controls["rollback_plan_ref"] = "runbook://strategy_competition_top5_rollback"
    controls["incident_owner"] = "risk_oncall"
    controls["monitoring_dashboard_ref"] = "dashboard://strategy_competition_top5"
    controls["human_approval_required"] = True
    return controls


def test_evidence_submission_review_blocks_unfilled_templates(tmp_db: Path, tmp_path: Path):
    audit = _valid_competition_audit_payload()
    audit["competition_run_id"] = "comp_test"
    audit["portfolio_constraints"] = {"max_order_value": 250000.0, "max_single_name_weight": 0.25, "max_single_risk_contribution": 0.4}
    audit_path = _write_json(tmp_path / "audit.json", audit)
    plan_path = _write_json(tmp_path / "shadow_plan.json", _shadow_plan())
    conn = sqlite3.connect(str(tmp_db))
    packet = build_strategy_competition_evidence_intake_packet(
        conn,
        competition_audit_artifact_path=str(audit_path),
        shadow_plan_artifact_path=str(plan_path),
        output_dir=tmp_path / "packet",
    )

    review = build_strategy_competition_evidence_submission_review(
        conn,
        intake_packet_artifact_path=packet["artifact_path"],
        shadow_feedback_artifact_path=packet["template_files"]["shadow_feedback"],
        independent_validator_decision_artifact_path=packet["template_files"]["independent_validator_decision"],
        operational_controls_input_artifact_path=packet["template_files"]["operational_controls_input"],
        output_dir=tmp_path / "review",
    )

    conn.close()
    assert review["passed"] is False
    assert review["production_candidate_allowed"] is False
    assert any(reason.startswith("shadow_feedback_status_invalid:") for reason in review["blocking_reasons"])
    assert "independent_validator_decision_not_approved" in review["blocking_reasons"]
    assert "operational_control_not_declared_true:kill_switch_enabled" in review["blocking_reasons"]


def test_evidence_submission_review_accepts_complete_submission_but_not_production(tmp_db: Path, tmp_path: Path):
    audit = _valid_competition_audit_payload()
    audit["competition_run_id"] = "comp_test"
    audit["portfolio_constraints"] = {"max_order_value": 250000.0, "max_single_name_weight": 0.25, "max_single_risk_contribution": 0.4}
    audit_path = _write_json(tmp_path / "audit.json", audit)
    plan_path = _write_json(tmp_path / "shadow_plan.json", _shadow_plan())
    conn = sqlite3.connect(str(tmp_db))
    packet = build_strategy_competition_evidence_intake_packet(
        conn,
        competition_audit_artifact_path=str(audit_path),
        shadow_plan_artifact_path=str(plan_path),
        output_dir=tmp_path / "packet",
    )
    shadow_path = _write_json(tmp_path / "filled_shadow.json", _filled_shadow_feedback_from_packet(packet))
    validator_path = _write_json(tmp_path / "filled_validator.json", _filled_validator_from_packet(packet))
    controls_path = _write_json(tmp_path / "filled_controls.json", _filled_controls_from_packet(packet))

    review = build_strategy_competition_evidence_submission_review(
        conn,
        intake_packet_artifact_path=packet["artifact_path"],
        shadow_feedback_artifact_path=str(shadow_path),
        independent_validator_decision_artifact_path=str(validator_path),
        operational_controls_input_artifact_path=str(controls_path),
        output_dir=tmp_path / "review",
    )

    conn.close()
    assert review["review_status"] == "evidence_submission_accepted_for_validation"
    assert review["passed"] is True
    assert review["production_candidate_allowed"] is False
    assert review["submitted_artifact_hashes"]["shadow_feedback"]
