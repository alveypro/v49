from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from openclaw.services.strategy_competition_evidence_intake_service import (
    build_strategy_competition_evidence_intake_packet,
    build_strategy_competition_evidence_submission_review,
)
from openclaw.services.strategy_competition_formal_validation_handoff_service import (
    build_strategy_competition_formal_validation_handoff,
    build_strategy_competition_formal_validation_result_review,
)
from tests.test_governance_gate_strategy_optimization import _valid_competition_audit_payload
from tests.test_strategy_competition_evidence_intake_service import (
    _filled_controls_from_packet,
    _filled_shadow_feedback_from_packet,
    _filled_validator_from_packet,
    _shadow_plan,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _packet_and_review(conn: sqlite3.Connection, tmp_path: Path, *, filled: bool) -> tuple[dict, dict]:
    audit = _valid_competition_audit_payload()
    audit["competition_run_id"] = "comp_test"
    audit["portfolio_constraints"] = {"max_order_value": 250000.0, "max_single_name_weight": 0.25, "max_single_risk_contribution": 0.4}
    audit_path = _write_json(tmp_path / "audit.json", audit)
    plan_path = _write_json(tmp_path / "shadow_plan.json", _shadow_plan())
    packet = build_strategy_competition_evidence_intake_packet(
        conn,
        competition_audit_artifact_path=str(audit_path),
        shadow_plan_artifact_path=str(plan_path),
        output_dir=tmp_path / "packet",
    )
    if filled:
        shadow_path = _write_json(tmp_path / "filled_shadow.json", _filled_shadow_feedback_from_packet(packet))
        validator_path = _write_json(tmp_path / "filled_validator.json", _filled_validator_from_packet(packet))
        controls_path = _write_json(tmp_path / "filled_controls.json", _filled_controls_from_packet(packet))
    else:
        shadow_path = Path(packet["template_files"]["shadow_feedback"])
        validator_path = Path(packet["template_files"]["independent_validator_decision"])
        controls_path = Path(packet["template_files"]["operational_controls_input"])
    review = build_strategy_competition_evidence_submission_review(
        conn,
        intake_packet_artifact_path=packet["artifact_path"],
        shadow_feedback_artifact_path=str(shadow_path),
        independent_validator_decision_artifact_path=str(validator_path),
        operational_controls_input_artifact_path=str(controls_path),
        output_dir=tmp_path / "review",
    )
    return packet, review


def test_formal_validation_handoff_blocks_unaccepted_submission_review(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))
    packet, review = _packet_and_review(conn, tmp_path, filled=False)

    handoff = build_strategy_competition_formal_validation_handoff(
        conn,
        intake_packet_artifact_path=packet["artifact_path"],
        evidence_submission_review_artifact_path=review["artifact_path"],
        output_dir=tmp_path / "handoff",
    )

    conn.close()
    assert handoff["handoff_status"] == "formal_validation_handoff_blocked"
    assert handoff["passed"] is False
    assert handoff["production_candidate_allowed"] is False
    assert "evidence_submission_review_not_accepted" in handoff["blocking_reasons"]
    assert handoff["formal_run_order"] == []


def test_formal_validation_handoff_builds_hash_bound_run_order_after_accepted_submission(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))
    packet, review = _packet_and_review(conn, tmp_path, filled=True)

    handoff = build_strategy_competition_formal_validation_handoff(
        conn,
        intake_packet_artifact_path=packet["artifact_path"],
        evidence_submission_review_artifact_path=review["artifact_path"],
        output_dir=tmp_path / "handoff",
        validation_output_root="logs/openclaw/formal_test_outputs",
    )

    conn.close()
    assert handoff["handoff_status"] == "formal_validation_ready"
    assert handoff["passed"] is True
    assert handoff["production_candidate_allowed"] is False
    assert [item["step"] for item in handoff["formal_run_order"]] == [
        "shadow_execution_evidence",
        "independent_validation",
        "operational_controls",
        "competition_audit_rerun",
        "production_readiness",
        "release_chain_adjudication",
    ]
    assert handoff["formal_run_order"][0]["required_input_hashes"]["shadow_feedback"]
    assert "formal_validation_handoff_is_not_production_readiness" in handoff["hard_boundaries"]
    assert handoff["handoff_hash"]


def test_formal_validation_handoff_detects_source_hash_drift(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))
    packet, review = _packet_and_review(conn, tmp_path, filled=True)
    Path(packet["source_artifacts"]["competition_audit"]).write_text('{"changed": true}', encoding="utf-8")

    handoff = build_strategy_competition_formal_validation_handoff(
        conn,
        intake_packet_artifact_path=packet["artifact_path"],
        evidence_submission_review_artifact_path=review["artifact_path"],
        output_dir=tmp_path / "handoff",
    )

    conn.close()
    assert handoff["passed"] is False
    assert "source_artifact_hash_mismatch:competition_audit" in handoff["blocking_reasons"]


def _formal_result_payload(step: str) -> dict:
    versions = {
        "shadow_execution_evidence": "strategy_competition_shadow_execution_evidence.v1",
        "independent_validation": "strategy_competition_independent_validation.v1",
        "operational_controls": "strategy_competition_production_operational_controls.v1",
        "competition_audit_rerun": "strategy_competition_portfolio_audit.v1",
        "production_readiness": "strategy_competition_production_readiness.v1",
        "release_chain_adjudication": "strategy_competition_release_chain_adjudication.v1",
    }
    payload = {
        "artifact_version": versions[step],
        "passed": True,
        "blocking_reasons": [],
    }
    if step == "competition_audit_rerun":
        payload["production_candidate_allowed"] = True
    if step == "production_readiness":
        payload["production_release_allowed"] = True
    if step == "release_chain_adjudication":
        payload["chain_status"] = "release_chain_passed_for_human_approval"
        payload["production_release_allowed"] = True
    return payload


def test_formal_validation_result_review_blocks_when_handoff_not_ready(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))
    packet, review = _packet_and_review(conn, tmp_path, filled=False)
    handoff = build_strategy_competition_formal_validation_handoff(
        conn,
        intake_packet_artifact_path=packet["artifact_path"],
        evidence_submission_review_artifact_path=review["artifact_path"],
        output_dir=tmp_path / "handoff",
    )

    result_review = build_strategy_competition_formal_validation_result_review(
        conn,
        formal_validation_handoff_artifact_path=handoff["artifact_path"],
        formal_result_artifact_paths={},
        output_dir=tmp_path / "result_review",
    )

    conn.close()
    assert result_review["result_review_status"] == "formal_validation_results_blocked"
    assert result_review["passed"] is False
    assert result_review["production_candidate_allowed"] is False
    assert "formal_validation_handoff_not_ready" in result_review["blocking_reasons"]
    assert "shadow_execution_evidence_artifact_missing" in result_review["blocking_reasons"]


def test_formal_validation_result_review_accepts_only_complete_passed_outputs(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))
    packet, review = _packet_and_review(conn, tmp_path, filled=True)
    handoff = build_strategy_competition_formal_validation_handoff(
        conn,
        intake_packet_artifact_path=packet["artifact_path"],
        evidence_submission_review_artifact_path=review["artifact_path"],
        output_dir=tmp_path / "handoff",
    )
    result_paths = {}
    for step in (
        "shadow_execution_evidence",
        "independent_validation",
        "operational_controls",
        "competition_audit_rerun",
        "production_readiness",
        "release_chain_adjudication",
    ):
        result_paths[step] = str(_write_json(tmp_path / f"{step}.json", _formal_result_payload(step)))

    result_review = build_strategy_competition_formal_validation_result_review(
        conn,
        formal_validation_handoff_artifact_path=handoff["artifact_path"],
        formal_result_artifact_paths=result_paths,
        output_dir=tmp_path / "result_review",
    )

    conn.close()
    assert result_review["result_review_status"] == "formal_validation_results_accepted"
    assert result_review["passed"] is True
    assert result_review["production_candidate_allowed"] is False
    assert all(item["passed"] is True for item in result_review["formal_result_status"])
    assert result_review["result_review_contract"]["does_not_create_live_order_authority"] is True
    assert "result_review_does_not_replace_human_release_approval" in result_review["hard_boundaries"]


def test_formal_validation_result_review_blocks_downstream_after_failed_step(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))
    packet, review = _packet_and_review(conn, tmp_path, filled=True)
    handoff = build_strategy_competition_formal_validation_handoff(
        conn,
        intake_packet_artifact_path=packet["artifact_path"],
        evidence_submission_review_artifact_path=review["artifact_path"],
        output_dir=tmp_path / "handoff",
    )
    result_paths = {}
    for step in (
        "shadow_execution_evidence",
        "independent_validation",
        "operational_controls",
        "competition_audit_rerun",
        "production_readiness",
        "release_chain_adjudication",
    ):
        payload = _formal_result_payload(step)
        if step == "independent_validation":
            payload["passed"] = False
        result_paths[step] = str(_write_json(tmp_path / f"{step}.json", payload))

    result_review = build_strategy_competition_formal_validation_result_review(
        conn,
        formal_validation_handoff_artifact_path=handoff["artifact_path"],
        formal_result_artifact_paths=result_paths,
        output_dir=tmp_path / "result_review",
    )

    conn.close()
    assert result_review["passed"] is False
    assert "independent_validation_not_passed" in result_review["blocking_reasons"]
    assert "previous_formal_step_not_passed" in result_review["blocking_reasons"]
