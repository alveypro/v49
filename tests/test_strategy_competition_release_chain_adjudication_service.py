from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from openclaw.services.strategy_competition_release_chain_adjudication_service import (
    build_strategy_competition_release_chain_adjudication,
)
from tests.test_governance_gate_strategy_optimization import _valid_competition_audit_payload


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _shadow(passed: bool = True) -> dict:
    return {
        "artifact_version": "strategy_competition_shadow_execution_evidence.v1",
        "execution_status": "shadow_execution_passed" if passed else "shadow_execution_blocked",
        "passed": passed,
        "blocking_reasons": [] if passed else ["shadow_attribution_missing"],
    }


def _independent(passed: bool = True) -> dict:
    return {
        "artifact_version": "strategy_competition_independent_validation.v1",
        "validation_status": "independent_validation_passed" if passed else "independent_validation_blocked",
        "passed": passed,
        "blocking_reasons": [] if passed else ["independent_validator_decision_missing"],
    }


def _controls(passed: bool = True) -> dict:
    return {
        "artifact_version": "strategy_competition_production_operational_controls.v1",
        "controls_status": "production_operational_controls_passed" if passed else "production_operational_controls_blocked",
        "passed": passed,
        "blocking_reasons": [] if passed else ["kill_switch_missing"],
    }


def _submission_review(passed: bool = True) -> dict:
    return {
        "artifact_version": "strategy_competition_evidence_submission_review.v1",
        "review_status": "evidence_submission_accepted_for_validation" if passed else "evidence_submission_blocked",
        "passed": passed,
        "production_candidate_allowed": False,
        "blocking_reasons": [] if passed else ["shadow_feedback_status_invalid:ord_1"],
    }


def _readiness(passed: bool = True) -> dict:
    return {
        "artifact_version": "strategy_competition_production_readiness.v1",
        "readiness_status": "production_readiness_passed" if passed else "production_readiness_blocked",
        "passed": passed,
        "production_release_allowed": passed,
        "blocking_reasons": [] if passed else ["competition_audit_not_passed_for_production"],
    }


def test_release_chain_adjudication_blocks_current_incomplete_top5_chain(tmp_db: Path, tmp_path: Path):
    audit = _valid_competition_audit_payload()
    audit["competition_run_id"] = "comp_test"
    audit["result_status"] = "industry_benchmark_competition_blocked"
    audit["passed"] = False
    audit["production_candidate_allowed"] = False
    audit["shadow_execution"] = {"passed": False}
    audit["independent_validation"] = {"passed": False}
    audit_path = _write_json(tmp_path / "audit.json", audit)
    shadow_path = _write_json(tmp_path / "shadow.json", _shadow(False))
    independent_path = _write_json(tmp_path / "independent.json", _independent(False))
    controls_path = _write_json(tmp_path / "controls.json", _controls(False))
    submission_path = _write_json(tmp_path / "submission.json", _submission_review(False))
    readiness_path = _write_json(tmp_path / "readiness.json", _readiness(False))
    conn = sqlite3.connect(str(tmp_db))

    verdict = build_strategy_competition_release_chain_adjudication(
        conn,
        competition_audit_artifact_path=str(audit_path),
        shadow_execution_artifact_path=str(shadow_path),
        independent_validation_artifact_path=str(independent_path),
        operational_controls_artifact_path=str(controls_path),
        evidence_submission_review_artifact_path=str(submission_path),
        production_readiness_artifact_path=str(readiness_path),
        output_dir=tmp_path / "adjudication",
    )

    validation = conn.execute(
        "SELECT validation_status FROM release_validations WHERE release_id = ?",
        (verdict["release_id"],),
    ).fetchone()
    conn.close()
    assert verdict["chain_status"] == "release_chain_blocked"
    assert verdict["passed"] is False
    assert verdict["production_candidate_allowed"] is False
    assert verdict["current_blocking_gate"] == "competition_audit"
    assert "shadow_execution:shadow_attribution_missing" in verdict["root_blockers"]
    assert "submit_real_shadow_feedback_and_run_shadow_evidence_validator" in verdict["allowed_next_actions"]
    assert "production_requires_passed_readiness_and_human_approval" in verdict["hard_boundaries"]
    assert verdict["source_artifact_hashes"]["competition_audit"]
    assert verdict["adjudication_hash"]
    assert validation[0] == "blocked"


def test_release_chain_adjudication_passes_only_after_all_formal_gates_pass(tmp_db: Path, tmp_path: Path):
    audit = _valid_competition_audit_payload()
    audit["competition_run_id"] = "comp_test"
    audit_path = _write_json(tmp_path / "audit.json", audit)
    shadow_path = _write_json(tmp_path / "shadow.json", _shadow(True))
    independent_path = _write_json(tmp_path / "independent.json", _independent(True))
    controls_path = _write_json(tmp_path / "controls.json", _controls(True))
    submission_path = _write_json(tmp_path / "submission.json", _submission_review(True))
    readiness_path = _write_json(tmp_path / "readiness.json", _readiness(True))
    conn = sqlite3.connect(str(tmp_db))

    verdict = build_strategy_competition_release_chain_adjudication(
        conn,
        competition_audit_artifact_path=str(audit_path),
        shadow_execution_artifact_path=str(shadow_path),
        independent_validation_artifact_path=str(independent_path),
        operational_controls_artifact_path=str(controls_path),
        evidence_submission_review_artifact_path=str(submission_path),
        production_readiness_artifact_path=str(readiness_path),
        output_dir=tmp_path / "adjudication",
    )

    conn.close()
    assert verdict["chain_status"] == "release_chain_passed_for_human_approval"
    assert verdict["passed"] is True
    assert verdict["production_release_allowed"] is True
    assert verdict["current_blocking_gate"] == ""
    assert verdict["allowed_next_actions"] == ["hold_for_human_release_approval_boundary"]
    assert verdict["release_contract"]["requires_human_approval_before_live_orders"] is True


def test_release_chain_adjudication_blocks_contradictory_readiness(tmp_db: Path, tmp_path: Path):
    audit = _valid_competition_audit_payload()
    audit["competition_run_id"] = "comp_test"
    audit_path = _write_json(tmp_path / "audit.json", audit)
    shadow_path = _write_json(tmp_path / "shadow.json", _shadow(True))
    independent_path = _write_json(tmp_path / "independent.json", _independent(True))
    controls_path = _write_json(tmp_path / "controls.json", _controls(True))
    submission_path = _write_json(tmp_path / "submission.json", _submission_review(True))
    readiness = _readiness(False)
    readiness["production_release_allowed"] = True
    readiness_path = _write_json(tmp_path / "readiness.json", readiness)
    conn = sqlite3.connect(str(tmp_db))

    verdict = build_strategy_competition_release_chain_adjudication(
        conn,
        competition_audit_artifact_path=str(audit_path),
        shadow_execution_artifact_path=str(shadow_path),
        independent_validation_artifact_path=str(independent_path),
        operational_controls_artifact_path=str(controls_path),
        evidence_submission_review_artifact_path=str(submission_path),
        production_readiness_artifact_path=str(readiness_path),
        output_dir=tmp_path / "adjudication",
    )

    conn.close()
    assert verdict["passed"] is False
    assert verdict["production_release_allowed"] is False
    assert verdict["current_blocking_gate"] == "production_readiness"
    assert "production_readiness:production_readiness_release_flag_inconsistent" in verdict["root_blockers"]
