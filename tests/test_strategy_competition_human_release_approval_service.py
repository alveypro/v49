from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from openclaw.services.strategy_competition_human_release_approval_service import (
    build_strategy_competition_human_release_approval,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _result_review(passed: bool = True) -> dict:
    return {
        "artifact_version": "strategy_competition_formal_validation_result_review.v1",
        "artifact_path": "result_review.json",
        "competition_run_id": "comp_test",
        "result_review_status": "formal_validation_results_accepted" if passed else "formal_validation_results_blocked",
        "passed": passed,
        "blocking_reasons": [] if passed else ["formal_validation_handoff_not_ready"],
    }


def _adjudication(passed: bool = True) -> dict:
    return {
        "artifact_version": "strategy_competition_release_chain_adjudication.v1",
        "artifact_path": "adjudication.json",
        "competition_run_id": "comp_test",
        "chain_status": "release_chain_passed_for_human_approval" if passed else "release_chain_blocked",
        "passed": passed,
        "production_release_allowed": passed,
        "blocking_reasons": [] if passed else ["shadow_execution_not_passed"],
    }


def _approval_decision(review_path: str, adjudication_path: str) -> dict:
    return {
        "artifact_version": "strategy_competition_human_release_approval_decision.v1",
        "decision": "approved",
        "approver_name": "release_committee_1",
        "approver_role": "release_approver",
        "conflict_of_interest_attestation": True,
        "reviewed_artifacts": [review_path, adjudication_path],
        "approval_ticket": "REL-20260508-001",
        "approval_summary": "Reviewed current formal result review and release-chain adjudication.",
    }


def test_human_release_approval_blocks_current_failed_upstream_without_decision(tmp_db: Path, tmp_path: Path):
    result_path = _write_json(tmp_path / "result_review.json", _result_review(False))
    adjudication_path = _write_json(tmp_path / "adjudication.json", _adjudication(False))
    conn = sqlite3.connect(str(tmp_db))

    approval = build_strategy_competition_human_release_approval(
        conn,
        formal_validation_result_review_artifact_path=str(result_path),
        release_chain_adjudication_artifact_path=str(adjudication_path),
        output_dir=tmp_path / "approval",
    )

    validation = conn.execute(
        "SELECT validation_status FROM release_validations WHERE release_id = ?",
        (approval["release_id"],),
    ).fetchone()
    conn.close()
    assert approval["approval_status"] == "human_release_approval_blocked"
    assert approval["passed"] is False
    assert approval["production_release_authorized"] is False
    assert approval["live_order_authority_granted"] is False
    assert "formal_validation_result_review_not_accepted" in approval["blocking_reasons"]
    assert "release_chain_adjudication_not_passed_for_human_approval" in approval["blocking_reasons"]
    assert "human_release_approval_decision_missing" in approval["blocking_reasons"]
    assert validation[0] == "blocked"


def test_human_release_approval_requires_approver_reviewed_artifacts(tmp_db: Path, tmp_path: Path):
    result_payload = _result_review(True)
    adjudication_payload = _adjudication(True)
    result_path = _write_json(tmp_path / "result_review.json", result_payload)
    adjudication_path = _write_json(tmp_path / "adjudication.json", adjudication_payload)
    decision = _approval_decision(str(result_path), str(adjudication_path))
    decision["reviewed_artifacts"] = [str(result_path)]
    decision_path = _write_json(tmp_path / "decision.json", decision)
    conn = sqlite3.connect(str(tmp_db))

    approval = build_strategy_competition_human_release_approval(
        conn,
        formal_validation_result_review_artifact_path=str(result_path),
        release_chain_adjudication_artifact_path=str(adjudication_path),
        human_approval_decision_artifact_path=str(decision_path),
        output_dir=tmp_path / "approval",
    )

    conn.close()
    assert approval["passed"] is False
    assert any(reason.startswith("release_approver_missing_reviewed_artifact:") for reason in approval["blocking_reasons"])


def test_human_release_approval_authorizes_only_after_all_gates_and_human_decision(tmp_db: Path, tmp_path: Path):
    result_payload = _result_review(True)
    adjudication_payload = _adjudication(True)
    result_path = _write_json(tmp_path / "result_review.json", result_payload)
    adjudication_path = _write_json(tmp_path / "adjudication.json", adjudication_payload)
    decision_path = _write_json(tmp_path / "decision.json", _approval_decision(str(result_path), str(adjudication_path)))
    conn = sqlite3.connect(str(tmp_db))

    approval = build_strategy_competition_human_release_approval(
        conn,
        formal_validation_result_review_artifact_path=str(result_path),
        release_chain_adjudication_artifact_path=str(adjudication_path),
        human_approval_decision_artifact_path=str(decision_path),
        output_dir=tmp_path / "approval",
    )

    validation = conn.execute(
        "SELECT validation_status FROM release_validations WHERE release_id = ?",
        (approval["release_id"],),
    ).fetchone()
    conn.close()
    assert approval["approval_status"] == "human_release_approved"
    assert approval["passed"] is True
    assert approval["production_release_authorized"] is True
    assert approval["live_order_authority_granted"] is True
    assert approval["approval_contract"]["live_order_authority_only_after_this_gate_passes"] is True
    assert approval["approval_hash"]
    assert validation[0] == "passed"
