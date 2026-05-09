from __future__ import annotations

import json
import sqlite3
from hashlib import sha256
from pathlib import Path

from openclaw.services.strategy_competition_formal_rerun_result_review_service import (
    build_strategy_competition_formal_rerun_result_review,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _plan(tmp_path: Path, ready: bool = True) -> Path:
    return _write_json(
        tmp_path / "plan.json",
        {
            "artifact_version": "strategy_competition_formal_rerun_plan.v1",
            "competition_run_id": "comp_test",
            "rerun_plan_status": "formal_rerun_plan_ready" if ready else "formal_rerun_plan_blocked",
            "passed": ready,
            "rerun_plan_hash": "plan_hash",
            "source_manifest_hash": "manifest_hash",
            "rerun_steps": [
                {"step": "shadow_execution_evidence", "command": "python3 tools/record_strategy_competition_shadow_feedback.py"},
                {"step": "independent_validation", "command": "python3 tools/build_strategy_competition_independent_validation.py"},
            ] if ready else [],
        },
    )


def test_formal_rerun_result_review_blocks_when_plan_not_ready(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))

    review = build_strategy_competition_formal_rerun_result_review(
        conn,
        rerun_plan_artifact_path=_plan(tmp_path, ready=False),
        output_dir=tmp_path / "review",
    )

    conn.close()
    assert review["rerun_result_review_status"] == "formal_rerun_results_blocked"
    assert review["passed"] is False
    assert "formal_rerun_plan_not_ready" in review["blocking_reasons"]
    assert review["production_candidate_allowed"] is False


def test_formal_rerun_result_review_accepts_ordered_passed_outputs(tmp_db: Path, tmp_path: Path):
    out1 = _write_json(tmp_path / "shadow.json", {"passed": True})
    out2 = _write_json(tmp_path / "independent.json", {"passed": True})
    submission = _write_json(
        tmp_path / "submission.json",
        {
            "rerun_plan_hash": "plan_hash",
            "rerun_outputs": [
                {"step": "shadow_execution_evidence", "artifact": str(out1), "artifact_hash": sha256(out1.read_bytes()).hexdigest(), "passed": True},
                {"step": "independent_validation", "artifact": str(out2), "artifact_hash": sha256(out2.read_bytes()).hexdigest(), "passed": True},
            ],
        },
    )
    conn = sqlite3.connect(str(tmp_db))

    review = build_strategy_competition_formal_rerun_result_review(
        conn,
        rerun_plan_artifact_path=_plan(tmp_path),
        rerun_output_submission_path=submission,
        output_dir=tmp_path / "review",
    )

    conn.close()
    assert review["rerun_result_review_status"] == "formal_rerun_results_accepted"
    assert review["passed"] is True
    assert review["live_order_authority_granted"] is False
    assert review["passed_step_count"] == 2
    assert review["allowed_next_actions"] == ["rebuild_evidence_chain_manifest_and_release_chain_court_of_record"]


def test_formal_rerun_result_review_blocks_partial_or_failed_outputs(tmp_db: Path, tmp_path: Path):
    out1 = _write_json(tmp_path / "shadow_failed.json", {"passed": False})
    submission = _write_json(
        tmp_path / "submission_bad.json",
        {
            "rerun_plan_hash": "wrong",
            "rerun_outputs": [
                {"step": "shadow_execution_evidence", "artifact": str(out1), "artifact_hash": "wrong_hash", "passed": True},
            ],
        },
    )
    conn = sqlite3.connect(str(tmp_db))

    review = build_strategy_competition_formal_rerun_result_review(
        conn,
        rerun_plan_artifact_path=_plan(tmp_path),
        rerun_output_submission_path=submission,
        output_dir=tmp_path / "review",
    )

    conn.close()
    assert review["passed"] is False
    assert "rerun_plan_hash_mismatch" in review["blocking_reasons"]
    assert "shadow_execution_evidence:rerun_output_artifact_hash_mismatch" in review["blocking_reasons"]
    assert "shadow_execution_evidence:rerun_output_payload_not_passed" in review["blocking_reasons"]
    assert "independent_validation:previous_step_not_passed" in review["blocking_reasons"]
