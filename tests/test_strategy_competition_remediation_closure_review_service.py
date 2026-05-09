from __future__ import annotations

import json
import sqlite3
from hashlib import sha256
from pathlib import Path

from openclaw.services.strategy_competition_remediation_closure_review_service import (
    build_strategy_competition_remediation_closure_review,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _work_order(tmp_path: Path) -> Path:
    return _write_json(
        tmp_path / "work_order.json",
        {
            "artifact_version": "strategy_competition_evidence_remediation_work_order.v1",
            "competition_run_id": "comp_test",
            "work_order_status": "remediation_required",
            "work_order_hash": "work_hash",
            "source_manifest_hash": "manifest_hash",
            "work_items": [
                {
                    "artifact": "competition_audit",
                    "owner_role": "strategy_governance_owner",
                    "validator_tool": "tools/build_current_strategy_competition_audit.py",
                    "blocking_reasons": ["shadow_execution_not_passed"],
                    "required_evidence": ["real_shadow_feedback_for_each_order"],
                    "acceptance_rule": "competition_audit_artifact_must_pass_and_manifest_hash_must_update_after_rerun",
                }
            ],
        },
    )


def test_remediation_closure_review_blocks_missing_submission(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))

    review = build_strategy_competition_remediation_closure_review(
        conn,
        work_order_artifact_path=_work_order(tmp_path),
        output_dir=tmp_path / "review",
    )

    conn.close()
    assert review["closure_review_status"] == "remediation_closure_blocked"
    assert review["passed"] is False
    assert review["production_candidate_allowed"] is False
    assert "closure_submission_missing" in review["blocking_reasons"]
    assert "competition_audit:closure_missing" in review["blocking_reasons"]


def test_remediation_closure_review_accepts_matching_validator_submission(tmp_db: Path, tmp_path: Path):
    validator = _write_json(tmp_path / "competition_audit_passed.json", {"passed": True})
    validator_hash = sha256(validator.read_bytes()).hexdigest()
    submission = _write_json(
        tmp_path / "submission.json",
        {
            "work_order_hash": "work_hash",
            "source_manifest_hash": "manifest_hash",
            "item_closures": [
                {
                    "artifact": "competition_audit",
                    "validator_tool": "tools/build_current_strategy_competition_audit.py",
                    "validator_artifact": str(validator),
                    "validator_artifact_hash": validator_hash,
                    "validator_passed": True,
                }
            ],
        },
    )
    conn = sqlite3.connect(str(tmp_db))

    review = build_strategy_competition_remediation_closure_review(
        conn,
        work_order_artifact_path=_work_order(tmp_path),
        closure_submission_path=submission,
        output_dir=tmp_path / "review",
    )

    conn.close()
    assert review["closure_review_status"] == "remediation_closure_accepted_for_rerun"
    assert review["passed"] is True
    assert review["live_order_authority_granted"] is False
    assert review["closed_work_item_count"] == 1
    assert review["allowed_next_actions"] == ["rerun_formal_validators_then_rebuild_manifest_and_court_of_record"]


def test_remediation_closure_review_blocks_hash_or_validator_mismatch(tmp_db: Path, tmp_path: Path):
    validator = _write_json(tmp_path / "competition_audit_failed.json", {"passed": False})
    submission = _write_json(
        tmp_path / "submission_bad.json",
        {
            "work_order_hash": "wrong",
            "source_manifest_hash": "manifest_hash",
            "item_closures": [
                {
                    "artifact": "competition_audit",
                    "validator_tool": "tools/wrong.py",
                    "validator_artifact": str(validator),
                    "validator_artifact_hash": "wrong_hash",
                    "validator_passed": True,
                }
            ],
        },
    )
    conn = sqlite3.connect(str(tmp_db))

    review = build_strategy_competition_remediation_closure_review(
        conn,
        work_order_artifact_path=_work_order(tmp_path),
        closure_submission_path=submission,
        output_dir=tmp_path / "review",
    )

    conn.close()
    assert review["passed"] is False
    assert "work_order_hash_mismatch" in review["blocking_reasons"]
    assert "competition_audit:validator_tool_mismatch" in review["blocking_reasons"]
    assert "competition_audit:validator_artifact_hash_mismatch" in review["blocking_reasons"]
    assert "competition_audit:validator_artifact_payload_not_passed" in review["blocking_reasons"]


def test_remediation_closure_review_accepts_post_rerun_human_release_approval_item(tmp_db: Path, tmp_path: Path):
    work_order = _write_json(
        tmp_path / "work_order_post_rerun.json",
        {
            "artifact_version": "strategy_competition_evidence_remediation_work_order.v1",
            "competition_run_id": "comp_post_rerun",
            "work_order_status": "remediation_required",
            "work_order_hash": "work_hash_post",
            "source_manifest_hash": "manifest_hash_post",
            "work_items": [
                {
                    "artifact": "post_rerun_human_release_approval_review",
                    "owner_role": "human_release_approver",
                    "validator_tool": "tools/review_strategy_competition_post_rerun_human_release_approval_review.py",
                    "blocking_reasons": ["post_rerun_evidence_chain_manifest_not_complete"],
                    "required_evidence": [
                        "post_rerun_evidence_chain_manifest_complete",
                        "independent_human_approval_decision",
                        "reviewed_artifacts_match_current_manifest",
                    ],
                    "acceptance_rule": "post_rerun_human_release_approval_review_artifact_must_pass_and_manifest_hash_must_update_after_rerun",
                }
            ],
        },
    )
    validator = _write_json(tmp_path / "post_rerun_human_release_approval_review_passed.json", {"passed": True})
    validator_hash = sha256(validator.read_bytes()).hexdigest()
    submission = _write_json(
        tmp_path / "submission_post_rerun.json",
        {
            "work_order_hash": "work_hash_post",
            "source_manifest_hash": "manifest_hash_post",
            "item_closures": [
                {
                    "artifact": "post_rerun_human_release_approval_review",
                    "validator_tool": "tools/review_strategy_competition_post_rerun_human_release_approval_review.py",
                    "validator_artifact": str(validator),
                    "validator_artifact_hash": validator_hash,
                    "validator_passed": True,
                }
            ],
        },
    )
    conn = sqlite3.connect(str(tmp_db))

    review = build_strategy_competition_remediation_closure_review(
        conn,
        work_order_artifact_path=work_order,
        closure_submission_path=submission,
        output_dir=tmp_path / "review_post_rerun",
    )

    conn.close()
    assert review["closure_review_status"] == "remediation_closure_accepted_for_rerun"
    assert review["passed"] is True
    assert review["production_candidate_allowed"] is False
    assert review["live_order_authority_granted"] is False
    assert review["closed_work_item_count"] == 1
    assert review["closure_reviews"][0]["artifact"] == "post_rerun_human_release_approval_review"
    assert review["closure_reviews"][0]["closed"] is True
