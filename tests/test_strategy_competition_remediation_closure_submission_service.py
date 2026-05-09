from __future__ import annotations

import json
import sqlite3
from hashlib import sha256
from pathlib import Path

from openclaw.services.strategy_competition_remediation_closure_submission_service import (
    build_strategy_competition_remediation_closure_submission,
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


def test_remediation_closure_submission_blocks_missing_item_closure(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))

    submission = build_strategy_competition_remediation_closure_submission(
        conn,
        work_order_artifact_path=_work_order(tmp_path),
        closure_artifact_paths={},
        output_dir=tmp_path / "submission",
    )

    conn.close()
    assert submission["closure_submission_status"] == "remediation_closure_submission_blocked"
    assert submission["passed"] is False
    assert submission["production_candidate_allowed"] is False
    assert submission["live_order_authority_granted"] is False
    assert "post_rerun_human_release_approval_review:closure_missing_or_unpassed" in submission["blocking_reasons"]


def test_remediation_closure_submission_accepts_matching_validator_artifact(tmp_db: Path, tmp_path: Path):
    work_order = _work_order(tmp_path)
    validator = _write_json(
        tmp_path / "post_rerun_human_release_approval_review_passed.json",
        {"passed": True},
    )
    validator_hash = sha256(validator.read_bytes()).hexdigest()
    conn = sqlite3.connect(str(tmp_db))

    submission = build_strategy_competition_remediation_closure_submission(
        conn,
        work_order_artifact_path=work_order,
        closure_artifact_paths={
            "post_rerun_human_release_approval_review": str(validator),
        },
        output_dir=tmp_path / "submission",
    )

    conn.close()
    assert submission["closure_submission_status"] == "remediation_closure_submission_ready"
    assert submission["passed"] is True
    item = submission["item_closures"][0]
    assert item["artifact"] == "post_rerun_human_release_approval_review"
    assert item["validator_artifact"] == str(validator)
    assert item["validator_artifact_hash"] == validator_hash
    assert item["validator_passed"] is True
    assert item["closed"] is True
    assert submission["allowed_next_actions"] == ["submit_to_remediation_closure_review"]
