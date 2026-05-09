from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from openclaw.services.strategy_competition_post_rerun_human_release_approval_review_service import (
    build_strategy_competition_post_rerun_human_release_approval_review,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _manifest(tmp_path: Path, passed: bool = True) -> Path:
    return _write_json(
        tmp_path / "manifest.json",
        {
            "artifact_version": "strategy_competition_post_rerun_evidence_chain_manifest.v1",
            "artifact_path": "manifest.json",
            "competition_run_id": "comp_test",
            "manifest_status": "post_rerun_evidence_chain_complete" if passed else "post_rerun_evidence_chain_blocked",
            "passed": passed,
            "production_candidate_allowed": False,
            "production_release_authorized": False,
            "source_artifact_hashes": {"post_rerun_release_readiness": "hash"},
        },
    )


def _decision(manifest_path: Path) -> dict:
    return {
        "artifact_version": "strategy_competition_human_release_approval_decision.v1",
        "decision": "approved",
        "approver_name": "release_committee_1",
        "approver_role": "release_approver",
        "conflict_of_interest_attestation": True,
        "reviewed_artifacts": [str(manifest_path)],
        "approval_ticket": "REL-20260509-001",
        "approval_summary": "Reviewed the post-rerun evidence-chain manifest.",
    }


def test_post_rerun_human_release_approval_review_blocks_missing_decision(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))
    verdict = build_strategy_competition_post_rerun_human_release_approval_review(
        conn,
        post_rerun_evidence_chain_manifest_artifact_path=_manifest(tmp_path),
        output_dir=tmp_path / "out",
    )
    conn.close()
    assert verdict["human_release_approval_review_status"] == "post_rerun_human_release_approval_blocked"
    assert verdict["passed"] is False
    assert verdict["live_order_authority_granted"] is False
    assert "post_rerun_human_release_approval_decision_missing" in verdict["blocking_reasons"]


def test_post_rerun_human_release_approval_review_passes_without_live_authority(tmp_db: Path, tmp_path: Path):
    manifest = _manifest(tmp_path)
    decision = _write_json(tmp_path / "decision.json", _decision(manifest))
    conn = sqlite3.connect(str(tmp_db))
    verdict = build_strategy_competition_post_rerun_human_release_approval_review(
        conn,
        post_rerun_evidence_chain_manifest_artifact_path=manifest,
        human_approval_decision_artifact_path=decision,
        output_dir=tmp_path / "out",
    )
    conn.close()
    assert verdict["human_release_approval_review_status"] == "post_rerun_human_release_approved"
    assert verdict["passed"] is True
    assert verdict["production_release_authorized"] is False
    assert verdict["live_order_authority_granted"] is False
    assert verdict["allowed_next_actions"] == ["run_live_order_authority_check_with_matching_post_rerun_human_release_hash"]


def test_post_rerun_human_release_approval_review_blocks_permission_claims(tmp_db: Path, tmp_path: Path):
    manifest = _write_json(
        tmp_path / "manifest_bad.json",
        {
            "artifact_version": "strategy_competition_post_rerun_evidence_chain_manifest.v1",
            "artifact_path": "manifest_bad.json",
            "competition_run_id": "comp_test",
            "manifest_status": "post_rerun_evidence_chain_blocked",
            "passed": False,
            "production_candidate_allowed": True,
            "production_release_authorized": True,
        },
    )
    decision = _write_json(tmp_path / "decision_bad.json", _decision(manifest))
    conn = sqlite3.connect(str(tmp_db))
    verdict = build_strategy_competition_post_rerun_human_release_approval_review(
        conn,
        post_rerun_evidence_chain_manifest_artifact_path=manifest,
        human_approval_decision_artifact_path=decision,
        output_dir=tmp_path / "out",
    )
    conn.close()
    assert verdict["passed"] is False
    assert "post_rerun_evidence_chain_manifest_not_complete" in verdict["blocking_reasons"]
    assert "post_rerun_evidence_chain_manifest_attempted_production_permission" in verdict["blocking_reasons"]
