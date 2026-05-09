from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from openclaw.services.strategy_competition_rerun_court_rebuild_review_service import (
    build_strategy_competition_rerun_court_rebuild_review,
)
from openclaw.services.strategy_competition_rerun_court_rebuild_submission_service import (
    build_strategy_competition_rerun_court_rebuild_submission,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _rerun_result(tmp_path: Path, accepted: bool = True) -> Path:
    return _write_json(
        tmp_path / "rerun_result.json",
        {
            "artifact_version": "strategy_competition_formal_rerun_result_review.v1",
            "competition_run_id": "comp_test",
            "rerun_result_review_status": "formal_rerun_results_accepted" if accepted else "formal_rerun_results_blocked",
            "passed": accepted,
            "rerun_result_review_hash": "rerun_hash",
        },
    )


def test_rerun_court_rebuild_review_blocks_unaccepted_rerun_result(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))

    review = build_strategy_competition_rerun_court_rebuild_review(
        conn,
        rerun_result_review_artifact_path=_rerun_result(tmp_path, accepted=False),
        output_dir=tmp_path / "review",
    )

    conn.close()
    assert review["court_rebuild_status"] == "rerun_court_rebuild_blocked"
    assert review["passed"] is False
    assert "formal_rerun_result_review_not_accepted" in review["blocking_reasons"]
    assert review["production_candidate_allowed"] is False


def test_rerun_court_rebuild_review_accepts_lineaged_rebuilds(tmp_db: Path, tmp_path: Path):
    manifest = _write_json(
        tmp_path / "manifest.json",
        {
            "artifact_version": "strategy_competition_evidence_chain_manifest.v1",
            "manifest_status": "evidence_chain_complete",
            "passed": True,
            "source_rerun_result_review_hash": "rerun_hash",
            "production_candidate_allowed": False,
            "production_release_authorized": False,
            "live_order_authority_granted": False,
        },
    )
    release = _write_json(
        tmp_path / "release.json",
        {
            "artifact_version": "strategy_competition_release_chain_adjudication.v1",
            "chain_status": "release_chain_passed_for_human_approval",
            "passed": True,
            "source_rerun_result_review_hash": "rerun_hash",
            "production_candidate_allowed": False,
            "production_release_allowed": False,
            "live_order_authority_granted": False,
        },
    )
    conn = sqlite3.connect(str(tmp_db))

    review = build_strategy_competition_rerun_court_rebuild_review(
        conn,
        rerun_result_review_artifact_path=_rerun_result(tmp_path),
        rebuilt_manifest_artifact_path=manifest,
        rebuilt_release_chain_artifact_path=release,
        output_dir=tmp_path / "review",
    )

    conn.close()
    assert review["court_rebuild_status"] == "rerun_court_rebuild_accepted"
    assert review["passed"] is True
    assert review["live_order_authority_granted"] is False
    assert review["allowed_next_actions"] == ["proceed_to_release_chain_adjudication_then_human_release_review"]


def test_rerun_court_rebuild_review_accepts_submission_artifact(tmp_db: Path, tmp_path: Path):
    manifest = _write_json(
        tmp_path / "manifest.json",
        {
            "artifact_version": "strategy_competition_evidence_chain_manifest.v1",
            "manifest_status": "evidence_chain_complete",
            "passed": True,
            "source_rerun_result_review_hash": "rerun_hash",
        },
    )
    release = _write_json(
        tmp_path / "release.json",
        {
            "artifact_version": "strategy_competition_release_chain_adjudication.v1",
            "chain_status": "release_chain_passed_for_human_approval",
            "passed": True,
            "source_rerun_result_review_hash": "rerun_hash",
        },
    )
    conn = sqlite3.connect(str(tmp_db))
    rerun_result = _rerun_result(tmp_path)
    submission = build_strategy_competition_rerun_court_rebuild_submission(
        conn,
        rerun_result_review_artifact_path=rerun_result,
        rebuilt_manifest_artifact_path=manifest,
        rebuilt_release_chain_artifact_path=release,
        output_dir=tmp_path / "submission",
    )

    review = build_strategy_competition_rerun_court_rebuild_review(
        conn,
        rerun_result_review_artifact_path=rerun_result,
        court_rebuild_submission_artifact_path=submission["artifact_path"],
        output_dir=tmp_path / "review_submission",
    )

    conn.close()
    assert review["court_rebuild_status"] == "rerun_court_rebuild_accepted"
    assert review["passed"] is True
    assert review["allowed_next_actions"] == ["proceed_to_release_chain_adjudication_then_human_release_review"]


def test_rerun_court_rebuild_review_blocks_permission_or_hash_mismatch(tmp_db: Path, tmp_path: Path):
    manifest = _write_json(
        tmp_path / "manifest_bad.json",
        {
            "artifact_version": "strategy_competition_evidence_chain_manifest.v1",
            "source_rerun_result_review_hash": "wrong",
            "production_candidate_allowed": True,
        },
    )
    release = _write_json(tmp_path / "release_bad.json", {"artifact_version": "strategy_competition_release_chain_adjudication.v1"})
    conn = sqlite3.connect(str(tmp_db))

    review = build_strategy_competition_rerun_court_rebuild_review(
        conn,
        rerun_result_review_artifact_path=_rerun_result(tmp_path),
        rebuilt_manifest_artifact_path=manifest,
        rebuilt_release_chain_artifact_path=release,
        output_dir=tmp_path / "review",
    )

    conn.close()
    assert review["passed"] is False
    assert "rebuilt_evidence_chain_manifest:rebuilt_evidence_chain_manifest_rerun_result_hash_mismatch" in review["blocking_reasons"]
    assert "rebuilt_evidence_chain_manifest:rebuilt_evidence_chain_manifest_attempted_production_eligibility" in review["blocking_reasons"]
    assert "rebuilt_release_chain_adjudication:rebuilt_release_chain_adjudication_rerun_result_hash_missing" in review["blocking_reasons"]
