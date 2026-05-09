from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from openclaw.services.strategy_competition_post_rerun_release_readiness_submission_service import (
    build_strategy_competition_post_rerun_release_readiness_submission,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _court(tmp_path: Path, accepted: bool = True) -> Path:
    return _write_json(
        tmp_path / "court.json",
        {
            "artifact_version": "strategy_competition_rerun_court_rebuild_review.v1",
            "competition_run_id": "comp_test",
            "court_rebuild_status": "rerun_court_rebuild_accepted" if accepted else "rerun_court_rebuild_blocked",
            "passed": accepted,
            "court_rebuild_review_hash": "court_hash",
        },
    )


def _release(tmp_path: Path, passed: bool = True) -> Path:
    return _write_json(
        tmp_path / "release.json",
        {
            "artifact_version": "strategy_competition_post_rerun_release_chain_adjudication.v1",
            "competition_run_id": "comp_test",
            "chain_status": "release_chain_passed_for_human_approval" if passed else "release_chain_blocked",
            "passed": passed,
            "source_rerun_court_rebuild_review_hash": "court_hash",
            "production_candidate_allowed": False,
            "production_release_allowed": False,
        },
    )


def _human(tmp_path: Path, approved: bool = True) -> Path:
    return _write_json(
        tmp_path / "human.json",
        {
            "artifact_version": "strategy_competition_post_rerun_human_release_approval_review.v1",
            "competition_run_id": "comp_test",
            "approval_status": "human_release_approved" if approved else "human_release_blocked",
            "passed": approved,
            "source_rerun_court_rebuild_review_hash": "court_hash",
            "live_order_authority_granted": False,
        },
    )


def test_post_rerun_release_readiness_submission_blocks_missing_inputs(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))

    submission = build_strategy_competition_post_rerun_release_readiness_submission(
        conn,
        rerun_court_rebuild_review_artifact_path=_court(tmp_path, accepted=False),
        release_chain_adjudication_artifact_path=_release(tmp_path, passed=False),
        human_release_approval_artifact_path=_human(tmp_path, approved=False),
        output_dir=tmp_path / "out",
    )

    conn.close()
    assert submission["release_readiness_submission_status"] == "post_rerun_release_readiness_submission_blocked"
    assert submission["passed"] is False
    assert "rerun_court_rebuild_review_not_accepted" in submission["blocking_reasons"]
    assert "release_chain_not_passed_for_human_approval" in submission["blocking_reasons"]
    assert "human_release_approval_not_approved" in submission["blocking_reasons"]
    assert submission["live_order_authority_granted"] is False


def test_post_rerun_release_readiness_submission_ready_still_does_not_grant_live_authority(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))

    submission = build_strategy_competition_post_rerun_release_readiness_submission(
        conn,
        rerun_court_rebuild_review_artifact_path=_court(tmp_path),
        release_chain_adjudication_artifact_path=_release(tmp_path),
        human_release_approval_artifact_path=_human(tmp_path),
        output_dir=tmp_path / "out",
    )

    conn.close()
    assert submission["release_readiness_submission_status"] == "post_rerun_release_readiness_submission_ready"
    assert submission["passed"] is True
    assert submission["production_release_authorized"] is False
    assert submission["live_order_authority_granted"] is False
    assert submission["allowed_next_actions"] == ["submit_to_post_rerun_release_readiness_review"]


def test_post_rerun_release_readiness_submission_blocks_permission_claims(tmp_db: Path, tmp_path: Path):
    release = _write_json(
        tmp_path / "release_bad.json",
        {
            "artifact_version": "strategy_competition_post_rerun_release_chain_adjudication.v1",
            "competition_run_id": "comp_test",
            "chain_status": "release_chain_passed_for_human_approval",
            "passed": True,
            "source_rerun_court_rebuild_review_hash": "wrong",
            "production_candidate_allowed": True,
        },
    )
    human = _write_json(
        tmp_path / "human_bad.json",
        {
            "artifact_version": "strategy_competition_post_rerun_human_release_approval_review.v1",
            "competition_run_id": "comp_test",
            "approval_status": "human_release_approved",
            "passed": True,
            "source_rerun_court_rebuild_review_hash": "wrong",
            "live_order_authority_granted": True,
        },
    )
    conn = sqlite3.connect(str(tmp_db))

    submission = build_strategy_competition_post_rerun_release_readiness_submission(
        conn,
        rerun_court_rebuild_review_artifact_path=_court(tmp_path),
        release_chain_adjudication_artifact_path=release,
        human_release_approval_artifact_path=human,
        output_dir=tmp_path / "out",
    )

    conn.close()
    assert submission["passed"] is False
    assert "release_chain_rerun_court_hash_mismatch" in submission["blocking_reasons"]
    assert "release_chain_attempted_direct_production_permission" in submission["blocking_reasons"]
    assert "human_release_attempted_direct_live_authority" in submission["blocking_reasons"]
