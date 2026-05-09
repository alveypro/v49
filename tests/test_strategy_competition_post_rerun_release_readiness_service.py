from __future__ import annotations

import json
from hashlib import sha256
import sqlite3
from pathlib import Path

from openclaw.services.strategy_competition_post_rerun_release_readiness_service import (
    build_strategy_competition_post_rerun_release_readiness,
)
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


def test_post_rerun_release_readiness_blocks_missing_release_chain(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))

    review = build_strategy_competition_post_rerun_release_readiness(
        conn,
        rerun_court_rebuild_review_artifact_path=_court(tmp_path, accepted=False),
        output_dir=tmp_path / "review",
    )

    conn.close()
    assert review["release_readiness_status"] == "post_rerun_release_blocked"
    assert "rerun_court_rebuild_review_not_accepted" in review["blocking_reasons"]
    assert "release_chain_adjudication_missing" in review["blocking_reasons"]
    assert review["live_order_authority_granted"] is False


def test_post_rerun_release_readiness_ready_still_grants_no_live_authority(tmp_db: Path, tmp_path: Path):
    release = _write_json(
        tmp_path / "release.json",
        {
            "chain_status": "release_chain_passed_for_human_approval",
            "passed": True,
            "source_rerun_court_rebuild_review_hash": "court_hash",
            "production_candidate_allowed": False,
            "production_release_allowed": False,
        },
    )
    human = _write_json(
        tmp_path / "human.json",
        {
            "approval_status": "human_release_approved",
            "passed": True,
            "source_rerun_court_rebuild_review_hash": "court_hash",
            "live_order_authority_granted": False,
        },
    )
    conn = sqlite3.connect(str(tmp_db))

    review = build_strategy_competition_post_rerun_release_readiness(
        conn,
        rerun_court_rebuild_review_artifact_path=_court(tmp_path),
        release_chain_adjudication_artifact_path=release,
        human_release_approval_artifact_path=human,
        output_dir=tmp_path / "review",
    )

    conn.close()
    assert review["release_readiness_status"] == "post_rerun_release_ready_for_live_authority_check"
    assert review["passed"] is True
    assert review["production_release_authorized"] is False
    assert review["live_order_authority_granted"] is False
    assert review["allowed_next_actions"] == ["run_live_order_authority_check_with_matching_human_release_hash"]


def test_post_rerun_release_readiness_blocks_lineage_and_permission_claims(tmp_db: Path, tmp_path: Path):
    release = _write_json(tmp_path / "release_bad.json", {"chain_status": "release_chain_passed_for_human_approval", "passed": True, "source_rerun_court_rebuild_review_hash": "wrong", "production_candidate_allowed": True})
    human = _write_json(tmp_path / "human_bad.json", {"approval_status": "human_release_approved", "passed": True, "source_rerun_court_rebuild_review_hash": "wrong", "live_order_authority_granted": True})
    conn = sqlite3.connect(str(tmp_db))

    review = build_strategy_competition_post_rerun_release_readiness(
        conn,
        rerun_court_rebuild_review_artifact_path=_court(tmp_path),
        release_chain_adjudication_artifact_path=release,
        human_release_approval_artifact_path=human,
        output_dir=tmp_path / "review",
    )

    conn.close()
    assert review["passed"] is False
    assert "release_chain_rerun_court_hash_mismatch" in review["blocking_reasons"]
    assert "release_chain_attempted_direct_production_permission" in review["blocking_reasons"]
    assert "human_release_attempted_direct_live_authority" in review["blocking_reasons"]


def test_post_rerun_release_readiness_accepts_submission_pack(tmp_db: Path, tmp_path: Path):
    court = _court(tmp_path)
    release = _write_json(
        tmp_path / "release.json",
        {
            "artifact_version": "strategy_competition_post_rerun_release_chain_adjudication.v1",
            "competition_run_id": "comp_test",
            "chain_status": "release_chain_passed_for_human_approval",
            "passed": True,
            "source_rerun_court_rebuild_review_hash": "court_hash",
            "production_candidate_allowed": False,
            "production_release_allowed": False,
        },
    )
    human = _write_json(
        tmp_path / "human.json",
        {
            "artifact_version": "strategy_competition_post_rerun_human_release_approval_review.v1",
            "competition_run_id": "comp_test",
            "approval_status": "human_release_approved",
            "passed": True,
            "source_rerun_court_rebuild_review_hash": "court_hash",
            "live_order_authority_granted": False,
        },
    )
    conn = sqlite3.connect(str(tmp_db))
    submission = build_strategy_competition_post_rerun_release_readiness_submission(
        conn,
        rerun_court_rebuild_review_artifact_path=court,
        release_chain_adjudication_artifact_path=release,
        human_release_approval_artifact_path=human,
        output_dir=tmp_path / "submission",
    )
    review = build_strategy_competition_post_rerun_release_readiness(
        conn,
        rerun_court_rebuild_review_artifact_path=court,
        release_readiness_submission_artifact_path=submission["artifact_path"],
        output_dir=tmp_path / "review",
    )
    conn.close()
    assert submission["release_readiness_submission_status"] == "post_rerun_release_readiness_submission_ready"
    assert review["release_readiness_status"] == "post_rerun_release_ready_for_live_authority_check"
    assert review["passed"] is True
    assert review["source_release_readiness_submission_hash"] == sha256(Path(submission["artifact_path"]).read_bytes()).hexdigest()
