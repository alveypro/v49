from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from openclaw.services.strategy_competition_post_rerun_release_chain_adjudication_service import (
    build_strategy_competition_post_rerun_release_chain_adjudication,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _readiness(tmp_path: Path, ready: bool = True) -> Path:
    return _write_json(
        tmp_path / "readiness.json",
        {
            "competition_run_id": "comp_test",
            "release_readiness_status": "post_rerun_release_ready_for_live_authority_check" if ready else "post_rerun_release_blocked",
            "passed": ready,
            "release_readiness_hash": "readiness_hash",
        },
    )


def _authority(tmp_path: Path, ready: bool = True) -> Path:
    return _write_json(
        tmp_path / "authority.json",
        {
            "competition_run_id": "comp_test",
            "live_authority_review_status": "post_rerun_live_authority_ready_for_broker_guard" if ready else "post_rerun_live_authority_blocked",
            "passed": ready,
            "source_post_rerun_release_readiness_hash": "readiness_hash",
            "live_authority_review_hash": "authority_hash",
        },
    )


def test_post_rerun_release_chain_blocks_missing_authority(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))
    verdict = build_strategy_competition_post_rerun_release_chain_adjudication(
        conn,
        post_rerun_release_readiness_artifact_path=_readiness(tmp_path),
        output_dir=tmp_path / "out",
    )
    conn.close()
    assert verdict["release_chain_status"] == "post_rerun_release_chain_blocked"
    assert verdict["passed"] is False
    assert verdict["production_candidate_allowed"] is False
    assert verdict["live_order_authority_granted"] is False
    assert "live_authority_review_artifact_missing" in verdict["blocking_reasons"]


def test_post_rerun_release_chain_ready_still_does_not_grant_broker_permission(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))
    verdict = build_strategy_competition_post_rerun_release_chain_adjudication(
        conn,
        post_rerun_release_readiness_artifact_path=_readiness(tmp_path),
        post_rerun_live_authority_review_artifact_path=_authority(tmp_path),
        output_dir=tmp_path / "out",
    )
    conn.close()
    assert verdict["release_chain_status"] == "post_rerun_release_chain_ready_for_broker_guard"
    assert verdict["passed"] is True
    assert verdict["broker_submission_allowed"] is False
    assert verdict["live_order_authority_granted"] is False
    assert verdict["allowed_next_actions"] == ["run_broker_submission_guard_with_matching_live_authority_hash"]


def test_post_rerun_release_chain_blocks_permission_claims(tmp_db: Path, tmp_path: Path):
    authority = _write_json(
        tmp_path / "authority_bad.json",
        {
            "competition_run_id": "comp_test",
            "live_authority_review_status": "post_rerun_live_authority_ready_for_broker_guard",
            "passed": True,
            "source_post_rerun_release_readiness_hash": "wrong",
            "live_order_authority_granted": True,
            "broker_submission_allowed": True,
        },
    )
    conn = sqlite3.connect(str(tmp_db))
    verdict = build_strategy_competition_post_rerun_release_chain_adjudication(
        conn,
        post_rerun_release_readiness_artifact_path=_readiness(tmp_path),
        post_rerun_live_authority_review_artifact_path=authority,
        output_dir=tmp_path / "out",
    )
    conn.close()
    assert verdict["passed"] is False
    assert "post_rerun_live_authority_readiness_hash_mismatch" in verdict["blocking_reasons"]
    assert "post_rerun_release_chain_attempted_permission_grant" in verdict["blocking_reasons"]
