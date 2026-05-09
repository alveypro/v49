from __future__ import annotations

import json
import sqlite3
from pathlib import Path

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


def test_rerun_court_rebuild_submission_blocks_missing_inputs(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))

    submission = build_strategy_competition_rerun_court_rebuild_submission(
        conn,
        rerun_result_review_artifact_path=_rerun_result(tmp_path),
        rebuilt_manifest_artifact_path=tmp_path / "missing_manifest.json",
        rebuilt_release_chain_artifact_path=tmp_path / "missing_release.json",
        output_dir=tmp_path / "submission",
    )

    conn.close()
    assert submission["rerun_court_rebuild_submission_status"] == "rerun_court_rebuild_submission_blocked"
    assert submission["passed"] is False
    assert submission["production_candidate_allowed"] is False
    assert submission["live_order_authority_granted"] is False


def test_rerun_court_rebuild_submission_accepts_lineaged_inputs(tmp_db: Path, tmp_path: Path):
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

    submission = build_strategy_competition_rerun_court_rebuild_submission(
        conn,
        rerun_result_review_artifact_path=_rerun_result(tmp_path),
        rebuilt_manifest_artifact_path=manifest,
        rebuilt_release_chain_artifact_path=release,
        output_dir=tmp_path / "submission",
    )

    conn.close()
    assert submission["rerun_court_rebuild_submission_status"] == "rerun_court_rebuild_submission_ready"
    assert submission["passed"] is True
    assert submission["allowed_next_actions"] == ["submit_to_rerun_court_rebuild_review"]
