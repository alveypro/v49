from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from openclaw.services.strategy_competition_post_rerun_broker_guard_submission_service import (
    build_strategy_competition_post_rerun_broker_guard_submission,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _authority_review(tmp_path: Path, ready: bool = True) -> Path:
    return _write_json(
        tmp_path / "authority_review.json",
        {
            "artifact_version": "strategy_competition_post_rerun_live_authority_review.v1",
            "competition_run_id": "comp_test",
            "live_authority_review_status": "post_rerun_live_authority_ready_for_broker_guard" if ready else "post_rerun_live_authority_blocked",
            "passed": ready,
            "live_authority_review_hash": "authority_review_hash",
        },
    )


def test_post_rerun_broker_guard_submission_blocks_missing_inputs(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))

    submission = build_strategy_competition_post_rerun_broker_guard_submission(
        conn,
        post_rerun_live_authority_review_artifact_path=_authority_review(tmp_path, ready=False),
        broker_submission_guard_artifact_path=_write_json(
            tmp_path / "guard.json",
            {
                "artifact_version": "strategy_competition_broker_submission_guard.v1",
                "guard_status": "broker_submission_guard_blocked",
                "passed": False,
                "source_post_rerun_live_authority_review_hash": "authority_review_hash",
            },
        ),
        output_dir=tmp_path / "out",
    )

    conn.close()
    assert submission["broker_guard_submission_status"] == "post_rerun_broker_guard_submission_blocked"
    assert submission["passed"] is False
    assert "post_rerun_live_authority_review_not_ready" in submission["blocking_reasons"]
    assert "broker_submission_guard_not_passed" in submission["blocking_reasons"]


def test_post_rerun_broker_guard_submission_ready_still_does_not_confirm_submission(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))

    submission = build_strategy_competition_post_rerun_broker_guard_submission(
        conn,
        post_rerun_live_authority_review_artifact_path=_authority_review(tmp_path),
        broker_submission_guard_artifact_path=_write_json(
            tmp_path / "guard.json",
            {
                "artifact_version": "strategy_competition_broker_submission_guard.v1",
                "guard_status": "broker_submission_guard_passed",
                "passed": True,
                "source_post_rerun_live_authority_review_hash": "authority_review_hash",
                "broker_adapter": "paper_broker",
                "idempotency_key": "idem",
                "submission_mode": "dry_run",
            },
        ),
        output_dir=tmp_path / "out",
    )

    conn.close()
    assert submission["broker_guard_submission_status"] == "post_rerun_broker_guard_submission_ready"
    assert submission["passed"] is True
    assert submission["broker_submission_confirmed"] is False


def test_post_rerun_broker_guard_submission_blocks_permission_claims(tmp_db: Path, tmp_path: Path):
    guard = _write_json(
        tmp_path / "guard_bad.json",
        {
            "artifact_version": "strategy_competition_broker_submission_guard.v1",
            "guard_status": "broker_submission_guard_passed",
            "passed": True,
            "source_post_rerun_live_authority_review_hash": "wrong",
            "broker_adapter": "",
            "idempotency_key": "idem",
            "submission_mode": "dry_run",
            "broker_submission_confirmed": True,
        },
    )
    conn = sqlite3.connect(str(tmp_db))

    submission = build_strategy_competition_post_rerun_broker_guard_submission(
        conn,
        post_rerun_live_authority_review_artifact_path=_authority_review(tmp_path),
        broker_submission_guard_artifact_path=guard,
        output_dir=tmp_path / "out",
    )

    conn.close()
    assert submission["passed"] is False
    assert "broker_guard_live_authority_review_hash_mismatch" in submission["blocking_reasons"]
    assert "broker_guard_broker_adapter_missing" in submission["blocking_reasons"]
    assert "broker_guard_attempted_submission_or_fill_confirmation" in submission["blocking_reasons"]
