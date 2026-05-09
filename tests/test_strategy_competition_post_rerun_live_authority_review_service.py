from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from openclaw.services.strategy_competition_post_rerun_live_authority_review_service import (
    build_strategy_competition_post_rerun_live_authority_review,
)
from openclaw.services.strategy_competition_post_rerun_live_authority_submission_service import (
    build_strategy_competition_post_rerun_live_authority_submission,
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


def test_post_rerun_live_authority_blocks_missing_authority(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))

    review = build_strategy_competition_post_rerun_live_authority_review(
        conn,
        post_rerun_release_readiness_artifact_path=_readiness(tmp_path, ready=False),
        output_dir=tmp_path / "review",
    )

    conn.close()
    assert review["live_authority_review_status"] == "post_rerun_live_authority_blocked"
    assert "post_rerun_release_readiness_not_ready" in review["blocking_reasons"]
    assert "live_order_authority_artifact_missing" in review["blocking_reasons"]
    assert review["broker_submission_allowed"] is False


def test_post_rerun_live_authority_ready_still_does_not_call_broker(tmp_db: Path, tmp_path: Path):
    authority = _write_json(
        tmp_path / "authority.json",
        {
            "authority_status": "live_order_submission_allowed",
            "passed": True,
            "source_post_rerun_release_readiness_hash": "readiness_hash",
            "orders": [{"ts_code": "000001.SZ", "side": "buy", "target_qty": 100}],
        },
    )
    conn = sqlite3.connect(str(tmp_db))

    review = build_strategy_competition_post_rerun_live_authority_review(
        conn,
        post_rerun_release_readiness_artifact_path=_readiness(tmp_path),
        live_order_authority_artifact_path=authority,
        output_dir=tmp_path / "review",
    )

    conn.close()
    assert review["live_authority_review_status"] == "post_rerun_live_authority_ready_for_broker_guard"
    assert review["passed"] is True
    assert review["live_order_authority_granted"] is False
    assert review["broker_submission_allowed"] is False


def test_post_rerun_live_authority_blocks_hash_mismatch_and_broker_claim(tmp_db: Path, tmp_path: Path):
    authority = _write_json(
        tmp_path / "authority_bad.json",
        {
            "authority_status": "live_order_submission_allowed",
            "passed": True,
            "source_post_rerun_release_readiness_hash": "wrong",
            "orders": [{"ts_code": "000001.SZ", "side": "buy", "target_qty": 100}],
            "broker_submission_allowed": True,
        },
    )
    conn = sqlite3.connect(str(tmp_db))

    review = build_strategy_competition_post_rerun_live_authority_review(
        conn,
        post_rerun_release_readiness_artifact_path=_readiness(tmp_path),
        live_order_authority_artifact_path=authority,
        output_dir=tmp_path / "review",
    )

    conn.close()
    assert review["passed"] is False
    assert "live_order_authority_readiness_hash_mismatch" in review["blocking_reasons"]
    assert "live_order_authority_attempted_broker_submission" in review["blocking_reasons"]


def test_post_rerun_live_authority_accepts_submission_pack(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))
    submission = build_strategy_competition_post_rerun_live_authority_submission(
        conn,
        post_rerun_release_readiness_artifact_path=_readiness(tmp_path),
        live_order_authority_artifact_path=_write_json(
            tmp_path / "authority.json",
            {
                "artifact_version": "strategy_competition_live_order_authority.v1",
                "competition_run_id": "comp_test",
                "authority_status": "live_order_submission_allowed",
                "passed": True,
                "source_post_rerun_release_readiness_hash": "readiness_hash",
                "orders": [{"ts_code": "000001.SZ", "side": "buy", "target_qty": 100}],
            },
        ),
        output_dir=tmp_path / "submission",
    )
    review = build_strategy_competition_post_rerun_live_authority_review(
        conn,
        post_rerun_release_readiness_artifact_path=_readiness(tmp_path),
        live_authority_submission_artifact_path=submission["artifact_path"],
        output_dir=tmp_path / "review",
    )
    conn.close()
    assert submission["live_authority_submission_status"] == "post_rerun_live_authority_submission_ready"
    assert review["live_authority_review_status"] == "post_rerun_live_authority_ready_for_broker_guard"
    assert review["passed"] is True
