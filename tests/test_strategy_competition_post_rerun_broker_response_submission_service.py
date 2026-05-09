from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from openclaw.services.strategy_competition_post_rerun_broker_response_submission_service import (
    build_strategy_competition_post_rerun_broker_response_submission,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _guard_review(tmp_path: Path, ready: bool = True) -> Path:
    return _write_json(
        tmp_path / "guard_review.json",
        {
            "competition_run_id": "comp_test",
            "broker_guard_review_status": "post_rerun_broker_guard_ready_for_adapter" if ready else "post_rerun_broker_guard_blocked",
            "passed": ready,
            "broker_submission_guard_hash": "guard_hash_1",
            "broker_guard_review_hash": "guard_review_hash_1",
        },
    )


def _response(tmp_path: Path, guard_hash: str = "guard_hash_1") -> Path:
    return _write_json(
        tmp_path / "response.json",
        {
            "artifact_version": "strategy_competition_broker_submission_response_evidence.v1",
            "competition_run_id": "comp_test",
            "response_status": "broker_submission_response_accepted",
            "passed": True,
            "broker_submission_confirmed": True,
            "execution_fills_confirmed": False,
            "source_artifact_hashes": {"broker_submission_guard": guard_hash},
            "order_responses": [
                {"ts_code": "000001.SZ", "side": "buy", "target_qty": 100, "status": "accepted", "broker_order_ref": "B1"}
            ],
        },
    )


def test_post_rerun_broker_response_submission_blocks_missing_response(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))

    submission = build_strategy_competition_post_rerun_broker_response_submission(
        conn,
        post_rerun_broker_guard_review_artifact_path=_guard_review(tmp_path, ready=False),
        broker_submission_response_evidence_artifact_path=_response(tmp_path),
        output_dir=tmp_path / "submission",
    )

    conn.close()
    assert submission["broker_response_submission_status"] == "post_rerun_broker_response_submission_blocked"
    assert "post_rerun_broker_guard_review_not_ready" in submission["blocking_reasons"]
    assert submission["passed"] is False


def test_post_rerun_broker_response_submission_ready(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))

    submission = build_strategy_competition_post_rerun_broker_response_submission(
        conn,
        post_rerun_broker_guard_review_artifact_path=_guard_review(tmp_path),
        broker_submission_response_evidence_artifact_path=_response(tmp_path),
        output_dir=tmp_path / "submission",
    )

    conn.close()
    assert submission["broker_response_submission_status"] == "post_rerun_broker_response_submission_ready"
    assert submission["passed"] is True
    assert submission["allowed_next_actions"] == ["submit_to_post_rerun_broker_response_review"]
    assert submission["broker_response_submission_contract"]["requires_post_rerun_broker_guard_review_ready"] is True
