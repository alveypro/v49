from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from openclaw.services.strategy_competition_post_rerun_broker_response_review_service import (
    build_strategy_competition_post_rerun_broker_response_review,
)
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
            "broker_submission_guard_hash": "guard_file_hash",
            "broker_guard_review_hash": "guard_review_hash",
        },
    )


def _response(tmp_path: Path, guard_hash: str = "guard_file_hash") -> Path:
    return _write_json(
        tmp_path / "response_evidence.json",
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


def test_post_rerun_broker_response_blocks_missing_response(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))

    review = build_strategy_competition_post_rerun_broker_response_review(
        conn,
        post_rerun_broker_guard_review_artifact_path=_guard_review(tmp_path, ready=False),
        output_dir=tmp_path / "review",
    )

    conn.close()
    assert review["broker_response_review_status"] == "post_rerun_broker_response_blocked"
    assert "post_rerun_broker_guard_review_not_ready" in review["blocking_reasons"]
    assert "broker_submission_response_evidence_missing" in review["blocking_reasons"]
    assert review["broker_submission_confirmed"] is False
    assert review["execution_fills_confirmed"] is False


def test_post_rerun_broker_response_ready_still_requires_execution_feedback(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))

    review = build_strategy_competition_post_rerun_broker_response_review(
        conn,
        post_rerun_broker_guard_review_artifact_path=_guard_review(tmp_path),
        broker_submission_response_evidence_artifact_path=_response(tmp_path),
        output_dir=tmp_path / "review",
    )

    conn.close()
    assert review["broker_response_review_status"] == "post_rerun_broker_response_ready_for_execution_feedback"
    assert review["passed"] is True
    assert review["broker_submission_confirmed"] is True
    assert review["execution_fills_confirmed"] is False
    assert review["post_trade_reconciliation_passed"] is False
    assert review["allowed_next_actions"] == ["record_broker_execution_feedback_with_matching_response_hash"]


def test_post_rerun_broker_response_blocks_hash_mismatch_and_fill_claim(tmp_db: Path, tmp_path: Path):
    response = _response(tmp_path, guard_hash="wrong")
    payload = json.loads(response.read_text(encoding="utf-8"))
    payload["execution_fills_confirmed"] = True
    payload["order_responses"][0]["fills"] = [{"fill_qty": 100, "fill_price": 10.0}]
    response.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    conn = sqlite3.connect(str(tmp_db))

    review = build_strategy_competition_post_rerun_broker_response_review(
        conn,
        post_rerun_broker_guard_review_artifact_path=_guard_review(tmp_path),
        broker_submission_response_evidence_artifact_path=response,
        output_dir=tmp_path / "review",
    )

    conn.close()
    assert review["passed"] is False
    assert "broker_response_guard_hash_mismatch" in review["blocking_reasons"]
    assert "broker_response_attempted_fill_confirmation" in review["blocking_reasons"]
    assert "broker_response_contains_fills" in review["blocking_reasons"]


def test_post_rerun_broker_response_accepts_submission_pack(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))
    submission = build_strategy_competition_post_rerun_broker_response_submission(
        conn,
        post_rerun_broker_guard_review_artifact_path=_guard_review(tmp_path),
        broker_submission_response_evidence_artifact_path=_response(tmp_path),
        output_dir=tmp_path / "submission",
    )
    review = build_strategy_competition_post_rerun_broker_response_review(
        conn,
        post_rerun_broker_guard_review_artifact_path=_guard_review(tmp_path),
        broker_response_submission_artifact_path=submission["artifact_path"],
        output_dir=tmp_path / "review",
    )
    conn.close()
    assert submission["broker_response_submission_status"] == "post_rerun_broker_response_submission_ready"
    assert review["broker_response_review_status"] == "post_rerun_broker_response_ready_for_execution_feedback"
    assert review["passed"] is True
