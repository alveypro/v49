from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from openclaw.services.strategy_competition_post_rerun_broker_execution_feedback_review_service import (
    build_strategy_competition_post_rerun_broker_execution_feedback_review,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _response_review(tmp_path: Path, ready: bool = True) -> Path:
    return _write_json(
        tmp_path / "response_review.json",
        {
            "competition_run_id": "comp_test",
            "broker_response_review_status": "post_rerun_broker_response_ready_for_execution_feedback" if ready else "post_rerun_broker_response_blocked",
            "passed": ready,
            "broker_submission_confirmed": ready,
            "broker_submission_response_evidence_hash": "response_hash_1",
            "broker_submission_guard_hash": "guard_hash_1",
            "idempotency_key": "idem_1",
            "broker_adapter": "paper_broker_adapter",
            "order_responses": [
                {"ts_code": "000001.SZ", "side": "buy", "target_qty": 100, "status": "accepted", "broker_order_ref": "B1"}
            ],
        },
    )


def _feedback(tmp_path: Path, response_hash: str = "response_hash_1") -> Path:
    return _write_json(
        tmp_path / "feedback.json",
        {
            "artifact_version": "strategy_competition_broker_execution_feedback.v1",
            "competition_run_id": "comp_test",
            "feedback_status": "broker_execution_feedback_accepted",
            "passed": True,
            "execution_feedback_complete": True,
            "broker_submission_response_hash": response_hash,
            "idempotency_key": "idem_1",
            "execution_reports": [
                {
                    "ts_code": "000001.SZ",
                    "side": "buy",
                    "target_qty": 100,
                    "status": "filled",
                    "close_price": 10.2,
                    "execution_attribution": True,
                    "fills": [
                        {
                            "fill_price": 10.1,
                            "fill_qty": 100,
                            "fill_fee": 1.0,
                            "fill_slippage_bp": 10.0,
                        }
                    ],
                }
            ],
        },
    )


def test_post_rerun_broker_execution_feedback_blocks_missing_feedback(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))

    review = build_strategy_competition_post_rerun_broker_execution_feedback_review(
        conn,
        post_rerun_broker_response_review_artifact_path=_response_review(tmp_path, ready=False),
        output_dir=tmp_path / "review",
    )

    conn.close()
    assert review["broker_execution_feedback_review_status"] == "post_rerun_broker_execution_feedback_blocked"
    assert "post_rerun_broker_response_review_not_ready" in review["blocking_reasons"]
    assert "broker_execution_feedback_artifact_missing" in review["blocking_reasons"]
    assert review["execution_feedback_complete"] is False
    assert review["trade_lifecycle_complete"] is False


def test_post_rerun_broker_execution_feedback_ready_still_not_post_trade(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))

    review = build_strategy_competition_post_rerun_broker_execution_feedback_review(
        conn,
        post_rerun_broker_response_review_artifact_path=_response_review(tmp_path),
        broker_execution_feedback_artifact_path=_feedback(tmp_path),
        output_dir=tmp_path / "review",
    )

    conn.close()
    assert review["broker_execution_feedback_review_status"] == "post_rerun_broker_execution_feedback_ready_for_post_trade"
    assert review["passed"] is True
    assert review["execution_feedback_complete"] is True
    assert review["post_trade_reconciliation_passed"] is False
    assert review["trade_lifecycle_complete"] is False
    assert review["allowed_next_actions"] == ["run_post_trade_reconciliation_with_matching_execution_feedback_hash"]


def test_post_rerun_broker_execution_feedback_blocks_hash_mismatch_and_fill_details(tmp_db: Path, tmp_path: Path):
    feedback = _feedback(tmp_path, response_hash="wrong_hash")
    payload = json.loads(feedback.read_text(encoding="utf-8"))
    payload["execution_reports"][0]["fills"][0].pop("fill_fee")
    payload["execution_reports"][0]["status"] = "accepted"
    feedback.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    conn = sqlite3.connect(str(tmp_db))

    review = build_strategy_competition_post_rerun_broker_execution_feedback_review(
        conn,
        post_rerun_broker_response_review_artifact_path=_response_review(tmp_path),
        broker_execution_feedback_artifact_path=feedback,
        output_dir=tmp_path / "review",
    )

    conn.close()
    assert review["passed"] is False
    assert "broker_execution_feedback_response_hash_mismatch" in review["blocking_reasons"]
    assert "broker_execution_feedback_fill_fee_missing:000001.SZ:0" in review["blocking_reasons"]
    assert "broker_execution_feedback_not_terminal:000001.SZ" in review["blocking_reasons"]
