from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from openclaw.services.strategy_competition_broker_execution_feedback_service import (
    build_strategy_competition_broker_execution_feedback_review,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _response(passed: bool = True) -> dict:
    return {
        "artifact_version": "strategy_competition_broker_submission_response_evidence.v1",
        "competition_run_id": "comp_test",
        "response_status": "broker_submission_response_accepted" if passed else "broker_submission_response_blocked",
        "passed": passed,
        "response_evidence_hash": "response_hash_1",
        "idempotency_key": "idem_1",
        "order_responses": [
            {"ts_code": "000001.SZ", "side": "buy", "target_qty": 100, "status": "accepted", "broker_order_ref": "B1"},
            {"ts_code": "000002.SZ", "side": "buy", "target_qty": 200, "status": "accepted", "broker_order_ref": "B2"},
        ],
    }


def _feedback(response_hash: str = "response_hash_1") -> dict:
    return {
        "artifact_version": "strategy_competition_broker_execution_feedback.v1",
        "competition_run_id": "comp_test",
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
            },
            {
                "ts_code": "000002.SZ",
                "side": "buy",
                "target_qty": 200,
                "status": "rejected",
                "miss_reason_code": "broker_reject_limit",
                "close_price": 9.8,
                "execution_attribution": True,
                "fills": [],
            },
        ],
    }


def test_execution_feedback_blocks_when_submission_response_not_accepted(tmp_db: Path, tmp_path: Path):
    response_path = _write_json(tmp_path / "response.json", _response(False))
    feedback_path = _write_json(tmp_path / "feedback.json", _feedback())
    conn = sqlite3.connect(str(tmp_db))

    review = build_strategy_competition_broker_execution_feedback_review(
        conn,
        broker_submission_response_evidence_artifact_path=str(response_path),
        broker_execution_feedback_artifact_path=str(feedback_path),
        output_dir=tmp_path / "review",
    )

    conn.close()
    assert review["feedback_status"] == "broker_execution_feedback_blocked"
    assert review["passed"] is False
    assert review["execution_feedback_complete"] is False
    assert "broker_submission_response_not_accepted" in review["blocking_reasons"]


def test_execution_feedback_requires_terminal_feedback_and_fill_details(tmp_db: Path, tmp_path: Path):
    response_path = _write_json(tmp_path / "response.json", _response(True))
    feedback = _feedback("wrong_hash")
    feedback["execution_reports"][0]["fills"][0].pop("fill_slippage_bp")
    feedback["execution_reports"][1]["status"] = "accepted"
    feedback_path = _write_json(tmp_path / "feedback.json", feedback)
    conn = sqlite3.connect(str(tmp_db))

    review = build_strategy_competition_broker_execution_feedback_review(
        conn,
        broker_submission_response_evidence_artifact_path=str(response_path),
        broker_execution_feedback_artifact_path=str(feedback_path),
        output_dir=tmp_path / "review",
    )

    conn.close()
    assert review["passed"] is False
    assert "broker_execution_feedback_response_hash_mismatch" in review["blocking_reasons"]
    assert "broker_execution_feedback_slippage_missing:000001.SZ:0" in review["blocking_reasons"]
    assert "broker_execution_feedback_not_terminal:000002.SZ" in review["blocking_reasons"]


def test_execution_feedback_accepts_complete_fill_and_reject_reports(tmp_db: Path, tmp_path: Path):
    response_path = _write_json(tmp_path / "response.json", _response(True))
    feedback_path = _write_json(tmp_path / "feedback.json", _feedback())
    conn = sqlite3.connect(str(tmp_db))

    review = build_strategy_competition_broker_execution_feedback_review(
        conn,
        broker_submission_response_evidence_artifact_path=str(response_path),
        broker_execution_feedback_artifact_path=str(feedback_path),
        output_dir=tmp_path / "review",
    )

    conn.close()
    assert review["feedback_status"] == "broker_execution_feedback_accepted"
    assert review["passed"] is True
    assert review["execution_feedback_complete"] is True
    assert review["feedback_contract"]["requires_cost_slippage_and_attribution"] is True
    assert "filled_orders_require_fills_costs_slippage_and_attribution" in review["hard_boundaries"]
    assert review["feedback_review_hash"]
