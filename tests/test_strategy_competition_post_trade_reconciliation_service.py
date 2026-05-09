from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from openclaw.services.strategy_competition_post_trade_reconciliation_service import (
    build_strategy_competition_post_trade_reconciliation,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _feedback(passed: bool = True) -> dict:
    return {
        "artifact_version": "strategy_competition_broker_execution_feedback_review.v1",
        "competition_run_id": "comp_test",
        "feedback_status": "broker_execution_feedback_accepted" if passed else "broker_execution_feedback_blocked",
        "passed": passed,
        "execution_feedback_complete": passed,
        "feedback_review_hash": "feedback_hash_1",
    }


def _reconciliation(feedback_hash: str = "feedback_hash_1") -> dict:
    return {
        "artifact_version": "strategy_competition_post_trade_reconciliation_input.v1",
        "competition_run_id": "comp_test",
        "execution_feedback_review_hash": feedback_hash,
        "thresholds": {
            "cash_diff_abs_max": 1.0,
            "position_qty_diff_abs_max": 0,
            "cost_slippage_bps_abs_max": 50.0,
        },
        "cash_reconciliation": {"cash_diff": 0.0},
        "position_reconciliation": [
            {"ts_code": "000001.SZ", "qty_diff": 0},
            {"ts_code": "000002.SZ", "qty_diff": 0},
        ],
        "cost_slippage_reconciliation": [
            {"ts_code": "000001.SZ", "slippage_bps": 10.0},
            {"ts_code": "000002.SZ", "slippage_bps": 0.0},
        ],
        "exceptions": [],
        "operations_signoff": True,
    }


def test_post_trade_reconciliation_blocks_when_feedback_not_accepted(tmp_db: Path, tmp_path: Path):
    feedback_path = _write_json(tmp_path / "feedback.json", _feedback(False))
    recon_path = _write_json(tmp_path / "recon.json", _reconciliation())
    conn = sqlite3.connect(str(tmp_db))

    result = build_strategy_competition_post_trade_reconciliation(
        conn,
        broker_execution_feedback_review_artifact_path=str(feedback_path),
        post_trade_reconciliation_input_artifact_path=str(recon_path),
        output_dir=tmp_path / "recon_out",
    )

    conn.close()
    assert result["reconciliation_status"] == "post_trade_reconciliation_blocked"
    assert result["passed"] is False
    assert result["trade_lifecycle_complete"] is False
    assert "broker_execution_feedback_not_accepted" in result["blocking_reasons"]


def test_post_trade_reconciliation_blocks_threshold_breaches_and_unowned_exceptions(tmp_db: Path, tmp_path: Path):
    feedback_path = _write_json(tmp_path / "feedback.json", _feedback(True))
    recon = _reconciliation("wrong_hash")
    recon["cash_reconciliation"]["cash_diff"] = 12.0
    recon["position_reconciliation"][0]["qty_diff"] = 100
    recon["cost_slippage_reconciliation"][0]["slippage_bps"] = 120.0
    recon["exceptions"] = [{"issue": "cash mismatch"}]
    recon_path = _write_json(tmp_path / "recon.json", recon)
    conn = sqlite3.connect(str(tmp_db))

    result = build_strategy_competition_post_trade_reconciliation(
        conn,
        broker_execution_feedback_review_artifact_path=str(feedback_path),
        post_trade_reconciliation_input_artifact_path=str(recon_path),
        output_dir=tmp_path / "recon_out",
    )

    conn.close()
    assert result["passed"] is False
    assert "post_trade_reconciliation_feedback_hash_mismatch" in result["blocking_reasons"]
    assert "post_trade_cash_diff_exceeds_threshold:12.0/1.0" in result["blocking_reasons"]
    assert "post_trade_position_qty_diff_exceeds_threshold:000001.SZ:100.0/0.0" in result["blocking_reasons"]
    assert "post_trade_slippage_exceeds_threshold:000001.SZ:120.0/50.0" in result["blocking_reasons"]
    assert "post_trade_exception_owner_missing:0" in result["blocking_reasons"]


def test_post_trade_reconciliation_passes_complete_reconciliation(tmp_db: Path, tmp_path: Path):
    feedback_path = _write_json(tmp_path / "feedback.json", _feedback(True))
    recon_path = _write_json(tmp_path / "recon.json", _reconciliation())
    conn = sqlite3.connect(str(tmp_db))

    result = build_strategy_competition_post_trade_reconciliation(
        conn,
        broker_execution_feedback_review_artifact_path=str(feedback_path),
        post_trade_reconciliation_input_artifact_path=str(recon_path),
        output_dir=tmp_path / "recon_out",
    )

    conn.close()
    assert result["reconciliation_status"] == "post_trade_reconciliation_passed"
    assert result["passed"] is True
    assert result["trade_lifecycle_complete"] is True
    assert result["reconciliation_contract"]["requires_operations_signoff"] is True
    assert "trade_lifecycle_complete_requires_cash_position_cost_reconciliation" in result["hard_boundaries"]
    assert result["reconciliation_hash"]
