from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from openclaw.services.strategy_competition_post_rerun_post_trade_reconciliation_service import (
    build_strategy_competition_post_rerun_post_trade_reconciliation,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _feedback_review(tmp_path: Path, ready: bool = True) -> Path:
    return _write_json(
        tmp_path / "feedback_review.json",
        {
            "competition_run_id": "comp_test",
            "broker_execution_feedback_review_status": "post_rerun_broker_execution_feedback_ready_for_post_trade" if ready else "post_rerun_broker_execution_feedback_blocked",
            "passed": ready,
            "broker_execution_feedback_review_hash": "feedback_review_hash",
        },
    )


def _reconciliation(tmp_path: Path, feedback_hash: str = "feedback_review_hash") -> Path:
    return _write_json(
        tmp_path / "recon.json",
        {
            "artifact_version": "strategy_competition_post_trade_reconciliation_input.v1",
            "competition_run_id": "comp_test",
            "execution_feedback_review_hash": feedback_hash,
            "thresholds": {"cash_diff_abs_max": 1.0, "position_qty_diff_abs_max": 0, "cost_slippage_bps_abs_max": 50.0},
            "cash_reconciliation": {"cash_diff": 0.0},
            "position_reconciliation": [{"ts_code": "000001.SZ", "qty_diff": 0}],
            "cost_slippage_reconciliation": [{"ts_code": "000001.SZ", "slippage_bps": 10.0}],
            "exceptions": [],
            "operations_signoff": True,
        },
    )


def test_post_rerun_post_trade_reconciliation_blocks_when_feedback_not_ready(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))
    result = build_strategy_competition_post_rerun_post_trade_reconciliation(
        conn,
        post_rerun_broker_execution_feedback_review_artifact_path=_feedback_review(tmp_path, ready=False),
        output_dir=tmp_path / "out",
    )
    conn.close()
    assert result["reconciliation_status"] == "post_rerun_post_trade_reconciliation_blocked"
    assert result["passed"] is False
    assert result["trade_lifecycle_complete"] is False
    assert "post_rerun_broker_execution_feedback_review_not_ready" in result["blocking_reasons"]


def test_post_rerun_post_trade_reconciliation_passes_complete_reconciliation(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))
    result = build_strategy_competition_post_rerun_post_trade_reconciliation(
        conn,
        post_rerun_broker_execution_feedback_review_artifact_path=_feedback_review(tmp_path),
        post_trade_reconciliation_input_artifact_path=_reconciliation(tmp_path),
        output_dir=tmp_path / "out",
    )
    conn.close()
    assert result["reconciliation_status"] == "post_rerun_post_trade_reconciliation_passed"
    assert result["passed"] is True
    assert result["trade_lifecycle_complete"] is True
    assert result["reconciliation_contract"]["does_not_create_new_trade_permission"] is True
