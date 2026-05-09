from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from openclaw.services.strategy_competition_post_rerun_trade_lifecycle_adjudication_service import (
    build_strategy_competition_post_rerun_trade_lifecycle_adjudication,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _artifact(name: str, passed: bool = True) -> dict:
    versions = {
        "post_rerun_broker_guard_review": "strategy_competition_post_rerun_broker_guard_review.v1",
        "post_rerun_broker_response_review": "strategy_competition_post_rerun_broker_response_review.v1",
        "post_rerun_broker_execution_feedback_review": "strategy_competition_post_rerun_broker_execution_feedback_review.v1",
        "post_rerun_post_trade_reconciliation": "strategy_competition_post_rerun_post_trade_reconciliation.v1",
    }
    status_fields = {
        "post_rerun_broker_guard_review": ("broker_guard_review_status", "post_rerun_broker_guard_ready_for_adapter", "post_rerun_broker_guard_blocked"),
        "post_rerun_broker_response_review": ("broker_response_review_status", "post_rerun_broker_response_ready_for_execution_feedback", "post_rerun_broker_response_blocked"),
        "post_rerun_broker_execution_feedback_review": ("broker_execution_feedback_review_status", "post_rerun_broker_execution_feedback_ready_for_post_trade", "post_rerun_broker_execution_feedback_blocked"),
        "post_rerun_post_trade_reconciliation": ("reconciliation_status", "post_rerun_post_trade_reconciliation_passed", "post_rerun_post_trade_reconciliation_blocked"),
    }
    field, ok, blocked = status_fields[name]
    payload = {
        "artifact_version": versions[name],
        "competition_run_id": "comp_test",
        field: ok if passed else blocked,
        "passed": passed,
        "blocking_reasons": [] if passed else [f"{name}_blocked"],
    }
    if name == "post_rerun_post_trade_reconciliation":
        payload["trade_lifecycle_complete"] = passed
    return payload


def test_post_rerun_trade_lifecycle_adjudication_blocks_current_failed_lifecycle(tmp_db: Path, tmp_path: Path):
    paths = {}
    for name in (
        "post_rerun_broker_guard_review",
        "post_rerun_broker_response_review",
        "post_rerun_broker_execution_feedback_review",
        "post_rerun_post_trade_reconciliation",
    ):
        paths[name] = _write_json(tmp_path / f"{name}.json", _artifact(name, passed=(name != "post_rerun_broker_guard_review")))
    conn = sqlite3.connect(str(tmp_db))
    verdict = build_strategy_competition_post_rerun_trade_lifecycle_adjudication(
        conn,
        post_rerun_broker_guard_review_artifact_path=str(paths["post_rerun_broker_guard_review"]),
        post_rerun_broker_response_review_artifact_path=str(paths["post_rerun_broker_response_review"]),
        post_rerun_broker_execution_feedback_review_artifact_path=str(paths["post_rerun_broker_execution_feedback_review"]),
        post_rerun_post_trade_reconciliation_artifact_path=str(paths["post_rerun_post_trade_reconciliation"]),
        output_dir=tmp_path / "verdict",
    )
    conn.close()
    assert verdict["lifecycle_status"] == "post_rerun_trade_lifecycle_blocked"
    assert verdict["passed"] is False
    assert verdict["trade_lifecycle_complete"] is False
    assert verdict["current_blocking_stage"] == "post_rerun_broker_guard_review"
    assert "post_rerun_broker_guard_review:post_rerun_broker_guard_review_blocked" in verdict["root_blockers"]
    assert verdict["allowed_next_actions"] == ["complete_post_rerun_broker_guard_review_and_broker_response_evidence"]


def test_post_rerun_trade_lifecycle_adjudication_requires_post_rerun_post_trade_complete(tmp_db: Path, tmp_path: Path):
    paths = {}
    for name in (
        "post_rerun_broker_guard_review",
        "post_rerun_broker_response_review",
        "post_rerun_broker_execution_feedback_review",
        "post_rerun_post_trade_reconciliation",
    ):
        paths[name] = _write_json(tmp_path / f"{name}.json", _artifact(name, passed=True))
    conn = sqlite3.connect(str(tmp_db))
    verdict = build_strategy_competition_post_rerun_trade_lifecycle_adjudication(
        conn,
        post_rerun_broker_guard_review_artifact_path=str(paths["post_rerun_broker_guard_review"]),
        post_rerun_broker_response_review_artifact_path=str(paths["post_rerun_broker_response_review"]),
        post_rerun_broker_execution_feedback_review_artifact_path=str(paths["post_rerun_broker_execution_feedback_review"]),
        post_rerun_post_trade_reconciliation_artifact_path=str(paths["post_rerun_post_trade_reconciliation"]),
        output_dir=tmp_path / "verdict",
    )
    conn.close()
    assert verdict["lifecycle_status"] == "post_rerun_trade_lifecycle_complete"
    assert verdict["passed"] is True
    assert verdict["trade_lifecycle_complete"] is True
    assert verdict["current_blocking_stage"] == ""
    assert verdict["allowed_next_actions"] == ["archive_post_rerun_trade_lifecycle_as_complete"]
    assert verdict["lifecycle_contract"]["does_not_create_new_trade_permission"] is True
