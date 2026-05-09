from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from openclaw.services.strategy_competition_trade_lifecycle_adjudication_service import (
    build_strategy_competition_trade_lifecycle_adjudication,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _artifact(name: str, passed: bool = True) -> dict:
    versions = {
        "human_release_approval": "strategy_competition_human_release_approval.v1",
        "live_order_authority": "strategy_competition_live_order_authority_check.v1",
        "broker_submission_guard": "strategy_competition_broker_submission_guard.v1",
        "broker_submission_response": "strategy_competition_broker_submission_response_evidence.v1",
        "broker_execution_feedback": "strategy_competition_broker_execution_feedback_review.v1",
        "post_trade_reconciliation": "strategy_competition_post_trade_reconciliation.v1",
    }
    status_fields = {
        "human_release_approval": ("approval_status", "human_release_approved", "human_release_approval_blocked"),
        "live_order_authority": ("authority_status", "live_order_submission_allowed", "live_order_submission_blocked"),
        "broker_submission_guard": ("guard_status", "broker_submission_guard_passed", "broker_submission_guard_blocked"),
        "broker_submission_response": ("response_status", "broker_submission_response_accepted", "broker_submission_response_blocked"),
        "broker_execution_feedback": ("feedback_status", "broker_execution_feedback_accepted", "broker_execution_feedback_blocked"),
        "post_trade_reconciliation": ("reconciliation_status", "post_trade_reconciliation_passed", "post_trade_reconciliation_blocked"),
    }
    field, ok, blocked = status_fields[name]
    payload = {
        "artifact_version": versions[name],
        "competition_run_id": "comp_test",
        field: ok if passed else blocked,
        "passed": passed,
        "blocking_reasons": [] if passed else [f"{name}_blocked"],
    }
    if name == "post_trade_reconciliation":
        payload["trade_lifecycle_complete"] = passed
    return payload


def test_trade_lifecycle_adjudication_blocks_current_failed_lifecycle(tmp_db: Path, tmp_path: Path):
    paths = {
        name: _write_json(tmp_path / f"{name}.json", _artifact(name, passed=(name != "human_release_approval")))
        for name in (
            "human_release_approval",
            "live_order_authority",
            "broker_submission_guard",
            "broker_submission_response",
            "broker_execution_feedback",
            "post_trade_reconciliation",
        )
    }
    conn = sqlite3.connect(str(tmp_db))

    verdict = build_strategy_competition_trade_lifecycle_adjudication(
        conn,
        human_release_approval_artifact_path=str(paths["human_release_approval"]),
        live_order_authority_artifact_path=str(paths["live_order_authority"]),
        broker_submission_guard_artifact_path=str(paths["broker_submission_guard"]),
        broker_submission_response_artifact_path=str(paths["broker_submission_response"]),
        broker_execution_feedback_artifact_path=str(paths["broker_execution_feedback"]),
        post_trade_reconciliation_artifact_path=str(paths["post_trade_reconciliation"]),
        output_dir=tmp_path / "verdict",
    )

    conn.close()
    assert verdict["lifecycle_status"] == "trade_lifecycle_blocked"
    assert verdict["passed"] is False
    assert verdict["trade_lifecycle_complete"] is False
    assert verdict["current_blocking_stage"] == "human_release_approval"
    assert "human_release_approval:human_release_approval_blocked" in verdict["root_blockers"]
    assert verdict["allowed_next_actions"] == ["complete_formal_result_review_release_chain_and_human_release_decision"]


def test_trade_lifecycle_adjudication_requires_post_trade_reconciliation_complete(tmp_db: Path, tmp_path: Path):
    paths = {
        name: _write_json(tmp_path / f"{name}.json", _artifact(name, passed=True))
        for name in (
            "human_release_approval",
            "live_order_authority",
            "broker_submission_guard",
            "broker_submission_response",
            "broker_execution_feedback",
            "post_trade_reconciliation",
        )
    }
    conn = sqlite3.connect(str(tmp_db))

    verdict = build_strategy_competition_trade_lifecycle_adjudication(
        conn,
        human_release_approval_artifact_path=str(paths["human_release_approval"]),
        live_order_authority_artifact_path=str(paths["live_order_authority"]),
        broker_submission_guard_artifact_path=str(paths["broker_submission_guard"]),
        broker_submission_response_artifact_path=str(paths["broker_submission_response"]),
        broker_execution_feedback_artifact_path=str(paths["broker_execution_feedback"]),
        post_trade_reconciliation_artifact_path=str(paths["post_trade_reconciliation"]),
        output_dir=tmp_path / "verdict",
    )

    conn.close()
    assert verdict["lifecycle_status"] == "trade_lifecycle_complete"
    assert verdict["passed"] is True
    assert verdict["trade_lifecycle_complete"] is True
    assert verdict["current_blocking_stage"] == ""
    assert verdict["allowed_next_actions"] == ["archive_trade_lifecycle_as_complete"]
    assert verdict["lifecycle_contract"]["does_not_create_new_trade_permission"] is True
    assert verdict["lifecycle_adjudication_hash"]


def test_trade_lifecycle_adjudication_does_not_skip_mid_chain_failure(tmp_db: Path, tmp_path: Path):
    paths = {}
    for name in (
        "human_release_approval",
        "live_order_authority",
        "broker_submission_guard",
        "broker_submission_response",
        "broker_execution_feedback",
        "post_trade_reconciliation",
    ):
        paths[name] = _write_json(tmp_path / f"{name}.json", _artifact(name, passed=(name != "broker_submission_response")))
    conn = sqlite3.connect(str(tmp_db))

    verdict = build_strategy_competition_trade_lifecycle_adjudication(
        conn,
        human_release_approval_artifact_path=str(paths["human_release_approval"]),
        live_order_authority_artifact_path=str(paths["live_order_authority"]),
        broker_submission_guard_artifact_path=str(paths["broker_submission_guard"]),
        broker_submission_response_artifact_path=str(paths["broker_submission_response"]),
        broker_execution_feedback_artifact_path=str(paths["broker_execution_feedback"]),
        post_trade_reconciliation_artifact_path=str(paths["post_trade_reconciliation"]),
        output_dir=tmp_path / "verdict",
    )

    conn.close()
    assert verdict["passed"] is False
    assert verdict["current_blocking_stage"] == "broker_submission_response"
    assert verdict["allowed_next_actions"] == ["collect_broker_adapter_response_and_review_submission_response"]
