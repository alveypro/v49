from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from openclaw.services.strategy_competition_post_rerun_evidence_chain_manifest_service import (
    CHAIN_ORDER,
    build_strategy_competition_post_rerun_evidence_chain_manifest,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _artifact(name: str, passed: bool = True) -> dict:
    versions = {
        "post_rerun_release_readiness": "strategy_competition_post_rerun_release_readiness.v1",
        "post_rerun_live_authority_review": "strategy_competition_post_rerun_live_authority_review.v1",
        "post_rerun_release_chain_adjudication": "strategy_competition_post_rerun_release_chain_adjudication.v1",
        "post_rerun_broker_guard_review": "strategy_competition_post_rerun_broker_guard_review.v1",
        "post_rerun_broker_response_review": "strategy_competition_post_rerun_broker_response_review.v1",
        "post_rerun_broker_execution_feedback_review": "strategy_competition_post_rerun_broker_execution_feedback_review.v1",
        "post_rerun_post_trade_reconciliation": "strategy_competition_post_rerun_post_trade_reconciliation.v1",
        "post_rerun_trade_lifecycle_adjudication": "strategy_competition_post_rerun_trade_lifecycle_adjudication.v1",
        "post_rerun_human_release_approval_review": "strategy_competition_post_rerun_human_release_approval_review.v1",
    }
    status_fields = {
        "post_rerun_release_readiness": ("release_readiness_status", "post_rerun_release_ready_for_live_authority_check", "post_rerun_release_blocked"),
        "post_rerun_live_authority_review": ("live_authority_review_status", "post_rerun_live_authority_ready_for_broker_guard", "post_rerun_live_authority_blocked"),
        "post_rerun_release_chain_adjudication": ("release_chain_status", "post_rerun_release_chain_ready_for_broker_guard", "post_rerun_release_chain_blocked"),
        "post_rerun_broker_guard_review": ("broker_guard_review_status", "post_rerun_broker_guard_ready_for_adapter", "post_rerun_broker_guard_blocked"),
        "post_rerun_broker_response_review": ("broker_response_review_status", "post_rerun_broker_response_ready_for_execution_feedback", "post_rerun_broker_response_blocked"),
        "post_rerun_broker_execution_feedback_review": ("broker_execution_feedback_review_status", "post_rerun_broker_execution_feedback_ready_for_post_trade", "post_rerun_broker_execution_feedback_blocked"),
        "post_rerun_post_trade_reconciliation": ("reconciliation_status", "post_rerun_post_trade_reconciliation_passed", "post_rerun_post_trade_reconciliation_blocked"),
        "post_rerun_trade_lifecycle_adjudication": ("lifecycle_status", "post_rerun_trade_lifecycle_complete", "post_rerun_trade_lifecycle_blocked"),
        "post_rerun_human_release_approval_review": ("human_release_approval_review_status", "post_rerun_human_release_approved", "post_rerun_human_release_approval_blocked"),
    }
    field, ok, blocked = status_fields[name]
    payload = {
        "artifact_version": versions[name],
        "competition_run_id": "comp_test",
        field: ok if passed else blocked,
        "passed": passed,
        "blocking_reasons": [] if passed else [f"{name}_blocked"],
    }
    if name == "post_rerun_trade_lifecycle_adjudication":
        payload["trade_lifecycle_complete"] = passed
    if name == "post_rerun_post_trade_reconciliation":
        payload["trade_lifecycle_complete"] = passed
    return payload


def _paths(tmp_path: Path, blocked: str = "") -> dict[str, Path]:
    return {
        name: _write_json(tmp_path / f"{name}.json", _artifact(name, passed=(name != blocked)))
        for name in CHAIN_ORDER
    }


def test_post_rerun_manifest_blocks_first_failed_artifact(tmp_db: Path, tmp_path: Path):
    paths = _paths(tmp_path, blocked="post_rerun_broker_guard_review")
    conn = sqlite3.connect(str(tmp_db))
    manifest = build_strategy_competition_post_rerun_evidence_chain_manifest(
        conn,
        output_dir=tmp_path / "manifest",
        post_rerun_release_readiness_artifact_path=str(paths["post_rerun_release_readiness"]),
        post_rerun_live_authority_review_artifact_path=str(paths["post_rerun_live_authority_review"]),
        post_rerun_release_chain_adjudication_artifact_path=str(paths["post_rerun_release_chain_adjudication"]),
        post_rerun_broker_guard_review_artifact_path=str(paths["post_rerun_broker_guard_review"]),
        post_rerun_broker_response_review_artifact_path=str(paths["post_rerun_broker_response_review"]),
        post_rerun_broker_execution_feedback_review_artifact_path=str(paths["post_rerun_broker_execution_feedback_review"]),
        post_rerun_post_trade_reconciliation_artifact_path=str(paths["post_rerun_post_trade_reconciliation"]),
        post_rerun_trade_lifecycle_adjudication_artifact_path=str(paths["post_rerun_trade_lifecycle_adjudication"]),
        post_rerun_human_release_approval_review_artifact_path=str(paths["post_rerun_human_release_approval_review"]),
    )
    conn.close()
    assert manifest["manifest_status"] == "post_rerun_evidence_chain_blocked"
    assert manifest["passed"] is False
    assert manifest["current_blocking_artifact"] == "post_rerun_broker_guard_review"
    assert "post_rerun_broker_guard_review:post_rerun_broker_guard_review_blocked" in manifest["root_blockers"]
    assert manifest["production_candidate_allowed"] is False


def test_post_rerun_manifest_records_missing_artifacts(tmp_db: Path, tmp_path: Path):
    paths = _paths(tmp_path)
    del paths["post_rerun_broker_execution_feedback_review"]
    conn = sqlite3.connect(str(tmp_db))
    manifest = build_strategy_competition_post_rerun_evidence_chain_manifest(
        conn,
        output_dir=tmp_path / "manifest",
        post_rerun_release_readiness_artifact_path=str(paths["post_rerun_release_readiness"]),
        post_rerun_live_authority_review_artifact_path=str(paths["post_rerun_live_authority_review"]),
        post_rerun_release_chain_adjudication_artifact_path=str(paths["post_rerun_release_chain_adjudication"]),
        post_rerun_broker_guard_review_artifact_path=str(paths["post_rerun_broker_guard_review"]),
        post_rerun_broker_response_review_artifact_path=str(paths["post_rerun_broker_response_review"]),
        post_rerun_broker_execution_feedback_review_artifact_path=str(paths.get("post_rerun_broker_execution_feedback_review", "")),
        post_rerun_post_trade_reconciliation_artifact_path=str(paths["post_rerun_post_trade_reconciliation"]),
        post_rerun_trade_lifecycle_adjudication_artifact_path=str(paths["post_rerun_trade_lifecycle_adjudication"]),
        post_rerun_human_release_approval_review_artifact_path=str(paths["post_rerun_human_release_approval_review"]),
    )
    conn.close()
    assert manifest["passed"] is False
    assert manifest["current_blocking_artifact"] == "post_rerun_broker_execution_feedback_review"
    assert manifest["missing_artifacts"] == ["post_rerun_broker_execution_feedback_review"]


def test_post_rerun_manifest_can_complete_but_never_grants_permission(tmp_db: Path, tmp_path: Path):
    paths = _paths(tmp_path)
    conn = sqlite3.connect(str(tmp_db))
    manifest = build_strategy_competition_post_rerun_evidence_chain_manifest(
        conn,
        output_dir=tmp_path / "manifest",
        post_rerun_release_readiness_artifact_path=str(paths["post_rerun_release_readiness"]),
        post_rerun_live_authority_review_artifact_path=str(paths["post_rerun_live_authority_review"]),
        post_rerun_release_chain_adjudication_artifact_path=str(paths["post_rerun_release_chain_adjudication"]),
        post_rerun_broker_guard_review_artifact_path=str(paths["post_rerun_broker_guard_review"]),
        post_rerun_broker_response_review_artifact_path=str(paths["post_rerun_broker_response_review"]),
        post_rerun_broker_execution_feedback_review_artifact_path=str(paths["post_rerun_broker_execution_feedback_review"]),
        post_rerun_post_trade_reconciliation_artifact_path=str(paths["post_rerun_post_trade_reconciliation"]),
        post_rerun_trade_lifecycle_adjudication_artifact_path=str(paths["post_rerun_trade_lifecycle_adjudication"]),
        post_rerun_human_release_approval_review_artifact_path=str(paths["post_rerun_human_release_approval_review"]),
    )
    conn.close()
    assert manifest["manifest_status"] == "post_rerun_evidence_chain_complete"
    assert manifest["passed"] is True
    assert manifest["production_candidate_allowed"] is False
    assert manifest["production_release_authorized"] is False
    assert manifest["live_order_authority_granted"] is False
    assert manifest["trade_lifecycle_complete"] is True
    assert manifest["manifest_contract"]["does_not_create_live_order_authority"] is True
