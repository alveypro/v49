from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from openclaw.services.strategy_competition_evidence_chain_manifest_service import (
    CHAIN_ORDER,
    build_strategy_competition_evidence_chain_manifest,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _artifact(name: str, passed: bool = True) -> dict:
    versions = {
        "competition_audit": "strategy_competition_portfolio_audit.v1",
        "evidence_intake_packet": "strategy_competition_evidence_intake_packet.v1",
        "evidence_submission_review": "strategy_competition_evidence_submission_review.v1",
        "formal_validation_handoff": "strategy_competition_formal_validation_handoff.v1",
        "formal_validation_result_review": "strategy_competition_formal_validation_result_review.v1",
        "release_chain_adjudication": "strategy_competition_release_chain_adjudication.v1",
        "human_release_approval": "strategy_competition_human_release_approval.v1",
        "live_order_authority": "strategy_competition_live_order_authority_check.v1",
        "broker_submission_guard": "strategy_competition_broker_submission_guard.v1",
        "broker_submission_response": "strategy_competition_broker_submission_response_evidence.v1",
        "broker_execution_feedback": "strategy_competition_broker_execution_feedback_review.v1",
        "post_trade_reconciliation": "strategy_competition_post_trade_reconciliation.v1",
        "trade_lifecycle_adjudication": "strategy_competition_trade_lifecycle_adjudication.v1",
    }
    status_fields = {
        "competition_audit": ("result_status", "industry_benchmark_competition_passed", "industry_benchmark_competition_blocked"),
        "evidence_intake_packet": ("packet_status", "evidence_intake_packet_ready", "evidence_intake_packet_blocked"),
        "evidence_submission_review": ("review_status", "evidence_submission_accepted_for_validation", "evidence_submission_blocked"),
        "formal_validation_handoff": ("handoff_status", "formal_validation_ready", "formal_validation_handoff_blocked"),
        "formal_validation_result_review": ("result_review_status", "formal_validation_results_accepted", "formal_validation_results_blocked"),
        "release_chain_adjudication": ("chain_status", "release_chain_passed_for_human_approval", "release_chain_blocked"),
        "human_release_approval": ("approval_status", "human_release_approved", "human_release_approval_blocked"),
        "live_order_authority": ("authority_status", "live_order_submission_allowed", "live_order_submission_blocked"),
        "broker_submission_guard": ("guard_status", "broker_submission_guard_passed", "broker_submission_guard_blocked"),
        "broker_submission_response": ("response_status", "broker_submission_response_accepted", "broker_submission_response_blocked"),
        "broker_execution_feedback": ("feedback_status", "broker_execution_feedback_accepted", "broker_execution_feedback_blocked"),
        "post_trade_reconciliation": ("reconciliation_status", "post_trade_reconciliation_passed", "post_trade_reconciliation_blocked"),
        "trade_lifecycle_adjudication": ("lifecycle_status", "trade_lifecycle_complete", "trade_lifecycle_blocked"),
    }
    field, ok, blocked = status_fields[name]
    payload = {
        "artifact_version": versions[name],
        "competition_run_id": "comp_test",
        field: ok if passed else blocked,
        "passed": passed,
        "blocking_reasons": [] if passed else [f"{name}_blocked"],
    }
    if name == "trade_lifecycle_adjudication":
        payload["trade_lifecycle_complete"] = passed
        payload["allowed_next_actions"] = ["complete_formal_result_review_release_chain_and_human_release_decision"]
    return payload


def _build_paths(tmp_path: Path, blocked: str = "") -> dict[str, Path]:
    return {
        name: _write_json(tmp_path / f"{name}.json", _artifact(name, passed=(name != blocked)))
        for name in CHAIN_ORDER
    }


def _call(conn: sqlite3.Connection, paths: dict[str, Path], output_dir: Path) -> dict:
    return build_strategy_competition_evidence_chain_manifest(
        conn,
        output_dir=output_dir,
        competition_audit_artifact_path=str(paths.get("competition_audit", "")),
        evidence_intake_packet_artifact_path=str(paths.get("evidence_intake_packet", "")),
        evidence_submission_review_artifact_path=str(paths.get("evidence_submission_review", "")),
        formal_validation_handoff_artifact_path=str(paths.get("formal_validation_handoff", "")),
        formal_validation_result_review_artifact_path=str(paths.get("formal_validation_result_review", "")),
        release_chain_adjudication_artifact_path=str(paths.get("release_chain_adjudication", "")),
        human_release_approval_artifact_path=str(paths.get("human_release_approval", "")),
        live_order_authority_artifact_path=str(paths.get("live_order_authority", "")),
        broker_submission_guard_artifact_path=str(paths.get("broker_submission_guard", "")),
        broker_submission_response_artifact_path=str(paths.get("broker_submission_response", "")),
        broker_execution_feedback_artifact_path=str(paths.get("broker_execution_feedback", "")),
        post_trade_reconciliation_artifact_path=str(paths.get("post_trade_reconciliation", "")),
        trade_lifecycle_adjudication_artifact_path=str(paths.get("trade_lifecycle_adjudication", "")),
    )


def test_evidence_chain_manifest_blocks_first_failed_artifact(tmp_db: Path, tmp_path: Path):
    paths = _build_paths(tmp_path, blocked="competition_audit")
    conn = sqlite3.connect(str(tmp_db))

    manifest = _call(conn, paths, tmp_path / "manifest")

    conn.close()
    assert manifest["manifest_status"] == "evidence_chain_blocked"
    assert manifest["passed"] is False
    assert manifest["production_candidate_allowed"] is False
    assert manifest["live_order_authority_granted"] is False
    assert manifest["current_blocking_artifact"] == "competition_audit"
    assert "competition_audit:competition_audit_blocked" in manifest["root_blockers"]
    assert manifest["manifest_hash"]


def test_evidence_chain_manifest_records_missing_artifacts(tmp_db: Path, tmp_path: Path):
    paths = _build_paths(tmp_path)
    del paths["broker_execution_feedback"]
    conn = sqlite3.connect(str(tmp_db))

    manifest = _call(conn, paths, tmp_path / "manifest")

    conn.close()
    assert manifest["passed"] is False
    assert manifest["current_blocking_artifact"] == "broker_execution_feedback"
    assert manifest["missing_artifacts"] == ["broker_execution_feedback"]
    assert "broker_execution_feedback:broker_execution_feedback_artifact_missing" in manifest["root_blockers"]


def test_evidence_chain_manifest_can_complete_but_never_grants_permission(tmp_db: Path, tmp_path: Path):
    paths = _build_paths(tmp_path)
    conn = sqlite3.connect(str(tmp_db))

    manifest = _call(conn, paths, tmp_path / "manifest")

    conn.close()
    assert manifest["manifest_status"] == "evidence_chain_complete"
    assert manifest["passed"] is True
    assert manifest["production_candidate_allowed"] is False
    assert manifest["production_release_authorized"] is False
    assert manifest["live_order_authority_granted"] is False
    assert manifest["trade_lifecycle_complete"] is True
    assert manifest["manifest_contract"]["does_not_create_live_order_authority"] is True
