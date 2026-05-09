from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from openclaw.services.strategy_competition_evidence_remediation_work_order_service import (
    build_strategy_competition_evidence_remediation_work_order,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def test_remediation_work_order_turns_blocked_manifest_into_work_items(tmp_db: Path, tmp_path: Path):
    manifest = _write_json(
        tmp_path / "manifest.json",
        {
            "artifact_version": "strategy_competition_evidence_chain_manifest.v1",
            "competition_run_id": "comp_test",
            "manifest_status": "evidence_chain_blocked",
            "passed": False,
            "current_blocking_artifact": "competition_audit",
            "allowed_next_actions": ["complete_or_repair_competition_audit_and_rerun_evidence_chain_manifest"],
            "artifact_statuses": [
                {
                    "name": "competition_audit",
                    "status": "industry_benchmark_competition_blocked",
                    "passed": False,
                    "artifact_hash": "abc",
                    "blocking_reasons": ["independent_validator_not_approved", "shadow_execution_not_passed"],
                },
                {"name": "trade_lifecycle_adjudication", "status": "trade_lifecycle_blocked", "passed": False, "blocking_reasons": []},
            ],
        },
    )
    conn = sqlite3.connect(str(tmp_db))

    work_order = build_strategy_competition_evidence_remediation_work_order(
        conn,
        manifest_artifact_path=manifest,
        output_dir=tmp_path / "work_order",
    )

    conn.close()
    assert work_order["work_order_status"] == "remediation_required"
    assert work_order["passed"] is False
    assert work_order["production_candidate_allowed"] is False
    assert work_order["live_order_authority_granted"] is False
    assert work_order["blocked_work_item_count"] == 2
    assert work_order["work_items"][0]["owner_role"] == "strategy_governance_owner"
    assert "independent_validator_decision" in work_order["work_items"][0]["required_evidence"]
    assert work_order["work_order_hash"]


def test_remediation_work_order_complete_manifest_still_grants_no_permission(tmp_db: Path, tmp_path: Path):
    manifest = _write_json(
        tmp_path / "manifest_complete.json",
        {
            "artifact_version": "strategy_competition_evidence_chain_manifest.v1",
            "competition_run_id": "comp_test",
            "manifest_status": "evidence_chain_complete",
            "passed": True,
            "current_blocking_artifact": "",
            "artifact_statuses": [{"name": "trade_lifecycle_adjudication", "passed": True}],
        },
    )
    conn = sqlite3.connect(str(tmp_db))

    work_order = build_strategy_competition_evidence_remediation_work_order(
        conn,
        manifest_artifact_path=manifest,
        output_dir=tmp_path / "work_order",
    )

    conn.close()
    assert work_order["work_order_status"] == "no_remediation_required"
    assert work_order["passed"] is True
    assert work_order["production_candidate_allowed"] is False
    assert work_order["production_release_authorized"] is False
    assert work_order["live_order_authority_granted"] is False
    assert work_order["blocked_work_item_count"] == 0


def test_remediation_work_order_includes_post_rerun_human_release_approval_review(tmp_db: Path, tmp_path: Path):
    manifest = _write_json(
        tmp_path / "post_rerun_manifest.json",
        {
            "artifact_version": "strategy_competition_post_rerun_evidence_chain_manifest.v1",
            "competition_run_id": "comp_test",
            "manifest_status": "post_rerun_evidence_chain_blocked",
            "passed": False,
            "current_blocking_artifact": "post_rerun_human_release_approval_review",
            "allowed_next_actions": ["complete_post_rerun_evidence_chain_manifest_and_human_approval_review"],
            "artifact_statuses": [
                {
                    "name": "post_rerun_human_release_approval_review",
                    "status": "post_rerun_human_release_approval_blocked",
                    "passed": False,
                    "artifact_hash": "abc",
                    "blocking_reasons": ["post_rerun_evidence_chain_manifest_not_complete"],
                },
            ],
        },
    )
    conn = sqlite3.connect(str(tmp_db))

    work_order = build_strategy_competition_evidence_remediation_work_order(
        conn,
        manifest_artifact_path=manifest,
        output_dir=tmp_path / "work_order",
    )

    conn.close()
    assert work_order["blocked_work_item_count"] == 1
    item = work_order["work_items"][0]
    assert item["artifact"] == "post_rerun_human_release_approval_review"
    assert item["owner_role"] == "human_release_approver"
    assert item["validator_tool"] == "tools/review_strategy_competition_post_rerun_human_release_approval_review.py"
    assert "post_rerun_evidence_chain_manifest_complete" in item["required_evidence"]
    assert "independent_human_approval_decision" in item["required_evidence"]
