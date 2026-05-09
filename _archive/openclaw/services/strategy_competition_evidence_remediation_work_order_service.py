from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from typing import Any, Dict, List

from openclaw.services.lineage_service import apply_professional_migrations, canonical_json


JsonDict = Dict[str, Any]

OWNER_BY_ARTIFACT = {
    "competition_audit": "strategy_governance_owner",
    "evidence_intake_packet": "research_ops_owner",
    "evidence_submission_review": "research_ops_owner",
    "formal_validation_handoff": "validation_ops_owner",
    "formal_validation_result_review": "independent_validation_owner",
    "release_chain_adjudication": "release_governance_owner",
    "human_release_approval": "human_release_approver",
    "post_rerun_release_readiness": "release_governance_owner",
    "post_rerun_live_authority_review": "trading_controls_owner",
    "post_rerun_release_chain_adjudication": "release_governance_owner",
    "post_rerun_broker_guard_review": "broker_controls_owner",
    "post_rerun_broker_response_review": "broker_adapter_owner",
    "post_rerun_broker_execution_feedback_review": "execution_ops_owner",
    "post_rerun_post_trade_reconciliation": "post_trade_ops_owner",
    "post_rerun_trade_lifecycle_adjudication": "trade_lifecycle_governance_owner",
    "post_rerun_human_release_approval_review": "human_release_approver",
    "live_order_authority": "trading_controls_owner",
    "broker_submission_guard": "broker_controls_owner",
    "broker_submission_response": "broker_adapter_owner",
    "broker_execution_feedback": "execution_ops_owner",
    "post_trade_reconciliation": "post_trade_ops_owner",
    "trade_lifecycle_adjudication": "trade_lifecycle_governance_owner",
}

VALIDATOR_BY_ARTIFACT = {
    "competition_audit": "tools/build_current_strategy_competition_audit.py",
    "evidence_intake_packet": "tools/build_strategy_competition_evidence_intake_packet.py",
    "evidence_submission_review": "tools/review_strategy_competition_evidence_submission.py",
    "formal_validation_handoff": "tools/build_strategy_competition_formal_validation_handoff.py",
    "formal_validation_result_review": "tools/review_strategy_competition_formal_validation_results.py",
    "release_chain_adjudication": "tools/adjudicate_strategy_competition_release_chain.py",
    "human_release_approval": "tools/build_strategy_competition_human_release_approval.py",
    "post_rerun_release_readiness": "tools/review_strategy_competition_post_rerun_release_readiness.py",
    "post_rerun_live_authority_review": "tools/review_strategy_competition_post_rerun_live_authority.py",
    "post_rerun_release_chain_adjudication": "tools/adjudicate_strategy_competition_post_rerun_release_chain.py",
    "post_rerun_broker_guard_review": "tools/review_strategy_competition_post_rerun_broker_guard.py",
    "post_rerun_broker_response_review": "tools/review_strategy_competition_post_rerun_broker_response.py",
    "post_rerun_broker_execution_feedback_review": "tools/review_strategy_competition_post_rerun_broker_execution_feedback.py",
    "post_rerun_post_trade_reconciliation": "tools/reconcile_strategy_competition_post_rerun_post_trade.py",
    "post_rerun_trade_lifecycle_adjudication": "tools/adjudicate_strategy_competition_post_rerun_trade_lifecycle.py",
    "post_rerun_human_release_approval_review": "tools/review_strategy_competition_post_rerun_human_release_approval_review.py",
    "live_order_authority": "tools/check_strategy_competition_live_order_authority.py",
    "broker_submission_guard": "tools/check_strategy_competition_broker_submission_guard.py",
    "broker_submission_response": "tools/review_strategy_competition_broker_submission_response.py",
    "broker_execution_feedback": "tools/review_strategy_competition_broker_execution_feedback.py",
    "post_trade_reconciliation": "tools/reconcile_strategy_competition_post_trade.py",
    "trade_lifecycle_adjudication": "tools/adjudicate_strategy_competition_trade_lifecycle.py",
}


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _load_json(path: str | Path) -> JsonDict:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"json payload must be object: {path}")
    return payload


def _hash_file(path: str | Path) -> str:
    file_path = Path(path)
    if not file_path.exists() or not file_path.is_file():
        return ""
    return sha256(file_path.read_bytes()).hexdigest()


def _evidence_requirements(name: str, reasons: List[str]) -> List[str]:
    joined = " ".join(reasons)
    requirements: List[str] = []
    if "shadow_feedback" in joined or "shadow_execution" in joined:
        requirements.extend([
            "real_shadow_feedback_for_each_order",
            "terminal_shadow_status_or_miss_reason",
            "close_or_reference_price",
            "shadow_attribution",
        ])
    if "independent_validator" in joined:
        requirements.extend([
            "independent_validator_decision",
            "validator_name_and_role",
            "conflict_attestation",
            "reviewed_artifact_hashes",
        ])
    if "operational_control" in joined or "kill_switch" in joined or "rollback" in joined:
        requirements.extend([
            "kill_switch_declaration",
            "rollback_plan_ref",
            "live_monitoring_ref",
            "incident_owner",
            "order_and_position_limits",
        ])
    if name == "broker_execution_feedback":
        requirements.extend(["terminal_execution_reports", "fills_or_miss_reasons", "fees_slippage_and_attribution"])
    if name == "post_trade_reconciliation":
        requirements.extend(["cash_reconciliation", "position_reconciliation", "cost_slippage_reconciliation", "operations_signoff"])
    if name == "human_release_approval":
        requirements.extend(["formal_result_review_accepted", "release_chain_passed", "human_approval_decision"])
    if name == "post_rerun_release_readiness":
        requirements.extend(["rerun_court_rebuild_review_accepted", "release_chain_adjudication", "human_release_approval"])
    if name == "post_rerun_live_authority_review":
        requirements.extend(["post_rerun_release_readiness_ready", "live_order_authority_request"])
    if name == "post_rerun_release_chain_adjudication":
        requirements.extend(["post_rerun_release_readiness_ready", "post_rerun_live_authority_review_ready"])
    if name == "post_rerun_broker_guard_review":
        requirements.extend(["post_rerun_live_authority_review_ready", "broker_submission_guard_decision"])
    if name == "post_rerun_broker_response_review":
        requirements.extend(["post_rerun_broker_guard_review_ready", "broker_submission_response_evidence"])
    if name == "post_rerun_broker_execution_feedback_review":
        requirements.extend(["post_rerun_broker_response_review_ready", "execution_feedback_records"])
    if name == "post_rerun_post_trade_reconciliation":
        requirements.extend(["post_rerun_broker_execution_feedback_review_ready", "cash_position_reconciliation"])
    if name == "post_rerun_trade_lifecycle_adjudication":
        requirements.extend(["post_rerun_post_trade_reconciliation_ready", "trade_lifecycle_closure_review"])
    if name == "post_rerun_human_release_approval_review":
        requirements.extend([
            "post_rerun_evidence_chain_manifest_complete",
            "independent_human_approval_decision",
            "reviewed_artifacts_match_current_manifest",
        ])
    if not requirements:
        requirements.append("repair_blocking_reasons_and_rerun_designated_validator")
    return list(dict.fromkeys(requirements))


def _work_items(manifest: JsonDict) -> List[JsonDict]:
    items: List[JsonDict] = []
    for status in manifest.get("artifact_statuses") or []:
        if not isinstance(status, dict) or status.get("passed") is True:
            continue
        name = _clean_text(status.get("name"))
        reasons = [_clean_text(item) for item in status.get("blocking_reasons") or [] if _clean_text(item)]
        items.append(
            {
                "artifact": name,
                "owner_role": OWNER_BY_ARTIFACT.get(name, "governance_owner"),
                "validator_tool": VALIDATOR_BY_ARTIFACT.get(name, ""),
                "status": _clean_text(status.get("status")),
                "artifact_hash": _clean_text(status.get("artifact_hash")),
                "blocking_reasons": reasons or ["failed_without_reason"],
                "required_evidence": _evidence_requirements(name, reasons),
                "acceptance_rule": f"{name}_artifact_must_pass_and_manifest_hash_must_update_after_rerun",
            }
        )
    return items


def build_strategy_competition_evidence_remediation_work_order(
    conn: sqlite3.Connection,
    *,
    manifest_artifact_path: str | Path,
    output_dir: str | Path,
    operator_name: str = "strategy_competition_evidence_remediation_work_order",
) -> JsonDict:
    """Turn an evidence-chain manifest into actionable remediation work items."""

    apply_professional_migrations(conn)
    manifest = _load_json(manifest_artifact_path)
    manifest_hash = _hash_file(manifest_artifact_path)
    items = _work_items(manifest)
    complete = manifest.get("passed") is True and not items
    payload: JsonDict = {
        "artifact_version": "strategy_competition_evidence_remediation_work_order.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": _clean_text(manifest.get("competition_run_id")),
        "work_order_status": "no_remediation_required" if complete else "remediation_required",
        "passed": complete,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "source_manifest_artifact": str(manifest_artifact_path),
        "source_manifest_hash": manifest_hash,
        "source_manifest_status": _clean_text(manifest.get("manifest_status")),
        "current_blocking_artifact": _clean_text(manifest.get("current_blocking_artifact")),
        "work_items": items,
        "blocked_work_item_count": len(items),
        "allowed_next_actions": _clean_text_list(manifest.get("allowed_next_actions")),
        "work_order_contract": {
            "requires_current_evidence_chain_manifest": True,
            "requires_manifest_hash_match_for_submission": True,
            "work_items_must_be_closed_by_designated_validators": True,
            "does_not_create_production_eligibility": True,
            "does_not_create_live_order_authority": True,
        },
        "hard_boundaries": [
            "remediation_work_order_is_not_validation_pass",
            "work_order_completion_requires_rerun_formal_validators",
            "partial_work_items_cannot_be_packaged_as_production_evidence",
            "work_order_does_not_create_broker_or_execution_authority",
        ],
    }
    payload["work_order_hash"] = sha256(
        canonical_json(
            {
                "competition_run_id": payload["competition_run_id"],
                "source_manifest_hash": payload["source_manifest_hash"],
                "work_order_status": payload["work_order_status"],
                "work_items": payload["work_items"],
            }
        ).encode("utf-8")
    ).hexdigest()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_evidence_remediation_work_order_{payload['competition_run_id'] or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def _clean_text_list(value: Any) -> List[str]:
    return [_clean_text(item) for item in value or [] if _clean_text(item)]
