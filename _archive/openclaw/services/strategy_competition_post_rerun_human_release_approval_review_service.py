from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from typing import Any, Dict, List

from openclaw.services.lineage_service import apply_professional_migrations, canonical_json


JsonDict = Dict[str, Any]
REQUIRED_APPROVER_ROLE = "release_approver"


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _load_json(path: str | Path) -> JsonDict:
    if not _clean_text(path):
        return {}
    file_path = Path(path)
    if not file_path.exists() or not file_path.is_file():
        return {}
    payload = json.loads(file_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"json payload must be object: {path}")
    return payload


def _hash_file(path: str | Path) -> str:
    text = _clean_text(path)
    if not text:
        return ""
    file_path = Path(text)
    if not file_path.exists() or not file_path.is_file():
        return ""
    return sha256(file_path.read_bytes()).hexdigest()


def _required_reviewed_artifacts(manifest_path: str, manifest_hash: str) -> List[str]:
    reviewed = [item for item in (_clean_text(manifest_path), _clean_text(manifest_hash)) if item]
    return reviewed[:1]


def _decision_failures(
    decision: JsonDict,
    *,
    required_artifacts: List[str],
    operator_name: str,
) -> List[str]:
    failures: List[str] = []
    if not decision:
        return ["post_rerun_human_release_approval_decision_missing"]
    if decision.get("artifact_version") != "strategy_competition_human_release_approval_decision.v1":
        failures.append("post_rerun_human_release_approval_decision_version_invalid")
    if _clean_text(decision.get("decision")).lower() != "approved":
        failures.append("post_rerun_human_release_not_approved")
    approver_name = _clean_text(decision.get("approver_name"))
    if not approver_name:
        failures.append("post_rerun_release_approver_name_missing")
    if approver_name and approver_name == _clean_text(operator_name):
        failures.append("post_rerun_release_approver_self_approval")
    if _clean_text(decision.get("approver_role")).lower() != REQUIRED_APPROVER_ROLE:
        failures.append("post_rerun_release_approver_role_invalid")
    if decision.get("conflict_of_interest_attestation") is not True:
        failures.append("post_rerun_release_approver_conflict_attestation_missing")
    reviewed = [_clean_text(item) for item in decision.get("reviewed_artifacts") or [] if _clean_text(item)]
    if not reviewed:
        failures.append("post_rerun_release_approver_reviewed_artifacts_missing")
    for artifact in required_artifacts:
        if artifact and artifact not in reviewed:
            failures.append(f"post_rerun_release_approver_missing_reviewed_artifact:{artifact}")
    if not _clean_text(decision.get("approval_ticket")):
        failures.append("post_rerun_release_approval_ticket_missing")
    if not _clean_text(decision.get("approval_summary")):
        failures.append("post_rerun_release_approval_summary_missing")
    return failures


def build_strategy_competition_post_rerun_human_release_approval_review(
    conn: sqlite3.Connection,
    *,
    post_rerun_evidence_chain_manifest_artifact_path: str | Path,
    human_approval_decision_artifact_path: str | Path = "",
    output_dir: str | Path,
    operator_name: str = "strategy_competition_post_rerun_human_release_approval_review",
) -> JsonDict:
    """Review the final human approval for a post-rerun evidence chain without granting live authority."""

    apply_professional_migrations(conn)
    manifest_path = str(post_rerun_evidence_chain_manifest_artifact_path or "")
    decision_path = str(human_approval_decision_artifact_path or "")
    manifest = _load_json(manifest_path)
    decision = _load_json(decision_path)
    manifest_hash = _hash_file(manifest_path)
    decision_hash = _hash_file(decision_path)
    blocking_reasons: List[str] = []

    if manifest.get("artifact_version") != "strategy_competition_post_rerun_evidence_chain_manifest.v1":
        blocking_reasons.append("post_rerun_evidence_chain_manifest_version_invalid")
    if manifest.get("manifest_status") != "post_rerun_evidence_chain_complete" or manifest.get("passed") is not True:
        blocking_reasons.append("post_rerun_evidence_chain_manifest_not_complete")
    if manifest.get("production_candidate_allowed") is True or manifest.get("production_release_authorized") is True:
        blocking_reasons.append("post_rerun_evidence_chain_manifest_attempted_production_permission")
    if not decision:
        blocking_reasons.append("post_rerun_human_release_approval_decision_missing")
    else:
        required_artifacts = _required_reviewed_artifacts(manifest_path, manifest_hash)
        blocking_reasons.extend(
            _decision_failures(
                decision,
                required_artifacts=required_artifacts,
                operator_name=operator_name,
            )
        )

    approved = not blocking_reasons
    competition_run_id = _clean_text(manifest.get("competition_run_id"))
    payload: JsonDict = {
        "artifact_version": "strategy_competition_post_rerun_human_release_approval_review.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": competition_run_id,
        "human_release_approval_review_status": "post_rerun_human_release_approved" if approved else "post_rerun_human_release_approval_blocked",
        "passed": approved,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "production_candidate_allowed": False,
        "post_rerun_evidence_chain_manifest_artifact": manifest_path,
        "post_rerun_evidence_chain_manifest_hash": manifest_hash,
        "human_approval_decision_artifact": decision_path,
        "human_approval_decision_hash": decision_hash,
        "blocking_reasons": blocking_reasons,
        "allowed_next_actions": (
            ["run_live_order_authority_check_with_matching_post_rerun_human_release_hash"]
            if approved
            else ["complete_post_rerun_evidence_chain_manifest_and_human_approval_review"]
        ),
        "human_release_approval_review_contract": {
            "requires_post_rerun_evidence_chain_manifest_complete": True,
            "requires_independent_human_release_approver": True,
            "requires_conflict_attestation": True,
            "requires_reviewed_artifacts_match_current_manifest": True,
            "does_not_create_live_order_authority": True,
        },
        "hard_boundaries": [
            "post_rerun_human_release_approval_review_is_not_live_order_authority",
            "approval_review_does_not_execute_orders",
            "approval_review_does_not_authorize_production",
            "approval_review_is_inventory_not_permission",
        ],
    }
    payload["human_release_approval_review_hash"] = sha256(
        canonical_json(
            {
                "competition_run_id": payload["competition_run_id"],
                "post_rerun_evidence_chain_manifest_hash": payload["post_rerun_evidence_chain_manifest_hash"],
                "human_approval_decision_hash": payload["human_approval_decision_hash"],
                "human_release_approval_review_status": payload["human_release_approval_review_status"],
                "blocking_reasons": payload["blocking_reasons"],
            }
        ).encode("utf-8")
    ).hexdigest()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_post_rerun_human_release_approval_review_{payload['competition_run_id'] or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload
