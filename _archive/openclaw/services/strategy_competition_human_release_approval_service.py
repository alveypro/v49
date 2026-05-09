from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from typing import Any, Dict, List

from openclaw.services.lineage_service import apply_professional_migrations, canonical_json, new_release_id
from openclaw.services.release_event_service import record_release_event, record_release_validation


JsonDict = Dict[str, Any]
REQUIRED_APPROVER_ROLE = "release_approver"


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _load_json(path: str | Path) -> JsonDict:
    if not _clean_text(path):
        return {}
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"json payload must be object: {path}")
    return payload


def _hash_file(path: str) -> str:
    text = _clean_text(path)
    if not text:
        return ""
    file_path = Path(text)
    if not file_path.exists() or not file_path.is_file():
        return ""
    return sha256(file_path.read_bytes()).hexdigest()


def _required_reviewed_artifacts(result_review_path: str, adjudication_path: str) -> List[str]:
    return [item for item in (_clean_text(result_review_path), _clean_text(adjudication_path)) if item]


def _approval_decision_failures(
    decision: JsonDict,
    *,
    required_artifacts: List[str],
    operator_name: str,
) -> List[str]:
    failures: List[str] = []
    if not decision:
        return ["human_release_approval_decision_missing"]
    if decision.get("artifact_version") != "strategy_competition_human_release_approval_decision.v1":
        failures.append("human_release_approval_decision_version_invalid")
    if _clean_text(decision.get("decision")).lower() != "approved":
        failures.append("human_release_not_approved")
    approver_name = _clean_text(decision.get("approver_name"))
    if not approver_name:
        failures.append("release_approver_name_missing")
    if approver_name and approver_name == _clean_text(operator_name):
        failures.append("release_approver_self_approval")
    if _clean_text(decision.get("approver_role")).lower() != REQUIRED_APPROVER_ROLE:
        failures.append("release_approver_role_invalid")
    if decision.get("conflict_of_interest_attestation") is not True:
        failures.append("release_approver_conflict_attestation_missing")
    reviewed = [_clean_text(item) for item in decision.get("reviewed_artifacts") or [] if _clean_text(item)]
    if not reviewed:
        failures.append("release_approver_reviewed_artifacts_missing")
    for artifact in required_artifacts:
        if artifact and artifact not in reviewed:
            failures.append(f"release_approver_missing_reviewed_artifact:{artifact}")
    if not _clean_text(decision.get("approval_ticket")):
        failures.append("release_approval_ticket_missing")
    if not _clean_text(decision.get("approval_summary")):
        failures.append("release_approval_summary_missing")
    return failures


def build_strategy_competition_human_release_approval(
    conn: sqlite3.Connection,
    *,
    formal_validation_result_review_artifact_path: str,
    release_chain_adjudication_artifact_path: str,
    output_dir: str | Path,
    human_approval_decision_artifact_path: str = "",
    operator_name: str = "strategy_competition_human_release_approval",
) -> JsonDict:
    """Build the final human approval gate after all automated production evidence passes."""

    apply_professional_migrations(conn)
    result_review_path = str(formal_validation_result_review_artifact_path or "")
    adjudication_path = str(release_chain_adjudication_artifact_path or "")
    decision_path = str(human_approval_decision_artifact_path or "")
    result_review = _load_json(result_review_path)
    adjudication = _load_json(adjudication_path)
    decision = _load_json(decision_path)
    blocking: List[str] = []
    if result_review.get("artifact_version") != "strategy_competition_formal_validation_result_review.v1":
        blocking.append("formal_validation_result_review_version_invalid")
    if result_review.get("result_review_status") != "formal_validation_results_accepted" or result_review.get("passed") is not True:
        blocking.append("formal_validation_result_review_not_accepted")
    if adjudication.get("artifact_version") != "strategy_competition_release_chain_adjudication.v1":
        blocking.append("release_chain_adjudication_version_invalid")
    if (
        adjudication.get("chain_status") != "release_chain_passed_for_human_approval"
        or adjudication.get("passed") is not True
        or adjudication.get("production_release_allowed") is not True
    ):
        blocking.append("release_chain_adjudication_not_passed_for_human_approval")
    required_artifacts = _required_reviewed_artifacts(result_review_path, adjudication_path)
    blocking.extend(
        _approval_decision_failures(
            decision,
            required_artifacts=required_artifacts,
            operator_name=operator_name,
        )
    )
    approved = not blocking
    release_id = new_release_id()
    competition_run_id = _clean_text(result_review.get("competition_run_id")) or _clean_text(adjudication.get("competition_run_id"))
    payload: JsonDict = {
        "artifact_version": "strategy_competition_human_release_approval.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "release_id": release_id,
        "competition_run_id": competition_run_id,
        "approval_status": "human_release_approved" if approved else "human_release_approval_blocked",
        "passed": approved,
        "production_release_authorized": approved,
        "live_order_authority_granted": approved,
        "formal_validation_result_review_artifact": result_review_path,
        "release_chain_adjudication_artifact": adjudication_path,
        "human_approval_decision_artifact": decision_path,
        "source_artifact_hashes": {
            "formal_validation_result_review": _hash_file(result_review_path),
            "release_chain_adjudication": _hash_file(adjudication_path),
            "human_approval_decision": _hash_file(decision_path),
        },
        "required_reviewed_artifacts": required_artifacts,
        "approver_name": _clean_text(decision.get("approver_name")),
        "approver_role": _clean_text(decision.get("approver_role")),
        "approval_ticket": _clean_text(decision.get("approval_ticket")),
        "approval_summary": _clean_text(decision.get("approval_summary")),
        "blocking_reasons": blocking,
        "approval_contract": {
            "requires_accepted_formal_result_review": True,
            "requires_passed_release_chain_adjudication": True,
            "requires_independent_human_release_approver": True,
            "requires_conflict_attestation": True,
            "requires_reviewed_artifacts_match_current_evidence": True,
            "live_order_authority_only_after_this_gate_passes": True,
        },
        "hard_boundaries": [
            "human_release_approval_is_final_pre_live_gate",
            "blocked_human_approval_cannot_release_live_orders",
            "release_approver_cannot_self_approve",
            "approval_does_not_modify_strategy_evidence",
        ],
    }
    payload["approval_hash"] = sha256(
        canonical_json(
            {
                "competition_run_id": payload["competition_run_id"],
                "approval_status": payload["approval_status"],
                "source_artifact_hashes": payload["source_artifact_hashes"],
                "approver_name": payload["approver_name"],
                "approval_ticket": payload["approval_ticket"],
            }
        ).encode("utf-8")
    ).hexdigest()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_human_release_approval_{competition_run_id or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    record_release_event(
        conn,
        release_id=release_id,
        release_type="strategy_competition_human_release_approval",
        code_version=_clean_text(decision.get("code_version")) or canonical_json(payload["source_artifact_hashes"]),
        config_version=_clean_text(decision.get("config_version")),
        operator_name=operator_name,
        gate_result={
            "passed": approved,
            "approval_status": payload["approval_status"],
            "blocking_reasons": blocking,
        },
        payload=payload,
    )
    record_release_validation(
        conn,
        release_id=release_id,
        validation_type="strategy_competition_human_release_approval",
        validation_status="passed" if approved else "blocked",
        validation_output_path=str(path),
    )
    return payload
