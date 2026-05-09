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

GATE_ORDER = (
    "competition_audit",
    "shadow_execution",
    "independent_validation",
    "operational_controls",
    "evidence_submission_review",
    "production_readiness",
)


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


def _as_reasons(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    return [_clean_text(item) for item in value if _clean_text(item)]


def _status_from_payload(name: str, payload: JsonDict, path: str) -> JsonDict:
    status_key = {
        "competition_audit": "result_status",
        "shadow_execution": "execution_status",
        "independent_validation": "validation_status",
        "operational_controls": "controls_status",
        "evidence_submission_review": "review_status",
        "production_readiness": "readiness_status",
    }.get(name, "status")
    artifact = _clean_text(payload.get("artifact_path")) or _clean_text(payload.get("artifact")) or _clean_text(path)
    passed = payload.get("passed") is True
    reasons = _as_reasons(payload.get("blocking_reasons"))
    if not payload:
        reasons = [f"{name}_artifact_missing"]
    if name == "production_readiness" and payload.get("production_release_allowed") is True and passed is not True:
        reasons.append("production_readiness_release_flag_inconsistent")
    return {
        "name": name,
        "artifact_version": _clean_text(payload.get("artifact_version")),
        "artifact": artifact,
        "artifact_hash": _hash_file(path),
        "status": _clean_text(payload.get(status_key)) or ("passed" if passed else "blocked"),
        "passed": passed,
        "blocking_reasons": reasons,
    }


def _root_blockers(gates: List[JsonDict]) -> List[str]:
    blockers: List[str] = []
    for gate in gates:
        if gate.get("passed") is True:
            continue
        name = _clean_text(gate.get("name"))
        reasons = _as_reasons(gate.get("blocking_reasons"))
        if not reasons:
            blockers.append(f"{name}:failed_without_reason")
            continue
        blockers.extend(f"{name}:{reason}" for reason in reasons)
    return blockers


def _current_blocking_gate(gates: List[JsonDict]) -> str:
    for gate_name in GATE_ORDER:
        for gate in gates:
            if gate.get("name") == gate_name and gate.get("passed") is not True:
                return gate_name
    return ""


def _allowed_next_actions(gates: List[JsonDict]) -> List[str]:
    failed = {gate["name"] for gate in gates if gate.get("passed") is not True}
    actions: List[str] = []
    if "shadow_execution" in failed:
        actions.append("submit_real_shadow_feedback_and_run_shadow_evidence_validator")
    if "independent_validation" in failed:
        actions.append("obtain_external_independent_validator_decision_and_run_validator")
    if "operational_controls" in failed:
        actions.append("submit_real_operational_controls_input_and_run_controls_validator")
    if "evidence_submission_review" in failed:
        actions.append("rerun_evidence_submission_review_with_same_source_hashes")
    if failed.intersection({"shadow_execution", "independent_validation", "operational_controls"}):
        actions.append("rerun_competition_audit_only_after_upstream_evidence_passes")
    if failed == {"production_readiness"}:
        actions.append("rerun_production_readiness_after_passed_audit_and_controls")
    if not actions:
        actions.append("hold_for_human_release_approval_boundary")
    return actions


def build_strategy_competition_release_chain_adjudication(
    conn: sqlite3.Connection,
    *,
    competition_audit_artifact_path: str,
    output_dir: str | Path,
    shadow_execution_artifact_path: str = "",
    independent_validation_artifact_path: str = "",
    operational_controls_artifact_path: str = "",
    evidence_submission_review_artifact_path: str = "",
    production_readiness_artifact_path: str = "",
    operator_name: str = "strategy_competition_release_chain_adjudication",
) -> JsonDict:
    """Build a court-of-record verdict for the full Top5 production evidence chain."""

    apply_professional_migrations(conn)
    paths = {
        "competition_audit": str(competition_audit_artifact_path or ""),
        "shadow_execution": str(shadow_execution_artifact_path or ""),
        "independent_validation": str(independent_validation_artifact_path or ""),
        "operational_controls": str(operational_controls_artifact_path or ""),
        "evidence_submission_review": str(evidence_submission_review_artifact_path or ""),
        "production_readiness": str(production_readiness_artifact_path or ""),
    }
    payloads = {key: _load_json(value) for key, value in paths.items()}
    audit = payloads["competition_audit"]
    gates = [_status_from_payload(name, payloads[name], paths[name]) for name in GATE_ORDER]
    current_gate = _current_blocking_gate(gates)
    readiness = payloads["production_readiness"]
    chain_passed = (
        not current_gate
        and readiness.get("readiness_status") == "production_readiness_passed"
        and readiness.get("production_release_allowed") is True
    )
    release_id = new_release_id()
    competition_run_id = _clean_text(audit.get("competition_run_id"))
    blockers = _root_blockers(gates)
    source_hashes = {key: _hash_file(value) for key, value in paths.items()}
    verdict: JsonDict = {
        "artifact_version": "strategy_competition_release_chain_adjudication.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "release_id": release_id,
        "competition_run_id": competition_run_id,
        "chain_status": "release_chain_passed_for_human_approval" if chain_passed else "release_chain_blocked",
        "passed": chain_passed,
        "production_candidate_allowed": chain_passed,
        "production_release_allowed": chain_passed,
        "current_blocking_gate": current_gate,
        "gate_order": list(GATE_ORDER),
        "gate_status": gates,
        "root_blockers": blockers,
        "allowed_next_actions": _allowed_next_actions(gates),
        "source_artifacts": paths,
        "source_artifact_hashes": source_hashes,
        "top5_symbols": [
            _clean_text(item.get("ts_code"))
            for item in audit.get("top5_portfolio_audit") or []
            if isinstance(item, dict) and _clean_text(item.get("ts_code"))
        ],
        "release_contract": {
            "requires_fixed_candidate_pool": True,
            "requires_model_cards_and_hashes": True,
            "requires_shadow_execution": True,
            "requires_independent_validation": True,
            "requires_pre_trade_controls": True,
            "requires_operational_controls": True,
            "requires_production_readiness": True,
            "requires_human_approval_before_live_orders": True,
        },
        "hard_boundaries": [
            "release_chain_adjudication_is_not_trade_instruction",
            "blocked_gate_cannot_be_skipped",
            "templates_or_submission_review_are_not_production_evidence",
            "production_requires_passed_readiness_and_human_approval",
            "failed_or_research_only_candidate_cannot_be_packaged_into_top5",
        ],
    }
    verdict["adjudication_hash"] = sha256(
        canonical_json(
            {
                "competition_run_id": verdict["competition_run_id"],
                "chain_status": verdict["chain_status"],
                "gate_status": verdict["gate_status"],
                "source_artifact_hashes": verdict["source_artifact_hashes"],
            }
        ).encode("utf-8")
    ).hexdigest()

    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_release_chain_adjudication_{competition_run_id or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    verdict["artifact_path"] = str(path)
    path.write_text(json.dumps(verdict, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    record_release_event(
        conn,
        release_id=release_id,
        release_type="strategy_competition_release_chain_adjudication",
        code_version=_clean_text((audit.get("code_hashes") or {}).get("v5")) or canonical_json(audit.get("code_hashes") or {}),
        config_version=_clean_text(audit.get("ranking_method_hash")),
        operator_name=operator_name,
        gate_result={
            "passed": chain_passed,
            "chain_status": verdict["chain_status"],
            "current_blocking_gate": current_gate,
            "root_blockers": blockers,
        },
        payload=verdict,
    )
    record_release_validation(
        conn,
        release_id=release_id,
        validation_type="strategy_competition_release_chain_adjudication",
        validation_status="passed" if chain_passed else "blocked",
        validation_output_path=str(path),
    )
    return verdict
