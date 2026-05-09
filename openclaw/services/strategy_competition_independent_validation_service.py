from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import sqlite3
from typing import Any, Dict, List

from openclaw.services.lineage_service import apply_professional_migrations, canonical_json


JsonDict = Dict[str, Any]
REQUIRED_REVIEW_SCOPE = {
    "fixed_candidate_pool",
    "model_cards",
    "top5_portfolio",
    "shadow_execution",
    "pre_trade_controls",
    "promotion_boundaries",
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


def _artifact_ref(path: str, payload: JsonDict) -> str:
    return _clean_text(payload.get("artifact_path")) or str(path or "")


def _required_artifacts(audit_path: str, audit: JsonDict) -> List[str]:
    refs = [_artifact_ref(audit_path, audit)]
    shadow = audit.get("shadow_execution") if isinstance(audit.get("shadow_execution"), dict) else {}
    pretrade = audit.get("pre_trade_risk_controls") if isinstance(audit.get("pre_trade_risk_controls"), dict) else {}
    for value in (
        shadow.get("artifact") or shadow.get("artifact_path") or shadow.get("source_evidence_artifact"),
        pretrade.get("artifact") or pretrade.get("artifact_path"),
    ):
        text = _clean_text(value)
        if text and text not in refs:
            refs.append(text)
    return refs


def _base_audit_failures(audit: JsonDict) -> List[str]:
    failures: List[str] = []
    if audit.get("artifact_version") != "strategy_competition_portfolio_audit.v1":
        failures.append("invalid_competition_audit_artifact")
    if audit.get("production_candidate_allowed") is True or audit.get("formal_top_allowed") is True:
        failures.append("source_audit_already_claimed_promotion")
    if not audit.get("fixed_candidate_pool"):
        failures.append("fixed_candidate_pool_missing")
    top5 = [item for item in audit.get("top5_portfolio_audit") or [] if isinstance(item, dict)]
    if len(top5) != 5:
        failures.append(f"top5_count_invalid:{len(top5)}")
    cards = audit.get("alpha_model_cards") if isinstance(audit.get("alpha_model_cards"), dict) else {}
    for strategy in [str(item or "").lower() for item in audit.get("fixed_candidate_pool") or []]:
        card = cards.get(strategy) if isinstance(cards.get(strategy), dict) else {}
        for field in ("model_card", "hypothesis", "rule_hash", "data_hash", "code_hash"):
            if not card or not _clean_text(card.get(field)):
                failures.append(f"model_card_field_missing:{strategy}:{field}")
        if _clean_text(card.get("status")).lower() in {"failed", "research_only", "archived"}:
            failures.append(f"failed_or_research_only_candidate_in_pool:{strategy}")
    shadow = audit.get("shadow_execution") if isinstance(audit.get("shadow_execution"), dict) else {}
    if shadow.get("passed") is not True:
        failures.append("shadow_execution_not_passed")
    pretrade = audit.get("pre_trade_risk_controls") if isinstance(audit.get("pre_trade_risk_controls"), dict) else {}
    if pretrade.get("passed") is not True:
        failures.append("pre_trade_risk_controls_not_passed")
    boundaries = json.dumps(audit.get("hard_boundaries") or [], ensure_ascii=False)
    for required in ("failed_research_only_candidate_banned", "independent_validator_required", "production_requires"):
        if required not in boundaries:
            failures.append(f"promotion_boundary_missing:{required}")
    return failures


def _decision_failures(report: JsonDict, *, required_artifacts: List[str], operator_name: str) -> List[str]:
    failures: List[str] = []
    if not report:
        return ["independent_validator_decision_missing"]
    if _clean_text(report.get("decision")).lower() != "approved":
        failures.append("independent_validator_not_approved")
    if _clean_text(report.get("validator_role")).lower() != "independent_validator":
        failures.append("independent_validator_role_invalid")
    validator_name = _clean_text(report.get("validator_name"))
    if not validator_name:
        failures.append("independent_validator_name_missing")
    if validator_name and validator_name == _clean_text(operator_name):
        failures.append("independent_validator_self_approval")
    if report.get("conflict_of_interest_attestation") is not True:
        failures.append("independent_validator_conflict_attestation_missing")
    reviewed = [_clean_text(item) for item in report.get("reviewed_artifacts") or [] if _clean_text(item)]
    if not reviewed:
        failures.append("independent_validator_reviewed_artifacts_missing")
    for artifact in required_artifacts:
        if artifact and artifact not in reviewed:
            failures.append(f"independent_validator_missing_reviewed_artifact:{artifact}")
    scope = {_clean_text(item) for item in report.get("review_scope") or [] if _clean_text(item)}
    missing_scope = sorted(REQUIRED_REVIEW_SCOPE - scope)
    for item in missing_scope:
        failures.append(f"independent_validator_review_scope_missing:{item}")
    if not _clean_text(report.get("validation_summary")):
        failures.append("independent_validator_summary_missing")
    return failures


def build_strategy_competition_independent_validation(
    conn: sqlite3.Connection,
    *,
    competition_audit_artifact_path: str,
    output_dir: str | Path,
    validator_decision_artifact_path: str = "",
    operator_name: str = "strategy_competition_independent_validation",
) -> JsonDict:
    """Validate an external independent validator decision against the competition audit facts."""

    apply_professional_migrations(conn)
    audit_path = str(competition_audit_artifact_path or "")
    audit = _load_json(audit_path)
    decision_path = str(validator_decision_artifact_path or "")
    decision_report = _load_json(decision_path) if decision_path else {}
    if decision_report and decision_report.get("artifact_version") not in {
        None,
        "",
        "strategy_competition_independent_validator_decision.v1",
        "strategy_competition_independent_validation.v1",
    }:
        raise ValueError("invalid_independent_validator_decision_artifact")

    required_artifacts = _required_artifacts(audit_path, audit)
    base_failures = _base_audit_failures(audit)
    validator_failures = _decision_failures(
        decision_report,
        required_artifacts=required_artifacts,
        operator_name=operator_name,
    )
    blocking = base_failures + validator_failures
    passed = not blocking
    competition_run_id = _clean_text(audit.get("competition_run_id"))
    payload: JsonDict = {
        "artifact_version": "strategy_competition_independent_validation.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": competition_run_id,
        "competition_audit_artifact": audit_path,
        "validator_decision_artifact": decision_path,
        "required_reviewed_artifacts": required_artifacts,
        "required_review_scope": sorted(REQUIRED_REVIEW_SCOPE),
        "decision": "approved" if passed else "blocked",
        "passed": passed,
        "validator_name": _clean_text(decision_report.get("validator_name")),
        "validator_role": _clean_text(decision_report.get("validator_role")) or "independent_validator",
        "conflict_of_interest_attestation": decision_report.get("conflict_of_interest_attestation") is True,
        "reviewed_artifacts": list(decision_report.get("reviewed_artifacts") or []),
        "review_scope": list(decision_report.get("review_scope") or []),
        "validation_summary": _clean_text(decision_report.get("validation_summary")),
        "risk_notes": list(decision_report.get("risk_notes") or []),
        "blocking_reasons": blocking,
        "base_audit_blocking_reasons": base_failures,
        "validator_decision_blocking_reasons": validator_failures,
        "production_candidate_allowed": False,
        "hard_boundaries": [
            "independent_validation_cannot_self_approve",
            "independent_validation_requires_shadow_execution_passed",
            "independent_validation_requires_pre_trade_controls_passed",
            "independent_validation_does_not_bypass_competition_audit",
        ],
    }

    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_independent_validation_{competition_run_id or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _persist_independent_validation(conn, payload)
    return payload


def _persist_independent_validation(conn: sqlite3.Connection, payload: JsonDict) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO independent_validation_decisions (
            validation_id, competition_run_id, validator_name, validator_role, decision,
            conflict_of_interest_attested, reviewed_artifacts_json, blocking_reasons_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            f"validation_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}",
            _clean_text(payload.get("competition_run_id")),
            _clean_text(payload.get("validator_name")),
            _clean_text(payload.get("validator_role")),
            _clean_text(payload.get("decision")) or "blocked",
            1 if payload.get("conflict_of_interest_attestation") is True else 0,
            canonical_json(payload.get("reviewed_artifacts") or []),
            canonical_json(payload.get("blocking_reasons") or []),
            _now_text(),
        ),
    )
    conn.commit()
