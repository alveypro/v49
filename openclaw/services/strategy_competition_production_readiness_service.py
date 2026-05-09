from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import sqlite3
from typing import Any, Dict, List

from openclaw.services.lineage_service import apply_professional_migrations, canonical_json, new_release_id
from openclaw.services.release_event_service import record_release_event, record_release_validation


JsonDict = Dict[str, Any]
REQUIRED_OPERATIONAL_CONTROLS = (
    "kill_switch_enabled",
    "rollback_plan_ready",
    "live_monitoring_ready",
    "incident_owner_assigned",
    "max_order_notional_configured",
    "max_position_weight_configured",
    "max_single_risk_contribution_configured",
)


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _load_json(path: str | Path) -> JsonDict:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"json payload must be object: {path}")
    return payload


def build_blocked_operational_controls_stub(*, reason: str = "production_operational_controls_not_available") -> JsonDict:
    return {
        "artifact_version": "strategy_competition_production_operational_controls.v1",
        "passed": False,
        "controls": {},
        "blocking_reasons": [str(reason or "production_operational_controls_not_available")],
    }


def _validate_competition_audit(audit: JsonDict) -> List[str]:
    failures: List[str] = []
    if audit.get("artifact_version") != "strategy_competition_portfolio_audit.v1":
        failures.append("invalid_competition_audit_artifact")
    if audit.get("passed") is not True or audit.get("production_candidate_allowed") is not True:
        failures.append("competition_audit_not_passed_for_production")
    if audit.get("result_status") != "industry_benchmark_competition_passed":
        failures.append("competition_audit_status_not_passed")
    for section in ("independent_validation", "shadow_execution", "pre_trade_risk_controls"):
        payload = audit.get(section) if isinstance(audit.get(section), dict) else {}
        if payload.get("passed") is not True:
            failures.append(f"{section}_not_passed")
    top5 = [item for item in audit.get("top5_portfolio_audit") or [] if isinstance(item, dict)]
    if len(top5) != 5:
        failures.append(f"top5_count_invalid:{len(top5)}")
    return failures


def _validate_operational_controls(payload: JsonDict) -> List[str]:
    failures: List[str] = []
    if not payload:
        return ["production_operational_controls_missing"]
    if payload.get("artifact_version") not in {
        None,
        "",
        "strategy_competition_production_operational_controls.v1",
    }:
        failures.append("production_operational_controls_version_invalid")
    controls = payload.get("controls") if isinstance(payload.get("controls"), dict) else {}
    for key in REQUIRED_OPERATIONAL_CONTROLS:
        if controls.get(key) is not True:
            failures.append(f"production_operational_control_missing_or_failed:{key}")
    if not _clean_text(payload.get("rollback_plan_ref")):
        failures.append("rollback_plan_ref_missing")
    if not _clean_text(payload.get("incident_owner")):
        failures.append("incident_owner_missing")
    if not _clean_text(payload.get("monitoring_dashboard_ref")):
        failures.append("monitoring_dashboard_ref_missing")
    if float(payload.get("max_order_notional") or 0.0) <= 0:
        failures.append("max_order_notional_missing")
    if float(payload.get("max_position_weight") or 0.0) <= 0:
        failures.append("max_position_weight_missing")
    if payload.get("human_approval_required") is not True:
        failures.append("human_approval_required_not_declared")
    return failures


def build_strategy_competition_production_readiness(
    conn: sqlite3.Connection,
    *,
    competition_audit_artifact_path: str,
    output_dir: str | Path,
    operational_controls_artifact_path: str = "",
    operator_name: str = "strategy_competition_production_readiness",
) -> JsonDict:
    """Build a final production readiness gate for a passed strategy competition artifact."""

    apply_professional_migrations(conn)
    audit_path = str(competition_audit_artifact_path or "")
    audit = _load_json(audit_path)
    controls_path = str(operational_controls_artifact_path or "")
    controls = _load_json(controls_path) if controls_path else build_blocked_operational_controls_stub()

    audit_failures = _validate_competition_audit(audit)
    controls_failures = _validate_operational_controls(controls)
    blocking = audit_failures + controls_failures
    ready = not blocking
    release_id = new_release_id()
    competition_run_id = _clean_text(audit.get("competition_run_id"))
    payload: JsonDict = {
        "artifact_version": "strategy_competition_production_readiness.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "release_id": release_id,
        "competition_run_id": competition_run_id,
        "competition_audit_artifact": audit_path,
        "operational_controls_artifact": controls_path,
        "readiness_status": "production_readiness_passed" if ready else "production_readiness_blocked",
        "passed": ready,
        "production_release_allowed": ready,
        "blocking_reasons": blocking,
        "competition_audit_checks": {
            "passed": not audit_failures,
            "blocking_reasons": audit_failures,
        },
        "operational_controls": {
            **controls,
            "passed": not controls_failures,
            "blocking_reasons": controls_failures,
        },
        "release_contract": {
            "requires_passed_competition_audit": True,
            "requires_independent_validation": True,
            "requires_shadow_execution": True,
            "requires_pre_trade_controls": True,
            "requires_kill_switch_and_rollback": True,
            "requires_human_approval_before_live_orders": True,
        },
        "top5_symbols": [
            _clean_text(item.get("ts_code"))
            for item in audit.get("top5_portfolio_audit") or []
            if isinstance(item, dict) and _clean_text(item.get("ts_code"))
        ],
        "hard_boundaries": [
            "production_readiness_does_not_create_shadow_or_independent_evidence",
            "blocked_readiness_cannot_release_live_orders",
            "human_approval_required_even_after_readiness_passed",
        ],
    }
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_production_readiness_{competition_run_id or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    record_release_event(
        conn,
        release_id=release_id,
        release_type="strategy_competition_top5_production_readiness",
        code_version=_clean_text((audit.get("code_hashes") or {}).get("v5")) or canonical_json(audit.get("code_hashes") or {}),
        config_version=_clean_text(audit.get("ranking_method_hash")),
        operator_name=operator_name,
        gate_result={
            "passed": ready,
            "readiness_status": payload["readiness_status"],
            "blocking_reasons": blocking,
        },
        payload=payload,
    )
    record_release_validation(
        conn,
        release_id=release_id,
        validation_type="strategy_competition_production_readiness",
        validation_status="passed" if ready else "blocked",
        validation_output_path=str(path),
    )
    return payload
