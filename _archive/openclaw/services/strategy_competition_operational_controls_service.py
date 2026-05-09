from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from typing import Any, Dict, List

from openclaw.services.lineage_service import apply_professional_migrations, canonical_json
from openclaw.services.strategy_competition_production_readiness_service import REQUIRED_OPERATIONAL_CONTROLS


JsonDict = Dict[str, Any]


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _to_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _load_json(path: str | Path) -> JsonDict:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"json payload must be object: {path}")
    return payload


def _hash_payload(payload: JsonDict) -> str:
    return sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def _audit_limits(audit: JsonDict) -> JsonDict:
    constraints = audit.get("portfolio_constraints") if isinstance(audit.get("portfolio_constraints"), dict) else {}
    pretrade = audit.get("pre_trade_risk_controls") if isinstance(audit.get("pre_trade_risk_controls"), dict) else {}
    pretrade_constraints = (
        pretrade.get("portfolio_constraints") if isinstance(pretrade.get("portfolio_constraints"), dict) else {}
    )
    merged = {**constraints, **pretrade_constraints}
    return {
        "max_order_notional": _to_float(merged.get("max_order_value")),
        "max_position_weight": _to_float(merged.get("max_single_name_weight")),
        "max_single_risk_contribution": _to_float(merged.get("max_single_risk_contribution")),
    }


def _validate_input_controls(*, controls_input: JsonDict, audit: JsonDict) -> List[str]:
    failures: List[str] = []
    if not controls_input:
        return ["operational_controls_input_missing"]
    if controls_input.get("artifact_version") not in {
        None,
        "",
        "strategy_competition_production_operational_controls_input.v1",
        "strategy_competition_production_operational_controls.v1",
    }:
        failures.append("operational_controls_input_version_invalid")
    controls = controls_input.get("controls") if isinstance(controls_input.get("controls"), dict) else {}
    for key in REQUIRED_OPERATIONAL_CONTROLS:
        if controls.get(key) is not True:
            failures.append(f"operational_control_not_declared_true:{key}")
    for key in ("rollback_plan_ref", "incident_owner", "monitoring_dashboard_ref"):
        if not _clean_text(controls_input.get(key)):
            failures.append(f"{key}_missing")
    if controls_input.get("human_approval_required") is not True:
        failures.append("human_approval_required_not_declared")

    declared_order = _to_float(controls_input.get("max_order_notional"))
    declared_weight = _to_float(controls_input.get("max_position_weight"))
    declared_risk_contribution = _to_float(controls_input.get("max_single_risk_contribution"))
    if declared_order <= 0:
        failures.append("max_order_notional_missing")
    if declared_weight <= 0:
        failures.append("max_position_weight_missing")
    if declared_risk_contribution <= 0:
        failures.append("max_single_risk_contribution_missing")
    limits = _audit_limits(audit)
    audit_order = _to_float(limits.get("max_order_notional"))
    audit_weight = _to_float(limits.get("max_position_weight"))
    audit_risk_contribution = _to_float(limits.get("max_single_risk_contribution"))
    if audit_order > 0 and declared_order > audit_order:
        failures.append(f"max_order_notional_exceeds_audit_limit:{declared_order}/{audit_order}")
    if audit_weight > 0 and declared_weight > audit_weight:
        failures.append(f"max_position_weight_exceeds_audit_limit:{declared_weight}/{audit_weight}")
    if audit_risk_contribution > 0 and declared_risk_contribution > audit_risk_contribution:
        failures.append(
            "max_single_risk_contribution_exceeds_audit_limit:"
            f"{declared_risk_contribution}/{audit_risk_contribution}"
        )
    return failures


def build_strategy_competition_operational_controls(
    conn: sqlite3.Connection,
    *,
    competition_audit_artifact_path: str,
    output_dir: str | Path,
    controls_input_artifact_path: str = "",
    operator_name: str = "strategy_competition_operational_controls",
) -> JsonDict:
    """Build a canonical production operational controls artifact for readiness gating."""

    apply_professional_migrations(conn)
    audit_path = str(competition_audit_artifact_path or "")
    audit = _load_json(audit_path)
    controls_input_path = str(controls_input_artifact_path or "")
    controls_input = _load_json(controls_input_path) if controls_input_path else {}
    blocking = _validate_input_controls(controls_input=controls_input, audit=audit)
    controls = controls_input.get("controls") if isinstance(controls_input.get("controls"), dict) else {}
    normalized_controls = {key: controls.get(key) is True for key in REQUIRED_OPERATIONAL_CONTROLS}
    passed = not blocking
    payload: JsonDict = {
        "artifact_version": "strategy_competition_production_operational_controls.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": _clean_text(audit.get("competition_run_id")),
        "competition_audit_artifact": audit_path,
        "source_controls_input_artifact": controls_input_path,
        "passed": passed,
        "controls": normalized_controls,
        "blocking_reasons": blocking,
        "rollback_plan_ref": _clean_text(controls_input.get("rollback_plan_ref")),
        "incident_owner": _clean_text(controls_input.get("incident_owner")),
        "monitoring_dashboard_ref": _clean_text(controls_input.get("monitoring_dashboard_ref")),
        "max_order_notional": _to_float(controls_input.get("max_order_notional")),
        "max_position_weight": _to_float(controls_input.get("max_position_weight")),
        "max_single_risk_contribution": _to_float(controls_input.get("max_single_risk_contribution")),
        "audit_limits": _audit_limits(audit),
        "human_approval_required": controls_input.get("human_approval_required") is True,
        "top5_symbols": [
            _clean_text(item.get("ts_code"))
            for item in audit.get("top5_portfolio_audit") or []
            if isinstance(item, dict) and _clean_text(item.get("ts_code"))
        ],
        "hard_boundaries": [
            "operational_controls_do_not_replace_competition_audit",
            "operational_controls_do_not_replace_shadow_or_independent_validation",
            "live_orders_still_require_production_readiness_and_human_approval",
            "runtime_limits_must_not_exceed_predeclared_audit_limits",
        ],
    }
    payload["controls_hash"] = _hash_payload(
        {
            "controls": payload["controls"],
            "rollback_plan_ref": payload["rollback_plan_ref"],
            "incident_owner": payload["incident_owner"],
            "monitoring_dashboard_ref": payload["monitoring_dashboard_ref"],
            "max_order_notional": payload["max_order_notional"],
            "max_position_weight": payload["max_position_weight"],
            "max_single_risk_contribution": payload["max_single_risk_contribution"],
            "human_approval_required": payload["human_approval_required"],
        }
    )
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_operational_controls_{payload['competition_run_id'] or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload
