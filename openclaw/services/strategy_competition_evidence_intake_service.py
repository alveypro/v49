from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from typing import Any, Dict, List

from openclaw.services.lineage_service import apply_professional_migrations, canonical_json
from openclaw.services.strategy_competition_independent_validation_service import REQUIRED_REVIEW_SCOPE
from openclaw.services.strategy_competition_operational_controls_service import _audit_limits
from openclaw.services.strategy_competition_production_readiness_service import REQUIRED_OPERATIONAL_CONTROLS


JsonDict = Dict[str, Any]


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _load_json(path: str | Path) -> JsonDict:
    if not str(path or "").strip():
        return {}
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"json payload must be object: {path}")
    return payload


def _hash_payload(payload: JsonDict) -> str:
    return sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def _hash_file(path: str) -> str:
    text = _clean_text(path)
    if not text:
        return ""
    file_path = Path(text)
    if not file_path.exists() or not file_path.is_file():
        return ""
    return sha256(file_path.read_bytes()).hexdigest()


def _write_json(path: Path, payload: JsonDict) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return str(path)


def _shadow_orders(*, shadow_plan: JsonDict, audit: JsonDict) -> List[JsonDict]:
    orders = shadow_plan.get("orders") if isinstance(shadow_plan.get("orders"), list) else []
    if orders:
        return [dict(item) for item in orders if isinstance(item, dict)]
    shadow = audit.get("shadow_execution") if isinstance(audit.get("shadow_execution"), dict) else {}
    cases = shadow.get("cases") if isinstance(shadow.get("cases"), list) else []
    return [
        {
            "order_id": _clean_text(item.get("order_id")),
            "ts_code": _clean_text(item.get("ts_code")),
            "target_qty": int(item.get("target_qty") or 0),
            "decision_price": float(item.get("decision_price") or 0.0),
        }
        for item in cases
        if isinstance(item, dict) and _clean_text(item.get("order_id"))
    ]


def _shadow_feedback_template(*, orders: List[JsonDict]) -> JsonDict:
    return {
        "artifact_version": "strategy_competition_shadow_execution_feedback.v1",
        "template_status": "pending_real_shadow_feedback",
        "reports": [
            {
                "order_id": _clean_text(order.get("order_id")),
                "ts_code": _clean_text(order.get("ts_code")),
                "status": "",
                "source_type": "shadow",
                "broker_ref": "",
                "close_price": 0.0,
                "delay_sec": 0,
                "miss_reason_code": "",
                "cancel_reason": "",
                "fills": [
                    {
                        "fill_ref": "",
                        "fill_price": 0.0,
                        "fill_qty": int(order.get("target_qty") or 0),
                        "fill_fee": 0.0,
                        "fill_slippage_bp": 0.0,
                        "venue": "shadow",
                    }
                ],
            }
            for order in orders
        ],
        "required_rule": "filled_or_partial_requires_fills; terminal_miss_requires_miss_reason; close_price_required",
    }


def _required_reviewed_artifacts(*, audit_path: str, audit: JsonDict, shadow_evidence_path: str, operational_controls_path: str) -> List[str]:
    refs = [_clean_text(audit.get("artifact_path")) or audit_path]
    shadow = audit.get("shadow_execution") if isinstance(audit.get("shadow_execution"), dict) else {}
    pretrade = audit.get("pre_trade_risk_controls") if isinstance(audit.get("pre_trade_risk_controls"), dict) else {}
    for value in (
        shadow_evidence_path,
        shadow.get("artifact") or shadow.get("artifact_path") or shadow.get("source_evidence_artifact"),
        pretrade.get("artifact") or pretrade.get("artifact_path"),
        operational_controls_path,
    ):
        text = _clean_text(value)
        if text and text not in refs:
            refs.append(text)
    return refs


def _independent_validator_template(*, reviewed_artifacts: List[str]) -> JsonDict:
    return {
        "artifact_version": "strategy_competition_independent_validator_decision.v1",
        "decision": "",
        "validator_name": "",
        "validator_role": "independent_validator",
        "conflict_of_interest_attestation": False,
        "reviewed_artifacts": reviewed_artifacts,
        "review_scope": sorted(REQUIRED_REVIEW_SCOPE),
        "validation_summary": "",
        "risk_notes": [],
        "required_rule": "external independent validator only; no self approval; reviewed_artifacts must match current evidence",
    }


def _operational_controls_template(*, audit: JsonDict) -> JsonDict:
    limits = _audit_limits(audit)
    return {
        "artifact_version": "strategy_competition_production_operational_controls_input.v1",
        "controls": {key: False for key in REQUIRED_OPERATIONAL_CONTROLS},
        "rollback_plan_ref": "",
        "incident_owner": "",
        "monitoring_dashboard_ref": "",
        "max_order_notional": limits.get("max_order_notional") or 0.0,
        "max_position_weight": limits.get("max_position_weight") or 0.0,
        "max_single_risk_contribution": limits.get("max_single_risk_contribution") or 0.0,
        "audit_limits": limits,
        "human_approval_required": False,
        "required_rule": "runtime limits must not exceed audit_limits; controls must be backed by real runbooks/dashboards/owners",
    }


def _status(name: str, payload: JsonDict) -> JsonDict:
    return {
        "name": name,
        "passed": payload.get("passed") is True,
        "artifact": _clean_text(payload.get("artifact_path")) or _clean_text(payload.get("artifact")) or "",
        "blocking_reasons": list(payload.get("blocking_reasons") or []),
    }


def _render_readme(*, payload: JsonDict) -> str:
    pending = ", ".join(payload.get("pending_evidence") or []) or "none"
    templates = payload.get("template_files") if isinstance(payload.get("template_files"), dict) else {}
    return "\n".join(
        [
            "# Strategy Competition Evidence Intake Packet",
            "",
            "This packet is not approval evidence and cannot be used for production release.",
            "",
            f"- competition_run_id: `{payload.get('competition_run_id')}`",
            f"- packet_status: `{payload.get('packet_status')}`",
            f"- production_candidate_allowed: `{payload.get('production_candidate_allowed')}`",
            f"- pending_evidence: `{pending}`",
            "",
            "## Template Files",
            "",
            f"- shadow feedback: `{templates.get('shadow_feedback', '')}`",
            f"- independent validator decision: `{templates.get('independent_validator_decision', '')}`",
            f"- operational controls input: `{templates.get('operational_controls_input', '')}`",
            "",
            "Fill these templates only with real external evidence, then rerun the corresponding validation tools.",
            "",
        ]
    )


def _expected_shadow_order_ids(packet: JsonDict) -> List[str]:
    template = packet.get("templates", {}).get("shadow_feedback") if isinstance(packet.get("templates"), dict) else {}
    reports = template.get("reports") if isinstance(template, dict) and isinstance(template.get("reports"), list) else []
    return [_clean_text(item.get("order_id")) for item in reports if isinstance(item, dict) and _clean_text(item.get("order_id"))]


def _review_shadow_feedback(packet: JsonDict, feedback: JsonDict) -> List[str]:
    failures: List[str] = []
    if feedback.get("artifact_version") != "strategy_competition_shadow_execution_feedback.v1":
        failures.append("shadow_feedback_version_invalid")
    reports = feedback.get("reports") if isinstance(feedback.get("reports"), list) else []
    expected_ids = _expected_shadow_order_ids(packet)
    actual_ids = [_clean_text(item.get("order_id")) for item in reports if isinstance(item, dict)]
    if sorted(actual_ids) != sorted(expected_ids):
        failures.append("shadow_feedback_order_set_mismatch")
    for report in reports:
        if not isinstance(report, dict):
            failures.append("shadow_feedback_report_invalid")
            continue
        order_id = _clean_text(report.get("order_id"))
        status = _clean_text(report.get("status")).lower()
        if not order_id:
            failures.append("shadow_feedback_order_id_missing")
        if status not in {"filled", "partial_fill", "cancelled", "rejected", "expired", "manual_override"}:
            failures.append(f"shadow_feedback_status_invalid:{order_id}")
        if not _clean_text(report.get("broker_ref")):
            failures.append(f"shadow_feedback_broker_ref_missing:{order_id}")
        if float(report.get("close_price") or 0.0) <= 0:
            failures.append(f"shadow_feedback_close_price_missing:{order_id}")
        fills = report.get("fills") if isinstance(report.get("fills"), list) else []
        if status in {"filled", "partial_fill"} and not fills:
            failures.append(f"shadow_feedback_fills_missing:{order_id}")
        if status in {"cancelled", "rejected", "expired", "manual_override"} and not _clean_text(report.get("miss_reason_code")):
            failures.append(f"shadow_feedback_miss_reason_missing:{order_id}")
        if status == "manual_override" and not _clean_text(report.get("cancel_reason")):
            failures.append(f"shadow_feedback_manual_override_reason_missing:{order_id}")
    return failures


def _review_independent_validator(packet: JsonDict, decision: JsonDict) -> List[str]:
    failures: List[str] = []
    if decision.get("artifact_version") != "strategy_competition_independent_validator_decision.v1":
        failures.append("independent_validator_decision_version_invalid")
    if _clean_text(decision.get("decision")).lower() != "approved":
        failures.append("independent_validator_decision_not_approved")
    if _clean_text(decision.get("validator_role")).lower() != "independent_validator":
        failures.append("independent_validator_role_invalid")
    if not _clean_text(decision.get("validator_name")):
        failures.append("independent_validator_name_missing")
    if decision.get("conflict_of_interest_attestation") is not True:
        failures.append("independent_validator_conflict_attestation_missing")
    expected = set(
        packet.get("templates", {})
        .get("independent_validator_decision", {})
        .get("reviewed_artifacts", [])
        if isinstance(packet.get("templates"), dict)
        else []
    )
    reviewed = {_clean_text(item) for item in decision.get("reviewed_artifacts") or [] if _clean_text(item)}
    missing = sorted(item for item in expected if item not in reviewed)
    for item in missing:
        failures.append(f"independent_validator_missing_reviewed_artifact:{item}")
    scope = {_clean_text(item) for item in decision.get("review_scope") or [] if _clean_text(item)}
    for item in sorted(REQUIRED_REVIEW_SCOPE - scope):
        failures.append(f"independent_validator_review_scope_missing:{item}")
    if not _clean_text(decision.get("validation_summary")):
        failures.append("independent_validator_summary_missing")
    return failures


def _review_operational_controls(packet: JsonDict, controls_input: JsonDict) -> List[str]:
    failures: List[str] = []
    if controls_input.get("artifact_version") != "strategy_competition_production_operational_controls_input.v1":
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
    template = packet.get("templates", {}).get("operational_controls_input") if isinstance(packet.get("templates"), dict) else {}
    limits = template.get("audit_limits") if isinstance(template, dict) and isinstance(template.get("audit_limits"), dict) else {}
    max_order = float(controls_input.get("max_order_notional") or 0.0)
    max_weight = float(controls_input.get("max_position_weight") or 0.0)
    max_risk_contribution = float(controls_input.get("max_single_risk_contribution") or 0.0)
    audit_order = float(limits.get("max_order_notional") or 0.0)
    audit_weight = float(limits.get("max_position_weight") or 0.0)
    audit_risk_contribution = float(limits.get("max_single_risk_contribution") or 0.0)
    if max_order <= 0:
        failures.append("max_order_notional_missing")
    elif audit_order > 0 and max_order > audit_order:
        failures.append(f"max_order_notional_exceeds_packet_limit:{max_order}/{audit_order}")
    if max_weight <= 0:
        failures.append("max_position_weight_missing")
    elif audit_weight > 0 and max_weight > audit_weight:
        failures.append(f"max_position_weight_exceeds_packet_limit:{max_weight}/{audit_weight}")
    if max_risk_contribution <= 0:
        failures.append("max_single_risk_contribution_missing")
    elif audit_risk_contribution > 0 and max_risk_contribution > audit_risk_contribution:
        failures.append(
            "max_single_risk_contribution_exceeds_packet_limit:"
            f"{max_risk_contribution}/{audit_risk_contribution}"
        )
    return failures


def _review_source_hashes(packet: JsonDict) -> List[str]:
    failures: List[str] = []
    sources = packet.get("source_artifacts") if isinstance(packet.get("source_artifacts"), dict) else {}
    expected = packet.get("source_artifact_hashes") if isinstance(packet.get("source_artifact_hashes"), dict) else {}
    for key, path in sources.items():
        expected_hash = _clean_text(expected.get(key))
        if not expected_hash:
            continue
        actual_hash = _hash_file(_clean_text(path))
        if actual_hash != expected_hash:
            failures.append(f"source_artifact_hash_mismatch:{key}")
    return failures


def build_strategy_competition_evidence_submission_review(
    conn: sqlite3.Connection,
    *,
    intake_packet_artifact_path: str,
    shadow_feedback_artifact_path: str,
    independent_validator_decision_artifact_path: str,
    operational_controls_input_artifact_path: str,
    output_dir: str | Path,
    operator_name: str = "strategy_competition_evidence_submission_review",
) -> JsonDict:
    """Review filled evidence-intake templates before they are passed to formal validation tools."""

    apply_professional_migrations(conn)
    packet_path = str(intake_packet_artifact_path or "")
    packet = _load_json(packet_path)
    shadow_feedback = _load_json(shadow_feedback_artifact_path)
    independent_decision = _load_json(independent_validator_decision_artifact_path)
    controls_input = _load_json(operational_controls_input_artifact_path)
    blocking = []
    if packet.get("artifact_version") != "strategy_competition_evidence_intake_packet.v1":
        blocking.append("intake_packet_version_invalid")
    blocking.extend(_review_source_hashes(packet))
    blocking.extend(_review_shadow_feedback(packet, shadow_feedback))
    blocking.extend(_review_independent_validator(packet, independent_decision))
    blocking.extend(_review_operational_controls(packet, controls_input))
    accepted = not blocking
    payload: JsonDict = {
        "artifact_version": "strategy_competition_evidence_submission_review.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": _clean_text(packet.get("competition_run_id")),
        "intake_packet_artifact": packet_path,
        "submitted_artifacts": {
            "shadow_feedback": str(shadow_feedback_artifact_path or ""),
            "independent_validator_decision": str(independent_validator_decision_artifact_path or ""),
            "operational_controls_input": str(operational_controls_input_artifact_path or ""),
        },
        "submitted_artifact_hashes": {
            "shadow_feedback": _hash_file(str(shadow_feedback_artifact_path or "")),
            "independent_validator_decision": _hash_file(str(independent_validator_decision_artifact_path or "")),
            "operational_controls_input": _hash_file(str(operational_controls_input_artifact_path or "")),
        },
        "review_status": "evidence_submission_accepted_for_validation" if accepted else "evidence_submission_blocked",
        "passed": accepted,
        "production_candidate_allowed": False,
        "blocking_reasons": blocking,
        "next_commands": packet.get("next_commands") if isinstance(packet.get("next_commands"), dict) else {},
        "hard_boundaries": [
            "submission_review_is_not_shadow_execution_pass",
            "submission_review_is_not_independent_validation_pass",
            "submission_review_is_not_operational_controls_pass",
            "production_requires_formal_validation_tools_after_submission_review",
        ],
    }
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_evidence_submission_review_{payload['competition_run_id'] or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def build_strategy_competition_evidence_intake_packet(
    conn: sqlite3.Connection,
    *,
    competition_audit_artifact_path: str,
    output_dir: str | Path,
    shadow_plan_artifact_path: str = "",
    shadow_evidence_artifact_path: str = "",
    independent_validation_artifact_path: str = "",
    operational_controls_artifact_path: str = "",
    production_readiness_artifact_path: str = "",
    operator_name: str = "strategy_competition_evidence_intake",
) -> JsonDict:
    """Build actionable templates for the remaining production evidence without approving anything."""

    apply_professional_migrations(conn)
    audit_path = str(competition_audit_artifact_path or "")
    audit = _load_json(audit_path)
    shadow_plan = _load_json(shadow_plan_artifact_path)
    shadow_evidence = _load_json(shadow_evidence_artifact_path)
    independent = _load_json(independent_validation_artifact_path)
    operational = _load_json(operational_controls_artifact_path)
    readiness = _load_json(production_readiness_artifact_path)

    audit_status = _status("competition_audit", audit)
    shadow_status = _status("shadow_execution", shadow_evidence or (audit.get("shadow_execution") or {}))
    independent_status = _status("independent_validation", independent or (audit.get("independent_validation") or {}))
    operational_status = _status("operational_controls", operational)
    readiness_status = _status("production_readiness", readiness)
    orders = _shadow_orders(shadow_plan=shadow_plan, audit=audit)
    reviewed_artifacts = _required_reviewed_artifacts(
        audit_path=audit_path,
        audit=audit,
        shadow_evidence_path=str(shadow_evidence_artifact_path or ""),
        operational_controls_path=str(operational_controls_artifact_path or ""),
    )
    gap_status = [audit_status, shadow_status, independent_status, operational_status, readiness_status]
    pending = [item["name"] for item in gap_status if item["passed"] is not True]
    source_artifacts = {
        "competition_audit": audit_path,
        "shadow_plan": str(shadow_plan_artifact_path or ""),
        "shadow_evidence": str(shadow_evidence_artifact_path or ""),
        "independent_validation": str(independent_validation_artifact_path or ""),
        "operational_controls": str(operational_controls_artifact_path or ""),
        "production_readiness": str(production_readiness_artifact_path or ""),
    }
    templates = {
        "shadow_feedback": _shadow_feedback_template(orders=orders),
        "independent_validator_decision": _independent_validator_template(reviewed_artifacts=reviewed_artifacts),
        "operational_controls_input": _operational_controls_template(audit=audit),
    }
    payload: JsonDict = {
        "artifact_version": "strategy_competition_evidence_intake_packet.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": _clean_text(audit.get("competition_run_id")),
        "packet_status": "evidence_intake_complete" if not pending else "evidence_intake_pending",
        "passed": False,
        "production_candidate_allowed": False,
        "source_artifacts": source_artifacts,
        "source_artifact_hashes": {key: _hash_file(value) for key, value in source_artifacts.items()},
        "gap_status": gap_status,
        "pending_evidence": pending,
        "templates": templates,
        "next_commands": {
            "record_shadow_feedback": "python3 tools/record_strategy_competition_shadow_feedback.py --shadow-plan-artifact <shadow_plan> --shadow-feedback-artifact <filled_shadow_feedback.json>",
            "build_independent_validation": "python3 tools/build_strategy_competition_independent_validation.py --competition-audit-artifact <audit> --validator-decision-artifact <validator_report.json>",
            "build_operational_controls": "python3 tools/build_strategy_competition_operational_controls.py --competition-audit-artifact <audit> --controls-input-artifact <controls_input.json>",
            "rerun_competition_audit": "python3 tools/build_current_strategy_competition_audit.py --derive-pre-trade-risk-controls --shadow-execution <shadow_evidence> --independent-validator <independent_validation>",
            "build_production_readiness": "python3 tools/build_strategy_competition_production_readiness.py --competition-audit-artifact <passed_audit> --operational-controls-artifact <operational_controls>",
        },
        "hard_boundaries": [
            "intake_packet_is_not_evidence_approval",
            "templates_require_real_external_inputs_before_use",
            "failed_or_blocked_sections_must_not_be_marketed_as_production_ready",
            "production_requires_passed_audit_shadow_independent_controls_and_readiness",
        ],
    }
    payload["packet_hash"] = _hash_payload(
        {
            "source_artifacts": payload["source_artifacts"],
            "gap_status": payload["gap_status"],
            "templates": payload["templates"],
        }
    )
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    template_dir = output / f"strategy_competition_evidence_intake_templates_{payload['competition_run_id'] or 'unknown'}_{stamp}"
    template_files = {
        "shadow_feedback": _write_json(template_dir / "shadow_feedback_template.json", templates["shadow_feedback"]),
        "independent_validator_decision": _write_json(
            template_dir / "independent_validator_decision_template.json",
            templates["independent_validator_decision"],
        ),
        "operational_controls_input": _write_json(
            template_dir / "operational_controls_input_template.json",
            templates["operational_controls_input"],
        ),
    }
    payload["template_dir"] = str(template_dir)
    payload["template_files"] = template_files
    payload["template_file_hashes"] = {key: _hash_file(value) for key, value in template_files.items()}
    readme_path = template_dir / "README.md"
    readme_path.write_text(_render_readme(payload=payload), encoding="utf-8")
    payload["template_readme"] = str(readme_path)
    payload["template_readme_hash"] = _hash_file(str(readme_path))
    path = output / f"strategy_competition_evidence_intake_packet_{payload['competition_run_id'] or 'unknown'}_{stamp}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload
