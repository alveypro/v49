from __future__ import annotations

from datetime import datetime, timezone


PRIMARY_RESULT_EXECUTION_VERSION = "primary_result_execution.v1"
PRIMARY_RESULT_EXECUTION_ALLOWED_STATUSES = {"ready", "blocked", "running", "completed", "failed", "cancelled"}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _isoformat_z(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _check(name: str, passed: bool, severity: str, details: dict[str, object] | None = None) -> dict[str, object]:
    return {
        "check": name,
        "passed": passed,
        "severity": severity,
        "details": details or {},
    }


def build_primary_result_execution(
    primary_result_payload: dict[str, object],
    *,
    now: datetime | None = None,
) -> dict[str, object]:
    generated_at = now or _utc_now()
    result_id = _normalize_text(primary_result_payload.get("result_id"))
    ts_code = _normalize_text(primary_result_payload.get("ts_code"))
    stock_name = _normalize_text(primary_result_payload.get("stock_name"))
    stage = _normalize_text(primary_result_payload.get("result_lifecycle_stage")).upper()
    audit_status = _normalize_text(primary_result_payload.get("audit_status")).lower()
    risk_level = _normalize_text(primary_result_payload.get("risk_level")).lower()
    disabled_reason = _normalize_text(primary_result_payload.get("disabled_reason"))
    invalid_reason = _normalize_text(primary_result_payload.get("invalid_reason"))

    identity_present = bool(result_id and ts_code and stock_name and ts_code not in {"暂无", "-"})
    audit_passed = audit_status == "passed"
    stage_ready = stage in {"L3", "L4", "L5"}
    risk_within_tolerance = risk_level in {"low", "medium"}
    not_disabled = not disabled_reason
    not_invalid = not invalid_reason

    checks = [
        _check(
            "primary_result_identity_present",
            identity_present,
            "blocking",
            {"result_id": result_id or None, "ts_code": ts_code or None, "stock_name": stock_name or None},
        ),
        _check("audit_passed", audit_passed, "blocking", {"audit_status": audit_status or None}),
        _check("audited_lifecycle_stage", stage_ready, "blocking", {"result_lifecycle_stage": stage or None}),
        _check("risk_within_execution_tolerance", risk_within_tolerance, "blocking", {"risk_level": risk_level or None}),
        _check("not_disabled", not_disabled, "blocking", {"disabled_reason": disabled_reason or None}),
        _check("not_invalid", not_invalid, "blocking", {"invalid_reason": invalid_reason or None}),
    ]
    blocking_failed = any(item["severity"] == "blocking" and not item["passed"] for item in checks)
    execution_status = "blocked" if blocking_failed else "ready"

    return {
        "execution_version": PRIMARY_RESULT_EXECUTION_VERSION,
        "generated_at": _isoformat_z(generated_at),
        "execution_status": execution_status,
        "result_id": result_id,
        "ts_code": ts_code,
        "stock_name": stock_name,
        "source_audit_status": audit_status or None,
        "source_result_lifecycle_stage": stage or None,
        "risk_level": risk_level or None,
        "checks": checks,
        "execution_plan": {
            "mode": "local_protocol",
            "external_broker_connected": False,
            "external_analytics_connected": False,
            "required_next_records": [
                "execution_observation",
                "rollback_decision",
                "terminal_outcome",
            ],
        },
        "primary_result_payload": primary_result_payload,
    }


def is_primary_result_execution_applicable(
    execution_payload: dict[str, object],
    *,
    result_id: str,
    ts_code: str,
) -> bool:
    if not execution_payload:
        return False
    execution_result_id = _normalize_text(execution_payload.get("result_id"))
    execution_ts_code = _normalize_text(execution_payload.get("ts_code"))
    return bool(execution_result_id == result_id or (execution_ts_code and execution_ts_code == ts_code))


def extract_primary_result_execution_status(
    execution_payload: dict[str, object],
    *,
    result_id: str,
    ts_code: str,
) -> str | None:
    if not is_primary_result_execution_applicable(execution_payload, result_id=result_id, ts_code=ts_code):
        return None
    execution_status = _normalize_text(execution_payload.get("execution_status")).lower()
    if execution_status in PRIMARY_RESULT_EXECUTION_ALLOWED_STATUSES:
        return execution_status
    return None
