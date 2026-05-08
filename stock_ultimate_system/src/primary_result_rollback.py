from __future__ import annotations

from datetime import datetime, timezone


PRIMARY_RESULT_ROLLBACK_VERSION = "primary_result_rollback.v1"
PRIMARY_RESULT_ROLLBACK_ALLOWED_STATUSES = {"not_required", "pending", "triggered", "completed", "failed"}


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


def build_primary_result_rollback(
    primary_result_payload: dict[str, object],
    *,
    now: datetime | None = None,
) -> dict[str, object]:
    generated_at = now or _utc_now()
    result_id = _normalize_text(primary_result_payload.get("result_id"))
    ts_code = _normalize_text(primary_result_payload.get("ts_code"))
    stock_name = _normalize_text(primary_result_payload.get("stock_name"))
    execution_status = _normalize_text(primary_result_payload.get("execution_status")).lower()
    risk_level = _normalize_text(primary_result_payload.get("risk_level")).lower()
    disabled_reason = _normalize_text(primary_result_payload.get("disabled_reason"))
    invalid_reason = _normalize_text(primary_result_payload.get("invalid_reason"))

    identity_present = bool(result_id and ts_code and stock_name and ts_code not in {"暂无", "-"})
    execution_known = execution_status in {"ready", "running", "completed", "failed", "cancelled"}
    rollback_needed = execution_status in {"failed", "cancelled"} or risk_level == "critical" or bool(disabled_reason or invalid_reason)
    rollback_status = "pending" if rollback_needed else "not_required"

    checks = [
        _check(
            "primary_result_identity_present",
            identity_present,
            "blocking",
            {"result_id": result_id or None, "ts_code": ts_code or None, "stock_name": stock_name or None},
        ),
        _check("execution_status_known", execution_known, "blocking", {"execution_status": execution_status or None}),
        _check(
            "rollback_decision_recorded",
            True,
            "record",
            {"rollback_status": rollback_status, "rollback_needed": rollback_needed},
        ),
    ]
    if not identity_present or not execution_known:
        rollback_status = "pending"

    return {
        "rollback_version": PRIMARY_RESULT_ROLLBACK_VERSION,
        "generated_at": _isoformat_z(generated_at),
        "rollback_status": rollback_status,
        "result_id": result_id,
        "ts_code": ts_code,
        "stock_name": stock_name,
        "source_execution_status": execution_status or None,
        "risk_level": risk_level or None,
        "checks": checks,
        "rollback_plan": {
            "mode": "local_protocol",
            "external_broker_connected": False,
            "rollback_needed": rollback_needed,
            "required_if_pending": [
                "operator_review",
                "impact_assessment",
                "terminal_outcome",
            ],
        },
        "primary_result_payload": primary_result_payload,
    }


def is_primary_result_rollback_applicable(
    rollback_payload: dict[str, object],
    *,
    result_id: str,
    ts_code: str,
) -> bool:
    if not rollback_payload:
        return False
    rollback_result_id = _normalize_text(rollback_payload.get("result_id"))
    rollback_ts_code = _normalize_text(rollback_payload.get("ts_code"))
    return bool(rollback_result_id == result_id or (rollback_ts_code and rollback_ts_code == ts_code))


def extract_primary_result_rollback_status(
    rollback_payload: dict[str, object],
    *,
    result_id: str,
    ts_code: str,
) -> str | None:
    if not is_primary_result_rollback_applicable(rollback_payload, result_id=result_id, ts_code=ts_code):
        return None
    rollback_status = _normalize_text(rollback_payload.get("rollback_status")).lower()
    if rollback_status in PRIMARY_RESULT_ROLLBACK_ALLOWED_STATUSES:
        return rollback_status
    return None
