from __future__ import annotations

from datetime import datetime, timezone

from src.primary_result_content_quality import evaluate_primary_result_content_quality


PRIMARY_RESULT_AUDIT_VERSION = "primary_result_audit.v1"
PRIMARY_RESULT_AUDIT_ALLOWED_STATUSES = {"passed", "failed", "in_review", "waived"}


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


def build_primary_result_audit(
    primary_result_payload: dict[str, object],
    *,
    now: datetime | None = None,
    max_source_age_hours: float = 36.0,
) -> dict[str, object]:
    generated_at = now or _utc_now()
    quality_now = generated_at.astimezone(timezone.utc).replace(tzinfo=None)
    content_quality = evaluate_primary_result_content_quality(
        primary_result_payload,
        now=quality_now,
        max_source_age_hours=max_source_age_hours,
    )

    risk_level = _normalize_text(primary_result_payload.get("risk_level")).lower()
    signal_level = _normalize_text(primary_result_payload.get("signal_level")).lower()
    result_id = _normalize_text(primary_result_payload.get("result_id"))
    ts_code = _normalize_text(primary_result_payload.get("ts_code"))
    stock_name = _normalize_text(primary_result_payload.get("stock_name"))
    stage = _normalize_text(primary_result_payload.get("result_lifecycle_stage")).upper()

    content_quality_passed = content_quality.get("status") == "passed"
    identity_present = bool(result_id and ts_code and stock_name and ts_code not in {"暂无", "-"})
    signal_present = signal_level in {"high", "medium", "low", "none"}
    actionable_stage = stage in {"L2", "L3", "L4", "L5"}
    risk_known = risk_level in {"low", "medium", "high", "critical"}
    risk_not_critical = risk_level != "critical"

    checks = [
        _check(
            "content_quality_passed",
            content_quality_passed,
            "blocking",
            {"content_quality_status": content_quality.get("status")},
        ),
        _check(
            "primary_result_identity_present",
            identity_present,
            "blocking",
            {"result_id": result_id or None, "ts_code": ts_code or None, "stock_name": stock_name or None},
        ),
        _check(
            "actionable_lifecycle_stage",
            actionable_stage,
            "blocking",
            {"result_lifecycle_stage": stage or None, "minimum_stage": "L2"},
        ),
        _check("signal_level_present", signal_present, "blocking", {"signal_level": signal_level or None}),
        _check("risk_level_known", risk_known, "blocking", {"risk_level": risk_level or None}),
        _check("risk_not_critical", risk_not_critical, "blocking", {"risk_level": risk_level or None}),
    ]

    blocking_failed = any(item["severity"] == "blocking" and not item["passed"] for item in checks)
    if blocking_failed:
        audit_status = "failed"
    elif risk_level == "high":
        audit_status = "in_review"
        checks.append(
            _check(
                "risk_within_auto_pass_tolerance",
                False,
                "review",
                {"risk_level": risk_level, "required_action": "manual_review"},
            )
        )
    else:
        audit_status = "passed"
        checks.append(
            _check("risk_within_auto_pass_tolerance", True, "review", {"risk_level": risk_level or None})
        )

    return {
        "audit_version": PRIMARY_RESULT_AUDIT_VERSION,
        "generated_at": _isoformat_z(generated_at),
        "audit_status": audit_status,
        "result_id": result_id,
        "ts_code": ts_code,
        "stock_name": stock_name,
        "result_lifecycle_stage": stage,
        "result_type": primary_result_payload.get("result_type"),
        "checks": checks,
        "content_quality": {
            "content_quality_version": content_quality.get("content_quality_version"),
            "status": content_quality.get("status"),
            "readiness_level": content_quality.get("readiness_level"),
            "blocking_failures": content_quality.get("blocking_failures", []),
            "warnings": content_quality.get("warnings", []),
            "summary": content_quality.get("summary", {}),
        },
        "primary_result_payload": primary_result_payload,
    }


def is_primary_result_audit_applicable(
    audit_payload: dict[str, object],
    *,
    result_id: str,
    ts_code: str,
) -> bool:
    if not audit_payload:
        return False
    audit_result_id = _normalize_text(audit_payload.get("result_id"))
    audit_ts_code = _normalize_text(audit_payload.get("ts_code"))
    return bool(audit_result_id == result_id or (audit_ts_code and audit_ts_code == ts_code))


def extract_primary_result_audit_status(
    audit_payload: dict[str, object],
    *,
    result_id: str,
    ts_code: str,
) -> str | None:
    if not is_primary_result_audit_applicable(audit_payload, result_id=result_id, ts_code=ts_code):
        return None
    audit_status = _normalize_text(audit_payload.get("audit_status")).lower()
    if audit_status in PRIMARY_RESULT_AUDIT_ALLOWED_STATUSES:
        return audit_status
    return None
