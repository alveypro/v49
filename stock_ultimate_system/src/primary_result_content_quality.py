from __future__ import annotations

from datetime import datetime, timedelta


PRIMARY_RESULT_CONTENT_QUALITY_VERSION = "primary_result_content_quality.v1"
REQUIRED_PRODUCTION_FIELDS = (
    "schema_version",
    "result_id",
    "ts_code",
    "stock_name",
    "result_lifecycle_stage",
    "result_type",
    "research_status",
    "candidate_status",
    "signal_level",
    "risk_level",
    "history_source_file",
    "history_source_timestamp",
)
OPTIONAL_GOVERNANCE_FIELDS = (
    "audit_status",
    "execution_status",
    "observation_status",
    "rollback_status",
    "terminal_outcome",
)


def _parse_timestamp(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text or text == "-":
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text[:19], fmt)
        except ValueError:
            continue
    return None


def evaluate_primary_result_content_quality(
    payload: dict[str, object],
    *,
    now: datetime | None = None,
    max_source_age_hours: float = 36.0,
) -> dict[str, object]:
    current_time = now or datetime.now()
    blocking_failures: list[dict[str, object]] = []
    warnings: list[dict[str, object]] = []

    if payload.get("schema_version") != "primary_result_v1":
        blocking_failures.append(
            {
                "check": "schema_version_valid",
                "details": {"schema_version": payload.get("schema_version")},
            }
        )

    missing_fields = [
        field
        for field in REQUIRED_PRODUCTION_FIELDS
        if str(payload.get(field, "") or "").strip() in {"", "-", "暂无", "null", "None"}
    ]
    if missing_fields:
        blocking_failures.append(
            {
                "check": "required_production_fields_present",
                "details": {"missing_fields": missing_fields},
            }
        )

    stage = str(payload.get("result_lifecycle_stage", "") or "").strip().upper()
    if stage not in {"L2", "L3", "L4", "L5"}:
        blocking_failures.append(
            {
                "check": "result_has_actionable_lifecycle_stage",
                "details": {"result_lifecycle_stage": stage or None, "minimum_stage": "L2"},
            }
        )

    source_timestamp = _parse_timestamp(payload.get("history_source_timestamp"))
    if source_timestamp is None:
        blocking_failures.append(
            {
                "check": "history_source_timestamp_parseable",
                "details": {"history_source_timestamp": payload.get("history_source_timestamp")},
            }
        )
    elif current_time - source_timestamp > timedelta(hours=max_source_age_hours):
        blocking_failures.append(
            {
                "check": "history_source_fresh",
                "details": {
                    "history_source_timestamp": payload.get("history_source_timestamp"),
                    "max_source_age_hours": max_source_age_hours,
                    "age_hours": round((current_time - source_timestamp).total_seconds() / 3600, 2),
                },
            }
        )

    missing_optional_governance = [
        field
        for field in OPTIONAL_GOVERNANCE_FIELDS
        if str(payload.get(field, "") or "").strip() in {"", "-", "null", "None"}
    ]
    if missing_optional_governance:
        warnings.append(
            {
                "check": "governance_fields_degraded",
                "details": {"missing_fields": missing_optional_governance},
            }
        )

    return {
        "content_quality_version": PRIMARY_RESULT_CONTENT_QUALITY_VERSION,
        "status": "passed" if not blocking_failures else "failed",
        "readiness_level": "production_candidate" if not blocking_failures else "not_production_candidate",
        "blocking_failures": blocking_failures,
        "warnings": warnings,
        "summary": {
            "ts_code": payload.get("ts_code"),
            "stock_name": payload.get("stock_name"),
            "result_lifecycle_stage": payload.get("result_lifecycle_stage"),
            "result_type": payload.get("result_type"),
            "signal_level": payload.get("signal_level"),
            "risk_level": payload.get("risk_level"),
            "history_source_file": payload.get("history_source_file"),
            "history_source_timestamp": payload.get("history_source_timestamp"),
        },
    }
