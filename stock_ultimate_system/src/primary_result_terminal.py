from __future__ import annotations

from datetime import datetime, timezone


PRIMARY_RESULT_TERMINAL_VERSION = "primary_result_terminal.v1"
PRIMARY_RESULT_TERMINAL_ALLOWED_OUTCOMES = {"success", "failed", "expired", "superseded", "rejected", "cancelled", "archived"}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _isoformat_z(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def build_primary_result_terminal(
    primary_result_payload: dict[str, object],
    *,
    terminal_outcome: str,
    reason: str,
    now: datetime | None = None,
) -> dict[str, object]:
    normalized_outcome = _normalize_text(terminal_outcome).lower()
    normalized_reason = _normalize_text(reason)
    if normalized_outcome not in PRIMARY_RESULT_TERMINAL_ALLOWED_OUTCOMES:
        raise ValueError(f"terminal_outcome must be one of {sorted(PRIMARY_RESULT_TERMINAL_ALLOWED_OUTCOMES)}")
    if not normalized_reason:
        raise ValueError("terminal outcome reason is required")
    observation_status = _normalize_text(primary_result_payload.get("observation_status")).lower()
    if normalized_outcome == "success" and observation_status != "completed":
        raise ValueError("terminal success requires completed observation_status")

    generated_at = now or _utc_now()
    return {
        "terminal_version": PRIMARY_RESULT_TERMINAL_VERSION,
        "generated_at": _isoformat_z(generated_at),
        "terminal_outcome": normalized_outcome,
        "terminal_reason": normalized_reason,
        "result_id": _normalize_text(primary_result_payload.get("result_id")),
        "ts_code": _normalize_text(primary_result_payload.get("ts_code")),
        "stock_name": _normalize_text(primary_result_payload.get("stock_name")),
        "source_execution_status": _normalize_text(primary_result_payload.get("execution_status")).lower() or None,
        "source_observation_status": observation_status or None,
        "source_rollback_status": _normalize_text(primary_result_payload.get("rollback_status")).lower() or None,
        "primary_result_payload": primary_result_payload,
    }


def is_primary_result_terminal_applicable(
    terminal_payload: dict[str, object],
    *,
    result_id: str,
    ts_code: str,
) -> bool:
    if not terminal_payload:
        return False
    terminal_result_id = _normalize_text(terminal_payload.get("result_id"))
    terminal_ts_code = _normalize_text(terminal_payload.get("ts_code"))
    return bool(terminal_result_id == result_id or (terminal_ts_code and terminal_ts_code == ts_code))


def extract_primary_result_terminal_outcome(
    terminal_payload: dict[str, object],
    *,
    result_id: str,
    ts_code: str,
) -> str | None:
    if not is_primary_result_terminal_applicable(terminal_payload, result_id=result_id, ts_code=ts_code):
        return None
    terminal_outcome = _normalize_text(terminal_payload.get("terminal_outcome")).lower()
    if terminal_outcome in PRIMARY_RESULT_TERMINAL_ALLOWED_OUTCOMES:
        return terminal_outcome
    return None
