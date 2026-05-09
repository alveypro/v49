from __future__ import annotations

from datetime import datetime, timezone


PRIMARY_RESULT_OBSERVATION_VERSION = "primary_result_observation.v1"
PRIMARY_RESULT_OBSERVATION_ALLOWED_STATUSES = {"observing", "blocked", "completed", "failed", "cancelled"}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _isoformat_z(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_float(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _check(name: str, passed: bool, severity: str, details: dict[str, object] | None = None) -> dict[str, object]:
    return {
        "check": name,
        "passed": passed,
        "severity": severity,
        "details": details or {},
    }


def build_primary_result_observation(
    primary_result_payload: dict[str, object],
    *,
    observation_status: str = "observing",
    reason: str = "local observation window opened",
    window_start: str | None = None,
    window_end: str | None = None,
    observed_return: float | None = None,
    benchmark_return: float | None = None,
    max_drawdown: float | None = None,
    min_success_return: float = 0.0,
    max_drawdown_floor: float = -0.08,
    now: datetime | None = None,
) -> dict[str, object]:
    generated_at = now or _utc_now()
    normalized_status = _normalize_text(observation_status).lower()
    normalized_reason = _normalize_text(reason)
    if normalized_status not in PRIMARY_RESULT_OBSERVATION_ALLOWED_STATUSES:
        raise ValueError(f"observation_status must be one of {sorted(PRIMARY_RESULT_OBSERVATION_ALLOWED_STATUSES)}")
    if not normalized_reason:
        raise ValueError("observation reason is required")

    result_id = _normalize_text(primary_result_payload.get("result_id"))
    ts_code = _normalize_text(primary_result_payload.get("ts_code"))
    stock_name = _normalize_text(primary_result_payload.get("stock_name"))
    execution_status = _normalize_text(primary_result_payload.get("execution_status")).lower()
    rollback_status = _normalize_text(primary_result_payload.get("rollback_status")).lower()
    terminal_outcome = _normalize_text(primary_result_payload.get("terminal_outcome")).lower()

    identity_present = bool(result_id and ts_code and stock_name and ts_code not in {"暂无", "-"})
    execution_ready = execution_status in {"ready", "running", "completed"}
    rollback_clear = rollback_status in {"not_required", "completed"}
    terminal_open = not terminal_outcome
    can_observe = identity_present and execution_ready and rollback_clear and terminal_open
    normalized_observed_return = _normalize_float(observed_return)
    normalized_benchmark_return = _normalize_float(benchmark_return)
    normalized_max_drawdown = _normalize_float(max_drawdown)
    metrics_present = normalized_observed_return is not None and normalized_max_drawdown is not None
    return_passed = normalized_observed_return is not None and normalized_observed_return >= min_success_return
    drawdown_passed = normalized_max_drawdown is not None and normalized_max_drawdown >= max_drawdown_floor
    completion_criteria_passed = metrics_present and return_passed and drawdown_passed
    effective_status = normalized_status
    if not can_observe and normalized_status in {"observing", "completed"}:
        effective_status = "blocked"
    elif normalized_status == "completed" and not completion_criteria_passed:
        effective_status = "failed" if metrics_present else "blocked"

    checks = [
        _check(
            "primary_result_identity_present",
            identity_present,
            "blocking",
            {"result_id": result_id or None, "ts_code": ts_code or None, "stock_name": stock_name or None},
        ),
        _check("execution_ready_for_observation", execution_ready, "blocking", {"execution_status": execution_status or None}),
        _check("rollback_clear_for_observation", rollback_clear, "blocking", {"rollback_status": rollback_status or None}),
        _check("terminal_outcome_not_recorded", terminal_open, "blocking", {"terminal_outcome": terminal_outcome or None}),
    ]
    if normalized_status == "completed":
        checks.extend(
            [
                _check(
                    "observation_metrics_present",
                    metrics_present,
                    "blocking",
                    {"observed_return": normalized_observed_return, "max_drawdown": normalized_max_drawdown},
                ),
                _check(
                    "observed_return_meets_success_floor",
                    return_passed,
                    "blocking",
                    {"observed_return": normalized_observed_return, "min_success_return": min_success_return},
                ),
                _check(
                    "max_drawdown_within_floor",
                    drawdown_passed,
                    "blocking",
                    {"max_drawdown": normalized_max_drawdown, "max_drawdown_floor": max_drawdown_floor},
                ),
            ]
        )

    return {
        "observation_version": PRIMARY_RESULT_OBSERVATION_VERSION,
        "generated_at": _isoformat_z(generated_at),
        "observation_status": effective_status,
        "requested_observation_status": normalized_status,
        "observation_reason": normalized_reason,
        "result_id": result_id,
        "ts_code": ts_code,
        "stock_name": stock_name,
        "source_execution_status": execution_status or None,
        "source_rollback_status": rollback_status or None,
        "checks": checks,
        "observation_window": {
            "started_at": window_start or _isoformat_z(generated_at),
            "ended_at": window_end,
            "status": "open" if effective_status == "observing" else "closed",
        },
        "observation_metrics": {
            "observed_return": normalized_observed_return,
            "benchmark_return": normalized_benchmark_return,
            "excess_return": (
                round(normalized_observed_return - normalized_benchmark_return, 6)
                if normalized_observed_return is not None and normalized_benchmark_return is not None
                else None
            ),
            "max_drawdown": normalized_max_drawdown,
        },
        "completion_criteria": {
            "min_success_return": min_success_return,
            "max_drawdown_floor": max_drawdown_floor,
            "passed": completion_criteria_passed,
        },
        "observation_plan": {
            "mode": "local_protocol",
            "external_analytics_connected": False,
            "can_observe": can_observe,
            "required_for_terminal_success": effective_status == "completed" and completion_criteria_passed,
        },
        "primary_result_payload": primary_result_payload,
    }


def is_primary_result_observation_applicable(
    observation_payload: dict[str, object],
    *,
    result_id: str,
    ts_code: str,
) -> bool:
    if not observation_payload:
        return False
    observation_result_id = _normalize_text(observation_payload.get("result_id"))
    observation_ts_code = _normalize_text(observation_payload.get("ts_code"))
    return bool(observation_result_id == result_id or (observation_ts_code and observation_ts_code == ts_code))


def extract_primary_result_observation_status(
    observation_payload: dict[str, object],
    *,
    result_id: str,
    ts_code: str,
) -> str | None:
    if not is_primary_result_observation_applicable(observation_payload, result_id=result_id, ts_code=ts_code):
        return None
    observation_status = _normalize_text(observation_payload.get("observation_status")).lower()
    if observation_status in PRIMARY_RESULT_OBSERVATION_ALLOWED_STATUSES:
        return observation_status
    return None
