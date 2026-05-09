from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from src.primary_result_observation_metrics import _date_key
from src.unified_result_builder import build_primary_result_api_payload
from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_OBSERVATION_WAIT_STATUS_VERSION = "primary_result_observation_wait_status.v1"
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


def _utc_now_iso(now: datetime | None = None) -> str:
    value = now or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _local_date_key(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(SHANGHAI_TZ).strftime("%Y-%m-%d")


def _window_start(observation: dict[str, object]) -> str | None:
    window = observation.get("observation_window")
    if not isinstance(window, dict):
        return None
    started_at = _normalize_text(window.get("started_at"))
    return started_at or None


def build_primary_result_observation_wait_status(
    *,
    exp_dir: str | Path = "data/experiments",
    candidate_index: int = 0,
    output_path: str | Path | None = None,
    now: datetime | None = None,
) -> tuple[int, dict[str, object]]:
    resolved_exp_dir = resolve_project_path(exp_dir)
    generated_at = _utc_now_iso(now)
    current_date = _local_date_key(now or datetime.now(timezone.utc))
    primary_result = build_primary_result_api_payload(
        resolved_exp_dir,
        candidate_index=candidate_index,
        require_current_pointer=True,
    )
    observation_path = resolved_exp_dir / "primary_result_observation_latest.json"
    observation = _read_json(observation_path) if observation_path.exists() else {}

    started_at = _window_start(observation)
    observation_status = _normalize_text(observation.get("observation_status") or primary_result.get("observation_status")).lower()
    result_id = primary_result.get("result_id")
    ts_code = _normalize_text(primary_result.get("ts_code"))

    checks = [
        {
            "name": "primary_result_identity_present",
            "passed": bool(result_id and ts_code),
            "detail": "primary result identity must exist",
        },
        {
            "name": "observation_artifact_exists",
            "passed": observation_path.exists(),
            "detail": "open observation artifact must exist",
        },
        {
            "name": "observation_is_observing",
            "passed": observation_status == "observing",
            "detail": "observation status must be observing",
        },
        {
            "name": "window_start_present",
            "passed": bool(started_at),
            "detail": "observation window start is required",
        },
    ]

    window_start_date = _date_key(started_at) if started_at else None
    window_started = bool(window_start_date and current_date >= window_start_date)
    if started_at:
        checks.append(
            {
                "name": "window_started",
                "passed": window_started,
                "detail": "current date must be on or after observation window start before closure checks",
                "details": {"current_date": current_date, "window_start_date": window_start_date},
            }
        )

    failed_prerequisites = [check for check in checks[:4] if check["passed"] is not True]
    if failed_prerequisites:
        status = "blocked"
        next_actions = [str(check["detail"]) for check in failed_prerequisites]
    elif not window_started:
        status = "pending_window"
        next_actions = [
            f"wait until observation window starts on {window_start_date}",
            "do not run closure or write performance ledger before the window starts",
        ]
    else:
        status = "ready_for_data_check"
        next_actions = [
            "run market data readiness and closure checks; only write ledger if those gates pass",
        ]

    payload = {
        "wait_status_version": PRIMARY_RESULT_OBSERVATION_WAIT_STATUS_VERSION,
        "generated_at": generated_at,
        "status": status,
        "result_id": result_id,
        "ts_code": ts_code,
        "stock_name": primary_result.get("stock_name"),
        "current_date": current_date,
        "observation_status": observation_status or None,
        "observation_window": {
            "started_at": started_at,
            "start_date": window_start_date,
            "has_started": window_started,
        },
        "checks": checks,
        "blocking_reasons": [str(check["detail"]) for check in checks if check["passed"] is not True],
        "next_actions": next_actions,
        "production_boundary": (
            "observation wait status is diagnostics only; it does not close observation, fetch market data, "
            "write performance ledger, trade, promote baselines, or change strategy state"
        ),
    }
    if output_path:
        _write_json(resolve_project_path(output_path), payload)
    return (0 if status in {"pending_window", "ready_for_data_check"} else 1), payload
