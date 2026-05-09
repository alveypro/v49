from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.stock_ai_runner_storage import StockAIRunnerStorage


def _load_json_object(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json file: {path}")
    return payload


def load_stock_ai_runner_read_model(storage_dir: str | Path = "artifacts/stock_ai_runner") -> dict[str, Any]:
    storage = StockAIRunnerStorage.from_path(storage_dir)
    payload = _load_json_object(storage.read_model_path)
    if payload is not None:
        return payload
    return storage.build_read_model()


def _parse_recorded_at(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized).astimezone(timezone.utc)


def _is_within_range(*, recorded_at: str, recorded_at_from: str = "", recorded_at_to: str = "") -> bool:
    current = _parse_recorded_at(recorded_at)
    if current is None:
        return not recorded_at_from and not recorded_at_to
    floor = _parse_recorded_at(recorded_at_from)
    ceil = _parse_recorded_at(recorded_at_to)
    if floor is not None and current < floor:
        return False
    if ceil is not None and current > ceil:
        return False
    return True


def read_stock_ai_provider_latest_status(storage_dir: str | Path = "artifacts/stock_ai_runner") -> list[dict[str, Any]]:
    read_model = load_stock_ai_runner_read_model(storage_dir)
    provider_latest_status = dict(read_model.get("provider_latest_health", {}) or {})
    rows: list[dict[str, Any]] = []
    for provider_name, snapshot in provider_latest_status.items():
        if not isinstance(snapshot, dict):
            continue
        row = {
            "provider_name": str(provider_name or ""),
            **dict(snapshot),
        }
        row["is_problem"] = bool(row.get("is_problem"))
        rows.append(row)
    rows.sort(key=lambda item: (not bool(item.get("is_problem")), str(item.get("provider_name", "") or "")))
    return rows


def read_stock_ai_provider_health_rollups(
    storage_dir: str | Path = "artifacts/stock_ai_runner",
    *,
    window: int = 8,
) -> list[dict[str, Any]]:
    storage = StockAIRunnerStorage.from_path(storage_dir)
    telemetry_rows = storage.load_telemetry_rows()
    provider_groups: dict[str, list[dict[str, Any]]] = {}
    for row in telemetry_rows:
        provider_name = str(row.get("provider_name", "") or "unknown")
        provider_groups.setdefault(provider_name, []).append(dict(row))

    rollups: list[dict[str, Any]] = []
    for provider_name, rows in sorted(provider_groups.items()):
        recent_rows = rows[-window:] if window > 0 else rows
        total_calls = len(recent_rows)
        if total_calls <= 0:
            continue
        timeout_calls = sum(1 for row in recent_rows if str(row.get("status", "") or "") == "timeout")
        blocked_calls = sum(1 for row in recent_rows if int(row.get("status_code", 0) or 0) >= 400)
        error_calls = sum(1 for row in recent_rows if str(row.get("status", "") or "") in {"error", "blocked", "timeout"})
        ok_calls = sum(1 for row in recent_rows if str(row.get("status", "") or "") == "ok")
        latest_row = dict(recent_rows[-1])
        rollups.append(
            {
                "provider_name": provider_name,
                "window": max(int(window or 0), 0),
                "total_calls": total_calls,
                "ok_calls": ok_calls,
                "error_calls": error_calls,
                "timeout_calls": timeout_calls,
                "blocked_calls": blocked_calls,
                "success_rate": round(ok_calls / total_calls, 4),
                "timeout_rate": round(timeout_calls / total_calls, 4),
                "blocked_rate": round(blocked_calls / total_calls, 4),
                "max_elapsed_ms": max(int(row.get("elapsed_ms", 0) or 0) for row in recent_rows),
                "latest_state": str(latest_row.get("status", "") or ""),
                "latest_status_code": int(latest_row.get("status_code", 0) or 0),
                "latest_recorded_at": str(latest_row.get("recorded_at", "") or ""),
                "is_degrading": timeout_calls > 0 or blocked_calls > 0 or error_calls > 0,
            }
        )
    rollups.sort(key=lambda item: (not bool(item.get("is_degrading")), -float(item.get("blocked_rate", 0)), str(item.get("provider_name", "") or "")))
    return rollups


def read_stock_ai_provider_trend_summaries(
    storage_dir: str | Path = "artifacts/stock_ai_runner",
    *,
    short_window: int = 8,
    long_window: int = 16,
) -> list[dict[str, Any]]:
    storage = StockAIRunnerStorage.from_path(storage_dir)
    telemetry_rows = storage.load_telemetry_rows()
    provider_groups: dict[str, list[dict[str, Any]]] = {}
    for row in telemetry_rows:
        provider_name = str(row.get("provider_name", "") or "unknown")
        provider_groups.setdefault(provider_name, []).append(dict(row))

    summaries: list[dict[str, Any]] = []
    for provider_name, rows in sorted(provider_groups.items()):
        recent_short = rows[-short_window:] if short_window > 0 else rows
        recent_long = rows[-long_window:] if long_window > 0 else rows
        if not recent_long:
            continue

        def _rate(items: list[dict[str, Any]], *, kind: str) -> float:
            total = len(items)
            if total <= 0:
                return 0.0
            if kind == "timeout":
                count = sum(1 for row in items if str(row.get("status", "") or "") == "timeout")
            elif kind == "blocked":
                count = sum(1 for row in items if int(row.get("status_code", 0) or 0) >= 400)
            else:
                count = sum(1 for row in items if str(row.get("status", "") or "") == "ok")
            return round(count / total, 4)

        short_timeout_rate = _rate(recent_short, kind="timeout")
        long_timeout_rate = _rate(recent_long, kind="timeout")
        short_blocked_rate = _rate(recent_short, kind="blocked")
        long_blocked_rate = _rate(recent_long, kind="blocked")
        short_success_rate = _rate(recent_short, kind="ok")
        long_success_rate = _rate(recent_long, kind="ok")
        timeout_delta = round(short_timeout_rate - long_timeout_rate, 4)
        blocked_delta = round(short_blocked_rate - long_blocked_rate, 4)
        success_delta = round(short_success_rate - long_success_rate, 4)
        latest_row = dict(recent_long[-1])
        summaries.append(
            {
                "provider_name": provider_name,
                "short_window": max(int(short_window or 0), 0),
                "long_window": max(int(long_window or 0), 0),
                "latest_state": str(latest_row.get("status", "") or ""),
                "latest_status_code": int(latest_row.get("status_code", 0) or 0),
                "latest_recorded_at": str(latest_row.get("recorded_at", "") or ""),
                "short_timeout_rate": short_timeout_rate,
                "long_timeout_rate": long_timeout_rate,
                "short_blocked_rate": short_blocked_rate,
                "long_blocked_rate": long_blocked_rate,
                "short_success_rate": short_success_rate,
                "long_success_rate": long_success_rate,
                "timeout_trend_delta": timeout_delta,
                "blocked_trend_delta": blocked_delta,
                "success_trend_delta": success_delta,
                "is_worsening": timeout_delta > 0 or blocked_delta > 0 or success_delta < 0,
                "is_flapping": len({str(row.get("status", "") or "") for row in recent_short}) > 1,
            }
        )
    summaries.sort(
        key=lambda item: (
            not bool(item.get("is_worsening")),
            not bool(item.get("is_flapping")),
            -float(item.get("blocked_trend_delta", 0)),
            -float(item.get("timeout_trend_delta", 0)),
            str(item.get("provider_name", "") or ""),
        )
    )
    return summaries


def read_stock_ai_failure_top_causes(
    storage_dir: str | Path = "artifacts/stock_ai_runner",
    *,
    top_n: int = 5,
) -> list[dict[str, Any]]:
    read_model = load_stock_ai_runner_read_model(storage_dir)
    rows = list(read_model.get("failure_top_causes", []) or [])
    normalized = [dict(row) for row in rows if isinstance(row, dict)]
    normalized.sort(key=lambda item: (-int(item.get("count", 0) or 0), str(item.get("reason", "") or "")))
    return normalized[: max(int(top_n or 0), 0)]


def read_stock_ai_result_replay(
    storage_dir: str | Path = "artifacts/stock_ai_runner",
    *,
    result_id: str,
    window: int = 8,
    recorded_at_from: str = "",
    recorded_at_to: str = "",
) -> dict[str, Any]:
    normalized_result_id = str(result_id or "").strip()
    storage = StockAIRunnerStorage.from_path(storage_dir)
    ledger_rows = storage.load_attempt_ledger_rows()
    normalized_attempts = []
    for row in ledger_rows:
        if str(row.get("result_id", "") or "") != normalized_result_id:
            continue
        recorded_at = str(row.get("recorded_at", "") or "")
        if not _is_within_range(
            recorded_at=recorded_at,
            recorded_at_from=recorded_at_from,
            recorded_at_to=recorded_at_to,
        ):
            continue
        normalized_attempts.append(
            {
                "result_id": normalized_result_id,
                "provider_name": str(row.get("provider_name", "") or ""),
                "request_id": str(row.get("request_id", "") or ""),
                "recorded_at": recorded_at,
                "state": str(row.get("state", "") or ""),
                "reason": str(row.get("reason", "") or ""),
                "final_status": str(row.get("final_status", "") or ""),
                "is_problem": str(row.get("state", "") or "") in {"timeout", "blocked", "error"},
            }
        )
    if window > 0:
        normalized_attempts = normalized_attempts[-window:]
    latest_state = normalized_attempts[-1]["state"] if normalized_attempts else ""
    failure_counts: dict[str, int] = {}
    for attempt in normalized_attempts:
        state = str(attempt.get("state", "") or "")
        if state in {"ok", "ready", "running"}:
            continue
        failure_counts[state] = failure_counts.get(state, 0) + 1
    return {
        "result_id": normalized_result_id,
        "window": max(int(window or 0), 0),
        "recorded_at_from": str(recorded_at_from or ""),
        "recorded_at_to": str(recorded_at_to or ""),
        "attempt_count": len(normalized_attempts),
        "latest_state": latest_state,
        "failure_counts": failure_counts,
        "attempts": normalized_attempts,
    }


def read_stock_ai_provider_attempt_replay(
    storage_dir: str | Path = "artifacts/stock_ai_runner",
    *,
    provider_name: str,
    window: int = 8,
    recorded_at_from: str = "",
    recorded_at_to: str = "",
) -> dict[str, Any]:
    normalized_provider_name = str(provider_name or "").strip()
    storage = StockAIRunnerStorage.from_path(storage_dir)
    ledger_rows = storage.load_attempt_ledger_rows()
    normalized_attempts = []
    for row in ledger_rows:
        if str(row.get("provider_name", "") or "") != normalized_provider_name:
            continue
        recorded_at = str(row.get("recorded_at", "") or "")
        if not _is_within_range(
            recorded_at=recorded_at,
            recorded_at_from=recorded_at_from,
            recorded_at_to=recorded_at_to,
        ):
            continue
        normalized_attempts.append(
            {
                "provider_name": normalized_provider_name,
                "result_id": str(row.get("result_id", "") or ""),
                "request_id": str(row.get("request_id", "") or ""),
                "recorded_at": recorded_at,
                "state": str(row.get("state", "") or ""),
                "reason": str(row.get("reason", "") or ""),
                "final_status": str(row.get("final_status", "") or ""),
                "is_problem": str(row.get("state", "") or "") in {"timeout", "blocked", "error"},
            }
        )
    if window > 0:
        normalized_attempts = normalized_attempts[-window:]
    latest_state = normalized_attempts[-1]["state"] if normalized_attempts else ""
    failure_counts: dict[str, int] = {}
    touched_result_ids: list[str] = []
    for attempt in normalized_attempts:
        result_id = str(attempt.get("result_id", "") or "")
        if result_id and result_id not in touched_result_ids:
            touched_result_ids.append(result_id)
        state = str(attempt.get("state", "") or "")
        if state in {"ok", "ready", "running"}:
            continue
        failure_counts[state] = failure_counts.get(state, 0) + 1
    return {
        "provider_name": normalized_provider_name,
        "window": max(int(window or 0), 0),
        "recorded_at_from": str(recorded_at_from or ""),
        "recorded_at_to": str(recorded_at_to or ""),
        "attempt_count": len(normalized_attempts),
        "latest_state": latest_state,
        "failure_counts": failure_counts,
        "result_ids": touched_result_ids,
        "attempts": normalized_attempts,
    }


def read_stock_ai_provider_latest_health_snapshot(
    storage_dir: str | Path = "artifacts/stock_ai_runner",
) -> list[dict[str, Any]]:
    return read_stock_ai_provider_latest_status(storage_dir)
