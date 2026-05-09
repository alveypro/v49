from __future__ import annotations

import json
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_OBSERVATION_WINDOW_ADVISOR_VERSION = "primary_result_observation_window_advisor.v1"
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
MARKET_OPEN = time(9, 30)


def _localize(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=SHANGHAI_TZ)
    return value.astimezone(SHANGHAI_TZ)


def _is_weekday(value: datetime) -> bool:
    return value.weekday() < 5


def _next_weekday(value: datetime) -> datetime:
    candidate = value
    while not _is_weekday(candidate):
        candidate = candidate + timedelta(days=1)
    return candidate


def _normalize_trade_dates(trade_dates: Iterable[str] | None) -> set[str] | None:
    if trade_dates is None:
        return None
    normalized = {str(item).strip() for item in trade_dates if str(item).strip()}
    return normalized or None


def _load_trade_dates_from_artifact(path: str | Path | None) -> list[str] | None:
    if path is None:
        return None
    resolved = resolve_project_path(path)
    if not resolved.exists():
        return None
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    dates = payload.get("trade_dates")
    if not isinstance(dates, list):
        raise ValueError(f"trade calendar artifact missing trade_dates: {resolved}")
    return [str(item) for item in dates]


def _next_calendar_session_open(value: datetime, trade_dates: set[str] | None) -> datetime:
    if trade_dates is None:
        return _next_weekday(value)
    candidate = value
    for _ in range(370):
        if candidate.strftime("%Y-%m-%d") in trade_dates:
            return candidate
        candidate = candidate + timedelta(days=1)
    raise ValueError("no trade date found within 370 days for observation window suggestion")


def suggest_observation_window_start(
    *,
    reference_time: datetime | None = None,
    candidate_file_path: str | Path | None = None,
    trade_dates: Iterable[str] | None = None,
    trade_calendar_path: str | Path | None = None,
) -> dict[str, object]:
    if reference_time is None:
        if candidate_file_path is not None:
            candidate_path = resolve_project_path(candidate_file_path)
            if candidate_path.exists():
                reference_time = datetime.fromtimestamp(candidate_path.stat().st_mtime, timezone.utc)
        if reference_time is None:
            reference_time = datetime.now(timezone.utc)
    local_reference = _localize(reference_time)
    session_date = local_reference.date()
    session_open = datetime.combine(session_date, MARKET_OPEN, tzinfo=SHANGHAI_TZ)
    if not _is_weekday(local_reference) or local_reference >= session_open:
        session_open = datetime.combine(session_date + timedelta(days=1), MARKET_OPEN, tzinfo=SHANGHAI_TZ)
    artifact_trade_dates = _load_trade_dates_from_artifact(trade_calendar_path)
    normalized_trade_dates = _normalize_trade_dates(trade_dates if trade_dates is not None else artifact_trade_dates)
    session_open = _next_calendar_session_open(session_open, normalized_trade_dates)
    suggested_utc = session_open.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    calendar_policy = (
        "provided_trade_calendar_open_0930"
        if normalized_trade_dates is not None
        else "weekday_ashare_open_0930_without_holiday_calendar"
    )
    risk_note = (
        "Suggestion was validated against the provided trade calendar dates."
        if normalized_trade_dates is not None
        else (
            "This is a conservative weekday-based suggestion. If an exchange holiday or special session applies, "
            "operator must override with the official trading calendar time."
        )
    )
    return {
        "advisor_version": PRIMARY_RESULT_OBSERVATION_WINDOW_ADVISOR_VERSION,
        "reference_time": local_reference.replace(microsecond=0).isoformat(),
        "suggested_window_start": suggested_utc,
        "suggested_window_start_local": session_open.replace(microsecond=0).isoformat(),
        "calendar_policy": calendar_policy,
        "risk_note": risk_note,
    }
