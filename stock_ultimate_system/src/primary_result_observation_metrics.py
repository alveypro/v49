from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_OBSERVATION_METRICS_VERSION = "primary_result_observation_metrics.v1"
REQUIRED_PRICE_COLUMNS = {"ts_code", "trade_date", "close"}


@dataclass(frozen=True)
class PricePoint:
    ts_code: str
    trade_date: str
    close: float


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _parse_date(value: object) -> datetime:
    text = _normalize_text(value)
    if not text:
        raise ValueError("date value must not be empty")
    if "T" in text:
        text = text.split("T", 1)[0]
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text[:10] if fmt == "%Y-%m-%d" else text[:8], fmt)
        except ValueError:
            continue
    raise ValueError(f"unsupported date format: {value}")


def _date_key(value: object) -> str:
    return _parse_date(value).strftime("%Y-%m-%d")


def _normalize_float(value: object, *, field_name: str) -> float:
    text = _normalize_text(value)
    if not text:
        raise ValueError(f"{field_name} must not be empty")
    try:
        return float(text)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be numeric") from exc


def _load_price_points(price_history_path: Path, ts_code: str) -> list[PricePoint]:
    if not price_history_path.exists():
        raise FileNotFoundError(f"price history csv missing: {price_history_path}")
    with price_history_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = set(reader.fieldnames or [])
        missing = sorted(REQUIRED_PRICE_COLUMNS - fieldnames)
        if missing:
            raise ValueError(f"price history csv missing columns: {', '.join(missing)}")
        points = [
            PricePoint(
                ts_code=_normalize_text(row.get("ts_code")),
                trade_date=_date_key(row.get("trade_date")),
                close=_normalize_float(row.get("close"), field_name="close"),
            )
            for row in reader
            if _normalize_text(row.get("ts_code")) == ts_code
        ]
    points.sort(key=lambda point: point.trade_date)
    return points


def _window_points(points: list[PricePoint], *, window_start: str, window_end: str) -> list[PricePoint]:
    start = _date_key(window_start)
    end = _date_key(window_end)
    if end < start:
        raise ValueError("window_end must be greater than or equal to window_start")
    return [point for point in points if start <= point.trade_date <= end]


def _total_return(points: list[PricePoint]) -> float:
    start_close = points[0].close
    end_close = points[-1].close
    if start_close <= 0:
        raise ValueError("start close must be positive")
    return round(end_close / start_close - 1.0, 6)


def _max_drawdown(points: list[PricePoint]) -> float:
    peak = points[0].close
    max_drawdown = 0.0
    for point in points:
        if point.close > peak:
            peak = point.close
        if peak <= 0:
            raise ValueError("peak close must be positive")
        drawdown = point.close / peak - 1.0
        if drawdown < max_drawdown:
            max_drawdown = drawdown
    return round(max_drawdown, 6)


def _summarize_points(points: list[PricePoint]) -> dict[str, object]:
    return {
        "point_total": len(points),
        "start_trade_date": points[0].trade_date,
        "end_trade_date": points[-1].trade_date,
        "start_close": points[0].close,
        "end_close": points[-1].close,
    }


def calculate_primary_result_observation_metrics(
    *,
    price_history_path: str | Path,
    ts_code: str,
    benchmark_ts_code: str,
    window_start: str,
    window_end: str,
) -> dict[str, object]:
    resolved_price_history_path = resolve_project_path(price_history_path)
    normalized_ts_code = _normalize_text(ts_code)
    normalized_benchmark_ts_code = _normalize_text(benchmark_ts_code)
    if not normalized_ts_code:
        raise ValueError("ts_code is required")
    if not normalized_benchmark_ts_code:
        raise ValueError("benchmark_ts_code is required")

    observed_points = _window_points(
        _load_price_points(resolved_price_history_path, normalized_ts_code),
        window_start=window_start,
        window_end=window_end,
    )
    benchmark_points = _window_points(
        _load_price_points(resolved_price_history_path, normalized_benchmark_ts_code),
        window_start=window_start,
        window_end=window_end,
    )
    if len(observed_points) < 2:
        raise ValueError("observed price history must include at least two points in the observation window")
    if len(benchmark_points) < 2:
        raise ValueError("benchmark price history must include at least two points in the observation window")

    observed_return = _total_return(observed_points)
    benchmark_return = _total_return(benchmark_points)
    max_drawdown = _max_drawdown(observed_points)
    return {
        "metrics_version": PRIMARY_RESULT_OBSERVATION_METRICS_VERSION,
        "source_price_history_path": str(resolved_price_history_path),
        "ts_code": normalized_ts_code,
        "benchmark_ts_code": normalized_benchmark_ts_code,
        "observation_window": {
            "started_at": window_start,
            "ended_at": window_end,
        },
        "observed_return": observed_return,
        "benchmark_return": benchmark_return,
        "excess_return": round(observed_return - benchmark_return, 6),
        "max_drawdown": max_drawdown,
        "observed_price_summary": _summarize_points(observed_points),
        "benchmark_price_summary": _summarize_points(benchmark_points),
        "data_quality_checks": [
            {
                "check": "observed_window_has_two_or_more_points",
                "passed": len(observed_points) >= 2,
                "severity": "blocking",
                "details": {"point_total": len(observed_points)},
            },
            {
                "check": "benchmark_window_has_two_or_more_points",
                "passed": len(benchmark_points) >= 2,
                "severity": "blocking",
                "details": {"point_total": len(benchmark_points)},
            },
        ],
    }
