from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from src.artifact_registry import sha256_file
from src.primary_result_observation_metrics import calculate_primary_result_observation_metrics
from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_PRICE_HISTORY_ARTIFACT_VERSION = "primary_result_price_history_artifact.v1"
REQUIRED_PRICE_HISTORY_COLUMNS = ("ts_code", "trade_date", "close")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _check(name: str, passed: bool, detail: str, details: dict[str, object] | None = None) -> dict[str, object]:
    return {
        "name": name,
        "passed": passed,
        "detail": detail,
        "details": details or {},
    }


def _load_rows(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        rows = [{key: _normalize_text(value) for key, value in row.items()} for row in reader]
    return rows, fieldnames


def _row_counts(rows: list[dict[str, str]], *, ts_code: str, benchmark_ts_code: str) -> dict[str, int]:
    return {
        "total": len(rows),
        "observed": sum(1 for row in rows if row.get("ts_code") == ts_code),
        "benchmark": sum(1 for row in rows if row.get("ts_code") == benchmark_ts_code),
    }


def build_primary_result_price_history_artifact(
    *,
    price_history_path: str | Path = "data/experiments/primary_result_price_history_latest.csv",
    ts_code: str,
    benchmark_ts_code: str = "BENCHMARK",
    window_start: str,
    window_end: str,
    output_path: str | Path | None = None,
) -> tuple[int, dict[str, object]]:
    resolved_price_history_path = resolve_project_path(price_history_path)
    normalized_ts_code = _normalize_text(ts_code)
    normalized_benchmark_ts_code = _normalize_text(benchmark_ts_code)
    checks: list[dict[str, object]] = [
        _check("ts_code_present", bool(normalized_ts_code), "ts_code is required"),
        _check("benchmark_ts_code_present", bool(normalized_benchmark_ts_code), "benchmark_ts_code is required"),
        _check("window_start_present", bool(_normalize_text(window_start)), "window_start is required"),
        _check("window_end_present", bool(_normalize_text(window_end)), "window_end is required"),
        _check("price_history_exists", resolved_price_history_path.exists(), "price history CSV must exist"),
    ]
    rows: list[dict[str, str]] = []
    fieldnames: list[str] = []
    row_counts = {"total": 0, "observed": 0, "benchmark": 0}
    source_hash: str | None = None
    metrics: dict[str, object] | None = None
    metrics_error: str | None = None

    if resolved_price_history_path.exists():
        source_hash = sha256_file(resolved_price_history_path)
        try:
            rows, fieldnames = _load_rows(resolved_price_history_path)
            missing_columns = [column for column in REQUIRED_PRICE_HISTORY_COLUMNS if column not in fieldnames]
            checks.append(
                _check(
                    "required_columns_present",
                    not missing_columns,
                    "price history CSV must include required columns",
                    {"required_columns": list(REQUIRED_PRICE_HISTORY_COLUMNS), "missing_columns": missing_columns},
                )
            )
            row_counts = _row_counts(rows, ts_code=normalized_ts_code, benchmark_ts_code=normalized_benchmark_ts_code)
            checks.extend(
                [
                    _check("observed_rows_present", row_counts["observed"] > 0, "observed ts_code rows must exist", row_counts),
                    _check("benchmark_rows_present", row_counts["benchmark"] > 0, "benchmark rows must exist", row_counts),
                ]
            )
        except Exception as exc:
            metrics_error = str(exc)
            checks.append(_check("csv_readable", False, "price history CSV must be readable", {"error": metrics_error}))

    if all(check["passed"] is True for check in checks):
        try:
            metrics = calculate_primary_result_observation_metrics(
                price_history_path=resolved_price_history_path,
                ts_code=normalized_ts_code,
                benchmark_ts_code=normalized_benchmark_ts_code,
                window_start=window_start,
                window_end=window_end,
            )
            checks.append(_check("metrics_calculable", True, "observation metrics can be calculated"))
        except Exception as exc:
            metrics_error = str(exc)
            checks.append(_check("metrics_calculable", False, str(exc)))
    else:
        checks.append(_check("metrics_calculable", False, "prerequisite checks failed"))

    blocking_checks = [check for check in checks if check["passed"] is not True]
    payload = {
        "artifact_version": PRIMARY_RESULT_PRICE_HISTORY_ARTIFACT_VERSION,
        "generated_at": _utc_now_iso(),
        "status": "valid" if not blocking_checks else "invalid",
        "source_price_history_path": str(resolved_price_history_path),
        "source_price_history_hash": source_hash,
        "ts_code": normalized_ts_code,
        "benchmark_ts_code": normalized_benchmark_ts_code,
        "window_start": window_start,
        "window_end": window_end,
        "required_columns": list(REQUIRED_PRICE_HISTORY_COLUMNS),
        "actual_columns": fieldnames,
        "row_counts": row_counts,
        "metrics": metrics,
        "metrics_error": metrics_error,
        "checks": checks,
        "blocking_reasons": [str(check["detail"]) for check in blocking_checks],
        "data_boundary": (
            "price history artifact validates local CSV evidence only; it does not fetch external market data, "
            "adjust prices, trade, or change observation state"
        ),
    }
    if output_path:
        _write_json(resolve_project_path(output_path), payload)
    return (0 if payload["status"] == "valid" else 1), payload
