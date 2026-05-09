from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from src.artifact_registry import sha256_file
from src.primary_result_observation_metrics import _date_key
from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_PRICE_HISTORY_INGEST_VERSION = "primary_result_price_history_ingest.v1"
REQUIRED_COLUMNS = ("ts_code", "trade_date", "close")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _check(name: str, passed: bool, detail: str, details: dict[str, object] | None = None) -> dict[str, object]:
    return {"name": name, "passed": passed, "detail": detail, "details": details or {}}


def _load_rows(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        rows = [{key: _normalize_text(value) for key, value in row.items()} for row in reader]
    return rows, fieldnames


def _in_window(row: dict[str, str], *, window_start: str, window_end: str) -> bool:
    trade_date = _date_key(row.get("trade_date"))
    return _date_key(window_start) <= trade_date <= _date_key(window_end)


def _normalize_output_row(row: dict[str, str]) -> dict[str, str]:
    close_text = _normalize_text(row.get("close"))
    close = float(close_text)
    if close <= 0:
        raise ValueError("close must be positive")
    return {
        "ts_code": _normalize_text(row.get("ts_code")),
        "trade_date": _date_key(row.get("trade_date")),
        "close": str(close),
    }


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(REQUIRED_COLUMNS))
        writer.writeheader()
        writer.writerows(rows)


def import_primary_result_price_history(
    *,
    source_csv_path: str | Path,
    output_csv_path: str | Path = "data/experiments/primary_result_price_history_latest.csv",
    manifest_output_path: str | Path | None = None,
    ts_code: str,
    benchmark_ts_code: str = "BENCHMARK",
    window_start: str,
    window_end: str,
    source_label: str,
) -> tuple[int, dict[str, object]]:
    source_path = resolve_project_path(source_csv_path)
    output_path = resolve_project_path(output_csv_path)
    manifest_path = resolve_project_path(manifest_output_path) if manifest_output_path else None
    normalized_ts_code = _normalize_text(ts_code)
    normalized_benchmark_ts_code = _normalize_text(benchmark_ts_code)
    normalized_source_label = _normalize_text(source_label)

    checks = [
        _check("source_label_present", bool(normalized_source_label), "source_label is required"),
        _check("ts_code_present", bool(normalized_ts_code), "ts_code is required"),
        _check("benchmark_ts_code_present", bool(normalized_benchmark_ts_code), "benchmark_ts_code is required"),
        _check("window_start_present", bool(_normalize_text(window_start)), "window_start is required"),
        _check("window_end_present", bool(_normalize_text(window_end)), "window_end is required"),
        _check("source_csv_exists", source_path.exists(), "source CSV must exist"),
    ]
    source_hash = sha256_file(source_path) if source_path.exists() else None
    fieldnames: list[str] = []
    filtered_rows: list[dict[str, str]] = []
    output_hash: str | None = None
    error: str | None = None

    if source_path.exists():
        try:
            raw_rows, fieldnames = _load_rows(source_path)
            missing_columns = [column for column in REQUIRED_COLUMNS if column not in fieldnames]
            checks.append(
                _check(
                    "required_columns_present",
                    not missing_columns,
                    "source CSV must include required columns",
                    {"required_columns": list(REQUIRED_COLUMNS), "missing_columns": missing_columns},
                )
            )
            if not missing_columns:
                for row in raw_rows:
                    if row.get("ts_code") not in {normalized_ts_code, normalized_benchmark_ts_code}:
                        continue
                    if not _in_window(row, window_start=window_start, window_end=window_end):
                        continue
                    filtered_rows.append(_normalize_output_row(row))
                filtered_rows.sort(key=lambda row: (row["ts_code"], row["trade_date"]))
        except Exception as exc:
            error = str(exc)
            checks.append(_check("source_csv_readable", False, "source CSV must be readable", {"error": error}))

    observed_rows = [row for row in filtered_rows if row["ts_code"] == normalized_ts_code]
    benchmark_rows = [row for row in filtered_rows if row["ts_code"] == normalized_benchmark_ts_code]
    checks.extend(
        [
            _check(
                "observed_window_has_two_or_more_rows",
                len(observed_rows) >= 2,
                "observed ts_code must have at least two rows in window",
                {"row_total": len(observed_rows)},
            ),
            _check(
                "benchmark_window_has_two_or_more_rows",
                len(benchmark_rows) >= 2,
                "benchmark must have at least two rows in window",
                {"row_total": len(benchmark_rows)},
            ),
        ]
    )

    blocking_checks = [check for check in checks if check["passed"] is not True]
    status = "imported" if not blocking_checks else "blocked"
    if status == "imported":
        _write_csv(output_path, filtered_rows)
        output_hash = sha256_file(output_path)

    payload = {
        "ingest_version": PRIMARY_RESULT_PRICE_HISTORY_INGEST_VERSION,
        "generated_at": _utc_now_iso(),
        "status": status,
        "source_label": normalized_source_label,
        "source_csv_path": str(source_path),
        "source_csv_hash": source_hash,
        "output_csv_path": str(output_path) if status == "imported" else None,
        "output_csv_hash": output_hash,
        "ts_code": normalized_ts_code,
        "benchmark_ts_code": normalized_benchmark_ts_code,
        "window_start": window_start,
        "window_end": window_end,
        "source_columns": fieldnames,
        "row_counts": {
            "output_total": len(filtered_rows),
            "observed": len(observed_rows),
            "benchmark": len(benchmark_rows),
        },
        "checks": checks,
        "blocking_reasons": [str(check["detail"]) for check in blocking_checks],
        "error": error,
        "data_boundary": (
            "price history ingest imports local CSV evidence only; it does not fetch external market data, "
            "adjust prices, trade, or change observation state"
        ),
    }
    if manifest_path:
        _write_json(manifest_path, payload)
    return (0 if status == "imported" else 1), payload
