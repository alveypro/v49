from __future__ import annotations

import csv
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from src.artifact_registry import sha256_file
from src.primary_result_observation_metrics import _date_key
from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_PRICE_HISTORY_SQLITE_INGEST_VERSION = "primary_result_price_history_sqlite_ingest.v1"
OUTPUT_COLUMNS = ("ts_code", "trade_date", "close")
DEFAULT_ALLOWED_TABLES = ("daily_trading_data",)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(OUTPUT_COLUMNS))
        writer.writeheader()
        writer.writerows(rows)


def _check(name: str, passed: bool, detail: str, details: dict[str, object] | None = None) -> dict[str, object]:
    return {"name": name, "passed": passed, "detail": detail, "details": details or {}}


def _connect_readonly(db_path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute("SELECT 1 FROM sqlite_master WHERE type=? AND name=? LIMIT 1", ("table", table_name)).fetchone()
    return row is not None


def _db_metadata(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    stat = path.stat()
    return {
        "path": str(path),
        "size_bytes": stat.st_size,
        "mtime_utc": datetime.fromtimestamp(stat.st_mtime, timezone.utc).replace(microsecond=0).isoformat(),
    }


def _row_to_output(row: tuple[object, object, object]) -> dict[str, str]:
    ts_code, trade_date, close_price = row
    close = float(close_price)
    if close <= 0:
        raise ValueError("close_price must be positive")
    return {
        "ts_code": _normalize_text(ts_code),
        "trade_date": _date_key(trade_date),
        "close": str(close),
    }


def _fetch_rows(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    ts_codes: tuple[str, str],
    window_start: str,
    window_end: str,
) -> list[dict[str, str]]:
    start = _date_key(window_start).replace("-", "")
    end = _date_key(window_end).replace("-", "")
    rows = conn.execute(
        f"""
        SELECT ts_code, trade_date, close_price
        FROM {table_name}
        WHERE ts_code IN (?, ?)
          AND trade_date >= ?
          AND trade_date <= ?
          AND close_price IS NOT NULL
        ORDER BY ts_code, trade_date
        """,
        (ts_codes[0], ts_codes[1], start, end),
    ).fetchall()
    return [_row_to_output(row) for row in rows]


def import_primary_result_price_history_from_sqlite(
    *,
    sqlite_db_path: str | Path,
    sqlite_table: str = "daily_trading_data",
    output_csv_path: str | Path = "data/experiments/primary_result_price_history_latest.csv",
    manifest_output_path: str | Path | None = None,
    ts_code: str,
    benchmark_ts_code: str = "BENCHMARK",
    window_start: str,
    window_end: str,
    source_label: str = "permanent_stock_database",
    allowed_tables: tuple[str, ...] = DEFAULT_ALLOWED_TABLES,
) -> tuple[int, dict[str, object]]:
    db_path = resolve_project_path(sqlite_db_path)
    output_path = resolve_project_path(output_csv_path)
    manifest_path = resolve_project_path(manifest_output_path) if manifest_output_path else None
    normalized_table = _normalize_text(sqlite_table)
    normalized_ts_code = _normalize_text(ts_code)
    normalized_benchmark_ts_code = _normalize_text(benchmark_ts_code)
    normalized_source_label = _normalize_text(source_label)

    checks = [
        _check("source_label_present", bool(normalized_source_label), "source_label is required"),
        _check("sqlite_db_exists", db_path.exists(), "SQLite database must exist"),
        _check("sqlite_table_allowed", normalized_table in allowed_tables, "SQLite table must be explicitly allowed"),
        _check("ts_code_present", bool(normalized_ts_code), "ts_code is required"),
        _check("benchmark_ts_code_present", bool(normalized_benchmark_ts_code), "benchmark_ts_code is required"),
        _check("window_start_present", bool(_normalize_text(window_start)), "window_start is required"),
        _check("window_end_present", bool(_normalize_text(window_end)), "window_end is required"),
    ]

    filtered_rows: list[dict[str, str]] = []
    output_hash: str | None = None
    error: str | None = None
    table_exists = False

    if db_path.exists() and normalized_table in allowed_tables:
        try:
            with _connect_readonly(db_path) as conn:
                table_exists = _table_exists(conn, normalized_table)
                checks.append(_check("sqlite_table_exists", table_exists, "SQLite table must exist"))
                if table_exists:
                    filtered_rows = _fetch_rows(
                        conn,
                        table_name=normalized_table,
                        ts_codes=(normalized_ts_code, normalized_benchmark_ts_code),
                        window_start=window_start,
                        window_end=window_end,
                    )
        except Exception as exc:
            error = str(exc)
            checks.append(_check("sqlite_readable", False, "SQLite database must be readable in read-only mode", {"error": error}))
    else:
        checks.append(_check("sqlite_table_exists", False, "SQLite table must exist"))

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
        "ingest_version": PRIMARY_RESULT_PRICE_HISTORY_SQLITE_INGEST_VERSION,
        "generated_at": _utc_now_iso(),
        "status": status,
        "source_label": normalized_source_label,
        "sqlite_db": _db_metadata(db_path),
        "sqlite_table": normalized_table,
        "output_csv_path": str(output_path) if status == "imported" else None,
        "output_csv_hash": output_hash,
        "ts_code": normalized_ts_code,
        "benchmark_ts_code": normalized_benchmark_ts_code,
        "window_start": window_start,
        "window_end": window_end,
        "row_counts": {
            "output_total": len(filtered_rows),
            "observed": len(observed_rows),
            "benchmark": len(benchmark_rows),
        },
        "checks": checks,
        "blocking_reasons": [str(check["detail"]) for check in blocking_checks],
        "error": error,
        "data_boundary": (
            "SQLite price history ingest reads a local database in read-only mode and writes canonical CSV evidence only; "
            "it does not update the database, fetch external market data, adjust prices, trade, or change observation state"
        ),
    }
    if manifest_path:
        _write_json(manifest_path, payload)
    return (0 if status == "imported" else 1), payload
