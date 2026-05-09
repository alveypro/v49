from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from src.primary_result_observation_metrics import _date_key
from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_MARKET_DATA_READINESS_VERSION = "primary_result_market_data_readiness.v1"
DEFAULT_ALLOWED_TABLES = ("daily_trading_data",)
QUALITY_REQUIRED_COLUMNS = ("open_price", "high_price", "low_price", "close_price", "pre_close", "pct_chg", "vol", "amount")
MAIN_BOARD_LIMIT = 0.10
GROWTH_BOARD_LIMIT = 0.20


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _check(
    name: str,
    passed: bool,
    detail: str,
    *,
    severity: str = "blocking",
    details: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "name": name,
        "passed": passed,
        "severity": severity,
        "detail": detail,
        "details": details or {},
    }


def _connect_readonly(db_path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"{db_path.resolve().as_uri()}?mode=ro", uri=True)


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute("SELECT 1 FROM sqlite_master WHERE type=? AND name=? LIMIT 1", ("table", table_name)).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}


def _db_metadata(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    stat = path.stat()
    return {
        "path": str(path),
        "size_bytes": stat.st_size,
        "mtime_utc": datetime.fromtimestamp(stat.st_mtime, timezone.utc).replace(microsecond=0).isoformat(),
    }


def _price_limit_for_code(ts_code: str) -> float:
    if ts_code.startswith("3") or ts_code.startswith("68"):
        return GROWTH_BOARD_LIMIT
    return MAIN_BOARD_LIMIT


def _quality_profile(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    ts_code: str,
    window_start: str,
    window_end: str,
    columns: set[str],
) -> dict[str, object]:
    start = _date_key(window_start).replace("-", "")
    end = _date_key(window_end).replace("-", "")
    selected = [
        "trade_date",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "pre_close",
        "pct_chg",
        "vol",
        "amount",
    ]
    available = [column for column in selected if column in columns]
    if "trade_date" not in available:
        available.insert(0, "trade_date")
    rows = conn.execute(
        f"""
        SELECT {", ".join(available)}
        FROM {table_name}
        WHERE ts_code=?
          AND trade_date >= ?
          AND trade_date <= ?
        ORDER BY trade_date
        """,
        (ts_code, start, end),
    ).fetchall()
    limit_threshold = _price_limit_for_code(ts_code)
    missing_required_columns = sorted(set(QUALITY_REQUIRED_COLUMNS) - columns)
    invalid_ohlc_dates: list[str] = []
    missing_pre_close_dates: list[str] = []
    inconsistent_pct_chg_dates: list[str] = []
    zero_volume_dates: list[str] = []
    zero_amount_dates: list[str] = []
    limit_state_dates: list[str] = []
    extreme_move_dates: list[str] = []
    latest_amount: float | None = None
    for raw in rows:
        row = dict(zip(available, raw))
        date = str(row.get("trade_date") or "")
        open_price = _to_float(row.get("open_price"))
        high_price = _to_float(row.get("high_price"))
        low_price = _to_float(row.get("low_price"))
        close_price = _to_float(row.get("close_price"))
        pre_close = _to_float(row.get("pre_close"))
        pct_chg = _to_float(row.get("pct_chg"))
        vol = _to_float(row.get("vol"))
        amount = _to_float(row.get("amount"))
        if amount is not None:
            latest_amount = amount
        if None not in (open_price, high_price, low_price, close_price):
            prices = [float(open_price), float(high_price), float(low_price), float(close_price)]
            if min(prices) <= 0 or float(high_price) < max(float(open_price), float(close_price), float(low_price)) or float(low_price) > min(float(open_price), float(close_price), float(high_price)):
                invalid_ohlc_dates.append(date)
        if pre_close is None or pre_close <= 0:
            missing_pre_close_dates.append(date)
            continue
        move = float(close_price or 0.0) / pre_close - 1.0
        if pct_chg is not None and abs(move * 100.0 - pct_chg) > 0.2:
            inconsistent_pct_chg_dates.append(date)
        if abs(move) >= limit_threshold - 0.0005:
            limit_state_dates.append(date)
        if abs(move) > limit_threshold + 0.03:
            extreme_move_dates.append(date)
        if vol is None or vol <= 0:
            zero_volume_dates.append(date)
        if amount is None or amount <= 0:
            zero_amount_dates.append(date)
    return {
        "ts_code": ts_code,
        "row_count": len(rows),
        "price_limit_threshold": limit_threshold,
        "missing_required_columns": missing_required_columns,
        "invalid_ohlc_dates": invalid_ohlc_dates,
        "missing_pre_close_dates": missing_pre_close_dates,
        "inconsistent_pct_chg_dates": inconsistent_pct_chg_dates,
        "zero_volume_dates": zero_volume_dates,
        "zero_amount_dates": zero_amount_dates,
        "limit_state_dates": limit_state_dates,
        "extreme_move_dates": extreme_move_dates,
        "latest_amount": latest_amount,
    }


def _to_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coverage(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    ts_code: str,
    window_start: str,
    window_end: str,
) -> dict[str, object]:
    start = _date_key(window_start).replace("-", "")
    end = _date_key(window_end).replace("-", "")
    latest_row = conn.execute(f"SELECT MAX(trade_date) FROM {table_name} WHERE ts_code=?", (ts_code,)).fetchone()
    window_row = conn.execute(
        f"""
        SELECT COUNT(*), MIN(trade_date), MAX(trade_date)
        FROM {table_name}
        WHERE ts_code=?
          AND trade_date >= ?
          AND trade_date <= ?
          AND close_price IS NOT NULL
          AND close_price > 0
        """,
        (ts_code, start, end),
    ).fetchone()
    return {
        "ts_code": ts_code,
        "latest_trade_date": str(latest_row[0]) if latest_row and latest_row[0] else None,
        "window_row_count": int(window_row[0]) if window_row and window_row[0] is not None else 0,
        "window_min_trade_date": str(window_row[1]) if window_row and window_row[1] else None,
        "window_max_trade_date": str(window_row[2]) if window_row and window_row[2] else None,
    }


def _latest_update_log(conn: sqlite3.Connection) -> dict[str, object] | None:
    if not _table_exists(conn, "data_update_log"):
        return None
    row = conn.execute(
        """
        SELECT update_type, start_date, end_date, stocks_count, success_count,
               error_count, status, error_message, created_at
        FROM data_update_log
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return None
    keys = (
        "update_type",
        "start_date",
        "end_date",
        "stocks_count",
        "success_count",
        "error_count",
        "status",
        "error_message",
        "created_at",
    )
    return dict(zip(keys, row))


def build_primary_result_market_data_readiness(
    *,
    sqlite_db_path: str | Path,
    sqlite_table: str = "daily_trading_data",
    ts_code: str,
    benchmark_ts_code: str,
    window_start: str,
    window_end: str,
    min_window_rows: int = 2,
    min_target_amount: float = 300_000.0,
    output_path: str | Path | None = None,
    allowed_tables: tuple[str, ...] = DEFAULT_ALLOWED_TABLES,
) -> tuple[int, dict[str, object]]:
    db_path = resolve_project_path(sqlite_db_path)
    normalized_table = _normalize_text(sqlite_table)
    normalized_ts_code = _normalize_text(ts_code)
    normalized_benchmark_ts_code = _normalize_text(benchmark_ts_code)
    normalized_window_start = _normalize_text(window_start)
    normalized_window_end = _normalize_text(window_end)
    min_rows = int(min_window_rows)
    window_order_valid = True
    if normalized_window_start and normalized_window_end:
        window_order_valid = _date_key(normalized_window_end) >= _date_key(normalized_window_start)
    checks = [
        _check("sqlite_db_exists", db_path.exists(), "SQLite database must exist"),
        _check("sqlite_table_allowed", normalized_table in allowed_tables, "SQLite table must be explicitly allowed"),
        _check("ts_code_present", bool(normalized_ts_code), "ts_code is required"),
        _check("benchmark_ts_code_present", bool(normalized_benchmark_ts_code), "benchmark_ts_code is required"),
        _check("window_start_present", bool(normalized_window_start), "window_start is required"),
        _check("window_end_present", bool(normalized_window_end), "window_end is required"),
        _check("window_order_valid", window_order_valid, "window_end must be greater than or equal to window_start"),
        _check("min_window_rows_valid", min_rows >= 2, "min_window_rows must be at least 2"),
    ]

    target_coverage: dict[str, object] | None = None
    benchmark_coverage: dict[str, object] | None = None
    target_quality: dict[str, object] | None = None
    benchmark_quality: dict[str, object] | None = None
    latest_update_log: dict[str, object] | None = None
    table_columns: set[str] = set()
    error: str | None = None

    if db_path.exists() and normalized_table in allowed_tables and window_order_valid:
        try:
            with _connect_readonly(db_path) as conn:
                table_exists = _table_exists(conn, normalized_table)
                checks.append(_check("sqlite_table_exists", table_exists, "SQLite table must exist"))
                latest_update_log = _latest_update_log(conn)
                checks.append(
                    _check(
                        "update_log_present",
                        latest_update_log is not None,
                        "latest update log should exist for operational traceability",
                        severity="warning",
                    )
                )
                if table_exists:
                    table_columns = _table_columns(conn, normalized_table)
                    target_coverage = _coverage(
                        conn,
                        table_name=normalized_table,
                        ts_code=normalized_ts_code,
                        window_start=normalized_window_start,
                        window_end=normalized_window_end,
                    )
                    benchmark_coverage = _coverage(
                        conn,
                        table_name=normalized_table,
                        ts_code=normalized_benchmark_ts_code,
                        window_start=normalized_window_start,
                        window_end=normalized_window_end,
                    )
                    target_quality = _quality_profile(
                        conn,
                        table_name=normalized_table,
                        ts_code=normalized_ts_code,
                        window_start=normalized_window_start,
                        window_end=normalized_window_end,
                        columns=table_columns,
                    )
                    benchmark_quality = _quality_profile(
                        conn,
                        table_name=normalized_table,
                        ts_code=normalized_benchmark_ts_code,
                        window_start=normalized_window_start,
                        window_end=normalized_window_end,
                        columns=table_columns,
                    )
        except Exception as exc:
            error = str(exc)
            checks.append(
                _check(
                    "sqlite_readable",
                    False,
                    "SQLite database must be readable in read-only mode",
                    details={"error": error},
                )
            )
    elif not db_path.exists() or normalized_table not in allowed_tables:
        checks.append(_check("sqlite_table_exists", False, "SQLite table must exist"))

    if target_coverage is not None and benchmark_coverage is not None:
        target_rows = int(target_coverage["window_row_count"])
        benchmark_rows = int(benchmark_coverage["window_row_count"])
        window_end_key = _date_key(normalized_window_end).replace("-", "")
        checks.extend(
            [
                _check(
                    "target_window_has_min_rows",
                    target_rows >= min_rows,
                    "target ts_code must have enough rows in the observation window",
                    details={"required": min_rows, "actual": target_rows},
                ),
                _check(
                    "benchmark_window_has_min_rows",
                    benchmark_rows >= min_rows,
                    "benchmark ts_code must have enough rows in the observation window",
                    details={"required": min_rows, "actual": benchmark_rows},
                ),
                _check(
                    "target_latest_covers_window_end",
                    bool(target_coverage.get("latest_trade_date"))
                    and str(target_coverage["latest_trade_date"]) >= window_end_key,
                    "target latest trade date must cover window_end",
                    details={"latest_trade_date": target_coverage.get("latest_trade_date"), "window_end": window_end_key},
                ),
                _check(
                    "benchmark_latest_covers_window_end",
                    bool(benchmark_coverage.get("latest_trade_date"))
                    and str(benchmark_coverage["latest_trade_date"]) >= window_end_key,
                    "benchmark latest trade date must cover window_end",
                    details={"latest_trade_date": benchmark_coverage.get("latest_trade_date"), "window_end": window_end_key},
                ),
            ]
        )

    if target_quality is not None and benchmark_quality is not None:
        target_missing_columns = list(target_quality["missing_required_columns"])
        benchmark_missing_columns = list(benchmark_quality["missing_required_columns"])
        latest_amount = target_quality.get("latest_amount")
        checks.extend(
            [
                _check(
                    "quality_required_columns_present",
                    not target_missing_columns and not benchmark_missing_columns,
                    "quality gate columns must exist for price integrity, liquidity, and limit-state checks",
                    details={
                        "required_columns": list(QUALITY_REQUIRED_COLUMNS),
                        "table_columns": sorted(table_columns),
                        "target_missing_columns": target_missing_columns,
                        "benchmark_missing_columns": benchmark_missing_columns,
                    },
                ),
                _check(
                    "target_ohlc_consistent",
                    not target_quality["invalid_ohlc_dates"],
                    "target OHLC prices must be positive and internally consistent",
                    details={"dates": target_quality["invalid_ohlc_dates"]},
                ),
                _check(
                    "benchmark_ohlc_consistent",
                    not benchmark_quality["invalid_ohlc_dates"],
                    "benchmark OHLC prices must be positive and internally consistent",
                    details={"dates": benchmark_quality["invalid_ohlc_dates"]},
                ),
                _check(
                    "target_pre_close_present",
                    not target_quality["missing_pre_close_dates"],
                    "target pre_close must be present for adjustment and limit-state validation",
                    details={"dates": target_quality["missing_pre_close_dates"]},
                ),
                _check(
                    "benchmark_pre_close_present",
                    not benchmark_quality["missing_pre_close_dates"],
                    "benchmark pre_close must be present for adjustment validation",
                    details={"dates": benchmark_quality["missing_pre_close_dates"]},
                ),
                _check(
                    "target_pct_chg_consistent",
                    not target_quality["inconsistent_pct_chg_dates"],
                    "target pct_chg must match close/pre_close within tolerance",
                    details={"dates": target_quality["inconsistent_pct_chg_dates"]},
                ),
                _check(
                    "benchmark_pct_chg_consistent",
                    not benchmark_quality["inconsistent_pct_chg_dates"],
                    "benchmark pct_chg must match close/pre_close within tolerance",
                    details={"dates": benchmark_quality["inconsistent_pct_chg_dates"]},
                ),
                _check(
                    "target_has_positive_volume_and_amount",
                    not target_quality["zero_volume_dates"] and not target_quality["zero_amount_dates"],
                    "target must have positive volume and amount in the observation window",
                    details={
                        "zero_volume_dates": target_quality["zero_volume_dates"],
                        "zero_amount_dates": target_quality["zero_amount_dates"],
                    },
                ),
                _check(
                    "benchmark_has_positive_volume_and_amount",
                    not benchmark_quality["zero_volume_dates"] and not benchmark_quality["zero_amount_dates"],
                    "benchmark must have positive volume and amount in the observation window",
                    details={
                        "zero_volume_dates": benchmark_quality["zero_volume_dates"],
                        "zero_amount_dates": benchmark_quality["zero_amount_dates"],
                    },
                ),
                _check(
                    "target_latest_amount_meets_capacity_floor",
                    latest_amount is not None and float(latest_amount) >= float(min_target_amount),
                    "target latest amount must meet the liquidity capacity floor",
                    details={"required": float(min_target_amount), "actual": latest_amount},
                ),
                _check(
                    "target_no_limit_state_in_observation_window",
                    not target_quality["limit_state_dates"],
                    "target observation window must not include limit-up or limit-down states",
                    details={"dates": target_quality["limit_state_dates"]},
                ),
                _check(
                    "target_no_extreme_move_beyond_board_limit",
                    not target_quality["extreme_move_dates"],
                    "target move must not exceed its board limit plus tolerance",
                    details={"dates": target_quality["extreme_move_dates"]},
                ),
            ]
        )

    blocking_checks = [check for check in checks if check["severity"] == "blocking" and check["passed"] is not True]
    payload = {
        "readiness_version": PRIMARY_RESULT_MARKET_DATA_READINESS_VERSION,
        "generated_at": _utc_now_iso(),
        "status": "ready" if not blocking_checks else "blocked",
        "sqlite_db": _db_metadata(db_path),
        "sqlite_table": normalized_table,
        "ts_code": normalized_ts_code,
        "benchmark_ts_code": normalized_benchmark_ts_code,
        "window_start": normalized_window_start,
        "window_end": normalized_window_end,
        "min_window_rows": min_rows,
        "min_target_amount": float(min_target_amount),
        "target_coverage": target_coverage,
        "benchmark_coverage": benchmark_coverage,
        "target_quality": target_quality,
        "benchmark_quality": benchmark_quality,
        "latest_update_log": latest_update_log,
        "checks": checks,
        "blocking_reasons": [str(check["detail"]) for check in blocking_checks],
        "error": error,
        "production_boundary": (
            "market data readiness is diagnostics only; it does not update market data, import CSV evidence, "
            "close observation, record terminal outcome, trade, or change strategy state"
        ),
    }
    if output_path:
        _write_json(resolve_project_path(output_path), payload)
    return (0 if payload["status"] == "ready" else 1), payload
