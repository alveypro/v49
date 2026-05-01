from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any, Dict, Iterable, Tuple


DEFAULT_QUALITY_TABLES: Tuple[Tuple[str, str, bool], ...] = (
    ("daily_trading_data", "trade_date", True),
    ("moneyflow_daily", "trade_date", False),
    ("top_list", "trade_date", False),
    ("margin_detail", "trade_date", False),
)


def _parse_date(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text[:10] if "-" in text or "/" in text else text[:8], fmt)
        except ValueError:
            continue
    return None


def _table_snapshot(conn: sqlite3.Connection, table: str, date_column: str, required: bool) -> Dict[str, Any]:
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    if not exists:
        return {
            "table": table,
            "date_column": date_column,
            "required": required,
            "exists": False,
            "row_count": 0,
            "latest_date": "",
            "status": "failed" if required else "warning",
            "reason": "missing_table",
        }
    row = conn.execute(f"SELECT COUNT(*), MAX({date_column}) FROM {table}").fetchone()
    row_count = int((row or [0, ""])[0] or 0)
    latest_date = str((row or [0, ""])[1] or "")
    if row_count <= 0:
        status = "failed" if required else "warning"
        reason = "empty_table"
    elif not latest_date:
        status = "failed" if required else "warning"
        reason = "missing_latest_date"
    else:
        status = "passed"
        reason = ""
    return {
        "table": table,
        "date_column": date_column,
        "required": required,
        "exists": True,
        "row_count": row_count,
        "latest_date": latest_date,
        "status": status,
        "reason": reason,
    }


def evaluate_data_quality(
    conn: sqlite3.Connection,
    *,
    tables: Iterable[Tuple[str, str, bool]] = DEFAULT_QUALITY_TABLES,
    max_required_age_days: int = 7,
) -> Dict[str, Any]:
    checks = [_table_snapshot(conn, table, date_column, required) for table, date_column, required in tables]
    blocking = []
    warnings = []
    latest_required_dates = []
    now = datetime.now()
    for check in checks:
        if check["status"] == "failed":
            blocking.append(check["reason"] + ":" + check["table"])
        elif check["status"] == "warning":
            warnings.append(check["reason"] + ":" + check["table"])
        parsed = _parse_date(check.get("latest_date"))
        if parsed and check.get("required"):
            latest_required_dates.append(parsed)
            age_days = max(0, (now.date() - parsed.date()).days)
            check["age_days"] = age_days
            if age_days > int(max_required_age_days):
                blocking.append(f"stale_required_table:{check['table']}")
        else:
            check["age_days"] = None

    latest_trade_date = max(latest_required_dates).strftime("%Y%m%d") if latest_required_dates else ""
    passed = not blocking
    return {
        "passed": passed,
        "gate": "passed" if passed else "blocked",
        "latest_trade_date": latest_trade_date,
        "blocking_reasons": list(dict.fromkeys(blocking)),
        "warning_reasons": list(dict.fromkeys(warnings)),
        "checks": checks,
    }
