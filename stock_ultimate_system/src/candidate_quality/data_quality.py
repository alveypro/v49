from __future__ import annotations

import csv
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DATA_QUALITY_REPORT_VERSION = "candidate_data_quality_report.v1"
DATA_QUALITY_GATE_VERSION = "candidate_data_quality_gate.v1"


@dataclass(frozen=True)
class DataQualityThresholds:
    min_coverage_ratio: float = 0.9
    max_zero_amount_ratio: float = 0.05
    max_nonpositive_price_rows: int = 0
    max_abs_pct_chg: float = 30.0
    max_price_jump_ratio: float = 0.35
    max_delay_trade_days: int = 1


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _as_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _quality_level(score: float, blocking_reasons: list[str]) -> str:
    if blocking_reasons:
        return "blocked"
    if score >= 90:
        return "pass"
    if score >= 70:
        return "review"
    return "blocked"


def _score_stock(
    *,
    coverage_ratio: float,
    zero_amount_ratio: float,
    nonpositive_price_rows: int,
    abnormal_pct_chg_rows: int,
    price_jump_rows: int,
    delay_trade_days: int,
) -> float:
    score = 100.0
    score -= max(0.0, 1.0 - coverage_ratio) * 45.0
    score -= zero_amount_ratio * 25.0
    score -= min(nonpositive_price_rows, 5) * 10.0
    score -= min(abnormal_pct_chg_rows, 5) * 5.0
    score -= min(price_jump_rows, 5) * 5.0
    score -= max(0, delay_trade_days) * 3.0
    return round(max(0.0, min(100.0, score)), 2)


def _fetch_recent_rows(conn: sqlite3.Connection, table: str, lookback_trade_days: int) -> tuple[list[str], list[dict[str, Any]]]:
    date_rows = conn.execute(
        f"SELECT DISTINCT trade_date FROM {table} ORDER BY trade_date DESC LIMIT ?",
        (int(lookback_trade_days),),
    ).fetchall()
    trade_dates = sorted(str(row[0]) for row in date_rows if row and row[0])
    if not trade_dates:
        return [], []
    placeholders = ",".join("?" for _ in trade_dates)
    cursor = conn.execute(
        f"""
        SELECT ts_code, trade_date, open_price, high_price, low_price, close_price,
               pre_close, pct_chg, vol, amount
        FROM {table}
        WHERE trade_date IN ({placeholders})
        ORDER BY ts_code, trade_date
        """,
        trade_dates,
    )
    columns = [desc[0] for desc in cursor.description]
    return trade_dates, [dict(zip(columns, row)) for row in cursor.fetchall()]


def build_data_quality_report(
    *,
    db_path: str | Path,
    table: str = "daily_trading_data",
    lookback_trade_days: int = 60,
    expected_latest_trade_date: str | None = None,
    thresholds: DataQualityThresholds | None = None,
) -> dict[str, Any]:
    resolved_db_path = Path(db_path)
    thresholds = thresholds or DataQualityThresholds()
    if not resolved_db_path.exists():
        return {
            "schema_version": DATA_QUALITY_REPORT_VERSION,
            "status": "failed",
            "generated_at": _utc_now(),
            "db_path": str(resolved_db_path),
            "table": table,
            "lookback_trade_days": int(lookback_trade_days),
            "blocking_reasons": ["database_missing"],
            "summary": {"stock_count": 0, "blocked_count": 0, "review_count": 0, "pass_count": 0},
            "stocks": [],
        }

    conn = sqlite3.connect(str(resolved_db_path))
    try:
        trade_dates, rows = _fetch_recent_rows(conn, table, lookback_trade_days)
    finally:
        conn.close()

    if not trade_dates:
        return {
            "schema_version": DATA_QUALITY_REPORT_VERSION,
            "status": "failed",
            "generated_at": _utc_now(),
            "db_path": str(resolved_db_path),
            "table": table,
            "lookback_trade_days": int(lookback_trade_days),
            "blocking_reasons": ["no_trade_dates"],
            "summary": {"stock_count": 0, "blocked_count": 0, "review_count": 0, "pass_count": 0},
            "stocks": [],
        }

    expected_latest = str(expected_latest_trade_date or trade_dates[-1])
    expected_date_count = len(trade_dates)
    latest_index = {date: idx for idx, date in enumerate(trade_dates)}
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("ts_code") or ""), []).append(row)

    stock_reports: list[dict[str, Any]] = []
    for ts_code, stock_rows in sorted(grouped.items()):
        if not ts_code:
            continue
        stock_rows = sorted(stock_rows, key=lambda item: str(item.get("trade_date") or ""))
        dates_present = {str(row.get("trade_date") or "") for row in stock_rows}
        missing_dates = [date for date in trade_dates if date not in dates_present]
        latest_date = max(dates_present) if dates_present else ""
        coverage_ratio = len(dates_present) / expected_date_count if expected_date_count else 0.0
        zero_amount_rows = 0
        nonpositive_price_rows = 0
        abnormal_pct_chg_rows = 0
        price_jump_rows = 0
        previous_close = 0.0

        for row in stock_rows:
            close_price = _as_float(row.get("close_price"))
            open_price = _as_float(row.get("open_price"))
            high_price = _as_float(row.get("high_price"))
            low_price = _as_float(row.get("low_price"))
            amount = _as_float(row.get("amount"))
            vol = _as_float(row.get("vol"))
            pct_chg = _as_float(row.get("pct_chg"))
            if amount <= 0 or vol <= 0:
                zero_amount_rows += 1
            if close_price <= 0 or open_price <= 0 or high_price <= 0 or low_price <= 0:
                nonpositive_price_rows += 1
            if abs(pct_chg) > thresholds.max_abs_pct_chg:
                abnormal_pct_chg_rows += 1
            if previous_close > 0 and close_price > 0:
                jump_ratio = abs(close_price / previous_close - 1.0)
                if jump_ratio > thresholds.max_price_jump_ratio:
                    price_jump_rows += 1
            if close_price > 0:
                previous_close = close_price

        latest_pos = latest_index.get(latest_date, -1)
        expected_pos = latest_index.get(expected_latest, len(trade_dates) - 1)
        delay_trade_days = max(0, expected_pos - latest_pos) if latest_pos >= 0 else expected_date_count
        zero_amount_ratio = zero_amount_rows / max(len(stock_rows), 1)

        blocking_reasons: list[str] = []
        if coverage_ratio < thresholds.min_coverage_ratio:
            blocking_reasons.append("insufficient_trade_date_coverage")
        if zero_amount_ratio > thresholds.max_zero_amount_ratio:
            blocking_reasons.append("excessive_zero_volume_or_amount")
        if nonpositive_price_rows > thresholds.max_nonpositive_price_rows:
            blocking_reasons.append("nonpositive_price")
        if abnormal_pct_chg_rows > 0:
            blocking_reasons.append("abnormal_pct_chg")
        if price_jump_rows > 0:
            blocking_reasons.append("abnormal_price_jump")
        if delay_trade_days > thresholds.max_delay_trade_days:
            blocking_reasons.append("data_delay")

        score = _score_stock(
            coverage_ratio=coverage_ratio,
            zero_amount_ratio=zero_amount_ratio,
            nonpositive_price_rows=nonpositive_price_rows,
            abnormal_pct_chg_rows=abnormal_pct_chg_rows,
            price_jump_rows=price_jump_rows,
            delay_trade_days=delay_trade_days,
        )
        level = _quality_level(score, blocking_reasons)
        stock_reports.append(
            {
                "ts_code": ts_code,
                "quality_score": score,
                "quality_level": level,
                "blocking_reasons": blocking_reasons,
                "latest_trade_date": latest_date,
                "delay_trade_days": delay_trade_days,
                "coverage_ratio": round(coverage_ratio, 4),
                "missing_trade_dates": missing_dates[:20],
                "missing_trade_date_count": len(missing_dates),
                "zero_amount_rows": zero_amount_rows,
                "zero_amount_ratio": round(zero_amount_ratio, 4),
                "nonpositive_price_rows": nonpositive_price_rows,
                "abnormal_pct_chg_rows": abnormal_pct_chg_rows,
                "price_jump_rows": price_jump_rows,
                "row_count": len(stock_rows),
            }
        )

    blocked_count = sum(1 for item in stock_reports if item["quality_level"] == "blocked")
    review_count = sum(1 for item in stock_reports if item["quality_level"] == "review")
    pass_count = sum(1 for item in stock_reports if item["quality_level"] == "pass")
    status = "failed" if blocked_count == len(stock_reports) and stock_reports else "passed"
    return {
        "schema_version": DATA_QUALITY_REPORT_VERSION,
        "status": status,
        "generated_at": _utc_now(),
        "db_path": str(resolved_db_path),
        "table": table,
        "lookback_trade_days": int(lookback_trade_days),
        "expected_latest_trade_date": expected_latest,
        "trade_date_start": trade_dates[0],
        "trade_date_end": trade_dates[-1],
        "blocking_reasons": ["all_stocks_blocked"] if status == "failed" and stock_reports else [],
        "summary": {
            "stock_count": len(stock_reports),
            "blocked_count": blocked_count,
            "review_count": review_count,
            "pass_count": pass_count,
            "avg_quality_score": round(
                sum(float(item["quality_score"]) for item in stock_reports) / len(stock_reports), 2
            )
            if stock_reports
            else 0.0,
        },
        "stocks": stock_reports,
    }


def write_data_quality_artifacts(
    report: dict[str, Any],
    *,
    output_dir: str | Path,
    report_name: str = "data_quality_report_latest.json",
    stock_csv_name: str = "stock_data_quality_latest.csv",
) -> dict[str, str]:
    resolved_output_dir = Path(output_dir)
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    report_path = resolved_output_dir / report_name
    csv_path = resolved_output_dir / stock_csv_name
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    rows = list(report.get("stocks", []) or [])
    fieldnames = [
        "ts_code",
        "quality_score",
        "quality_level",
        "blocking_reasons",
        "latest_trade_date",
        "delay_trade_days",
        "coverage_ratio",
        "missing_trade_date_count",
        "zero_amount_rows",
        "zero_amount_ratio",
        "nonpositive_price_rows",
        "abnormal_pct_chg_rows",
        "price_jump_rows",
        "row_count",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    key: "|".join(row.get(key, [])) if key == "blocking_reasons" else row.get(key, "")
                    for key in fieldnames
                }
            )
    return {"report_path": str(report_path), "stock_csv_path": str(csv_path)}


def build_candidate_data_quality_gate(
    *,
    report: dict[str, Any],
    candidate_codes: list[str],
) -> dict[str, Any]:
    by_code = {str(item.get("ts_code") or ""): item for item in report.get("stocks", []) or []}
    candidate_results = []
    blocked_codes: list[str] = []
    review_codes: list[str] = []
    for code in candidate_codes:
        normalized = str(code or "").strip()
        stock_quality = by_code.get(normalized)
        if not stock_quality:
            level = "blocked"
            reasons = ["missing_data_quality_record"]
            score = 0.0
        else:
            level = str(stock_quality.get("quality_level") or "blocked")
            reasons = list(stock_quality.get("blocking_reasons") or [])
            score = float(stock_quality.get("quality_score") or 0.0)
        if level == "blocked":
            blocked_codes.append(normalized)
        elif level == "review":
            review_codes.append(normalized)
        candidate_results.append(
            {
                "ts_code": normalized,
                "quality_level": level,
                "quality_score": score,
                "blocking_reasons": reasons,
            }
        )
    status = "failed" if blocked_codes else "review" if review_codes else "passed"
    return {
        "schema_version": DATA_QUALITY_GATE_VERSION,
        "status": status,
        "generated_at": _utc_now(),
        "report_schema_version": report.get("schema_version"),
        "candidate_count": len(candidate_results),
        "blocked_count": len(blocked_codes),
        "review_count": len(review_codes),
        "blocked_codes": blocked_codes,
        "review_codes": review_codes,
        "candidates": candidate_results,
    }


def write_candidate_data_quality_gate(
    gate: dict[str, Any],
    *,
    output_dir: str | Path,
    gate_name: str = "candidate_data_quality_gate_latest.json",
) -> str:
    resolved_output_dir = Path(output_dir)
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    gate_path = resolved_output_dir / gate_name
    gate_path.write_text(json.dumps(gate, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(gate_path)
