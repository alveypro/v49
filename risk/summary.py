from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime
from typing import Dict

from data.dao import (
    DataAccessError,
    recent_trade_profile,
    table_exists,
    table_has_column,
    table_max_value,
)


def compute_health_report(
    db_path: str,
    enable_fund_bonus: bool = True,
    fund_portfolio_funds: str = "",
    evolution_last_run_path: str = "",
) -> Dict:
    report = {
        "run_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "ok": True,
        "warnings": [],
        "stats": {},
    }
    report["stats"]["db_path"] = str(db_path or "")
    has_hard_issue = False

    if not db_path or not os.path.exists(db_path):
        report["ok"] = False
        report["warnings"].append("database not found")
        has_hard_issue = True
        return report

    conn = sqlite3.connect(db_path)
    try:
        try:
            profile = recent_trade_profile(conn, date_limit=10, recent_window=3)
        except DataAccessError:
            profile = {
                "daily_table": "daily_trading_data",
                "last_trade_date": "",
                "records_last_trade_date": 0,
                "recent_trade_dates": [],
                "recent_counts": {},
            }

        daily_table = str(profile.get("daily_table", "daily_trading_data"))
        last_trade = str(profile.get("last_trade_date", "") or "")
        distinct_dates = list(profile.get("recent_trade_dates", []) or [])
        recent_counts = dict(profile.get("recent_counts", {}) or {})

        report["stats"]["daily_table"] = daily_table
        report["stats"]["last_trade_date"] = last_trade
        report["stats"]["records_last_trade_date"] = int(profile.get("records_last_trade_date", 0) or 0)
        report["stats"]["recent_trade_dates"] = distinct_dates
        report["stats"]["recent_counts"] = recent_counts

        if last_trade and report["stats"]["records_last_trade_date"] < 2000:
            report["warnings"].append(f"{daily_table} records low: {report['stats']['records_last_trade_date']}")
        if last_trade and len(distinct_dates) < 5:
            report["warnings"].append("recent trade dates < 5")
        if recent_counts:
            vals = list(recent_counts.values())
            if min(vals) < 2000:
                report["warnings"].append("recent trade day records low (<2000)")
            if len(vals) >= 2 and vals[0] > 0:
                drop_ratio = (vals[0] - vals[-1]) / max(vals[0], 1)
                if drop_ratio > 0.3:
                    report["warnings"].append("recent trade day records drop >30%")

        table_checks = {
            "northbound_flow": "trade_date",
            "margin_summary": "trade_date",
            "margin_detail": "trade_date",
            "moneyflow_daily": "trade_date",
            "moneyflow_ind_ths": "trade_date",
            "top_list": "trade_date",
            "top_inst": "trade_date",
        }
        if fund_portfolio_funds.strip():
            table_checks["fund_portfolio_cache"] = "trade_date"

        max_fund_table_lag = int(os.getenv("OPENCLAW_FUND_TABLE_MAX_LAG_TRADING_DAYS", "7"))
        max_lag_by_table = {
            "northbound_flow": int(os.getenv("OPENCLAW_LAG_NORTHBOUND_FLOW", "3")),
            "margin_summary": int(os.getenv("OPENCLAW_LAG_MARGIN_SUMMARY", "3")),
            "margin_detail": int(os.getenv("OPENCLAW_LAG_MARGIN_DETAIL", "5")),
            "moneyflow_daily": int(os.getenv("OPENCLAW_LAG_MONEYFLOW_DAILY", "7")),
            "moneyflow_ind_ths": int(os.getenv("OPENCLAW_LAG_MONEYFLOW_IND", "7")),
            "top_list": int(os.getenv("OPENCLAW_LAG_TOP_LIST", "7")),
            "top_inst": int(os.getenv("OPENCLAW_LAG_TOP_INST", "7")),
            "fund_portfolio_cache": int(os.getenv("OPENCLAW_LAG_FUND_PORTFOLIO", "7")),
        }
        strict_fund_lag = os.getenv("OPENCLAW_FUND_TABLE_LAG_STRICT", "0") == "1"

        if enable_fund_bonus:
            for table, col in table_checks.items():
                if not table_exists(conn, table):
                    report["warnings"].append(f"table missing: {table}")
                    has_hard_issue = True
                    continue
                if not table_has_column(conn, table, col):
                    report["warnings"].append(f"{table} missing column: {col}")
                    has_hard_issue = True
                    continue
                max_date = table_max_value(conn, table, col)
                report["stats"][f"{table}_max_date"] = max_date
                lag_trading_days = None
                if distinct_dates and max_date:
                    try:
                        lag_trading_days = distinct_dates.index(str(max_date))
                    except ValueError:
                        lag_trading_days = len(distinct_dates)
                report["stats"][f"{table}_lag_trading_days"] = lag_trading_days

                table_lag_limit = int(max_lag_by_table.get(table, max_fund_table_lag))
                if lag_trading_days is not None and lag_trading_days > table_lag_limit:
                    if last_trade and max_date and str(max_date) < str(last_trade):
                        report["warnings"].append(f"{table} lagging: {max_date} < {last_trade}")
                    report["warnings"].append(
                        f"{table} not updated for {lag_trading_days} trading days (> {table_lag_limit})"
                    )
                    if strict_fund_lag:
                        has_hard_issue = True
    finally:
        conn.close()

    try:
        if evolution_last_run_path and os.path.exists(evolution_last_run_path):
            with open(evolution_last_run_path, "r", encoding="utf-8") as f:
                evo = json.load(f)
            stats = evo.get("stats", {})
            win_rate = stats.get("win_rate")
            max_dd = stats.get("max_drawdown")
            if win_rate is not None and win_rate < 40:
                report["warnings"].append(f"win_rate low: {win_rate}")
                has_hard_issue = True
            if max_dd is not None and max_dd < -30:
                report["warnings"].append(f"max_drawdown high: {max_dd}")
                has_hard_issue = True
    except Exception as e:
        report["warnings"].append(f"evolution stats read failed: {e}")
        has_hard_issue = True

    if has_hard_issue:
        report["ok"] = False
    return report
