#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
自动进化引擎（v49）
- 收盘后自动更新数据（Tushare）
- 自动回测 + 全参数网格优化（v4.0评分器）
- 写入最佳参数与报告
- 可选自动 git push 同步到网站
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import tushare as ts

from comprehensive_stock_evaluator_v4 import ComprehensiveStockEvaluatorV4
from comprehensive_stock_evaluator_v6_ultimate import ComprehensiveStockEvaluatorV6Ultimate
from comprehensive_stock_evaluator_v7_ultimate import ComprehensiveStockEvaluatorV7Ultimate
from comprehensive_stock_evaluator_v8_ultimate import ComprehensiveStockEvaluatorV8Ultimate
from optimized_backtest_strategy_v49 import backtest_with_dynamic_strategy


ROOT = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(ROOT, "config.json")
TOKEN_PATH = os.path.join(ROOT, "tushare_token.txt")
EVOLUTION_DIR = os.path.join(ROOT, "evolution")
LOG_PATH = os.path.join(ROOT, "auto_evolve.log")
LOCK_PATH = "/tmp/auto_evolve.lock"
SSE_INDEX_CODE = "000001.SH"
UNIFIED_PROFILE_PATH = os.path.join(EVOLUTION_DIR, "production_unified_profile.json")
PROMOTION_HISTORY_PATH = os.path.join(EVOLUTION_DIR, "promotion_history.jsonl")
RISK_SENTINEL_PATH = os.path.join(EVOLUTION_DIR, "risk_sentinel.json")
RISK_SENTINEL_CANDIDATE_PATH = os.path.join(EVOLUTION_DIR, "risk_sentinel_last_candidate.json")


def _fetch_trade_cal_with_retry(
    pro: ts.pro_api,
    start_date: str,
    end_date: str,
    is_open: str | None = None,
    retries: int = 3,
    retry_sleep_seconds: float = 0.6,
) -> pd.DataFrame:
    """Fetch trade calendar with retries to reduce transient network failures."""
    last_error = None
    for attempt in range(retries):
        try:
            kwargs = {"exchange": "SSE", "start_date": start_date, "end_date": end_date}
            if is_open is not None:
                kwargs["is_open"] = is_open
            trade_cal = pro.trade_cal(**kwargs)
            if trade_cal is not None:
                return trade_cal
        except Exception as e:
            last_error = e
        if attempt < retries - 1:
            time.sleep(retry_sleep_seconds * (attempt + 1))
    if last_error:
        LOGGER.warning("trade_cal fetch failed after retries: %s", last_error)
    return pd.DataFrame()


def _get_last_trade_date(pro: ts.pro_api) -> str | None:
    """Get last open trade date from SSE calendar."""
    cn_now = _get_cn_now()
    end_date = cn_now.strftime("%Y%m%d")
    start_date = (cn_now - timedelta(days=20)).strftime("%Y%m%d")
    try:
        trade_cal = _fetch_trade_cal_with_retry(pro, start_date, end_date, is_open="1")
        if trade_cal is None or trade_cal.empty:
            return _infer_last_trade_date()
        return trade_cal["cal_date"].iloc[-1]
    except Exception:
        return _infer_last_trade_date()


def _get_cn_now() -> datetime:
    """Return current time in Asia/Shanghai, fallback to local time if zoneinfo unavailable."""
    try:
        return datetime.now(ZoneInfo("Asia/Shanghai"))
    except Exception:
        return datetime.now()


def _in_heavy_job_window(cn_now: datetime) -> bool:
    """
    Guard heavy optimization jobs to run in an off-peak night window.
    Default window: [01:00, 09:00) Asia/Shanghai.
    """
    start_hour = int(os.getenv("AUTO_EVOLVE_WINDOW_START_HOUR", "1"))
    end_hour = int(os.getenv("AUTO_EVOLVE_WINDOW_END_HOUR", "9"))
    if start_hour == end_hour:
        return True
    if start_hour < end_hour:
        return start_hour <= cn_now.hour < end_hour
    # Cross-midnight window (e.g. 22 -> 6)
    return cn_now.hour >= start_hour or cn_now.hour < end_hour


def _previous_weekday(d: datetime) -> datetime:
    """Move back to the most recent weekday (Mon-Fri)."""
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def _infer_last_trade_date() -> str:
    """Infer last trade date when trade calendar is unavailable."""
    cn_now = _get_cn_now()
    if cn_now.weekday() >= 5:
        return _previous_weekday(cn_now).strftime("%Y%m%d")
    # If it's a trading day but before close, assume last trade was previous weekday.
    if cn_now.hour < 16:
        return _previous_weekday(cn_now - timedelta(days=1)).strftime("%Y%m%d")
    return cn_now.strftime("%Y%m%d")


def _get_db_latest_trade_date(db_path: str) -> str | None:
    try:
        conn = _connect(db_path)
        # Prefer the newer one between SSE index and overall max trade_date.
        df_idx = pd.read_sql_query(
            f"SELECT MAX(trade_date) AS max_date FROM daily_trading_data WHERE ts_code = '{SSE_INDEX_CODE}'",
            conn,
        )
        idx_max = str(df_idx["max_date"].iloc[0]) if (df_idx is not None and not df_idx.empty and df_idx["max_date"].iloc[0]) else None

        df_all = pd.read_sql_query("SELECT MAX(trade_date) AS max_date FROM daily_trading_data", conn)
        conn.close()
        if df_all is None or df_all.empty:
            return None
        all_max = str(df_all["max_date"].iloc[0]) if df_all["max_date"].iloc[0] else None
        if idx_max and all_max:
            return max(idx_max, all_max)
        return idx_max or all_max
    except Exception:
        return None


def _get_recent_trade_date(pro: ts.pro_api, lookback_days: int = 30) -> str | None:
    """Get most recent open trade date within lookback window."""
    cn_now = _get_cn_now()
    end_date = cn_now.strftime("%Y%m%d")
    start_date = (cn_now - timedelta(days=lookback_days)).strftime("%Y%m%d")
    try:
        trade_cal = _fetch_trade_cal_with_retry(pro, start_date, end_date, is_open="1")
        if trade_cal is None or trade_cal.empty:
            return _infer_last_trade_date()
        return trade_cal["cal_date"].iloc[-1]
    except Exception:
        return _infer_last_trade_date()


def _get_recent_trade_dates_from_db(db_path: str, limit: int = 8) -> List[str]:
    if not db_path or not os.path.exists(db_path):
        return []
    try:
        conn = _connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT DISTINCT trade_date FROM daily_trading_data ORDER BY trade_date DESC LIMIT ?",
            (limit,),
        )
        rows = [row[0] for row in cursor.fetchall() if row and row[0]]
        conn.close()
        return [str(r) for r in rows]
    except Exception:
        return []


def _is_trade_day(pro: ts.pro_api, date_str: str) -> bool:
    try:
        cal = _fetch_trade_cal_with_retry(pro, date_str, date_str, retries=3, retry_sleep_seconds=0.4)
        if cal is None or cal.empty:
            # Fallback: avoid false red caused by transient calendar API issues.
            try:
                inferred = _infer_last_trade_date()
                return inferred == date_str
            except Exception:
                return False
        return str(cal["is_open"].iloc[0]) == "1"
    except Exception:
        return False


def _get_data_ready_time(cn_now: datetime) -> datetime:
    close_hour = int(os.getenv("TRADE_CLOSE_HOUR", "15"))
    delay_hours = int(os.getenv("DATA_READY_DELAY_HOURS", "3"))
    return cn_now.replace(hour=close_hour + delay_hours, minute=0, second=0, microsecond=0)


def _pick_best_trade_date(
    pro: ts.pro_api,
    db_path: str,
    fetch_fn,
    lookback_days: int = 60,
) -> Tuple[str | None, pd.DataFrame]:
    """Try DB recent dates first, then fallback to trade_cal last date."""
    candidates = _get_recent_trade_dates_from_db(db_path, limit=8)
    trade_cal_date = _get_recent_trade_date(pro, lookback_days=lookback_days)
    if trade_cal_date:
        candidates.append(trade_cal_date)
    seen = set()
    for d in candidates:
        if not d or d in seen:
            continue
        seen.add(d)
        try:
            df = fetch_fn(d)
            if df is not None and not df.empty:
                return d, df
        except Exception:
            continue
    return None, pd.DataFrame()


def _data_freshness_status(db_path: str) -> Tuple[bool, bool, str | None, str | None, bool, str | None]:
    """Return (fresh, enforce, db_last, last_trade, is_trade_day, ready_time_str)."""
    token = _load_tushare_token()
    if not token:
        return False, True, None, None, False, None
    pro = ts.pro_api(token)
    cn_now = _get_cn_now()
    today = cn_now.strftime("%Y%m%d")
    is_trade_day = _is_trade_day(pro, today)
    ready_time = _get_data_ready_time(cn_now)
    last_trade = _get_last_trade_date(pro)
    db_last = _get_db_latest_trade_date(db_path)
    if db_last is None:
        return False, True, db_last, last_trade, is_trade_day, ready_time.strftime("%Y-%m-%d %H:%M")
    # Allow DB to be ahead when trade_cal is delayed.
    if last_trade is None:
        return True, False, db_last, last_trade, is_trade_day, ready_time.strftime("%Y-%m-%d %H:%M")
    fresh = db_last >= last_trade
    # Enforce freshness only after data is expected to be ready (trade day + close + delay).
    if is_trade_day and cn_now >= ready_time:
        enforce = True
    else:
        enforce = False
    return fresh, enforce, db_last, last_trade, is_trade_day, ready_time.strftime("%Y-%m-%d %H:%M")


def _ensure_table(conn: sqlite3.Connection, ddl: str) -> None:
    try:
        conn.execute(ddl)
        conn.commit()
    except Exception:
        pass


def _append_df(conn: sqlite3.Connection, table: str, df: pd.DataFrame, trade_date: str | None = None) -> int:
    if df is None or df.empty:
        return 0
    if trade_date and "trade_date" in df.columns:
        try:
            conn.execute(f"DELETE FROM {table} WHERE trade_date = ?", (trade_date,))
        except Exception:
            pass
    df.to_sql(table, conn, if_exists="append", index=False)
    return len(df)


def _append_df_safe(
    conn: sqlite3.Connection,
    table: str,
    df: pd.DataFrame,
    trade_date: str | None = None,
) -> int:
    """Append with schema repair if table has incompatible columns."""
    if df is None or df.empty:
        return 0
    try:
        return _append_df(conn, table, df, trade_date=trade_date)
    except Exception as e:
        msg = str(e)
        if "has no column named" in msg or "no such column" in msg:
            try:
                conn.execute(f"DROP TABLE IF EXISTS {table}")
                conn.commit()
            except Exception:
                pass
            df.to_sql(table, conn, if_exists="replace", index=False)
            return len(df)
        raise


def _normalize_akshare_northbound(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    cols = df.columns.tolist()
    date_col = None
    for c in ("date", "日期", "trade_date"):
        if c in cols:
            date_col = c
            break
    value_col = None
    for c in ("value", "当日成交净买额", "当日资金流入"):
        if c in cols:
            value_col = c
            break
    if not date_col or not value_col:
        return pd.DataFrame()
    out = df[[date_col, value_col]].copy()
    out.columns = ["trade_date", "north_money"]
    out["trade_date"] = out["trade_date"].astype(str).str.replace("-", "")
    out["north_money"] = pd.to_numeric(out["north_money"], errors="coerce").fillna(0.0)
    return out


def _fetch_akshare_northbound() -> pd.DataFrame:
    try:
        import akshare as ak  # type: ignore
    except Exception:
        return pd.DataFrame()
    # Try multiple function names across versions
    fn_candidates = [
        ("stock_em_hsgt_north_net_flow_in", {"indicator": "北上"}),
        ("stock_hsgt_north_net_flow_in_em", {"symbol": "北上"}),
        ("stock_hsgt_hist_em", {"symbol": "北向资金"}),
    ]
    for fn_name, kwargs in fn_candidates:
        fn = getattr(ak, fn_name, None)
        if not callable(fn):
            continue
        try:
            df = fn(**kwargs)
            norm = _normalize_akshare_northbound(df)
            if not norm.empty:
                return norm
        except Exception:
            continue
    return pd.DataFrame()


def _update_northbound(db_path: str) -> Dict:
    token = _load_tushare_token()
    if not token:
        return {"success": False, "error": "Tushare token not found"}
    pro = ts.pro_api(token)

    last_trade = _get_db_latest_trade_date(db_path) or _get_recent_trade_date(pro, lookback_days=60)
    if not last_trade:
        return {"success": False, "error": "no recent trade date"}

    try:
        start_date = (datetime.strptime(last_trade, "%Y%m%d") - timedelta(days=60)).strftime("%Y%m%d")
    except Exception:
        start_date = (datetime.now() - timedelta(days=60)).strftime("%Y%m%d")
    try:
        df = pro.moneyflow_hsgt(start_date=start_date, end_date=last_trade)
    except Exception as e:
        return {"success": False, "error": f"moneyflow_hsgt failed: {e}"}

    # 如果Tushare数据滞后，尝试用AkShare补齐
    try:
        db_last = _get_db_latest_trade_date(db_path)
        df_max = str(df["trade_date"].max()) if (df is not None and not df.empty and "trade_date" in df.columns) else None
    except Exception:
        db_last, df_max = None, None

    if df is None or df.empty or (db_last and df_max and df_max < db_last):
        ak_df = _fetch_akshare_northbound()
        if ak_df is not None and not ak_df.empty:
            df = ak_df
            try:
                df_max = str(df["trade_date"].max()) if "trade_date" in df.columns else None
            except Exception:
                df_max = None

    if df is None or df.empty:
        return {"success": False, "error": "moneyflow_hsgt empty"}
    if df_max:
        last_trade = df_max

    conn = _connect(db_path)
    _ensure_table(conn, """
        CREATE TABLE IF NOT EXISTS northbound_flow (
            trade_date TEXT PRIMARY KEY,
            north_money REAL,
            south_money REAL,
            gg_net REAL,
            hg_net REAL,
            sg_net REAL
        )
    """)

    cur = conn.cursor()
    updated = 0
    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT OR REPLACE INTO northbound_flow
            (trade_date, north_money, south_money, gg_net, hg_net, sg_net)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                row.get("trade_date"),
                row.get("north_money", 0),
                row.get("south_money", 0),
                row.get("gg_net", 0),
                row.get("hg_net", 0),
                row.get("sg_net", 0),
            ),
        )
        updated += 1
        if updated % 500 == 0:
            conn.commit()
    conn.commit()
    conn.close()
    return {"success": True, "updated": updated, "last_trade": last_trade}


def _update_margin(db_path: str) -> Dict:
    token = _load_tushare_token()
    if not token:
        return {"success": False, "error": "Tushare token not found"}
    pro = ts.pro_api(token)

    last_trade = _get_db_latest_trade_date(db_path) or _get_recent_trade_date(pro, lookback_days=60)
    if not last_trade:
        return {"success": False, "error": "no recent trade date"}

    try:
        start_date = (datetime.strptime(last_trade, "%Y%m%d") - timedelta(days=60)).strftime("%Y%m%d")
    except Exception:
        start_date = (datetime.now() - timedelta(days=60)).strftime("%Y%m%d")
    try:
        df = pro.margin(start_date=start_date, end_date=last_trade)
    except Exception as e:
        return {"success": False, "error": f"margin failed: {e}"}

    if df is None or df.empty:
        return {"success": False, "error": "margin empty"}

    conn = _connect(db_path)
    _ensure_table(conn, """
        CREATE TABLE IF NOT EXISTS margin_summary (
            trade_date TEXT PRIMARY KEY,
            rzye REAL,
            rqye REAL,
            rzrqye REAL
        )
    """)

    cur = conn.cursor()
    updated = 0
    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT OR REPLACE INTO margin_summary
            (trade_date, rzye, rqye, rzrqye)
            VALUES (?, ?, ?, ?)
            """,
            (
                row.get("trade_date"),
                row.get("rzye", 0),
                row.get("rqye", 0),
                row.get("rzrqye", 0),
            ),
        )
        updated += 1
        if updated % 500 == 0:
            conn.commit()
    conn.commit()
    conn.close()
    return {"success": True, "updated": updated, "last_trade": last_trade}


def _update_margin_detail(db_path: str) -> Dict:
    token = _load_tushare_token()
    if not token:
        return {"success": False, "error": "Tushare token not found"}
    pro = ts.pro_api(token)
    last_trade, df = _pick_best_trade_date(
        pro,
        db_path,
        fetch_fn=lambda d: pro.margin_detail(trade_date=d),
        lookback_days=60,
    )
    if not last_trade or df is None or df.empty:
        return {"success": False, "error": "margin_detail empty"}
    conn = _connect(db_path)
    updated = _append_df_safe(conn, "margin_detail", df, trade_date=last_trade)
    conn.commit()
    conn.close()
    return {"success": True, "updated": updated, "last_trade": last_trade}


def _update_moneyflow_daily(db_path: str) -> Dict:
    token = _load_tushare_token()
    if not token:
        return {"success": False, "error": "Tushare token not found"}
    pro = ts.pro_api(token)
    last_trade, df = _pick_best_trade_date(
        pro,
        db_path,
        fetch_fn=lambda d: pro.moneyflow(trade_date=d),
        lookback_days=60,
    )
    if not last_trade or df is None or df.empty:
        return {"success": False, "error": "moneyflow empty"}
    conn = _connect(db_path)
    updated = _append_df_safe(conn, "moneyflow_daily", df, trade_date=last_trade)
    conn.commit()
    conn.close()
    return {"success": True, "updated": updated, "last_trade": last_trade}


def _update_moneyflow_industry(db_path: str) -> Dict:
    token = _load_tushare_token()
    if not token:
        return {"success": False, "error": "Tushare token not found"}
    pro = ts.pro_api(token)
    last_trade, df = _pick_best_trade_date(
        pro,
        db_path,
        fetch_fn=lambda d: pro.moneyflow_ind_ths(trade_date=d),
        lookback_days=60,
    )
    if not last_trade or df is None or df.empty:
        return {"success": False, "error": "moneyflow_ind_ths empty"}
    conn = _connect(db_path)
    updated = _append_df_safe(conn, "moneyflow_ind_ths", df, trade_date=last_trade)
    conn.commit()
    conn.close()
    return {"success": True, "updated": updated, "last_trade": last_trade}


def _update_top_list(db_path: str) -> Dict:
    token = _load_tushare_token()
    if not token:
        return {"success": False, "error": "Tushare token not found"}
    pro = ts.pro_api(token)
    last_trade, df = _pick_best_trade_date(
        pro,
        db_path,
        fetch_fn=lambda d: pro.top_list(trade_date=d),
        lookback_days=60,
    )
    if not last_trade or df is None or df.empty:
        return {"success": False, "error": "top_list empty"}
    conn = _connect(db_path)
    updated = _append_df_safe(conn, "top_list", df, trade_date=last_trade)
    conn.commit()
    conn.close()
    return {"success": True, "updated": updated, "last_trade": last_trade}


def _update_top_inst(db_path: str) -> Dict:
    token = _load_tushare_token()
    if not token:
        return {"success": False, "error": "Tushare token not found"}
    pro = ts.pro_api(token)
    last_trade, df = _pick_best_trade_date(
        pro,
        db_path,
        fetch_fn=lambda d: pro.top_inst(trade_date=d),
        lookback_days=60,
    )
    if not last_trade or df is None or df.empty:
        return {"success": False, "error": "top_inst empty"}
    conn = _connect(db_path)
    updated = _append_df_safe(conn, "top_inst", df, trade_date=last_trade)
    conn.commit()
    conn.close()
    return {"success": True, "updated": updated, "last_trade": last_trade}


def _update_fund_portfolio(db_path: str) -> Dict:
    """Optional fund holdings snapshot. Controlled via FUND_PORTFOLIO_FUNDS env (comma-separated)."""
    funds_raw = os.getenv("FUND_PORTFOLIO_FUNDS", "").strip()
    if not funds_raw:
        return {"success": False, "error": "FUND_PORTFOLIO_FUNDS not set"}
    funds = [f.strip() for f in funds_raw.split(",") if f.strip()]
    if not funds:
        return {"success": False, "error": "no funds provided"}

    token = _load_tushare_token()
    if not token:
        return {"success": False, "error": "Tushare token not found"}
    pro = ts.pro_api(token)

    # Default to last year/quarter if not provided
    year = os.getenv("FUND_PORTFOLIO_YEAR")
    quarter = os.getenv("FUND_PORTFOLIO_QUARTER")
    if not year or not quarter:
        today = datetime.now()
        year = str(today.year - (1 if today.month < 4 else 0))
        quarter = "4"

    conn = _connect(db_path)
    _ensure_table(conn, """
        CREATE TABLE IF NOT EXISTS fund_portfolio_cache (
            ts_code TEXT,
            end_date TEXT,
            symbol TEXT,
            mkt_value REAL,
            stk_mkt_value REAL,
            hold_ratio REAL,
            PRIMARY KEY (ts_code, end_date, symbol)
        )
    """)

    cur = conn.cursor()
    updated = 0
    for fund in funds:
        try:
            df = pro.fund_portfolio(ts_code=fund, year=year, quarter=quarter)
        except Exception:
            continue
        if df is None or df.empty:
            continue
        for _, row in df.iterrows():
            cur.execute(
                """
                INSERT OR REPLACE INTO fund_portfolio_cache
                (ts_code, end_date, symbol, mkt_value, stk_mkt_value, hold_ratio)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    row.get("ts_code"),
                    row.get("end_date"),
                    row.get("symbol"),
                    row.get("mkt_value", 0),
                    row.get("stk_mkt_value", 0),
                    row.get("hold_ratio", 0),
                ),
            )
            updated += 1
        conn.commit()
    conn.close()
    return {"success": True, "updated": updated, "year": year, "quarter": quarter}


def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("auto_evolve")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    fh = logging.FileHandler(LOG_PATH)
    fh.setFormatter(formatter)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


LOGGER = _setup_logger()


def _env_int(name: str, default: int) -> int:
    try:
        val = os.getenv(name)
        return int(val) if val is not None and val != "" else default
    except Exception:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "y", "on"}


def _load_unified_profile() -> Dict:
    if not os.path.exists(UNIFIED_PROFILE_PATH):
        return {}
    try:
        with open(UNIFIED_PROFILE_PATH, "r", encoding="utf-8") as f:
            obj = json.load(f) or {}
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _get_evolve_settings() -> Dict[str, int | bool]:
    fast = _env_bool("EVOLVE_FAST", False)
    max_seconds = _env_int("EVOLVE_MAX_SECONDS", 900 if fast else 0)
    log_every = _env_int("EVOLVE_LOG_EVERY", 5 if fast else 10)
    settings = {
        "fast": fast,
        "max_seconds": max_seconds,
        "log_every": max(1, log_every),
        "sample_v4": _env_int("EVOLVE_SAMPLE_SIZE", 300 if fast else 800),
        "sample_v5": _env_int("EVOLVE_SAMPLE_SIZE_V5", 400 if fast else 1200),
        "sample_v6": _env_int("EVOLVE_SAMPLE_SIZE_V6", 400 if fast else 1200),
        "sample_v7": _env_int("EVOLVE_SAMPLE_SIZE_V7", 250 if fast else 600),
        "sample_v8": _env_int("EVOLVE_SAMPLE_SIZE_V8", 250 if fast else 500),
        "sample_v9": _env_int("EVOLVE_SAMPLE_SIZE_V9", 300 if fast else 600),
        "sample_stable": _env_int("EVOLVE_SAMPLE_SIZE_STABLE", 200 if fast else 400),
        "sample_ai": _env_int("EVOLVE_SAMPLE_SIZE_AI", 200 if fast else 400),
    }
    unified = _load_unified_profile()
    strategies = (unified.get("strategies") or {}) if isinstance(unified, dict) else {}
    for sk, key in (("v5", "sample_v5"), ("v8", "sample_v8"), ("v9", "sample_v9")):
        p = strategies.get(sk) or {}
        try:
            candidate_count = int(float(p.get("candidate_count", 0) or 0))
        except Exception:
            candidate_count = 0
        if candidate_count > 0:
            # Keep optimization practical: sample ~= 30% of candidate pool, clamped.
            settings[key] = int(max(200, min(2000, round(candidate_count * 0.30))))
    return settings


EVOLVE_SETTINGS = _get_evolve_settings()


def _safe_int(value, default: int) -> int:
    try:
        if value is None:
            return int(default)
        return int(round(float(value)))
    except Exception:
        return int(default)


def _safe_float_local(value, default: float) -> float:
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _grid_values_around(base: int, low: int, high: int, step: int, radius: int = 1) -> List[int]:
    out: List[int] = []
    for i in range(-radius, radius + 1):
        v = base + i * step
        if low <= v <= high:
            out.append(v)
    uniq = sorted(set(out))
    return uniq if uniq else [max(low, min(high, base))]


def _unified_strategy_params(strategy: str, defaults: Dict) -> Dict:
    unified = _load_unified_profile()
    strategies = (unified.get("strategies") or {}) if isinstance(unified, dict) else {}
    st = str(strategy or "").strip().lower()
    p = strategies.get(st) if isinstance(strategies, dict) else None
    if not isinstance(p, dict):
        return dict(defaults)
    out = dict(defaults)
    out.update({k: v for k, v in p.items() if v is not None})
    return out


def _evolve_targets() -> set[str]:
    targets = {"v9", "v8", "v5", "combo"}
    only = os.getenv("EVOLVE_ONLY", "").strip()
    exclude = os.getenv("EVOLVE_EXCLUDE", "").strip()
    include_experimental = os.getenv("EVOLVE_INCLUDE_EXPERIMENTAL", "0").strip().lower() in {"1", "true", "yes"}
    if include_experimental:
        targets |= {"v4", "ai_v5", "ai_v2", "v6", "v7", "stable"}
    if only:
        targets = {t.strip().lower() for t in only.split(",") if t.strip()}
    if exclude:
        targets = {t for t in targets if t not in {x.strip().lower() for x in exclude.split(",") if x.strip()}}
    return targets


def _run_phase() -> str:
    phase = (os.getenv("AUTO_EVOLVE_PHASE", "full") or "full").strip().lower()
    if phase not in {"full", "data_only", "optimize_only"}:
        return "full"
    return phase


def _load_config() -> Dict:
    if not os.path.exists(CONFIG_PATH):
        return {}
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _load_tushare_token() -> str | None:
    token = os.getenv("TUSHARE_TOKEN")
    if token:
        return token.strip()
    if os.path.exists(TOKEN_PATH):
        with open(TOKEN_PATH, "r", encoding="utf-8") as f:
            return f.read().strip()
    config = _load_config()
    token = config.get("TUSHARE_TOKEN")
    return token.strip() if token else None


def _connect(db_path: str, timeout: int = 30) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=timeout, check_same_thread=False)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
    except Exception:
        pass
    return conn


def _update_stock_data(db_path: str, days: int = 30) -> Dict:
    token = _load_tushare_token()
    if not token:
        return {"success": False, "error": "Tushare token not found"}
    # Avoid writing token files to user home (launchd + sandbox safe)
    pro = ts.pro_api(token)

    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=days + 10)).strftime("%Y%m%d")

    try:
        trade_cal = _fetch_trade_cal_with_retry(pro, start_date, end_date, is_open="1")
    except Exception:
        trade_cal = pd.DataFrame()

    if trade_cal is None or trade_cal.empty:
        fallback_date = _infer_last_trade_date()
        trade_dates = [fallback_date]
        LOGGER.warning("trade calendar unavailable, fallback to inferred trade date: %s", fallback_date)
    else:
        trade_dates = trade_cal["cal_date"].tolist()[-days:]

    conn = _connect(db_path)
    cur = conn.cursor()

    cur.execute("SELECT ts_code FROM stock_basic LIMIT 5000")
    stock_codes = [row[0] for row in cur.fetchall()]
    if not stock_codes:
        conn.close()
        return {"success": False, "error": "no stock list in db"}

    updated_days = 0
    failed_days = 0
    total_records = 0

    for i, trade_date in enumerate(trade_dates, 1):
        try:
            df = pro.daily(trade_date=trade_date)
            if df is None or df.empty:
                continue
            df = df[df["ts_code"].isin(stock_codes)]
            for _, row in df.iterrows():
                cur.execute(
                    """
                    INSERT OR REPLACE INTO daily_trading_data
                    (ts_code, trade_date, open_price, high_price, low_price,
                     close_price, pre_close, vol, amount, pct_chg)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["ts_code"],
                        row["trade_date"],
                        row.get("open", 0),
                        row.get("high", 0),
                        row.get("low", 0),
                        row.get("close", 0),
                        row.get("pre_close", 0),
                        row.get("vol", 0),
                        row.get("amount", 0),
                        row.get("pct_chg", 0),
                    ),
                )
                total_records += 1
            updated_days += 1
            if i % 10 == 0:
                conn.commit()
                LOGGER.info("update progress %s/%s", i, len(trade_dates))
        except Exception:
            failed_days += 1
            continue

    conn.commit()
    conn.close()

    return {
        "success": True,
        "updated_days": updated_days,
        "failed_days": failed_days,
        "total_records": total_records,
    }


def _update_market_cap(db_path: str) -> Dict:
    token = _load_tushare_token()
    if not token:
        return {"success": False, "error": "Tushare token not found"}
    # Avoid writing token files to user home (launchd + sandbox safe)
    pro = ts.pro_api(token)

    conn = _connect(db_path)
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(stock_basic)")
    columns = [row[1] for row in cur.fetchall()]
    if "circ_mv" not in columns:
        cur.execute("ALTER TABLE stock_basic ADD COLUMN circ_mv REAL DEFAULT 0")
    if "total_mv" not in columns:
        cur.execute("ALTER TABLE stock_basic ADD COLUMN total_mv REAL DEFAULT 0")

    cur.execute("SELECT ts_code FROM stock_basic")
    local_stocks = set(row[0] for row in cur.fetchall())

    market_data = None
    for i in range(8):
        check_date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
        try:
            market_data = pro.daily_basic(
                trade_date=check_date,
                fields="ts_code,trade_date,close,circ_mv,total_mv",
            )
            if market_data is not None and not market_data.empty:
                break
        except Exception:
            continue

    if market_data is None or market_data.empty:
        conn.close()
        return {"success": False, "error": "failed to fetch market cap"}

    updated_count = 0
    for _, row in market_data.iterrows():
        ts_code = row["ts_code"]
        if ts_code not in local_stocks:
            continue
        circ_mv = row.get("circ_mv", 0) if pd.notna(row.get("circ_mv")) else 0
        total_mv = row.get("total_mv", 0) if pd.notna(row.get("total_mv")) else 0
        cur.execute(
            "UPDATE stock_basic SET circ_mv = ?, total_mv = ? WHERE ts_code = ?",
            (circ_mv, total_mv, ts_code),
        )
        updated_count += 1
        if updated_count % 500 == 0:
            conn.commit()

    conn.commit()
    conn.close()

    return {"success": True, "updated_count": updated_count}


def _load_backtest_data(db_path: str, lookback_days: int = 420) -> pd.DataFrame:
    start_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y%m%d")
    conn = _connect(db_path)
    query = """
        SELECT dtd.ts_code, sb.name, sb.industry, dtd.trade_date,
               dtd.open_price, dtd.high_price, dtd.low_price,
               dtd.close_price, dtd.vol, dtd.amount, dtd.pct_chg
        FROM daily_trading_data dtd
        INNER JOIN stock_basic sb ON dtd.ts_code = sb.ts_code
        WHERE dtd.trade_date >= ?
        ORDER BY dtd.ts_code, dtd.trade_date
    """
    df = pd.read_sql_query(query, conn, params=(start_date,))
    conn.close()
    return df


def _load_index_data(db_path: str, lookback_days: int = 420) -> pd.DataFrame:
    start_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y%m%d")
    conn = _connect(db_path)
    query = """
        SELECT trade_date, open_price, high_price, low_price, close_price, vol, amount, pct_chg
        FROM daily_trading_data
        WHERE ts_code = '000001.SH' AND trade_date >= ?
        ORDER BY trade_date
    """
    df = pd.read_sql_query(query, conn, params=(start_date,))
    conn.close()
    return df


def _ensure_evolution_tables(db_path: str) -> None:
    conn = _connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS evolution_best_params (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy TEXT,
            run_at TEXT,
            params_json TEXT,
            stats_json TEXT,
            score REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS evolution_run_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy TEXT,
            run_at TEXT,
            params_json TEXT,
            stats_json TEXT,
            score REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS evolution_ai_best (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy TEXT,
            run_at TEXT,
            params_json TEXT,
            stats_json TEXT,
            score REAL
        )
        """
    )
    conn.commit()
    conn.close()


MIN_WIN_RATE = 45.0
MAX_DRAWDOWN = -12.0

STRATEGY_GATES = {
    "V4": {
        "min_signals": 30,
        "min_win_rate": 48.0,
        "min_sharpe": 0.15,
        "min_score_delta": 0.35,
        "max_drawdown_floor": -15.0,
        "cooldown_hours": 6,
        "oos_min_win_rate": 45.0,
    },
    "V5": {
        "min_signals": 20,
        "min_win_rate": 45.0,
        "min_sharpe": 0.0,
        "min_score_delta": 0.25,
        "max_drawdown_floor": -18.0,
        "cooldown_hours": 6,
        "oos_min_win_rate": 42.0,
        "allow_orange_if_no_hard_fail": True,
        "max_score_regress": 12.0,
        "max_param_drift": 1.0,
        "require_rolling_validation": True,
    },
    "V6": {
        "min_signals": 20,
        "min_win_rate": 45.0,
        "min_sharpe": 0.0,
        "min_score_delta": 0.25,
        "max_drawdown_floor": -18.0,
        "cooldown_hours": 6,
        "oos_min_win_rate": 42.0,
    },
    "V7": {
        "min_signals": 20,
        "min_win_rate": 45.0,
        "min_sharpe": 0.0,
        "min_score_delta": 0.25,
        "max_drawdown_floor": -18.0,
        "cooldown_hours": 6,
        "oos_min_win_rate": 42.0,
    },
    "V8": {
        "min_signals": 20,
        "min_win_rate": 45.0,
        "min_sharpe": 0.0,
        "min_score_delta": 0.25,
        "max_drawdown_floor": -18.0,
        "cooldown_hours": 6,
        "oos_min_win_rate": 42.0,
        "require_rolling_validation": True,
    },
    "V9": {
        "min_signals": 16,
        "min_win_rate": 45.0,
        "min_sharpe": -0.2,
        "min_score_delta": 0.25,
        "max_drawdown_floor": -28.0,
        "cooldown_hours": 6,
        "oos_min_win_rate": 38.0,
        "allow_orange_if_no_hard_fail": True,
        "max_score_regress": 10.0,
        "max_param_drift": 1.0,
        "require_rolling_validation": True,
    },
    "COMBO": {
        "min_signals": 15,
        "min_win_rate": 45.0,
        "min_sharpe": 0.0,
        "min_score_delta": 0.20,
        "max_drawdown_floor": -18.0,
        "cooldown_hours": 6,
        "oos_min_win_rate": 42.0,
        "require_rolling_validation": True,
    },
    "STABLE_UPTREND": {
        "min_signals": 10,
        "min_win_rate": 45.0,
        "min_sharpe": 0.0,
        "min_score_delta": 0.20,
        "max_drawdown_floor": -15.0,
        "cooldown_hours": 12,
        "oos_min_win_rate": 42.0,
    },
    "AI_V5": {
        "min_signals": 15,
        "min_win_rate": 45.0,
        "min_sharpe": 0.0,
        "min_score_delta": 0.20,
        "max_drawdown_floor": -18.0,
        "cooldown_hours": 6,
        "oos_min_win_rate": 42.0,
    },
    "AI_V2": {
        "min_signals": 15,
        "min_win_rate": 45.0,
        "min_sharpe": 0.0,
        "min_score_delta": 0.20,
        "max_drawdown_floor": -18.0,
        "cooldown_hours": 6,
        "oos_min_win_rate": 42.0,
    },
}


def _score_result(stats: Dict) -> float:
    sharpe = stats.get("sharpe_ratio", 0) or 0
    w_avg = stats.get("weighted_avg_return", 0) or 0
    win = stats.get("win_rate", 0) or 0
    avg_ret = stats.get("avg_return", 0) or 0
    max_dd = stats.get("max_drawdown", 0) or 0
    score = (sharpe * 1.5) + (w_avg * 0.12) + (avg_ret * 0.08) + (win * 0.02) - (abs(max_dd) * 0.05)
    # Soft penalties instead of hard-drop to keep optimizer producing candidates.
    if win < MIN_WIN_RATE:
        score -= (MIN_WIN_RATE - win) * 0.35
    if max_dd < MAX_DRAWDOWN:
        score -= (abs(max_dd - MAX_DRAWDOWN)) * 0.4
    return float(score)


def _composite_score(stats: Dict) -> float:
    sharpe = float(stats.get("sharpe_ratio", 0) or 0)
    w_avg = float(stats.get("weighted_avg_return", 0) or 0)
    win = float(stats.get("win_rate", 0) or 0)
    avg_ret = float(stats.get("avg_return", 0) or 0)
    max_dd = float(stats.get("max_drawdown", 0) or 0)
    return float((sharpe * 1.5) + (w_avg * 0.12) + (avg_ret * 0.08) + (win * 0.02) - (abs(max_dd) * 0.05))


def _safe_dt(s: str) -> datetime | None:
    text = str(s or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y%m%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text[:19], fmt)
        except Exception:
            continue
    return None


def _safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _load_json(path: str) -> Dict:
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f) or {}
    except Exception:
        pass
    return {}


def _write_json(path: str, payload: Dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _append_jsonl(path: str, payload: Dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _strategy_active_file(strategy: str) -> str:
    st = strategy.strip().upper()
    mapping = {
        "V4": "best_params.json",
        "V5": "v5_best.json",
        "V6": "v6_best.json",
        "V7": "v7_best.json",
        "V8": "v8_best.json",
        "V9": "v9_best.json",
        "COMBO": "combo_best.json",
        "STABLE_UPTREND": "stable_uptrend_best.json",
        "AI_V5": "ai_v5_best.json",
        "AI_V2": "ai_v2_best.json",
    }
    return os.path.join(EVOLUTION_DIR, mapping.get(st, f"{st.lower()}_best.json"))


def _strategy_candidate_file(strategy: str) -> str:
    st = strategy.strip().upper()
    return os.path.join(EVOLUTION_DIR, f"{st.lower()}_candidate.json")


def _calc_param_drift(old_params: Dict, new_params: Dict) -> float:
    if not old_params:
        return 0.0
    keys = sorted(set(old_params.keys()) | set(new_params.keys()))
    if not keys:
        return 0.0
    changed = 0
    for k in keys:
        ov = old_params.get(k)
        nv = new_params.get(k)
        if isinstance(ov, (int, float)) and isinstance(nv, (int, float)):
            base = abs(float(ov)) if abs(float(ov)) > 1e-9 else 1.0
            if abs(float(nv) - float(ov)) / base > 0.15:
                changed += 1
        else:
            if ov != nv:
                changed += 1
    return float(changed / len(keys))


def _build_risk_sentinel(
    strategy: str,
    candidate_stats: Dict,
    incumbent_stats: Dict,
    db_path: str,
) -> Dict:
    st = strategy.strip().upper()
    gates = STRATEGY_GATES.get(st, STRATEGY_GATES["V5"])
    rules: List[str] = []
    evidence: Dict[str, object] = {}
    level = "green"
    actions: List[str] = []

    win_rate = _safe_float(candidate_stats.get("win_rate"), 0.0)
    max_dd = _safe_float(candidate_stats.get("max_drawdown"), 0.0)
    total_signals = int(_safe_float(candidate_stats.get("total_signals"), 0))
    inc_win = _safe_float((incumbent_stats or {}).get("win_rate"), 0.0)
    win_drop = (inc_win - win_rate) if inc_win > 0 else 0.0

    evidence.update(
        {
            "strategy": st,
            "candidate_win_rate": win_rate,
            "candidate_max_drawdown": max_dd,
            "candidate_total_signals": total_signals,
            "incumbent_win_rate": inc_win,
            "win_rate_drop": win_drop,
        }
    )

    if total_signals < int(gates["min_signals"]):
        rules.append("signal_density_collapse")
        level = "orange"
    if win_drop >= 8.0:
        rules.append("rolling_win_rate_drift")
        level = "orange"
    if max_dd < float(gates["max_drawdown_floor"]):
        rules.append("max_drawdown_breach")
        level = "red"
    if win_rate < float(gates["min_win_rate"]) - 6.0:
        rules.append("hard_win_rate_breach")
        level = "red"

    fresh, enforce, db_last, last_trade, is_trade_day, ready_time = _data_freshness_status(db_path)
    evidence.update(
        {
            "data_fresh": fresh,
            "freshness_enforce": enforce,
            "db_last_trade": db_last,
            "calendar_last_trade": last_trade,
            "is_trade_day": is_trade_day,
            "ready_time": ready_time,
        }
    )
    if not fresh and enforce:
        rules.append("data_freshness_breach")
        level = "red"
    elif not fresh:
        rules.append("data_freshness_warning")
        if level == "green":
            level = "yellow"

    if level == "green":
        actions = ["保持当前自动进化频率"]
    elif level == "yellow":
        actions = ["保持自动化，但提高参数晋升门槛", "优先检查数据更新链路"]
    elif level == "orange":
        actions = ["降低自动化激进度", "仅允许小步参数变更", "增加人工复核"]
    else:
        actions = ["停止自动晋升，维持现有生产参数", "人工排查数据与策略健康后再恢复"]

    return {
        "run_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "risk_level": level,
        "triggered_rules": rules,
        "recommended_actions": actions,
        "evidence": evidence,
    }


def _promote_candidate(
    strategy: str,
    candidate_report: Dict,
    db_path: str,
) -> Dict:
    st = strategy.strip().upper()
    gates = STRATEGY_GATES.get(st, STRATEGY_GATES["V5"])
    now = datetime.now()
    active_path = _strategy_active_file(st)
    incumbent = _load_json(active_path)
    incumbent_stats = incumbent.get("stats", {}) if isinstance(incumbent, dict) else {}
    incumbent_params = incumbent.get("params", {}) if isinstance(incumbent, dict) else {}
    incumbent_score = _safe_float(incumbent.get("score"), -1e9) if incumbent else -1e9
    incumbent_run_at = _safe_dt(str(incumbent.get("run_at", ""))) if incumbent else None

    cand_stats = candidate_report.get("stats", {}) or {}
    cand_params = candidate_report.get("params", {}) or {}
    cand_score = _safe_float(candidate_report.get("score"), -1e9)
    validation_mode = str(candidate_report.get("validation_mode", "") or "").strip().lower()
    oos = candidate_report.get("oos_stats", {}) or {}
    bootstrap_mode = not bool(incumbent)

    hard_fail_reasons: List[str] = []
    if int(_safe_float(cand_stats.get("total_signals"), 0)) < int(gates["min_signals"]):
        hard_fail_reasons.append("total_signals below min")
    if _safe_float(cand_stats.get("win_rate"), 0.0) < float(gates["min_win_rate"]):
        hard_fail_reasons.append("win_rate below min")
    if _safe_float(cand_stats.get("sharpe_ratio"), 0.0) < float(gates["min_sharpe"]):
        hard_fail_reasons.append("sharpe below min")
    if _safe_float(cand_stats.get("max_drawdown"), 0.0) < float(gates["max_drawdown_floor"]):
        hard_fail_reasons.append("max_drawdown breach")
    if bool(gates.get("require_rolling_validation", False)) and validation_mode != "rolling":
        hard_fail_reasons.append(f"validation_mode={validation_mode or 'unknown'} not rolling")
    if oos and (not bootstrap_mode):
        if _safe_float(oos.get("win_rate"), 0.0) < float(gates["oos_min_win_rate"]):
            hard_fail_reasons.append("oos win_rate below min")
        if int(_safe_float(oos.get("total_signals"), 0)) < max(5, int(gates["min_signals"] // 3)):
            hard_fail_reasons.append("oos signals too low")

    cooldown_block = False
    if incumbent_run_at is not None:
        hours = (now - incumbent_run_at).total_seconds() / 3600.0
        if hours < float(gates["cooldown_hours"]):
            cooldown_block = True

    score_delta = cand_score - incumbent_score if incumbent else cand_score
    drift = _calc_param_drift(incumbent_params, cand_params) if incumbent_params else 0.0
    enough_improve = score_delta >= float(gates["min_score_delta"])
    conservative_override = (
        _safe_float(cand_stats.get("max_drawdown"), 0.0) > _safe_float(incumbent_stats.get("max_drawdown"), -999.0) + 2.0
        and _safe_float(cand_stats.get("win_rate"), 0.0) >= _safe_float(incumbent_stats.get("win_rate"), 0.0) - 1.5
    )

    risk = _build_risk_sentinel(st, cand_stats, incumbent_stats, db_path)
    risk_level = risk.get("risk_level", "green")
    _write_json(RISK_SENTINEL_CANDIDATE_PATH, risk)

    allow_orange = bool(gates.get("allow_orange_if_no_hard_fail", False))
    max_score_regress = float(gates.get("max_score_regress", 0.0))
    max_param_drift = float(gates.get("max_param_drift", 0.8))
    risk_gate_ok = (risk_level != "red") and (risk_level != "orange" or (allow_orange and len(hard_fail_reasons) == 0))
    score_gate_ok = enough_improve or conservative_override or bootstrap_mode or (score_delta >= -max_score_regress)

    approved = (
        (len(hard_fail_reasons) == 0)
        and risk_gate_ok
        and ((not cooldown_block) or enough_improve)
        and score_gate_ok
        and (drift <= max_param_drift or score_delta >= float(gates["min_score_delta"]) + 0.3)
    )

    decision = {
        "strategy": st,
        "approved": approved,
        "run_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "hard_fail_reasons": hard_fail_reasons,
        "risk_level": risk_level,
        "risk_rules": risk.get("triggered_rules", []),
        "cooldown_block": cooldown_block,
        "score_delta": round(score_delta, 6),
        "param_drift": round(drift, 6),
        "validation_mode": validation_mode or "unknown",
        "incumbent_score": incumbent_score if incumbent else None,
        "candidate_score": cand_score,
    }

    _append_jsonl(PROMOTION_HISTORY_PATH, {"decision": decision, "candidate": candidate_report})
    _write_json(_strategy_candidate_file(st), candidate_report)

    if approved:
        _write_json(RISK_SENTINEL_PATH, risk)
        if os.path.exists(active_path):
            bak_path = f"{active_path}.bak_{now.strftime('%Y%m%d_%H%M%S')}"
            try:
                with open(active_path, "r", encoding="utf-8") as src, open(bak_path, "w", encoding="utf-8") as dst:
                    dst.write(src.read())
            except Exception:
                pass
        _write_json(active_path, candidate_report)
    else:
        # Do not let rejected low-sample candidates poison global risk state.
        hard_rules = set(risk.get("triggered_rules") or [])
        severe = bool(hard_rules & {"data_freshness_breach", "max_drawdown_breach", "hard_win_rate_breach"})
        if severe:
            _write_json(RISK_SENTINEL_PATH, risk)
    return decision


def _grid_search(df: pd.DataFrame) -> Tuple[Dict, Dict, float]:
    evaluator = ComprehensiveStockEvaluatorV4()
    np.random.seed(42)

    if EVOLVE_SETTINGS.get("fast"):
        thresholds = [60, 65]
        max_holding_days = [10, 15]
        stop_losses = [-6.0, -5.0]
        take_profits = [6.0, 8.0]
    else:
        thresholds = [55, 60, 65, 70]
        max_holding_days = [10, 15, 20]
        stop_losses = [-6.0, -5.0, -4.0]
        take_profits = [6.0, 8.0, 10.0]

    best_params = {}
    best_stats = {}
    best_score = -1e9
    start_ts = datetime.now().timestamp()
    max_seconds = int(EVOLVE_SETTINGS.get("max_seconds", 0))
    log_every = int(EVOLVE_SETTINGS.get("log_every", 10))
    total = len(thresholds) * len(max_holding_days) * len(stop_losses) * len(take_profits)
    tried = 0
    stop_early = False

    for threshold in thresholds:
        for hold_days in max_holding_days:
            for stop_loss in stop_losses:
                for take_profit in take_profits:
                    if max_seconds > 0 and (datetime.now().timestamp() - start_ts) > max_seconds:
                        stop_early = True
                        LOGGER.warning("grid search v4 time limit reached (%ss), returning best so far", max_seconds)
                        break
                    tried += 1
                    result = backtest_with_dynamic_strategy(
                        evaluator,
                        df,
                        sample_size=int(EVOLVE_SETTINGS.get("sample_v4", 800)),
                        score_threshold=threshold,
                        max_holding_days=hold_days,
                        stop_loss_pct=stop_loss,
                        take_profit_pct=take_profit,
                    )
                    if not result.get("success"):
                        continue
                    stats = result.get("stats", {})
                    score = _score_result(stats)
                    if score > best_score:
                        best_score = score
                        best_params = {
                            "score_threshold": threshold,
                            "max_holding_days": hold_days,
                            "stop_loss_pct": stop_loss,
                            "take_profit_pct": take_profit,
                        }
                        best_stats = stats
                        LOGGER.info("new best score=%.3f params=%s", best_score, best_params)
                    if tried % log_every == 0:
                        elapsed = int(datetime.now().timestamp() - start_ts)
                        LOGGER.info("grid v4 progress: %s/%s combos, elapsed=%ss, best=%.3f",
                                    tried, total, elapsed, best_score)
                if stop_early:
                    break
            if stop_early:
                break
        if stop_early:
            break

    return best_params, best_stats, best_score


def _write_reports(best_params: Dict, best_stats: Dict, best_score: float, oos_stats: Dict | None = None, db_path: str = "") -> Dict:
    os.makedirs(EVOLUTION_DIR, exist_ok=True)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    report = {
        "strategy": "综合优选v4.0",
        "run_at": now,
        "score": best_score,
        "params": best_params,
        "stats": best_stats,
        "oos_stats": oos_stats or {},
    }
    decision = _promote_candidate("V4", report, db_path=db_path) if db_path else {"approved": True}
    report["promotion_decision"] = decision
    if decision.get("approved", False):
        with open(os.path.join(EVOLUTION_DIR, "last_run.json"), "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        flat = {
            "run_at": now,
            "score": best_score,
            **{f"param_{k}": v for k, v in best_params.items()},
            **{f"stat_{k}": v for k, v in best_stats.items()},
        }
        df = pd.DataFrame([flat])
        df.to_csv(os.path.join(EVOLUTION_DIR, "last_run.csv"), index=False)
    else:
        with open(os.path.join(EVOLUTION_DIR, "last_run_attempt_v4.json"), "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
    return decision


def _write_ai_report(
    filename: str,
    strategy: str,
    best_params: Dict,
    best_stats: Dict,
    best_score: float,
    oos_stats: Dict | None = None,
    db_path: str = "",
    validation_mode: str = "single",
) -> Dict:
    os.makedirs(EVOLUTION_DIR, exist_ok=True)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    report = {
        "strategy": strategy,
        "run_at": now,
        "score": best_score,
        "params": best_params,
        "stats": best_stats,
        "oos_stats": oos_stats or {},
        "validation_mode": str(validation_mode or "single"),
    }
    decision = _promote_candidate(strategy, report, db_path=db_path) if db_path else {"approved": True}
    report["promotion_decision"] = decision
    if decision.get("approved", False):
        with open(os.path.join(EVOLUTION_DIR, filename), "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
    else:
        with open(os.path.join(EVOLUTION_DIR, f"{strategy.lower()}_last_attempt.json"), "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
    return decision


def _save_to_db(db_path: str, best_params: Dict, best_stats: Dict, best_score: float, approved: bool = True) -> None:
    _ensure_evolution_tables(db_path)
    conn = _connect(db_path)
    cur = conn.cursor()
    run_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    params_json = json.dumps(best_params, ensure_ascii=False)
    stats_json = json.dumps(best_stats, ensure_ascii=False)
    if approved:
        cur.execute(
            """
            INSERT INTO evolution_best_params (strategy, run_at, params_json, stats_json, score)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("综合优选v4.0", run_at, params_json, stats_json, best_score),
        )
    cur.execute(
        """
        INSERT INTO evolution_run_history (strategy, run_at, params_json, stats_json, score)
        VALUES (?, ?, ?, ?, ?)
        """,
        ("综合优选v4.0", run_at, params_json, stats_json, best_score),
    )
    conn.commit()
    conn.close()


def _save_ai_to_db(db_path: str, strategy: str, best_params: Dict, best_stats: Dict, best_score: float, approved: bool = True) -> None:
    _ensure_evolution_tables(db_path)
    conn = _connect(db_path)
    cur = conn.cursor()
    run_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    params_json = json.dumps(best_params, ensure_ascii=False)
    stats_json = json.dumps(best_stats, ensure_ascii=False)
    if approved:
        cur.execute(
            """
            INSERT INTO evolution_ai_best (strategy, run_at, params_json, stats_json, score)
            VALUES (?, ?, ?, ?, ?)
            """,
            (strategy, run_at, params_json, stats_json, best_score),
        )
    conn.commit()
    conn.close()


def _load_mcap_map(db_path: str) -> Dict[str, float]:
    conn = _connect(db_path)
    cur = conn.cursor()
    try:
        cur.execute("SELECT ts_code, circ_mv FROM stock_basic")
        rows = cur.fetchall()
    except Exception:
        rows = []
    conn.close()
    mcap = {}
    for ts_code, circ_mv in rows:
        if circ_mv is None:
            continue
        try:
            mcap[ts_code] = float(circ_mv) / 10000.0  # 万元 -> 亿元
        except Exception:
            continue
    return mcap


def _prepare_stock_cache(df: pd.DataFrame, sample_size: int = 400) -> Dict[str, Dict[str, np.ndarray]]:
    cache = {}
    grouped = df.groupby("ts_code")
    ts_codes = [ts for ts, g in grouped if len(g) >= 80]
    if len(ts_codes) > sample_size:
        np.random.seed(42)
        ts_codes = list(np.random.choice(ts_codes, sample_size, replace=False))
    for ts_code in ts_codes:
        g = grouped.get_group(ts_code).sort_values("trade_date")
        dates = g["trade_date"].astype(int).to_numpy()
        close = pd.to_numeric(g["close_price"], errors="coerce").to_numpy()
        pct = pd.to_numeric(g["pct_chg"], errors="coerce").to_numpy() / 100.0
        amount = pd.to_numeric(g["amount"], errors="coerce").to_numpy()
        cache[ts_code] = {
            "dates": dates,
            "close": close,
            "pct": pct,
            "amount": amount,
        }
    return cache


def _get_eval_dates(df: pd.DataFrame, lookback_days: int = 240, step: int = 10) -> List[int]:
    dates = sorted(df["trade_date"].unique())
    dates = [int(x) for x in dates]
    if len(dates) == 0:
        return []
    # keep only last lookback_days trading dates
    if len(dates) > lookback_days:
        dates = dates[-lookback_days:]
    return dates[::step]


def _ai_backtest_v5(cache: Dict[str, Dict[str, np.ndarray]], mcap_map: Dict[str, float], eval_dates: List[int], params: Dict) -> Dict:
    target_return = params["target_return"]
    min_amount = params["min_amount"]
    max_volatility = params["max_volatility"]
    min_mcap = params["min_market_cap"]
    max_mcap = params["max_market_cap"]

    returns = []
    total_signals = 0
    for eval_date in eval_dates:
        for ts_code, data in cache.items():
            dates = data["dates"]
            idx = np.searchsorted(dates, eval_date, side="right") - 1
            if idx < 60 or idx + 20 >= len(dates):
                continue
            close = data["close"]
            pct = data["pct"]
            amount = data["amount"]

            mcap = mcap_map.get(ts_code)
            if mcap is not None and (mcap < min_mcap or mcap > max_mcap):
                continue

            ret_20 = (close[idx] / close[idx - 20]) - 1.0 if close[idx - 20] else 0.0
            avg_amount_20 = np.nanmean(amount[idx - 19:idx + 1])
            avg_amount_20_yi = avg_amount_20 / 1e5
            volatility = np.nanstd(pct[idx - 19:idx + 1])
            recent_close = close[idx - 19:idx + 1]
            max_drawdown = np.nanmin(recent_close / np.maximum.accumulate(recent_close) - 1.0)
            max_drawdown = abs(max_drawdown)

            if avg_amount_20_yi < min_amount:
                continue
            if volatility > max_volatility:
                continue
            if max_drawdown > 0.25:
                continue
            if ret_20 < target_return * 0.6:
                continue

            # forward 20 days return
            fwd_ret = (close[idx + 20] / close[idx]) - 1.0 if close[idx] else 0.0
            returns.append(fwd_ret * 100)
            total_signals += 1

    if total_signals == 0:
        return {"total_signals": 0, "avg_return": 0.0, "win_rate": 0.0}

    avg_return = float(np.mean(returns))
    win_rate = float(np.sum(np.array(returns) > 0) / len(returns) * 100)
    return {"total_signals": total_signals, "avg_return": avg_return, "win_rate": win_rate}


def _ai_backtest_v2(cache: Dict[str, Dict[str, np.ndarray]], eval_dates: List[int], params: Dict) -> Dict:
    target_return = params["target_return"]
    min_amount = params["min_amount"]
    max_volatility = params["max_volatility"]

    returns = []
    total_signals = 0
    for eval_date in eval_dates:
        for ts_code, data in cache.items():
            dates = data["dates"]
            idx = np.searchsorted(dates, eval_date, side="right") - 1
            if idx < 60 or idx + 20 >= len(dates):
                continue
            close = data["close"]
            pct = data["pct"]
            amount = data["amount"]

            ret_20 = (close[idx] / close[idx - 20]) - 1.0 if close[idx - 20] else 0.0
            avg_amount_20 = np.nanmean(amount[idx - 19:idx + 1])
            avg_amount_20_yi = avg_amount_20 / 1e5
            volatility = np.nanstd(pct[idx - 19:idx + 1])

            if ret_20 < target_return:
                continue
            if avg_amount_20_yi < min_amount:
                continue
            if volatility > max_volatility:
                continue

            fwd_ret = (close[idx + 20] / close[idx]) - 1.0 if close[idx] else 0.0
            returns.append(fwd_ret * 100)
            total_signals += 1

    if total_signals == 0:
        return {"total_signals": 0, "avg_return": 0.0, "win_rate": 0.0}

    avg_return = float(np.mean(returns))
    win_rate = float(np.sum(np.array(returns) > 0) / len(returns) * 100)
    return {"total_signals": total_signals, "avg_return": avg_return, "win_rate": win_rate}


def _score_ai(stats: Dict) -> float:
    total = stats.get("total_signals", 0) or 0
    if total < 5:
        return -1e9
    avg_return = stats.get("avg_return", 0) or 0
    win_rate = stats.get("win_rate", 0) or 0
    score = avg_return * 0.6 + win_rate * 0.02 + np.log1p(total)
    return float(score)


def _calc_stats(returns: List[float], holding_days: int) -> Dict:
    if not returns:
        return {"total_signals": 0, "avg_return": 0.0, "win_rate": 0.0, "sharpe_ratio": 0.0, "max_drawdown": 0.0}
    rets = np.array(returns, dtype=float)
    total_signals = len(rets)
    avg_return = float(np.mean(rets))
    win_rate = float(np.sum(rets > 0) / total_signals * 100)
    std = float(np.std(rets))
    sharpe = (avg_return / std * np.sqrt(252 / max(holding_days, 1))) if std > 0 else 0.0
    equity = np.cumprod(1 + rets / 100.0)
    running_max = np.maximum.accumulate(equity)
    drawdown = (equity - running_max) / running_max * 100
    max_drawdown = float(np.min(drawdown)) if len(drawdown) else 0.0
    return {
        "total_signals": total_signals,
        "avg_return": avg_return,
        "win_rate": win_rate,
        "sharpe_ratio": sharpe,
        "max_drawdown": max_drawdown,
    }


def _rolling_eval_stats(
    df: pd.DataFrame,
    eval_fn,
    train_window_days: int = 180,
    test_window_days: int = 60,
    step_days: int = 60,
) -> Dict:
    if df is None or df.empty or "trade_date" not in df.columns:
        return {}
    work = df.copy()
    work["trade_date"] = work["trade_date"].astype(str)
    dates = sorted(work["trade_date"].dropna().unique().tolist())
    min_need = train_window_days + test_window_days + 10
    if len(dates) < min_need:
        stats = eval_fn(work)
        if isinstance(stats, dict):
            stats = dict(stats)
            stats["validation_mode"] = "single"
            stats["rolling_test_windows"] = 0
            stats["rolling_failed_windows"] = 0
        return stats

    fold_stats: List[Dict[str, float]] = []
    failed = 0
    start = 0
    while start + train_window_days + test_window_days <= len(dates):
        i0 = start + train_window_days
        i1 = i0 + test_window_days - 1
        d0 = dates[i0]
        d1 = dates[i1]
        test_df = work[(work["trade_date"] >= d0) & (work["trade_date"] <= d1)].copy()
        if test_df.empty:
            failed += 1
            start += step_days
            continue
        try:
            st = eval_fn(test_df)
        except Exception:
            st = {}
        signals = int(_safe_float((st or {}).get("total_signals"), 0))
        if not st or signals <= 0:
            failed += 1
        else:
            fold_stats.append(
                {
                    "total_signals": float(signals),
                    "win_rate": _safe_float(st.get("win_rate"), 0.0),
                    "avg_return": _safe_float(st.get("avg_return"), 0.0),
                    "sharpe_ratio": _safe_float(st.get("sharpe_ratio"), 0.0),
                    "max_drawdown": _safe_float(st.get("max_drawdown"), 0.0),
                }
            )
        start += step_days

    if not fold_stats:
        stats = eval_fn(work)
        if isinstance(stats, dict):
            stats = dict(stats)
            stats["validation_mode"] = "single"
            stats["rolling_test_windows"] = 0
            stats["rolling_failed_windows"] = failed
        return stats

    total_w = sum(x["total_signals"] for x in fold_stats) or 1.0
    agg = {
        "total_signals": int(sum(x["total_signals"] for x in fold_stats)),
        "win_rate": float(sum(x["win_rate"] * x["total_signals"] for x in fold_stats) / total_w),
        "avg_return": float(sum(x["avg_return"] * x["total_signals"] for x in fold_stats) / total_w),
        "sharpe_ratio": float(sum(x["sharpe_ratio"] * x["total_signals"] for x in fold_stats) / total_w),
        "max_drawdown": float(min(x["max_drawdown"] for x in fold_stats)),
        "rolling_test_windows": int(len(fold_stats)),
        "rolling_failed_windows": int(failed),
        "validation_mode": "rolling",
    }
    return agg


def _backtest_simple_v5(df: pd.DataFrame, score_threshold: float, holding_days: int) -> Dict:
    evaluator = ComprehensiveStockEvaluatorV4()
    returns = []
    unique_stocks = df["ts_code"].unique()
    sample_size = min(int(EVOLVE_SETTINGS.get("sample_v5", 1200)), len(unique_stocks))
    if len(unique_stocks) > sample_size:
        np.random.seed(42)
        sample_stocks = np.random.choice(unique_stocks, sample_size, replace=False)
    else:
        sample_stocks = unique_stocks
    for ts_code in sample_stocks:
        stock_data = df[df["ts_code"] == ts_code].copy()
        if len(stock_data) < 60 + holding_days:
            continue
        stock_data = stock_data.sort_values("trade_date")
        last_valid_idx = len(stock_data) - holding_days - 1
        if last_valid_idx < 60:
            continue
        historical = stock_data.iloc[: last_valid_idx + 1].copy()
        result = evaluator.evaluate_stock_v4(historical)
        if not result.get("success"):
            continue
        score = result.get("final_score", 0)
        if score >= score_threshold:
            buy_price = historical["close_price"].iloc[-1]
            sell_price = stock_data.iloc[last_valid_idx + holding_days]["close_price"]
            if buy_price:
                returns.append((sell_price - buy_price) / buy_price * 100)
    return _calc_stats(returns, holding_days)


def _backtest_simple_v6(df: pd.DataFrame, score_threshold: float, holding_days: int) -> Dict:
    evaluator = ComprehensiveStockEvaluatorV6Ultimate()
    returns = []
    unique_stocks = df["ts_code"].unique()
    sample_size = min(int(EVOLVE_SETTINGS.get("sample_v6", 1200)), len(unique_stocks))
    if len(unique_stocks) > sample_size:
        np.random.seed(42)
        sample_stocks = np.random.choice(unique_stocks, sample_size, replace=False)
    else:
        sample_stocks = unique_stocks
    for ts_code in sample_stocks:
        stock_data = df[df["ts_code"] == ts_code].copy()
        if len(stock_data) < 60 + holding_days:
            continue
        stock_data = stock_data.sort_values("trade_date")
        last_valid_idx = len(stock_data) - holding_days - 1
        if last_valid_idx < 60:
            continue
        historical = stock_data.iloc[: last_valid_idx + 1].copy()
        result = evaluator.evaluate_stock_v6(historical, ts_code)
        if not result.get("success"):
            continue
        score = result.get("final_score", 0)
        if score >= score_threshold:
            buy_price = historical["close_price"].iloc[-1]
            sell_price = stock_data.iloc[last_valid_idx + holding_days]["close_price"]
            if buy_price:
                returns.append((sell_price - buy_price) / buy_price * 100)
    return _calc_stats(returns, holding_days)


def _backtest_simple_v7(df: pd.DataFrame, score_threshold: float, holding_days: int, db_path: str) -> Dict:
    evaluator = ComprehensiveStockEvaluatorV7Ultimate(db_path)
    evaluator.reset_cache()
    returns = []
    unique_stocks = df[["ts_code", "industry"]].drop_duplicates()
    sample_size = min(int(EVOLVE_SETTINGS.get("sample_v7", 600)), len(unique_stocks))
    if len(unique_stocks) > sample_size:
        unique_stocks = unique_stocks.sample(n=sample_size, random_state=42)
    for row in unique_stocks.itertuples(index=False):
        ts_code = row.ts_code
        industry = row.industry
        stock_data = df[df["ts_code"] == ts_code].copy()
        if len(stock_data) < 60 + holding_days:
            continue
        stock_data = stock_data.sort_values("trade_date")
        last_valid_idx = len(stock_data) - holding_days - 1
        if last_valid_idx < 60:
            continue
        historical = stock_data.iloc[: last_valid_idx + 1].copy()
        result = evaluator.evaluate_stock_v7(historical, ts_code, industry)
        if not result.get("success"):
            continue
        score = result.get("final_score", 0)
        if score >= score_threshold:
            buy_price = historical["close_price"].iloc[-1]
            sell_price = stock_data.iloc[last_valid_idx + holding_days]["close_price"]
            if buy_price:
                returns.append((sell_price - buy_price) / buy_price * 100)
    return _calc_stats(returns, holding_days)


def _backtest_simple_v8(df: pd.DataFrame, score_threshold: float, holding_days: int, db_path: str, index_data: pd.DataFrame) -> Dict:
    evaluator = ComprehensiveStockEvaluatorV8Ultimate(db_path)
    returns = []
    unique_stocks = df[["ts_code", "industry"]].drop_duplicates()
    sample_size = min(int(EVOLVE_SETTINGS.get("sample_v8", 500)), len(unique_stocks))
    if len(unique_stocks) > sample_size:
        unique_stocks = unique_stocks.sample(n=sample_size, random_state=42)
    for row in unique_stocks.itertuples(index=False):
        ts_code = row.ts_code
        industry = row.industry
        stock_data = df[df["ts_code"] == ts_code].copy()
        if len(stock_data) < 60 + holding_days:
            continue
        stock_data = stock_data.sort_values("trade_date")
        last_valid_idx = len(stock_data) - holding_days - 1
        if last_valid_idx < 60:
            continue
        historical = stock_data.iloc[: last_valid_idx + 1].copy()
        result = evaluator.evaluate_stock_v8(historical, ts_code=ts_code, index_data=index_data, industry=industry)
        if not result.get("success"):
            continue
        score = result.get("final_score", 0)
        if score >= score_threshold:
            buy_price = historical["close_price"].iloc[-1]
            sell_price = stock_data.iloc[last_valid_idx + holding_days]["close_price"]
            if buy_price:
                returns.append((sell_price - buy_price) / buy_price * 100)
    return _calc_stats(returns, holding_days)


def _grid_search_v5(df: pd.DataFrame) -> Tuple[Dict, Dict, float]:
    up = _unified_strategy_params("v5", {"score_threshold": 60, "holding_days": 8})
    base_thr = _safe_int(up.get("score_threshold"), 60)
    base_hold = _safe_int(up.get("holding_days"), 8)
    thresholds = _grid_values_around(base_thr, 50, 75, 5, radius=2)
    holding_days = _grid_values_around(base_hold, 3, 20, 2, radius=2)
    best_params, best_stats, best_score = {}, {}, -1e9
    start_ts = datetime.now().timestamp()
    max_seconds = int(EVOLVE_SETTINGS.get("max_seconds", 0))
    log_every = int(EVOLVE_SETTINGS.get("log_every", 10))
    total = len(thresholds) * len(holding_days)
    tried = 0
    stop_early = False
    for thr in thresholds:
        for hd in holding_days:
            if max_seconds > 0 and (datetime.now().timestamp() - start_ts) > max_seconds:
                stop_early = True
                LOGGER.warning("grid search v5 time limit reached (%ss), returning best so far", max_seconds)
                break
            tried += 1
            stats = _backtest_simple_v5(df, thr, hd)
            score = _score_result(stats)
            if score > best_score:
                best_score, best_params, best_stats = score, {"score_threshold": thr, "holding_days": hd}, stats
                LOGGER.info("v5 best score=%.3f params=%s", best_score, best_params)
            if tried % log_every == 0:
                elapsed = int(datetime.now().timestamp() - start_ts)
                LOGGER.info("grid v5 progress: %s/%s combos, elapsed=%ss, best=%.3f",
                            tried, total, elapsed, best_score)
        if stop_early:
            break
    return best_params, best_stats, best_score


def _grid_search_v6(df: pd.DataFrame) -> Tuple[Dict, Dict, float]:
    thresholds = [70, 75, 80, 85]
    holding_days = [3, 5]
    best_params, best_stats, best_score = {}, {}, -1e9
    start_ts = datetime.now().timestamp()
    max_seconds = int(EVOLVE_SETTINGS.get("max_seconds", 0))
    log_every = int(EVOLVE_SETTINGS.get("log_every", 10))
    total = len(thresholds) * len(holding_days)
    tried = 0
    stop_early = False
    for thr in thresholds:
        for hd in holding_days:
            if max_seconds > 0 and (datetime.now().timestamp() - start_ts) > max_seconds:
                stop_early = True
                LOGGER.warning("grid search v6 time limit reached (%ss), returning best so far", max_seconds)
                break
            tried += 1
            stats = _backtest_simple_v6(df, thr, hd)
            score = _score_result(stats)
            if score > best_score:
                best_score, best_params, best_stats = score, {"score_threshold": thr, "holding_days": hd}, stats
                LOGGER.info("v6 best score=%.3f params=%s", best_score, best_params)
            if tried % log_every == 0:
                elapsed = int(datetime.now().timestamp() - start_ts)
                LOGGER.info("grid v6 progress: %s/%s combos, elapsed=%ss, best=%.3f",
                            tried, total, elapsed, best_score)
        if stop_early:
            break
    return best_params, best_stats, best_score


def _grid_search_v7(df: pd.DataFrame, db_path: str) -> Tuple[Dict, Dict, float]:
    thresholds = [60, 65, 70, 75]
    holding_days = [3, 5, 8]
    best_params, best_stats, best_score = {}, {}, -1e9
    start_ts = datetime.now().timestamp()
    max_seconds = int(EVOLVE_SETTINGS.get("max_seconds", 0))
    log_every = int(EVOLVE_SETTINGS.get("log_every", 10))
    total = len(thresholds) * len(holding_days)
    tried = 0
    stop_early = False
    for thr in thresholds:
        for hd in holding_days:
            if max_seconds > 0 and (datetime.now().timestamp() - start_ts) > max_seconds:
                stop_early = True
                LOGGER.warning("grid search v7 time limit reached (%ss), returning best so far", max_seconds)
                break
            tried += 1
            stats = _backtest_simple_v7(df, thr, hd, db_path)
            score = _score_result(stats)
            if score > best_score:
                best_score, best_params, best_stats = score, {"score_threshold": thr, "holding_days": hd}, stats
                LOGGER.info("v7 best score=%.3f params=%s", best_score, best_params)
            if tried % log_every == 0:
                elapsed = int(datetime.now().timestamp() - start_ts)
                LOGGER.info("grid v7 progress: %s/%s combos, elapsed=%ss, best=%.3f",
                            tried, total, elapsed, best_score)
        if stop_early:
            break
    return best_params, best_stats, best_score


def _grid_search_v8(df: pd.DataFrame, db_path: str, index_data: pd.DataFrame) -> Tuple[Dict, Dict, float]:
    up = _unified_strategy_params("v8", {"score_threshold": 65, "holding_days": 10})
    base_thr = _safe_int(up.get("score_threshold"), 65)
    base_hold = _safe_int(up.get("holding_days"), 10)
    thresholds = _grid_values_around(base_thr, 55, 85, 5, radius=2)
    holding_days = _grid_values_around(base_hold, 3, 20, 2, radius=2)
    best_params, best_stats, best_score = {}, {}, -1e9
    start_ts = datetime.now().timestamp()
    max_seconds = int(EVOLVE_SETTINGS.get("max_seconds", 0))
    log_every = int(EVOLVE_SETTINGS.get("log_every", 10))
    total = len(thresholds) * len(holding_days)
    tried = 0
    stop_early = False
    for thr in thresholds:
        for hd in holding_days:
            if max_seconds > 0 and (datetime.now().timestamp() - start_ts) > max_seconds:
                stop_early = True
                LOGGER.warning("grid search v8 time limit reached (%ss), returning best so far", max_seconds)
                break
            tried += 1
            stats = _rolling_eval_stats(
                df,
                lambda x: _backtest_simple_v8(x, thr, hd, db_path, index_data),
                train_window_days=180,
                test_window_days=60,
                step_days=60,
            )
            score = _score_result(stats)
            if score > best_score:
                best_score, best_params, best_stats = score, {"score_threshold": thr, "holding_days": hd}, stats
                LOGGER.info("v8 best score=%.3f params=%s", best_score, best_params)
            if tried % log_every == 0:
                elapsed = int(datetime.now().timestamp() - start_ts)
                LOGGER.info("grid v8 progress: %s/%s combos, elapsed=%ss, best=%.3f",
                            tried, total, elapsed, best_score)
        if stop_early:
            break
    return best_params, best_stats, best_score


def _calc_v9_score_from_hist(hist: pd.DataFrame, industry_strength: float = 0.0) -> float:
    if hist is None or hist.empty or len(hist) < 80:
        return 0.0
    h = hist.sort_values("trade_date")
    close = pd.to_numeric(h["close_price"], errors="coerce").ffill()
    vol = pd.to_numeric(h.get("vol", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
    amount = pd.to_numeric(h.get("amount", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
    pct = pd.to_numeric(h.get("pct_chg", pd.Series(dtype=float)), errors="coerce")
    if pct.isna().all():
        pct = close.pct_change() * 100

    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(60).mean()
    ma120 = close.rolling(120).mean()
    trend_strong = bool(ma20.iloc[-1] > ma60.iloc[-1] > ma120.iloc[-1])
    trend_ok = bool((ma20.iloc[-1] > ma60.iloc[-1]) and (ma20.iloc[-1] > ma20.iloc[-5]) and (ma60.iloc[-1] >= ma60.iloc[-5]))

    momentum_20 = (close.iloc[-1] / close.iloc[-21] - 1.0) if len(close) > 21 else 0.0
    momentum_60 = (close.iloc[-1] / close.iloc[-61] - 1.0) if len(close) > 61 else 0.0
    vol_ratio = (vol.iloc[-1] / vol.tail(20).mean()) if vol.tail(20).mean() > 0 else 0.0

    flow_sign = pct.fillna(0).apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    flow_val = (amount * flow_sign).tail(20).sum()
    flow_base = amount.tail(20).sum() if amount.tail(20).sum() > 0 else 1.0
    flow_ratio = flow_val / flow_base

    vol_20 = pct.tail(20).std() / 100.0 if pct.tail(20).std() is not None else 0.0

    fund_score = max(0.0, min(20.0, (flow_ratio + 0.03) / 0.12 * 20.0))
    volume_score = max(0.0, min(15.0, (vol_ratio - 0.5) / 1.0 * 15.0))
    momentum_score = max(0.0, min(8.0, momentum_20 * 100 / 8.0 * 8.0)) + \
                     max(0.0, min(7.0, momentum_60 * 100 / 16.0 * 7.0))
    sector_score = max(0.0, min(15.0, (industry_strength + 2.0) / 6.0 * 15.0))

    if vol_20 <= 0.03:
        vola_score = 12.0
    elif vol_20 <= 0.06:
        vola_score = 15.0
    elif vol_20 <= 0.10:
        vola_score = 8.0
    else:
        vola_score = 0.0

    trend_score = 15.0 if trend_strong else (10.0 if trend_ok else 0.0)

    rolling_peak = close.cummax()
    drawdown = (rolling_peak - close) / rolling_peak
    max_dd = float(drawdown.tail(60).max())
    dd_penalty = 0.0
    if max_dd > 0.15:
        dd_penalty = min(10.0, (max_dd - 0.15) / 0.15 * 10.0)

    total_score = fund_score + volume_score + momentum_score + sector_score + vola_score + trend_score - dd_penalty
    if total_score < 0:
        total_score = 0.0
    return float(total_score)


def _backtest_simple_v9(
    df: pd.DataFrame,
    score_threshold: float,
    holding_days: int,
    lookback_days: int,
    min_turnover: float,
) -> Dict:
    returns = []
    unique_stocks = df["ts_code"].unique()
    sample_size = min(int(EVOLVE_SETTINGS.get("sample_v9", 600)), len(unique_stocks))
    if len(unique_stocks) > sample_size:
        np.random.seed(42)
        sample_stocks = np.random.choice(unique_stocks, sample_size, replace=False)
    else:
        sample_stocks = unique_stocks

    # industry strength based on last 20d return
    industry_strength = {}
    try:
        ind_vals = {}
        for ts_code in sample_stocks:
            g = df[df["ts_code"] == ts_code].sort_values("trade_date")
            if len(g) >= 21:
                close = pd.to_numeric(g["close_price"], errors="coerce").ffill()
                r20 = (close.iloc[-1] / close.iloc[-21] - 1.0) * 100
                ind = g["industry"].iloc[-1] if "industry" in g.columns else None
                if ind:
                    ind_vals.setdefault(ind, []).append(float(r20))
        industry_strength = {k: float(np.mean(v)) for k, v in ind_vals.items()}
    except Exception:
        industry_strength = {}

    for ts_code in sample_stocks:
        stock_data = df[df["ts_code"] == ts_code].copy()
        if len(stock_data) < lookback_days + holding_days + 5:
            continue
        stock_data = stock_data.sort_values("trade_date")
        last_valid_idx = len(stock_data) - holding_days - 1
        if last_valid_idx < lookback_days:
            continue
        hist = stock_data.iloc[last_valid_idx - lookback_days + 1:last_valid_idx + 1].copy()
        ind = stock_data["industry"].iloc[-1] if "industry" in stock_data.columns else None
        ind_strength = industry_strength.get(ind, 0.0)
        amount = pd.to_numeric(stock_data["amount"], errors="coerce").fillna(0.0)
        avg_amount = float(amount.iloc[last_valid_idx - 19:last_valid_idx + 1].mean()) if last_valid_idx >= 19 else 0.0
        # amount unit is thousand yuan; convert to hundred-million yuan (亿)
        avg_amount_yi = avg_amount / 1e5
        if avg_amount_yi < min_turnover:
            continue

        score = _calc_v9_score_from_hist(hist, industry_strength=ind_strength)
        if score >= score_threshold:
            buy_price = stock_data.iloc[last_valid_idx]["close_price"]
            sell_price = stock_data.iloc[last_valid_idx + holding_days]["close_price"]
            if buy_price:
                returns.append((sell_price - buy_price) / buy_price * 100)

    return _calc_stats(returns, holding_days)


def _grid_search_v9(df: pd.DataFrame) -> Tuple[Dict, Dict, float]:
    up = _unified_strategy_params("v9", {"score_threshold": 65, "holding_days": 20, "lookback_days": 160, "min_turnover": 5.0})
    base_thr = _safe_int(up.get("score_threshold"), 65)
    base_hold = _safe_int(up.get("holding_days"), 20)
    base_lookback = _safe_int(up.get("lookback_days"), 160)
    base_turnover = _safe_float_local(up.get("min_turnover"), 5.0)
    thresholds = _grid_values_around(base_thr, 50, 80, 5, radius=2)
    holding_days = _grid_values_around(base_hold, 5, 30, 3, radius=2)
    lookback_days = _grid_values_around(base_lookback, 80, 220, 20, radius=1)
    min_turnovers = sorted(set([
        round(max(1.0, min(20.0, base_turnover - 2.0)), 1),
        round(max(1.0, min(20.0, base_turnover)), 1),
        round(max(1.0, min(20.0, base_turnover + 2.0)), 1),
    ]))
    best_params, best_stats, best_score = {}, {}, -1e9
    start_ts = datetime.now().timestamp()
    max_seconds = int(EVOLVE_SETTINGS.get("max_seconds", 0))
    log_every = int(EVOLVE_SETTINGS.get("log_every", 10))
    total = len(thresholds) * len(holding_days) * len(lookback_days) * len(min_turnovers)
    tried = 0
    stop_early = False
    for thr in thresholds:
        for hd in holding_days:
            for lb in lookback_days:
                for mt in min_turnovers:
                    if max_seconds > 0 and (datetime.now().timestamp() - start_ts) > max_seconds:
                        stop_early = True
                        LOGGER.warning("grid search v9 time limit reached (%ss), returning best so far", max_seconds)
                        break
                    tried += 1
                    stats = _rolling_eval_stats(
                        df,
                        lambda x: _backtest_simple_v9(x, thr, hd, lb, mt),
                        train_window_days=180,
                        test_window_days=60,
                        step_days=60,
                    )
                    score = _score_result(stats)
                    if score > best_score:
                        best_score, best_params, best_stats = score, {
                            "score_threshold": thr,
                            "holding_days": hd,
                            "lookback_days": lb,
                            "min_turnover": mt,
                        }, stats
                        LOGGER.info("v9 best score=%.3f params=%s", best_score, best_params)
                    if tried % log_every == 0:
                        elapsed = int(datetime.now().timestamp() - start_ts)
                        LOGGER.info("grid v9 progress: %s/%s combos, elapsed=%ss, best=%.3f",
                                    tried, total, elapsed, best_score)
                if stop_early:
                    break
            if stop_early:
                break
        if stop_early:
            break
    return best_params, best_stats, best_score


def _grid_search_stable_uptrend(df: pd.DataFrame) -> Tuple[Dict, Dict, float]:
    # Use last ~240 trading days for evaluation
    df = df.copy()
    df = df.sort_values(["ts_code", "trade_date"])
    grouped = df.groupby("ts_code")
    ts_codes = [ts for ts, g in grouped if len(g) >= 120]
    max_samples = int(EVOLVE_SETTINGS.get("sample_stable", 400))
    if len(ts_codes) > max_samples:
        np.random.seed(42)
        ts_codes = list(np.random.choice(ts_codes, max_samples, replace=False))

    lookback_days_list = [80, 120, 160]
    max_drawdown_list = [0.10, 0.15, 0.20]
    vol_max_list = [0.03, 0.04, 0.05]
    rebound_min_list = [0.08, 0.10, 0.12]
    min_turnover_list = [3.0, 5.0, 8.0]

    best_params, best_stats, best_score = {}, {}, -1e9
    start_ts = datetime.now().timestamp()
    max_seconds = int(EVOLVE_SETTINGS.get("max_seconds", 0))
    log_every = int(EVOLVE_SETTINGS.get("log_every", 10))
    total = len(lookback_days_list) * len(max_drawdown_list) * len(vol_max_list) * len(rebound_min_list) * len(min_turnover_list)
    tried = 0
    stop_early = False

    for lb in lookback_days_list:
        for mdd in max_drawdown_list:
            for vol in vol_max_list:
                for rb in rebound_min_list:
                    for mt in min_turnover_list:
                        if max_seconds > 0 and (datetime.now().timestamp() - start_ts) > max_seconds:
                            stop_early = True
                            LOGGER.warning("grid search stable_uptrend time limit reached (%ss), returning best so far", max_seconds)
                            break
                        tried += 1
                        returns = []
                        total_signals = 0
                        for ts_code in ts_codes:
                            g = grouped.get_group(ts_code)
                            if len(g) < lb + 20:
                                continue
                            close = pd.to_numeric(g["close_price"], errors="coerce").to_numpy()
                            amount = pd.to_numeric(g["amount"], errors="coerce").to_numpy()
                            if len(close) < lb + 20:
                                continue
                            avg_amount = np.nanmean(amount[-20:])
                            avg_amount_yi = avg_amount / 1e5
                            if avg_amount_yi < mt:
                                continue
                            series = pd.Series(close)
                            recent = series.iloc[-lb:]
                            if recent.isna().any():
                                continue

                            ma20 = recent.rolling(20).mean()
                            ma60 = recent.rolling(60).mean()
                            if ma60.isna().iloc[-1]:
                                continue
                            trend_ok = ma20.iloc[-1] > ma60.iloc[-1] and ma20.iloc[-1] > ma20.iloc[-5]
                            if not trend_ok:
                                continue

                            rolling_peak = recent.cummax()
                            drawdown = (rolling_peak - recent) / rolling_peak
                            max_dd = float(drawdown.tail(60).max())
                            if max_dd > mdd:
                                continue

                            recent_low = float(recent.tail(20).min())
                            rebound = (recent.iloc[-1] / recent_low - 1.0) if recent_low > 0 else 0.0
                            if rebound < rb:
                                continue

                            returns_ = recent.pct_change().dropna()
                            vol_20 = float(returns_.tail(20).std())
                            if vol_20 > vol:
                                continue

                            # forward 20 days return
                            fwd_ret = (close[-1] / close[-21] - 1.0) * 100 if close[-21] else 0.0
                            returns.append(fwd_ret)
                            total_signals += 1

                        stats = _calc_stats(returns, holding_days=20)
                        score = _score_result(stats)
                        if score > best_score and total_signals >= 5:
                            best_score = score
                            best_params = {
                                "lookback_days": lb,
                                "max_drawdown_pct": int(mdd * 100),
                                "vol_max_pct": round(vol * 100, 2),
                                "rebound_min_pct": int(rb * 100),
                                "min_turnover": mt,
                                "candidate_count": 200,
                                "result_count": 30,
                                "min_mv": 100,
                                "max_mv": 5000,
                            }
                            best_stats = stats
                            LOGGER.info("stable uptrend best score=%.3f params=%s", best_score, best_params)
                        if tried % log_every == 0:
                            elapsed = int(datetime.now().timestamp() - start_ts)
                            LOGGER.info("grid stable progress: %s/%s combos, elapsed=%ss, best=%.3f",
                                        tried, total, elapsed, best_score)
                    if stop_early:
                        break
                if stop_early:
                    break
            if stop_early:
                break
        if stop_early:
            break

    return best_params, best_stats, best_score


def _grid_search_ai_v5(df: pd.DataFrame, db_path: str) -> Tuple[Dict, Dict, float]:
    cache = _prepare_stock_cache(df, sample_size=int(EVOLVE_SETTINGS.get("sample_ai", 400)))
    if not cache:
        return {}, {}, -1e9
    mcap_map = _load_mcap_map(db_path)
    eval_dates = _get_eval_dates(df, lookback_days=220, step=12)

    target_returns = [0.12, 0.16, 0.20]
    min_amounts = [1.5, 2.5, 4.0]
    max_vols = [0.12, 0.16, 0.20]
    min_mcaps = [50, 100]
    max_mcaps = [1000, 3000, 5000]

    best_params = {}
    best_stats = {}
    best_score = -1e9
    start_ts = datetime.now().timestamp()
    max_seconds = int(EVOLVE_SETTINGS.get("max_seconds", 0))
    log_every = int(EVOLVE_SETTINGS.get("log_every", 10))
    total = len(target_returns) * len(min_amounts) * len(max_vols) * len(min_mcaps) * len(max_mcaps)
    tried = 0
    stop_early = False

    for tr in target_returns:
        for ma in min_amounts:
            for mv in max_vols:
                for min_mc in min_mcaps:
                    for max_mc in max_mcaps:
                        if max_seconds > 0 and (datetime.now().timestamp() - start_ts) > max_seconds:
                            stop_early = True
                            LOGGER.warning("grid search AI V5 time limit reached (%ss), returning best so far", max_seconds)
                            break
                        tried += 1
                        if min_mc >= max_mc:
                            continue
                        params = {
                            "target_return": tr,
                            "min_amount": ma,
                            "max_volatility": mv,
                            "min_market_cap": min_mc,
                            "max_market_cap": max_mc,
                        }
                        stats = _ai_backtest_v5(cache, mcap_map, eval_dates, params)
                        score = _score_ai(stats)
                        if score > best_score:
                            best_score = score
                            best_params = params
                            best_stats = stats
                            LOGGER.info("AI V5 new best score=%.3f params=%s", best_score, best_params)
                        if tried % log_every == 0:
                            elapsed = int(datetime.now().timestamp() - start_ts)
                            LOGGER.info("grid AI V5 progress: %s/%s combos, elapsed=%ss, best=%.3f",
                                        tried, total, elapsed, best_score)
                    if stop_early:
                        break
                if stop_early:
                    break
            if stop_early:
                break
        if stop_early:
            break

    return best_params, best_stats, best_score


def _grid_search_ai_v2(df: pd.DataFrame) -> Tuple[Dict, Dict, float]:
    cache = _prepare_stock_cache(df, sample_size=int(EVOLVE_SETTINGS.get("sample_ai", 400)))
    if not cache:
        return {}, {}, -1e9
    eval_dates = _get_eval_dates(df, lookback_days=220, step=12)

    target_returns = [0.15, 0.20, 0.25]
    min_amounts = [1.5, 2.5, 4.0]
    max_vols = [0.10, 0.12, 0.15]

    best_params = {}
    best_stats = {}
    best_score = -1e9
    start_ts = datetime.now().timestamp()
    max_seconds = int(EVOLVE_SETTINGS.get("max_seconds", 0))
    log_every = int(EVOLVE_SETTINGS.get("log_every", 10))
    total = len(target_returns) * len(min_amounts) * len(max_vols)
    tried = 0
    stop_early = False

    for tr in target_returns:
        for ma in min_amounts:
            for mv in max_vols:
                if max_seconds > 0 and (datetime.now().timestamp() - start_ts) > max_seconds:
                    stop_early = True
                    LOGGER.warning("grid search AI V2 time limit reached (%ss), returning best so far", max_seconds)
                    break
                tried += 1
                params = {
                    "target_return": tr,
                    "min_amount": ma,
                    "max_volatility": mv,
                }
                stats = _ai_backtest_v2(cache, eval_dates, params)
                score = _score_ai(stats)
                if score > best_score:
                    best_score = score
                    best_params = params
                    best_stats = stats
                    LOGGER.info("AI V2 new best score=%.3f params=%s", best_score, best_params)
                if tried % log_every == 0:
                    elapsed = int(datetime.now().timestamp() - start_ts)
                    LOGGER.info("grid AI V2 progress: %s/%s combos, elapsed=%ss, best=%.3f",
                                tried, total, elapsed, best_score)
            if stop_early:
                break
        if stop_early:
            break

    return best_params, best_stats, best_score


def _split_train_test(df: pd.DataFrame, test_ratio: float = 0.25) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if df is None or df.empty or "trade_date" not in df.columns:
        return pd.DataFrame(), pd.DataFrame()
    work = df.copy().sort_values("trade_date")
    dates = sorted(work["trade_date"].dropna().astype(str).unique().tolist())
    if len(dates) < 40:
        return work, pd.DataFrame()
    cut_idx = max(1, int(len(dates) * (1.0 - test_ratio)))
    cut_date = dates[cut_idx - 1]
    train_df = work[work["trade_date"].astype(str) <= cut_date].copy()
    test_df = work[work["trade_date"].astype(str) > cut_date].copy()
    return train_df, test_df


def _compute_oos_stats(strategy: str, params: Dict, train_df: pd.DataFrame, test_df: pd.DataFrame, db_path: str, index_df: pd.DataFrame | None = None) -> Dict:
    if test_df is None or test_df.empty:
        return {}
    st = strategy.strip().upper()
    try:
        if st == "V4":
            evaluator = ComprehensiveStockEvaluatorV4()
            result = backtest_with_dynamic_strategy(
                evaluator,
                test_df,
                sample_size=int(EVOLVE_SETTINGS.get("sample_v4", 800)),
                score_threshold=float(params.get("score_threshold", 60)),
                max_holding_days=int(params.get("max_holding_days", 15)),
                stop_loss_pct=float(params.get("stop_loss_pct", -5.0)),
                take_profit_pct=float(params.get("take_profit_pct", 8.0)),
            )
            return result.get("stats", {}) if result.get("success") else {}
        if st == "V5":
            return _backtest_simple_v5(test_df, float(params.get("score_threshold", 60)), int(params.get("holding_days", 10)))
        if st == "V6":
            return _backtest_simple_v6(test_df, float(params.get("score_threshold", 75)), int(params.get("holding_days", 5)))
        if st == "V7":
            return _backtest_simple_v7(test_df, float(params.get("score_threshold", 65)), int(params.get("holding_days", 5)), db_path)
        if st == "V8":
            if index_df is None or index_df.empty:
                return {}
            return _backtest_simple_v8(test_df, float(params.get("score_threshold", 70)), int(params.get("holding_days", 5)), db_path, index_df)
        if st == "V9":
            return _backtest_simple_v9(
                test_df,
                float(params.get("score_threshold", 65)),
                int(params.get("holding_days", 20)),
                int(params.get("lookback_days", 160)),
                float(params.get("min_turnover", 5.0)),
            )
        if st == "AI_V5":
            return {}
        if st == "AI_V2":
            return {}
        if st == "STABLE_UPTREND":
            return {}
    except Exception as e:
        LOGGER.warning("oos stats compute failed for %s: %s", st, e)
    return {}


def _build_combo_candidate_from_production() -> Tuple[Dict, Dict, float, str]:
    def _load(name: str) -> Dict:
        return _load_json(os.path.join(EVOLUTION_DIR, name))

    v5 = _load("v5_best.json")
    v8 = _load("v8_best.json")
    v9 = _load("v9_best.json")
    refs = [x for x in (v5, v8, v9) if x]
    if not refs:
        return {}, {}, -1e9, "single"

    def _st(x: Dict, k: str, default: float = 0.0) -> float:
        return _safe_float((x.get("stats") or {}).get(k), default)

    weights = {
        "v5": max(0.1, _safe_float((v5.get("stats") or {}).get("sharpe_ratio"), 0.2) + 0.3) if v5 else 0.0,
        "v8": max(0.1, _safe_float((v8.get("stats") or {}).get("sharpe_ratio"), 0.2) + 0.3) if v8 else 0.0,
        "v9": max(0.1, _safe_float((v9.get("stats") or {}).get("sharpe_ratio"), 0.2) + 0.3) if v9 else 0.0,
    }
    total_w = sum(weights.values()) or 1.0
    for k in list(weights.keys()):
        weights[k] = round(weights[k] / total_w, 4)

    thr_vals = []
    for src in (v5, v8, v9):
        if not src:
            continue
        p = src.get("params") or {}
        thr = p.get("score_threshold")
        if isinstance(thr, (int, float)):
            thr_vals.append(float(thr))
    base_thr = int(round(float(np.mean(thr_vals)))) if thr_vals else 68
    combo_threshold = int(max(58, min(80, base_thr + 3)))
    min_agree = 3 if combo_threshold >= 70 else 2
    top_percent = 2 if combo_threshold >= 68 else 3
    lookback_days = int(max(80, min(200, _safe_float((v9.get("params") or {}).get("lookback_days"), 120))))
    min_turnover = float(max(3.0, min(20.0, _safe_float((v9.get("params") or {}).get("min_turnover"), 5.0))))

    params = {
        "combo_threshold": combo_threshold,
        "top_percent": top_percent,
        "select_mode": "双重筛选(阈值+Top%)",
        "min_agree": min_agree,
        "lookback_days": lookback_days,
        "min_turnover": min_turnover,
        "candidate_count": 1200,
        "enable_consistency": True,
        "min_align": 2,
        "auto_weights": True,
        "w_v4": 0.0,
        "w_v5": float(weights["v5"]),
        "w_v7": 0.0,
        "w_v8": float(weights["v8"]),
        "w_v9": float(weights["v9"]),
        "thr_v4": 60,
        "thr_v5": int((v5.get("params") or {}).get("score_threshold", 60)) if v5 else 60,
        "thr_v7": 65,
        "thr_v8": int((v8.get("params") or {}).get("score_threshold", 65)) if v8 else 65,
        "thr_v9": int((v9.get("params") or {}).get("score_threshold", 65)) if v9 else 65,
    }

    wrs = [_st(x, "win_rate", 0.0) for x in refs]
    dds = [_st(x, "max_drawdown", 0.0) for x in refs]
    shs = [_st(x, "sharpe_ratio", 0.0) for x in refs]
    sigs = [_st(x, "total_signals", 0.0) for x in refs]
    avg_wr = float(np.mean(wrs)) if wrs else 0.0
    avg_dd = float(np.mean(dds)) if dds else 0.0
    avg_sh = float(np.mean(shs)) if shs else 0.0
    total_signals = int(sum(sigs))
    score = float((avg_sh * 1.5) + (avg_wr * 0.03) - (abs(avg_dd) * 0.04))
    stats = {
        "total_signals": total_signals,
        "win_rate": avg_wr,
        "max_drawdown": avg_dd,
        "sharpe_ratio": avg_sh,
        "source": {
            "v5_score": _safe_float(v5.get("score"), 0.0) if v5 else None,
            "v8_score": _safe_float(v8.get("score"), 0.0) if v8 else None,
            "v9_score": _safe_float(v9.get("score"), 0.0) if v9 else None,
        },
    }
    source_modes = []
    for src in (v5, v8, v9):
        if not src:
            continue
        mode = str(src.get("validation_mode", "") or "").strip().lower()
        if mode:
            source_modes.append(mode)
    validation_mode = "rolling" if source_modes and all(m == "rolling" for m in source_modes) else "single"
    stats["source_validation_modes"] = source_modes
    return params, stats, score, validation_mode


def _git_push() -> None:
    auto_push = os.getenv("AUTO_PUSH", "1").strip() in ("1", "true", "yes")
    if not auto_push:
        LOGGER.info("AUTO_PUSH disabled")
        return
    if not os.path.exists(os.path.join(ROOT, ".git")):
        LOGGER.info("AUTO_PUSH skipped: current path is not a git repo")
        return
    try:
        candidates = [
            "evolution/best_params.json",
            "evolution/last_run.json",
            "evolution/last_run.csv",
            "evolution/health_report.json",
            "evolution/ai_v5_best.json",
            "evolution/ai_v2_best.json",
            "evolution/v5_best.json",
            "evolution/v6_best.json",
            "evolution/v7_best.json",
            "evolution/v8_best.json",
            "evolution/v9_best.json",
            "evolution/combo_best.json",
            "evolution/stable_uptrend_best.json",
            "evolution/risk_sentinel.json",
            "evolution/promotion_history.jsonl",
        ]
        files = [f for f in candidates if os.path.exists(os.path.join(ROOT, f))]
        if not files:
            LOGGER.info("no evolution files to git add")
            return
        subprocess.run(["git", "add", *files], check=True, cwd=ROOT)
        msg = f"auto evolve: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        subprocess.run(["git", "commit", "-m", msg], check=False, cwd=ROOT)
        subprocess.run(["git", "push"], check=True, cwd=ROOT)
        LOGGER.info("git push done")
    except Exception as e:
        LOGGER.error("git push failed: %s", e)


def _should_auto_repair_funding() -> bool:
    return str(os.getenv("AUTO_REPAIR_FUNDING_TABLES", "1")).strip().lower() in {"1", "true", "yes", "on"}


def _auto_repair_lagging_funding_tables(db_path: str, lagging_warnings: List[str]) -> Dict[str, Dict]:
    out: Dict[str, Dict] = {}
    if not lagging_warnings or not _should_auto_repair_funding():
        return out
    need_margin = any(("margin_summary" in w or "margin_detail" in w) for w in lagging_warnings)
    need_moneyflow = any(("moneyflow_daily" in w or "moneyflow_ind_ths" in w) for w in lagging_warnings)
    need_top = any(("top_list" in w or "top_inst" in w) for w in lagging_warnings)
    need_north = any("northbound_flow" in w for w in lagging_warnings)

    try:
        if need_margin:
            out["margin_summary"] = _update_margin(db_path)
            out["margin_detail"] = _update_margin_detail(db_path)
        if need_moneyflow:
            out["moneyflow_daily"] = _update_moneyflow_daily(db_path)
            out["moneyflow_ind_ths"] = _update_moneyflow_industry(db_path)
        if need_top:
            out["top_list"] = _update_top_list(db_path)
            out["top_inst"] = _update_top_inst(db_path)
        if need_north:
            out["northbound_flow"] = _update_northbound(db_path)
    except Exception as e:
        LOGGER.warning("auto repair funding tables failed: %s", e)
    return out


def _write_health_report(db_path: str, _allow_auto_repair: bool = True) -> None:
    report = {
        "run_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "ok": True,
        "warnings": [],
        "stats": {},
    }

    try:
        from risk.summary import compute_health_report as _compute_health_report_v2  # type: ignore
        report = _compute_health_report_v2(
            db_path=db_path,
            enable_fund_bonus=True,
            fund_portfolio_funds=os.getenv("FUND_PORTFOLIO_FUNDS", ""),
            evolution_last_run_path=os.path.join(ROOT, "evolution", "last_run.json"),
        )
    except Exception as e:
        LOGGER.warning("compute_health_report v2 unavailable, fallback to minimal report: %s", e)
        report = {
            "run_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "ok": False,
            "warnings": [f"health report unavailable: {e}"],
            "stats": {"db_path": str(db_path or "")},
        }

    # Auto-repair lagging funding tables once, then rebuild report.
    if _allow_auto_repair:
        lagging = [
            w for w in (report.get("warnings", []) or [])
            if (" lagging: " in str(w) or "not updated for " in str(w) or "not updated in last 3 trading days" in str(w))
        ]
        repair_result = _auto_repair_lagging_funding_tables(db_path, lagging)
        if repair_result:
            report.setdefault("stats", {})["auto_repair"] = repair_result
            LOGGER.info("auto repaired lagging funding tables: %s", repair_result)
            _write_health_report(db_path, _allow_auto_repair=False)
            return

    # Attach risk sentinel details for UI observability.
    try:
        risk = _load_json(RISK_SENTINEL_PATH)
        if risk:
            stats = report.setdefault("stats", {})
            risk_level = str(risk.get("risk_level", "unknown") or "unknown")
            risk_run_at = str(risk.get("run_at", "") or "")
            risk_age_mins = None
            risk_stale = False
            stale_mins = int(os.getenv("OPENCLAW_RISK_SENTINEL_STALE_MINUTES", "180"))
            try:
                risk_dt = datetime.strptime(risk_run_at[:19], "%Y-%m-%d %H:%M:%S") if risk_run_at else None
                if risk_dt is not None:
                    risk_age_mins = int(max(0.0, (datetime.now() - risk_dt).total_seconds() // 60))
                    risk_stale = bool(risk_age_mins > stale_mins)
            except Exception:
                risk_dt = None

            stats["risk_level"] = risk_level
            stats["risk_rules"] = risk.get("triggered_rules", [])
            stats["risk_run_at"] = risk_run_at
            stats["risk_age_mins"] = risk_age_mins
            stats["risk_stale"] = risk_stale
            stats["risk_stale_threshold_mins"] = stale_mins
            if risk_level.lower() in {"orange", "red"} and (not risk_stale):
                report.setdefault("warnings", []).append(f"risk sentinel={risk_level}")
    except Exception as e:
        report.setdefault("warnings", []).append(f"risk sentinel read failed: {e}")

    try:
        out_path = os.path.join(ROOT, "evolution", "health_report.json")
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        LOGGER.info("health report updated: %s", out_path)
    except Exception as e:
        LOGGER.error("health report write failed: %s", e)


def main() -> None:
    # Prevent overlapping runs
    try:
        lock_fd = os.open(LOCK_PATH, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(lock_fd, str(os.getpid()).encode("utf-8"))
        os.close(lock_fd)
    except FileExistsError:
        LOGGER.warning("another auto_evolve run is in progress, exiting")
        return

    try:
        config = _load_config()
        db_path = config.get("PERMANENT_DB_PATH")
        if not db_path or not os.path.exists(db_path):
            LOGGER.error("db path not found in config.json")
            return

        phase = _run_phase()
        LOGGER.info("auto evolve started (phase=%s)", phase)

        enforce_window = str(os.getenv("AUTO_EVOLVE_ENFORCE_WINDOW", "1")).strip().lower() not in {"0", "false", "no"}
        cn_now = _get_cn_now()
        if enforce_window and phase in {"full", "optimize_only"} and not _in_heavy_job_window(cn_now):
            LOGGER.warning(
                "skip heavy phase outside window: now=%s, allowed_window=[%s:00,%s:00)",
                cn_now.strftime("%Y-%m-%d %H:%M:%S"),
                os.getenv("AUTO_EVOLVE_WINDOW_START_HOUR", "1"),
                os.getenv("AUTO_EVOLVE_WINDOW_END_HOUR", "9"),
            )
            return

        if phase in {"full", "data_only"}:
            # 1) 更新数据（收盘后）
            update_days = int(os.getenv("UPDATE_DAYS", "30"))
            LOGGER.info("updating data: last %s days", update_days)
            update_result = _update_stock_data(db_path, days=update_days)
            LOGGER.info("update result: %s", update_result)

            # 1.1) 数据完整性检查（交易日收盘后+延迟才严格要求最新）
            fresh, enforce, db_last, last_trade, is_trade_day, ready_time = _data_freshness_status(db_path)
            LOGGER.info(
                "data freshness: db_last=%s, last_trade=%s, fresh=%s, enforce=%s, is_trade_day=%s, ready_time=%s",
                db_last,
                last_trade,
                fresh,
                enforce,
                is_trade_day,
                ready_time,
            )
            if not fresh and enforce:
                LOGGER.warning("data not fresh yet (enforced window), exiting early to wait for next run")
                return
            if not fresh and not enforce:
                LOGGER.warning("data not fresh but within non-enforced window, continue with latest available data")

            # 2) 更新市值
            mc_result = _update_market_cap(db_path)
            LOGGER.info("market cap update: %s", mc_result)

            # 2.1) 扩展资金类数据（北向/融资/基金持仓）
            nb_result = _update_northbound(db_path)
            LOGGER.info("northbound update: %s", nb_result)
            margin_result = _update_margin(db_path)
            LOGGER.info("margin update: %s", margin_result)
            margin_detail_result = _update_margin_detail(db_path)
            LOGGER.info("margin detail update: %s", margin_detail_result)
            moneyflow_result = _update_moneyflow_daily(db_path)
            LOGGER.info("moneyflow update: %s", moneyflow_result)
            industry_flow_result = _update_moneyflow_industry(db_path)
            LOGGER.info("industry moneyflow update: %s", industry_flow_result)
            top_list_result = _update_top_list(db_path)
            LOGGER.info("top list update: %s", top_list_result)
            top_inst_result = _update_top_inst(db_path)
            LOGGER.info("top inst update: %s", top_inst_result)
            fund_result = _update_fund_portfolio(db_path)
            LOGGER.info("fund portfolio update: %s", fund_result)

            if phase == "data_only":
                _write_health_report(db_path)
                LOGGER.info("auto evolve finished (phase=data_only)")
                return

        # 3) 加载回测数据
        LOGGER.info("loading backtest data")
        df = _load_backtest_data(db_path, lookback_days=420)
        if df is None or df.empty:
            LOGGER.error("no data for backtest (empty dataframe)")
            return
        try:
            LOGGER.info("backtest rows=%s, stocks=%s, date_min=%s, date_max=%s",
                        len(df),
                        df["ts_code"].nunique() if "ts_code" in df.columns else "n/a",
                        df["trade_date"].min() if "trade_date" in df.columns else "n/a",
                        df["trade_date"].max() if "trade_date" in df.columns else "n/a")
        except Exception as e:
            LOGGER.warning("backtest stats failed: %s", e)

        index_df = _load_index_data(db_path, lookback_days=420)
        if index_df is None or index_df.empty:
            LOGGER.warning("index data empty (v8 evolution may be skipped)")
        train_df, test_df = _split_train_test(df, test_ratio=0.25)
        LOGGER.info("walk-forward split: train_rows=%s test_rows=%s", len(train_df), len(test_df))

        targets = _evolve_targets()

        # 4) 全参数网格优化
        if "v4" in targets:
            try:
                LOGGER.info("grid search v4 started")
                best_params, best_stats, best_score = _grid_search(df)
                LOGGER.info("grid search v4 finished: best_score=%s", best_score)
            except Exception as e:
                LOGGER.error("grid search v4 failed: %s", e, exc_info=True)
                return
            if not best_params:
                LOGGER.error("no valid params found")
                return

            # 5) 写入结果
            oos_v4 = _compute_oos_stats("V4", best_params, train_df, test_df, db_path, index_df=index_df)
            d_v4 = _write_reports(best_params, best_stats, best_score, oos_stats=oos_v4, db_path=db_path)
            _save_to_db(db_path, best_params, best_stats, best_score, approved=bool(d_v4.get("approved", False)))
            LOGGER.info("promotion decision V4: %s", d_v4)

        # 6) AI智能选股进化（V5 + V2）
        if "ai_v5" in targets:
            try:
                LOGGER.info("grid search ai_v5 started")
                ai_v5_params, ai_v5_stats, ai_v5_score = _grid_search_ai_v5(df, db_path)
                LOGGER.info("grid search ai_v5 finished: score=%s", ai_v5_score)
            except Exception as e:
                LOGGER.error("grid search ai_v5 failed: %s", e, exc_info=True)
                ai_v5_params = None
            if ai_v5_params:
                oos_ai_v5 = _compute_oos_stats("AI_V5", ai_v5_params, train_df, test_df, db_path, index_df=index_df)
                d_ai_v5 = _write_ai_report("ai_v5_best.json", "AI_V5", ai_v5_params, ai_v5_stats, ai_v5_score, oos_stats=oos_ai_v5, db_path=db_path)
                _save_ai_to_db(db_path, "AI_V5", ai_v5_params, ai_v5_stats, ai_v5_score, approved=bool(d_ai_v5.get("approved", False)))
                LOGGER.info("promotion decision AI_V5: %s", d_ai_v5)
            else:
                LOGGER.warning("AI V5 evolution failed: no params")

        if "ai_v2" in targets:
            try:
                LOGGER.info("grid search ai_v2 started")
                ai_v2_params, ai_v2_stats, ai_v2_score = _grid_search_ai_v2(df)
                LOGGER.info("grid search ai_v2 finished: score=%s", ai_v2_score)
            except Exception as e:
                LOGGER.error("grid search ai_v2 failed: %s", e, exc_info=True)
                ai_v2_params = None
            if ai_v2_params:
                oos_ai_v2 = _compute_oos_stats("AI_V2", ai_v2_params, train_df, test_df, db_path, index_df=index_df)
                d_ai_v2 = _write_ai_report("ai_v2_best.json", "AI_V2", ai_v2_params, ai_v2_stats, ai_v2_score, oos_stats=oos_ai_v2, db_path=db_path)
                _save_ai_to_db(db_path, "AI_V2", ai_v2_params, ai_v2_stats, ai_v2_score, approved=bool(d_ai_v2.get("approved", False)))
                LOGGER.info("promotion decision AI_V2: %s", d_ai_v2)
            else:
                LOGGER.warning("AI V2 evolution failed: no params")

        # 7) 核心策略进化（v5/v6/v7/v8）
        if "v5" in targets:
            try:
                LOGGER.info("grid search v5 started")
                v5_params, v5_stats, v5_score = _grid_search_v5(df)
                LOGGER.info("grid search v5 finished: score=%s", v5_score)
            except Exception as e:
                LOGGER.error("grid search v5 failed: %s", e, exc_info=True)
                v5_params = None
            if v5_params:
                oos_v5 = _compute_oos_stats("V5", v5_params, train_df, test_df, db_path, index_df=index_df)
                d_v5 = _write_ai_report(
                    "v5_best.json",
                    "V5",
                    v5_params,
                    v5_stats,
                    v5_score,
                    oos_stats=oos_v5,
                    db_path=db_path,
                    validation_mode="rolling",
                )
                _save_ai_to_db(db_path, "V5", v5_params, v5_stats, v5_score, approved=bool(d_v5.get("approved", False)))
                LOGGER.info("promotion decision V5: %s", d_v5)
            else:
                LOGGER.warning("V5 evolution failed: no params")

        if "v6" in targets:
            try:
                LOGGER.info("grid search v6 started")
                v6_params, v6_stats, v6_score = _grid_search_v6(df)
                LOGGER.info("grid search v6 finished: score=%s", v6_score)
            except Exception as e:
                LOGGER.error("grid search v6 failed: %s", e, exc_info=True)
                v6_params = None
            if v6_params:
                oos_v6 = _compute_oos_stats("V6", v6_params, train_df, test_df, db_path, index_df=index_df)
                d_v6 = _write_ai_report("v6_best.json", "V6", v6_params, v6_stats, v6_score, oos_stats=oos_v6, db_path=db_path)
                _save_ai_to_db(db_path, "V6", v6_params, v6_stats, v6_score, approved=bool(d_v6.get("approved", False)))
                LOGGER.info("promotion decision V6: %s", d_v6)
            else:
                LOGGER.warning("V6 evolution failed: no params")

        if "v7" in targets:
            try:
                LOGGER.info("grid search v7 started")
                v7_params, v7_stats, v7_score = _grid_search_v7(df, db_path)
                LOGGER.info("grid search v7 finished: score=%s", v7_score)
            except Exception as e:
                LOGGER.error("grid search v7 failed: %s", e, exc_info=True)
                v7_params = None
            if v7_params:
                oos_v7 = _compute_oos_stats("V7", v7_params, train_df, test_df, db_path, index_df=index_df)
                d_v7 = _write_ai_report("v7_best.json", "V7", v7_params, v7_stats, v7_score, oos_stats=oos_v7, db_path=db_path)
                _save_ai_to_db(db_path, "V7", v7_params, v7_stats, v7_score, approved=bool(d_v7.get("approved", False)))
                LOGGER.info("promotion decision V7: %s", d_v7)
            else:
                LOGGER.warning("V7 evolution failed: no params")

        if "v8" in targets:
            if index_df is not None and not index_df.empty:
                try:
                    LOGGER.info("grid search v8 started")
                    v8_params, v8_stats, v8_score = _grid_search_v8(df, db_path, index_df)
                    LOGGER.info("grid search v8 finished: score=%s", v8_score)
                except Exception as e:
                    LOGGER.error("grid search v8 failed: %s", e, exc_info=True)
                    v8_params = None
                if v8_params:
                    oos_v8 = _compute_oos_stats("V8", v8_params, train_df, test_df, db_path, index_df=index_df)
                    d_v8 = _write_ai_report(
                        "v8_best.json",
                        "V8",
                        v8_params,
                        v8_stats,
                        v8_score,
                        oos_stats=oos_v8,
                        db_path=db_path,
                        validation_mode=str((v8_stats or {}).get("validation_mode", "single")),
                    )
                    _save_ai_to_db(db_path, "V8", v8_params, v8_stats, v8_score, approved=bool(d_v8.get("approved", False)))
                    LOGGER.info("promotion decision V8: %s", d_v8)
                else:
                    LOGGER.warning("V8 evolution failed: no params")
            else:
                LOGGER.warning("V8 evolution skipped: no index data")

        # 8) v9 中线均衡版进化
        if "v9" in targets:
            try:
                LOGGER.info("grid search v9 started")
                v9_params, v9_stats, v9_score = _grid_search_v9(df)
                LOGGER.info("grid search v9 finished: score=%s", v9_score)
            except Exception as e:
                LOGGER.error("grid search v9 failed: %s", e, exc_info=True)
                v9_params = None
            if v9_params:
                oos_v9 = _compute_oos_stats("V9", v9_params, train_df, test_df, db_path, index_df=index_df)
                d_v9 = _write_ai_report(
                    "v9_best.json",
                    "V9",
                    v9_params,
                    v9_stats,
                    v9_score,
                    oos_stats=oos_v9,
                    db_path=db_path,
                    validation_mode=str((v9_stats or {}).get("validation_mode", "single")),
                )
                _save_ai_to_db(db_path, "V9", v9_params, v9_stats, v9_score, approved=bool(d_v9.get("approved", False)))
                LOGGER.info("promotion decision V9: %s", d_v9)
            else:
                LOGGER.warning("V9 evolution failed: no params")

        # 8.5) 组合策略（生产共识评分）参数进化
        if "combo" in targets:
            try:
                LOGGER.info("combo evolve started (from production v5/v8/v9)")
                combo_params, combo_stats, combo_score, combo_validation_mode = _build_combo_candidate_from_production()
            except Exception as e:
                LOGGER.error("combo evolve failed: %s", e, exc_info=True)
                combo_params = {}
                combo_stats = {}
                combo_score = -1e9
                combo_validation_mode = "single"
            if combo_params:
                d_combo = _write_ai_report(
                    "combo_best.json",
                    "COMBO",
                    combo_params,
                    combo_stats,
                    combo_score,
                    oos_stats={},
                    db_path=db_path,
                    validation_mode=str(combo_validation_mode or "single"),
                )
                _save_ai_to_db(db_path, "COMBO", combo_params, combo_stats, combo_score, approved=bool(d_combo.get("approved", False)))
                LOGGER.info("promotion decision COMBO: %s", d_combo)
            else:
                LOGGER.warning("COMBO evolution failed: no params")

        # 9) 稳定上涨策略进化
        if "stable" in targets:
            try:
                LOGGER.info("grid search stable_uptrend started")
                stable_params, stable_stats, stable_score = _grid_search_stable_uptrend(df)
                LOGGER.info("grid search stable_uptrend finished: score=%s", stable_score)
            except Exception as e:
                LOGGER.error("grid search stable_uptrend failed: %s", e, exc_info=True)
                stable_params = None
            if stable_params:
                oos_stable = _compute_oos_stats("STABLE_UPTREND", stable_params, train_df, test_df, db_path, index_df=index_df)
                d_stable = _write_ai_report("stable_uptrend_best.json", "STABLE_UPTREND", stable_params, stable_stats, stable_score, oos_stats=oos_stable, db_path=db_path)
                _save_ai_to_db(db_path, "STABLE_UPTREND", stable_params, stable_stats, stable_score, approved=bool(d_stable.get("approved", False)))
                LOGGER.info("promotion decision STABLE_UPTREND: %s", d_stable)
            else:
                LOGGER.warning("Stable uptrend evolution failed: no params")

        # 9.5) 自动健康检测报告
        _write_health_report(db_path)

        # 10) 可选推送
        _git_push()

        LOGGER.info("auto evolve finished")
    except Exception as e:
        LOGGER.error("auto evolve crashed: %s", e, exc_info=True)
    finally:
        try:
            os.remove(LOCK_PATH)
        except Exception:
            pass


if __name__ == "__main__":
    main()
