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
from datetime import datetime, timedelta
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


def _get_last_trade_date(pro: ts.pro_api) -> str | None:
    """Get last open trade date from SSE calendar."""
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=20)).strftime("%Y%m%d")
    try:
        trade_cal = pro.trade_cal(exchange="SSE", start_date=start_date, end_date=end_date, is_open="1")
        if trade_cal is None or trade_cal.empty:
            return None
        return trade_cal["cal_date"].iloc[-1]
    except Exception:
        return None


def _get_db_latest_trade_date(db_path: str) -> str | None:
    try:
        conn = _connect(db_path)
        df = pd.read_sql_query(
            f"SELECT MAX(trade_date) AS max_date FROM daily_trading_data WHERE ts_code = '{SSE_INDEX_CODE}'",
            conn,
        )
        conn.close()
        if df is None or df.empty:
            return None
        return str(df["max_date"].iloc[0]) if df["max_date"].iloc[0] else None
    except Exception:
        return None


def _get_recent_trade_date(pro: ts.pro_api, lookback_days: int = 30) -> str | None:
    """Get most recent open trade date within lookback window."""
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y%m%d")
    try:
        trade_cal = pro.trade_cal(exchange="SSE", start_date=start_date, end_date=end_date, is_open="1")
        if trade_cal is None or trade_cal.empty:
            return None
        return trade_cal["cal_date"].iloc[-1]
    except Exception:
        return None


def _is_data_fresh(db_path: str) -> Tuple[bool, str | None, str | None]:
    """Check if DB latest trade date matches exchange last trade date."""
    token = _load_tushare_token()
    if not token:
        return False, None, None
    pro = ts.pro_api(token)
    last_trade = _get_last_trade_date(pro)
    db_last = _get_db_latest_trade_date(db_path)
    return (last_trade is not None and db_last == last_trade), db_last, last_trade


def _ensure_table(conn: sqlite3.Connection, ddl: str) -> None:
    try:
        conn.execute(ddl)
        conn.commit()
    except Exception:
        pass


def _update_northbound(db_path: str) -> Dict:
    token = _load_tushare_token()
    if not token:
        return {"success": False, "error": "Tushare token not found"}
    pro = ts.pro_api(token)

    last_trade = _get_recent_trade_date(pro, lookback_days=60)
    if not last_trade:
        return {"success": False, "error": "no recent trade date"}

    start_date = (datetime.now() - timedelta(days=60)).strftime("%Y%m%d")
    try:
        df = pro.moneyflow_hsgt(start_date=start_date, end_date=last_trade)
    except Exception as e:
        return {"success": False, "error": f"moneyflow_hsgt failed: {e}"}

    if df is None or df.empty:
        return {"success": False, "error": "moneyflow_hsgt empty"}

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

    last_trade = _get_recent_trade_date(pro, lookback_days=60)
    if not last_trade:
        return {"success": False, "error": "no recent trade date"}

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
        trade_cal = pro.trade_cal(exchange="SSE", start_date=start_date, end_date=end_date, is_open="1")
    except Exception:
        return {"success": False, "error": "failed to fetch trade calendar"}

    if trade_cal.empty:
        return {"success": False, "error": "empty trade calendar"}

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


def _score_result(stats: Dict) -> float:
    sharpe = stats.get("sharpe_ratio", 0) or 0
    w_avg = stats.get("weighted_avg_return", 0) or 0
    win = stats.get("win_rate", 0) or 0
    avg_ret = stats.get("avg_return", 0) or 0
    max_dd = stats.get("max_drawdown", 0) or 0
    score = (sharpe * 1.5) + (w_avg * 0.12) + (avg_ret * 0.08) + (win * 0.02) - (abs(max_dd) * 0.05)
    return float(score)


def _grid_search(df: pd.DataFrame) -> Tuple[Dict, Dict, float]:
    evaluator = ComprehensiveStockEvaluatorV4()
    np.random.seed(42)

    thresholds = [55, 60, 65, 70]
    max_holding_days = [10, 15, 20]
    stop_losses = [-6.0, -5.0, -4.0]
    take_profits = [6.0, 8.0, 10.0]

    best_params = {}
    best_stats = {}
    best_score = -1e9

    for threshold in thresholds:
        for hold_days in max_holding_days:
            for stop_loss in stop_losses:
                for take_profit in take_profits:
                    result = backtest_with_dynamic_strategy(
                        evaluator,
                        df,
                        sample_size=800,
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

    return best_params, best_stats, best_score


def _write_reports(best_params: Dict, best_stats: Dict, best_score: float) -> None:
    os.makedirs(EVOLUTION_DIR, exist_ok=True)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    report = {
        "strategy": "综合优选v4.0",
        "run_at": now,
        "score": best_score,
        "params": best_params,
        "stats": best_stats,
    }
    with open(os.path.join(EVOLUTION_DIR, "best_params.json"), "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
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


def _write_ai_report(filename: str, strategy: str, best_params: Dict, best_stats: Dict, best_score: float) -> None:
    os.makedirs(EVOLUTION_DIR, exist_ok=True)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    report = {
        "strategy": strategy,
        "run_at": now,
        "score": best_score,
        "params": best_params,
        "stats": best_stats,
    }
    with open(os.path.join(EVOLUTION_DIR, filename), "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)


def _save_to_db(db_path: str, best_params: Dict, best_stats: Dict, best_score: float) -> None:
    _ensure_evolution_tables(db_path)
    conn = _connect(db_path)
    cur = conn.cursor()
    run_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    params_json = json.dumps(best_params, ensure_ascii=False)
    stats_json = json.dumps(best_stats, ensure_ascii=False)
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


def _save_ai_to_db(db_path: str, strategy: str, best_params: Dict, best_stats: Dict, best_score: float) -> None:
    _ensure_evolution_tables(db_path)
    conn = _connect(db_path)
    cur = conn.cursor()
    run_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    params_json = json.dumps(best_params, ensure_ascii=False)
    stats_json = json.dumps(best_stats, ensure_ascii=False)
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


def _backtest_simple_v5(df: pd.DataFrame, score_threshold: float, holding_days: int) -> Dict:
    evaluator = ComprehensiveStockEvaluatorV4()
    returns = []
    unique_stocks = df["ts_code"].unique()
    sample_size = min(1200, len(unique_stocks))
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
    sample_size = min(1200, len(unique_stocks))
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
    sample_size = min(600, len(unique_stocks))
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
    sample_size = min(500, len(unique_stocks))
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
    thresholds = [50, 55, 60, 65]
    holding_days = [5, 10, 15]
    best_params, best_stats, best_score = {}, {}, -1e9
    for thr in thresholds:
        for hd in holding_days:
            stats = _backtest_simple_v5(df, thr, hd)
            score = _score_result(stats)
            if score > best_score:
                best_score, best_params, best_stats = score, {"score_threshold": thr, "holding_days": hd}, stats
                LOGGER.info("v5 best score=%.3f params=%s", best_score, best_params)
    return best_params, best_stats, best_score


def _grid_search_v6(df: pd.DataFrame) -> Tuple[Dict, Dict, float]:
    thresholds = [70, 75, 80, 85]
    holding_days = [3, 5]
    best_params, best_stats, best_score = {}, {}, -1e9
    for thr in thresholds:
        for hd in holding_days:
            stats = _backtest_simple_v6(df, thr, hd)
            score = _score_result(stats)
            if score > best_score:
                best_score, best_params, best_stats = score, {"score_threshold": thr, "holding_days": hd}, stats
                LOGGER.info("v6 best score=%.3f params=%s", best_score, best_params)
    return best_params, best_stats, best_score


def _grid_search_v7(df: pd.DataFrame, db_path: str) -> Tuple[Dict, Dict, float]:
    thresholds = [60, 65, 70, 75]
    holding_days = [3, 5, 8]
    best_params, best_stats, best_score = {}, {}, -1e9
    for thr in thresholds:
        for hd in holding_days:
            stats = _backtest_simple_v7(df, thr, hd, db_path)
            score = _score_result(stats)
            if score > best_score:
                best_score, best_params, best_stats = score, {"score_threshold": thr, "holding_days": hd}, stats
                LOGGER.info("v7 best score=%.3f params=%s", best_score, best_params)
    return best_params, best_stats, best_score


def _grid_search_v8(df: pd.DataFrame, db_path: str, index_data: pd.DataFrame) -> Tuple[Dict, Dict, float]:
    thresholds = [60, 65, 70, 75, 80]
    holding_days = [3, 5, 8]
    best_params, best_stats, best_score = {}, {}, -1e9
    for thr in thresholds:
        for hd in holding_days:
            stats = _backtest_simple_v8(df, thr, hd, db_path, index_data)
            score = _score_result(stats)
            if score > best_score:
                best_score, best_params, best_stats = score, {"score_threshold": thr, "holding_days": hd}, stats
                LOGGER.info("v8 best score=%.3f params=%s", best_score, best_params)
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
    sample_size = min(600, len(unique_stocks))
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
    thresholds = [55, 60, 65, 70]
    holding_days = [10, 15, 20, 25]
    lookback_days = [120, 160]
    min_turnovers = [3.0, 5.0, 8.0]
    best_params, best_stats, best_score = {}, {}, -1e9
    for thr in thresholds:
        for hd in holding_days:
            for lb in lookback_days:
                for mt in min_turnovers:
                    stats = _backtest_simple_v9(df, thr, hd, lb, mt)
                    score = _score_result(stats)
                    if score > best_score:
                        best_score, best_params, best_stats = score, {
                            "score_threshold": thr,
                            "holding_days": hd,
                            "lookback_days": lb,
                            "min_turnover": mt,
                        }, stats
                        LOGGER.info("v9 best score=%.3f params=%s", best_score, best_params)
    return best_params, best_stats, best_score


def _grid_search_stable_uptrend(df: pd.DataFrame) -> Tuple[Dict, Dict, float]:
    # Use last ~240 trading days for evaluation
    df = df.copy()
    df = df.sort_values(["ts_code", "trade_date"])
    grouped = df.groupby("ts_code")
    ts_codes = [ts for ts, g in grouped if len(g) >= 120]
    if len(ts_codes) > 400:
        np.random.seed(42)
        ts_codes = list(np.random.choice(ts_codes, 400, replace=False))

    lookback_days_list = [80, 120, 160]
    max_drawdown_list = [0.10, 0.15, 0.20]
    vol_max_list = [0.03, 0.04, 0.05]
    rebound_min_list = [0.08, 0.10, 0.12]
    min_turnover_list = [3.0, 5.0, 8.0]

    best_params, best_stats, best_score = {}, {}, -1e9

    for lb in lookback_days_list:
        for mdd in max_drawdown_list:
            for vol in vol_max_list:
                for rb in rebound_min_list:
                    for mt in min_turnover_list:
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

    return best_params, best_stats, best_score


def _grid_search_ai_v5(df: pd.DataFrame, db_path: str) -> Tuple[Dict, Dict, float]:
    cache = _prepare_stock_cache(df, sample_size=400)
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

    for tr in target_returns:
        for ma in min_amounts:
            for mv in max_vols:
                for min_mc in min_mcaps:
                    for max_mc in max_mcaps:
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

    return best_params, best_stats, best_score


def _grid_search_ai_v2(df: pd.DataFrame) -> Tuple[Dict, Dict, float]:
    cache = _prepare_stock_cache(df, sample_size=400)
    if not cache:
        return {}, {}, -1e9
    eval_dates = _get_eval_dates(df, lookback_days=220, step=12)

    target_returns = [0.15, 0.20, 0.25]
    min_amounts = [1.5, 2.5, 4.0]
    max_vols = [0.10, 0.12, 0.15]

    best_params = {}
    best_stats = {}
    best_score = -1e9

    for tr in target_returns:
        for ma in min_amounts:
            for mv in max_vols:
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

    return best_params, best_stats, best_score


def _git_push() -> None:
    auto_push = os.getenv("AUTO_PUSH", "1").strip() in ("1", "true", "yes")
    if not auto_push:
        LOGGER.info("AUTO_PUSH disabled")
        return
    try:
        candidates = [
            "evolution/best_params.json",
            "evolution/last_run.json",
            "evolution/last_run.csv",
            "evolution/ai_v5_best.json",
            "evolution/ai_v2_best.json",
            "evolution/v5_best.json",
            "evolution/v6_best.json",
            "evolution/v7_best.json",
            "evolution/v8_best.json",
            "evolution/v9_best.json",
            "evolution/stable_uptrend_best.json",
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

        LOGGER.info("auto evolve started")

        # 1) 更新数据（收盘后）
        update_days = int(os.getenv("UPDATE_DAYS", "30"))
        LOGGER.info("updating data: last %s days", update_days)
        update_result = _update_stock_data(db_path, days=update_days)
        LOGGER.info("update result: %s", update_result)

        # 1.1) 数据完整性检查（当日未更新则退出，等待下一次任务）
        fresh, db_last, last_trade = _is_data_fresh(db_path)
        LOGGER.info("data freshness: db_last=%s, last_trade=%s, fresh=%s", db_last, last_trade, fresh)
        if not fresh:
            LOGGER.warning("data not fresh yet, exiting early to wait for next run")
            return

        # 2) 更新市值
        mc_result = _update_market_cap(db_path)
        LOGGER.info("market cap update: %s", mc_result)

        # 2.1) 扩展资金类数据（北向/融资/基金持仓）
        nb_result = _update_northbound(db_path)
        LOGGER.info("northbound update: %s", nb_result)
        margin_result = _update_margin(db_path)
        LOGGER.info("margin update: %s", margin_result)
        fund_result = _update_fund_portfolio(db_path)
        LOGGER.info("fund portfolio update: %s", fund_result)

        # 3) 加载回测数据
        df = _load_backtest_data(db_path, lookback_days=420)
        if df.empty:
            LOGGER.error("no data for backtest")
            return
        index_df = _load_index_data(db_path, lookback_days=420)

        # 4) 全参数网格优化
        best_params, best_stats, best_score = _grid_search(df)
        if not best_params:
            LOGGER.error("no valid params found")
            return

        # 5) 写入结果
        _save_to_db(db_path, best_params, best_stats, best_score)
        _write_reports(best_params, best_stats, best_score)

        # 6) AI智能选股进化（V5 + V2）
        ai_v5_params, ai_v5_stats, ai_v5_score = _grid_search_ai_v5(df, db_path)
        if ai_v5_params:
            _write_ai_report("ai_v5_best.json", "AI_V5", ai_v5_params, ai_v5_stats, ai_v5_score)
            _save_ai_to_db(db_path, "AI_V5", ai_v5_params, ai_v5_stats, ai_v5_score)
        else:
            LOGGER.warning("AI V5 evolution failed: no params")

        ai_v2_params, ai_v2_stats, ai_v2_score = _grid_search_ai_v2(df)
        if ai_v2_params:
            _write_ai_report("ai_v2_best.json", "AI_V2", ai_v2_params, ai_v2_stats, ai_v2_score)
            _save_ai_to_db(db_path, "AI_V2", ai_v2_params, ai_v2_stats, ai_v2_score)
        else:
            LOGGER.warning("AI V2 evolution failed: no params")

        # 7) 核心策略进化（v5/v6/v7/v8）
        v5_params, v5_stats, v5_score = _grid_search_v5(df)
        if v5_params:
            _write_ai_report("v5_best.json", "V5", v5_params, v5_stats, v5_score)
            _save_ai_to_db(db_path, "V5", v5_params, v5_stats, v5_score)
        else:
            LOGGER.warning("V5 evolution failed: no params")

        v6_params, v6_stats, v6_score = _grid_search_v6(df)
        if v6_params:
            _write_ai_report("v6_best.json", "V6", v6_params, v6_stats, v6_score)
            _save_ai_to_db(db_path, "V6", v6_params, v6_stats, v6_score)
        else:
            LOGGER.warning("V6 evolution failed: no params")

        v7_params, v7_stats, v7_score = _grid_search_v7(df, db_path)
        if v7_params:
            _write_ai_report("v7_best.json", "V7", v7_params, v7_stats, v7_score)
            _save_ai_to_db(db_path, "V7", v7_params, v7_stats, v7_score)
        else:
            LOGGER.warning("V7 evolution failed: no params")

        if index_df is not None and not index_df.empty:
            v8_params, v8_stats, v8_score = _grid_search_v8(df, db_path, index_df)
            if v8_params:
                _write_ai_report("v8_best.json", "V8", v8_params, v8_stats, v8_score)
                _save_ai_to_db(db_path, "V8", v8_params, v8_stats, v8_score)
            else:
                LOGGER.warning("V8 evolution failed: no params")
        else:
            LOGGER.warning("V8 evolution skipped: no index data")

        # 8) v9 中线均衡版进化
        v9_params, v9_stats, v9_score = _grid_search_v9(df)
        if v9_params:
            _write_ai_report("v9_best.json", "V9", v9_params, v9_stats, v9_score)
            _save_ai_to_db(db_path, "V9", v9_params, v9_stats, v9_score)
        else:
            LOGGER.warning("V9 evolution failed: no params")

        # 9) 稳定上涨策略进化
        stable_params, stable_stats, stable_score = _grid_search_stable_uptrend(df)
        if stable_params:
            _write_ai_report("stable_uptrend_best.json", "STABLE_UPTREND", stable_params, stable_stats, stable_score)
            _save_ai_to_db(db_path, "STABLE_UPTREND", stable_params, stable_stats, stable_score)
        else:
            LOGGER.warning("Stable uptrend evolution failed: no params")

        # 10) 可选推送
        _git_push()

        LOGGER.info("auto evolve finished")
    finally:
        try:
            os.remove(LOCK_PATH)
        except Exception:
            pass


if __name__ == "__main__":
    main()
