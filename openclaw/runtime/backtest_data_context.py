from __future__ import annotations

from datetime import datetime, timedelta
import glob
import json
import os
import sqlite3
from typing import Any, Callable, Dict, List, Tuple

import pandas as pd


_BACKTEST_HISTORY_CACHE: Dict[str, Any] = {}


def load_backtest_history_df(
    *,
    app_root: str,
    permanent_db_path: str,
    connect_permanent_db: Callable[[], sqlite3.Connection],
    ensure_price_aliases: Callable[[pd.DataFrame], pd.DataFrame],
    now_ts: Callable[[], float],
    days: int = 240,
    use_cache: bool = True,
) -> pd.DataFrame:
    from data.dao import DataAccessError, detect_daily_table  # type: ignore

    cache_ttl_sec = int(os.getenv("OPENCLAW_BACKTEST_HISTORY_CACHE_TTL", "300"))
    safe_days = max(30, int(days))
    db_mtime = 0.0
    try:
        db_mtime = float(os.path.getmtime(permanent_db_path))
    except Exception:
        db_mtime = 0.0
    current_ts = now_ts()
    if use_cache:
        cached_df = _BACKTEST_HISTORY_CACHE.get("df")
        cached_at = float(_BACKTEST_HISTORY_CACHE.get("created_at", 0.0) or 0.0)
        cached_days = int(_BACKTEST_HISTORY_CACHE.get("days", 0) or 0)
        cached_db_mtime = float(_BACKTEST_HISTORY_CACHE.get("db_mtime", 0.0) or 0.0)
        if (
            isinstance(cached_df, pd.DataFrame)
            and not cached_df.empty
            and cached_days == safe_days
            and abs(cached_db_mtime - db_mtime) < 1e-6
            and (current_ts - cached_at) <= cache_ttl_sec
        ):
            return cached_df.copy()

    conn = connect_permanent_db()
    try:
        try:
            daily_table = detect_daily_table(conn)
        except DataAccessError as exc:
            raise RuntimeError(f"无法识别日线数据表: {exc}")
        start_date = (datetime.now() - timedelta(days=safe_days)).strftime("%Y%m%d")
        query = f"""
            SELECT dtd.ts_code, sb.name, sb.industry, dtd.trade_date,
                   dtd.open_price, dtd.high_price, dtd.low_price,
                   dtd.close_price, dtd.vol, dtd.pct_chg, dtd.amount
            FROM {daily_table} dtd
            INNER JOIN stock_basic sb ON dtd.ts_code = sb.ts_code
            WHERE dtd.trade_date >= ?
            ORDER BY dtd.ts_code, dtd.trade_date
        """
        df = pd.read_sql_query(query, conn, params=(start_date,))
        out = ensure_price_aliases(df)
        if use_cache and isinstance(out, pd.DataFrame) and not out.empty:
            _BACKTEST_HISTORY_CACHE["created_at"] = current_ts
            _BACKTEST_HISTORY_CACHE["db_mtime"] = db_mtime
            _BACKTEST_HISTORY_CACHE["days"] = safe_days
            _BACKTEST_HISTORY_CACHE["df"] = out.copy()
        return out
    finally:
        conn.close()


def get_db_last_trade_date(*, db_path: str) -> str:
    try:
        from data.history import get_db_last_trade_date as get_db_last_trade_date_v2  # type: ignore
        return get_db_last_trade_date_v2(db_path)
    except Exception:
        pass
    try:
        from data.dao import latest_trade_date as latest_trade_date_v2  # type: ignore
        conn = sqlite3.connect(db_path)
        max_date = latest_trade_date_v2(conn)
        conn.close()
        if max_date:
            return str(max_date)
    except Exception:
        pass
    return ""


def load_latest_production_backtest_audit(*, app_root: str) -> Tuple[pd.DataFrame, str]:
    logs_dir = os.path.join(app_root, "logs", "openclaw")
    if not os.path.isdir(logs_dir):
        return pd.DataFrame(), ""

    prod = ["v9", "v8", "v5", "combo"]
    rows: List[Dict[str, Any]] = []
    sources: List[str] = []

    def _risk_level(sample_size: int, win_rate: float, max_drawdown: float, failed_windows: int) -> str:
        if sample_size < 30 or max_drawdown > 0.30 or failed_windows > 0:
            return "red"
        if win_rate < 0.50 or max_drawdown > 0.20:
            return "yellow"
        return "green"

    def _next_action(strategy: str, win_rate: float, max_drawdown: float) -> str:
        if strategy == "v5":
            if max_drawdown > 0.25:
                return "先降持仓到5天验证回撤，再评估3天。"
            return "可小流量灰度验证。"
        if strategy == "v8":
            return "暂停晋升；扩大阈值到55-70并启用滚动验证。"
        if strategy == "v9":
            return "暂停晋升；先降信号密度并将回撤压到25%内。"
        return "保留共识层观察，补滚动样本外后再定。"

    for strategy in prod:
        files = sorted(glob.glob(os.path.join(logs_dir, f"backtest_sweep_{strategy}_*.json")))
        if not files:
            continue
        latest = files[-1]
        try:
            with open(latest, "r", encoding="utf-8") as handle:
                payload = json.load(handle) or {}
            best = payload.get("best") or {}
            if not isinstance(best, dict):
                continue
            sample_size = int(best.get("sample_size", 0) or 0)
            win_rate = float(best.get("win_rate", 0.0) or 0.0)
            max_drawdown = float(best.get("max_drawdown", 0.0) or 0.0)
            objective = float(best.get("objective", 0.0) or 0.0)
            signal_density = float(best.get("signal_density", 0.0) or 0.0)
            failed_windows = int(best.get("rolling_failed_windows", 0) or 0)
            total_windows = int(best.get("rolling_test_windows", 0) or 0)
            holding_days = int(best.get("holding_days", 0) or 0)
            threshold = float(best.get("score_threshold", 0.0) or 0.0)
            approved = (
                sample_size >= 50
                and win_rate >= 0.52
                and max_drawdown <= 0.25
                and failed_windows <= 0
            )
            risk = _risk_level(sample_size, win_rate, max_drawdown, failed_windows)
            rows.append(
                {
                    "策略": strategy,
                    "样本": sample_size,
                    "胜率(%)": round(win_rate * 100.0, 1),
                    "最大回撤(%)": round(max_drawdown * 100.0, 1),
                    "信号密度": round(signal_density, 3),
                    "目标值": round(objective, 3),
                    "滚动失败窗": f"{failed_windows}/{total_windows}",
                    "参数(阈值/持仓)": f"{int(round(threshold))}/{holding_days}",
                    "评估风险": risk.upper(),
                    "建议晋升": "YES" if approved else "NO",
                    "建议动作": _next_action(strategy, win_rate, max_drawdown),
                }
            )
            sources.append(os.path.basename(latest))
        except Exception:
            continue

    if not rows:
        return pd.DataFrame(), ""
    out = pd.DataFrame(rows)
    order_map = {k: i for i, k in enumerate(prod)}
    out["__order"] = out["策略"].map(order_map).fillna(999).astype(int)
    out = out.sort_values("__order").drop(columns=["__order"])
    return out, " | ".join(sources[-4:])
