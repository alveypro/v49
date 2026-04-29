from __future__ import annotations

import re
import sqlite3
from typing import Any, Dict, List

import pandas as pd


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if isinstance(value, str):
            value = value.replace(",", "").strip()
        return float(value)
    except Exception:
        return default


def safe_daily_table_name(name: str, fallback: str = "daily_trading_data") -> str:
    return name if name in {"daily_trading_data", "daily_data"} else fallback


def iter_sqlite_in_chunks(items: List[str], max_vars: int = 900) -> List[List[str]]:
    if not items:
        return []
    size = max(1, int(max_vars))
    return [items[i:i + size] for i in range(0, len(items), size)]


def canonical_ts_code(code: Any) -> str:
    raw = str(code or "").strip().upper()
    if not raw:
        return ""
    if re.fullmatch(r"\d{6}\.(SH|SZ)", raw):
        return raw
    if re.fullmatch(r"\d{6}", raw):
        return f"{raw}.SH" if raw.startswith(("5", "6", "9")) else f"{raw}.SZ"
    m = re.fullmatch(r"(SH|SZ)\.?(\d{6})", raw)
    if m:
        exch, digits = m.groups()
        return f"{digits}.{exch}"
    if re.fullmatch(r"\d{6}\.(XSHG|XSHE)", raw):
        digits, exch = raw.split(".")
        return f"{digits}.SH" if exch == "XSHG" else f"{digits}.SZ"
    return raw


def expand_ts_code_keys(code: Any) -> List[str]:
    raw = str(code or "").strip().upper()
    canonical = canonical_ts_code(raw)
    keys: List[str] = []
    for item in (raw, canonical):
        if item and item not in keys:
            keys.append(item)
    if canonical:
        digits = canonical.split(".")[0]
        if digits and digits not in keys:
            keys.append(digits)
    return keys


def connect_permanent_db(db_path: str) -> sqlite3.Connection:
    try:
        from data.dao import resolve_db_path as resolve_db_path_v2  # type: ignore

        conn = sqlite3.connect(str(resolve_db_path_v2(db_path)))
    except Exception:
        conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def load_real_stock_data(db_path: str) -> pd.DataFrame:
    from data.dao import detect_daily_table, recent_trade_profile  # type: ignore

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        daily_table = detect_daily_table(conn)
        profile = recent_trade_profile(conn, date_limit=1, recent_window=1)
        latest_date = str(profile.get("last_trade_date", "") or "")
        if not latest_date:
            return pd.DataFrame()
        query = f"""
            SELECT dtd.ts_code AS "股票代码",
                   sb.name AS "股票名称",
                   dtd.amount AS "成交额",
                   dtd.close_price AS "价格",
                   sb.circ_mv AS "流通市值"
            FROM {daily_table} dtd
            INNER JOIN stock_basic sb ON dtd.ts_code = sb.ts_code
            WHERE dtd.trade_date = ?
        """
        df = pd.read_sql_query(query, conn, params=(latest_date,))
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()
    if df is None or df.empty:
        return pd.DataFrame()
    df["成交额"] = pd.to_numeric(df["成交额"], errors="coerce").fillna(0.0)
    df["价格"] = pd.to_numeric(df["价格"], errors="coerce").fillna(0.0)
    df["流通市值"] = pd.to_numeric(df["流通市值"], errors="coerce").fillna(0.0)
    try:
        median_amount = float(df["成交额"].median())
        if 0 < median_amount < 1e7:
            df["成交额"] = df["成交额"] * 1000.0
    except Exception:
        pass
    return df


def load_candidate_stocks(
    conn: sqlite3.Connection,
    *,
    scan_all: bool = True,
    cap_min_yi: float = 0.0,
    cap_max_yi: float = 0.0,
    require_industry: bool = False,
    distinct: bool = True,
    random_order: bool = False,
) -> pd.DataFrame:
    select_kw = "SELECT DISTINCT" if distinct else "SELECT"
    where_parts: List[str] = []
    params: List[Any] = []
    use_cap = not (scan_all and cap_min_yi == 0 and cap_max_yi == 0)
    if require_industry:
        where_parts.append("sb.industry IS NOT NULL")
    if use_cap:
        cap_min_wan = cap_min_yi * 10000 if cap_min_yi > 0 else 0
        cap_max_wan = cap_max_yi * 10000 if cap_max_yi > 0 else 999999999
        where_parts.append("sb.circ_mv >= ?")
        where_parts.append("sb.circ_mv <= ?")
        params.extend([cap_min_wan, cap_max_wan])
    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    order_sql = "ORDER BY RANDOM()" if random_order else "ORDER BY sb.circ_mv DESC"
    query = f"""
        {select_kw} sb.ts_code, sb.name, sb.industry, sb.circ_mv
        FROM stock_basic sb
        {where_sql}
        {order_sql}
    """
    return pd.read_sql_query(query, conn, params=params)
