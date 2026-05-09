from __future__ import annotations

import sqlite3
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd


def load_external_bonus_maps(
    *,
    conn: sqlite3.Connection,
    fund_bonus_enabled: Callable[[], bool],
) -> Tuple[float, Dict[str, float], set, set, Dict[str, float]]:
    """Load external money/flow bonus maps from DB."""
    if not fund_bonus_enabled():
        return 0.0, {}, set(), set(), {}
    bonus_global = 0.0
    bonus_stock: Dict[str, float] = {}
    bonus_industry: Dict[str, float] = {}
    top_list_set = set()
    top_inst_set = set()
    last_trade = None

    try:
        from data.dao import latest_trade_date as latest_trade_date_v2  # type: ignore
        last_trade = latest_trade_date_v2(conn)
    except Exception:
        last_trade = None

    try:
        nb = pd.read_sql_query("SELECT north_money FROM northbound_flow ORDER BY trade_date DESC LIMIT 5", conn)
        if not nb.empty:
            nb_mean = float(nb["north_money"].mean())
            if nb_mean > 0:
                bonus_global += 2.0
            elif nb_mean < 0:
                bonus_global -= 2.0
    except Exception:
        pass

    try:
        mg = pd.read_sql_query("SELECT rzye FROM margin_summary ORDER BY trade_date DESC LIMIT 5", conn)
        if len(mg) >= 2:
            if mg["rzye"].iloc[0] > mg["rzye"].iloc[-1]:
                bonus_global += 1.0
            elif mg["rzye"].iloc[0] < mg["rzye"].iloc[-1]:
                bonus_global -= 1.0
    except Exception:
        pass

    try:
        if last_trade:
            mf = pd.read_sql_query(
                "SELECT ts_code, net_mf_amount FROM moneyflow_daily WHERE trade_date = ?",
                conn,
                params=(last_trade,),
            )
            for _, row in mf.iterrows():
                bonus_stock[row["ts_code"]] = float(row.get("net_mf_amount", 0) or 0)
    except Exception:
        pass

    try:
        if last_trade:
            tl = pd.read_sql_query("SELECT DISTINCT ts_code FROM top_list WHERE trade_date = ?", conn, params=(last_trade,))
            top_list_set = set(tl["ts_code"].tolist())
    except Exception:
        pass

    try:
        if last_trade:
            ti = pd.read_sql_query("SELECT DISTINCT ts_code FROM top_inst WHERE trade_date = ?", conn, params=(last_trade,))
            top_inst_set = set(ti["ts_code"].tolist())
    except Exception:
        pass

    try:
        if last_trade:
            ind = pd.read_sql_query("SELECT * FROM moneyflow_ind_ths WHERE trade_date = ?", conn, params=(last_trade,))
            for _, row in ind.iterrows():
                ind_name = row.get("industry") or row.get("industry_name") or row.get("name")
                net = row.get("net_flow") if "net_flow" in row else row.get("net_flow_amt")
                if ind_name and net is not None:
                    bonus_industry[str(ind_name)] = float(net)
    except Exception:
        pass

    return bonus_global, bonus_stock, top_list_set, top_inst_set, bonus_industry


def calc_external_bonus(
    *,
    ts_code: str,
    industry: str,
    bonus_global: float,
    bonus_stock_map: Dict[str, float],
    top_list_set: set,
    top_inst_set: set,
    bonus_industry_map: Dict[str, float],
) -> float:
    extra = bonus_global
    mf_net = bonus_stock_map.get(ts_code, 0.0)
    if mf_net > 1e8:
        extra += 2.0
    elif mf_net > 0:
        extra += 1.0
    elif mf_net < 0:
        extra -= 1.0
    if ts_code in top_list_set:
        extra += 1.5
    if ts_code in top_inst_set:
        extra += 1.0
    ind_flow = bonus_industry_map.get(industry, 0.0)
    if ind_flow > 0:
        extra += 1.0
    elif ind_flow < 0:
        extra -= 1.0
    return extra


def get_latest_prices(
    *,
    ts_codes: List[str],
    db_path: str,
    canonical_ts_code: Callable[[Any], str],
    expand_ts_code_keys: Callable[[Any], List[str]],
    iter_sqlite_in_chunks: Callable[[List[str], int], List[List[str]]],
    safe_daily_table_name: Callable[[str, str], str],
    safe_float: Callable[[Any, float], float],
) -> Dict[str, Dict[str, Any]]:
    if not ts_codes:
        return {}
    query_codes: List[str] = []
    for code in ts_codes:
        canonical = canonical_ts_code(code)
        if canonical and canonical not in query_codes:
            query_codes.append(canonical)
    if not query_codes:
        return {}
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        from data.dao import DataAccessError, detect_daily_table  # type: ignore
        try:
            table = detect_daily_table(conn)
        except DataAccessError:
            table = "daily_trading_data"
        table = safe_daily_table_name(table, "daily_trading_data")
        cursor = conn.cursor()
        rows: List[Tuple[str, float, str]] = []
        for chunk in iter_sqlite_in_chunks(query_codes, 900):
            placeholders = ",".join(["?"] * len(chunk))
            query = f"""
                WITH ranked AS (
                    SELECT ts_code, close_price, trade_date,
                           ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
                    FROM {table}
                    WHERE ts_code IN ({placeholders})
                )
                SELECT ts_code, close_price, trade_date
                FROM ranked
                WHERE rn = 1
            """
            cursor.execute(query, chunk)
            rows.extend(cursor.fetchall())
    finally:
        conn.close()
    latest_by_code: Dict[str, Dict[str, Any]] = {}
    for ts_code, close_price, trade_date in rows:
        latest_by_code[str(ts_code)] = {"price": safe_float(close_price), "trade_date": trade_date}
    latest: Dict[str, Dict[str, Any]] = {}
    for code in ts_codes:
        keys = expand_ts_code_keys(code)
        hit: Optional[Dict[str, Any]] = None
        for key in keys:
            info = latest_by_code.get(key)
            if info:
                hit = info
                break
        if hit:
            for key in keys:
                latest[key] = hit
    return latest
