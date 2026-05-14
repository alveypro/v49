from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd


StrategyCenterCache = Dict[str, Any]


def load_stock_history(
    *,
    conn: sqlite3.Connection,
    ts_code: str,
    limit: int,
    columns: str,
    normalize_stock_df: Callable[[pd.DataFrame], pd.DataFrame],
    safe_daily_table_name: Callable[[str, str], str],
) -> pd.DataFrame:
    try:
        from data.history import load_stock_recent as load_stock_recent_v2  # type: ignore
        db_rows = conn.execute("PRAGMA database_list").fetchall()
        db_path = str(db_rows[0][2]) if db_rows and len(db_rows[0]) >= 3 else ""
        if db_path:
            df = load_stock_recent_v2(
                db_path=db_path,
                ts_code=ts_code,
                limit=int(limit),
                columns=columns,
            )
            return normalize_stock_df(df)
    except Exception:
        pass
    try:
        from data.dao import DataAccessError, detect_daily_table  # type: ignore
        try:
            table = detect_daily_table(conn)
        except DataAccessError:
            table = "daily_trading_data"
        table = safe_daily_table_name(table, "daily_trading_data")
    except Exception:
        table = "daily_trading_data"
    query = f"""
        SELECT {columns}
        FROM {table}
        WHERE ts_code = ?
        ORDER BY trade_date DESC
        LIMIT {int(limit)}
    """
    df = pd.read_sql_query(query, conn, params=(ts_code,))
    return normalize_stock_df(df)


def load_stock_history_bulk(
    *,
    conn: sqlite3.Connection,
    ts_codes: List[str],
    limit: int,
    columns: str,
    bulk_history_chunk: int,
    normalize_stock_df: Callable[[pd.DataFrame], pd.DataFrame],
    table: str = "daily_trading_data",
) -> Dict[str, pd.DataFrame]:
    try:
        from data.history import load_stock_history_bulk as load_stock_history_bulk_v2  # type: ignore
        return load_stock_history_bulk_v2(
            conn=conn,
            ts_codes=ts_codes,
            limit=limit,
            columns=columns,
            normalize_fn=normalize_stock_df,
            bulk_chunk=bulk_history_chunk,
            table=table,
        )
    except Exception:
        pass
    if not ts_codes:
        return {}
    out: Dict[str, pd.DataFrame] = {}
    use_window = True
    chunk_size = max(1, min(int(bulk_history_chunk), 900))
    for i in range(0, len(ts_codes), chunk_size):
        chunk = ts_codes[i:i + chunk_size]
        placeholders = ",".join(["?"] * len(chunk))
        if use_window:
            query = f"""
                SELECT {columns} FROM (
                    SELECT {columns},
                           ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
                    FROM {table}
                    WHERE ts_code IN ({placeholders})
                ) t
                WHERE rn <= {int(limit)}
                ORDER BY ts_code, trade_date DESC
            """
            try:
                df = pd.read_sql_query(query, conn, params=chunk)
            except Exception:
                use_window = False
                df = pd.DataFrame()
        else:
            df = pd.DataFrame()
        if df is None or df.empty:
            continue
        for ts_code, grp in df.groupby("ts_code"):
            out[ts_code] = normalize_stock_df(grp)
    return out


def load_history_range_bulk(
    *,
    conn: sqlite3.Connection,
    ts_codes: List[str],
    start_date: str,
    end_date: str,
    columns: str,
    bulk_history_chunk: int,
    normalize_stock_df: Callable[[pd.DataFrame], pd.DataFrame],
    table: str = "daily_trading_data",
) -> Dict[str, pd.DataFrame]:
    try:
        from data.history import load_history_range_bulk as load_history_range_bulk_v2  # type: ignore
        return load_history_range_bulk_v2(
            conn=conn,
            ts_codes=ts_codes,
            start_date=start_date,
            end_date=end_date,
            columns=columns,
            normalize_fn=normalize_stock_df,
            bulk_chunk=bulk_history_chunk,
            table=table,
        )
    except Exception:
        pass
    if not ts_codes:
        return {}
    out: Dict[str, pd.DataFrame] = {}
    chunk_size = max(1, min(int(bulk_history_chunk), 900 - 2))
    for i in range(0, len(ts_codes), chunk_size):
        chunk = ts_codes[i:i + chunk_size]
        placeholders = ",".join(["?"] * len(chunk))
        query = f"""
            SELECT {columns}
            FROM {table}
            WHERE ts_code IN ({placeholders})
              AND trade_date >= ?
              AND trade_date <= ?
            ORDER BY ts_code, trade_date
        """
        df = pd.read_sql_query(query, conn, params=chunk + [start_date, end_date])
        if df is None or df.empty:
            continue
        for ts_code, grp in df.groupby("ts_code"):
            out[ts_code] = normalize_stock_df(grp)
    return out


def batch_load_stock_histories(
    *,
    conn: sqlite3.Connection,
    ts_codes: List[str],
    limit: int,
    columns: str,
    iter_sqlite_in_chunks: Callable[[List[str], int], List[List[str]]],
    safe_daily_table_name: Callable[[str, str], str],
    normalize_stock_df: Callable[[pd.DataFrame], pd.DataFrame],
) -> Dict[str, pd.DataFrame]:
    if not ts_codes:
        return {}
    try:
        from data.dao import detect_daily_table  # type: ignore
        table = detect_daily_table(conn)
        table = safe_daily_table_name(table, "daily_trading_data")
    except Exception:
        table = "daily_trading_data"
    result: Dict[str, pd.DataFrame] = {}
    for chunk in iter_sqlite_in_chunks(ts_codes, 900):
        placeholders = ",".join(["?"] * len(chunk))
        query = f"""
            SELECT {columns}
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
                FROM {table}
                WHERE ts_code IN ({placeholders})
            ) sub
            WHERE rn <= {int(limit)}
            ORDER BY ts_code, trade_date
        """
        big_df = pd.read_sql_query(query, conn, params=chunk)
        if big_df is None or big_df.empty:
            continue
        for code, grp in big_df.groupby("ts_code"):
            result[str(code)] = normalize_stock_df(
                grp.drop(columns=["ts_code"], errors="ignore").reset_index(drop=True)
            )
    return result


def load_strategy_center_scan_defaults(
    *,
    app_root: str,
    strategy: str,
    strategy_center_cache: Optional[StrategyCenterCache],
) -> Tuple[Dict[str, Any], str, StrategyCenterCache]:
    cache = strategy_center_cache
    try:
        if cache is None:
            cfg_path = os.path.join(app_root, "openclaw", "config", "strategy_center.yaml")
            cfg: Dict[str, Any] = {}
            try:
                from strategies.center_config import load_center_config as load_center_config  # type: ignore
                cfg = load_center_config(Path(cfg_path)) if os.path.exists(cfg_path) else {}
            except Exception:
                cfg = {}
            cache = cfg if isinstance(cfg, dict) else {}

        runtime_defaults = (cache or {}).get("runtime_defaults", {})
        if not isinstance(runtime_defaults, dict):
            return {}, "none", cache or {}
        params = runtime_defaults.get(strategy, {})
        if not isinstance(params, dict):
            return {}, "none", cache or {}
        out: Dict[str, Any] = {}
        for key in ("score_threshold", "sample_size", "holding_days"):
            if key in params:
                out[key] = params.get(key)
        if out:
            return out, "strategy_center", cache or {}
    except Exception:
        pass
    return {}, "none", cache or {}
