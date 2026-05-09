from __future__ import annotations

import os
import sqlite3
from typing import Callable, Dict, Iterable, List, Optional

import pandas as pd


NormalizeFn = Callable[[pd.DataFrame], pd.DataFrame]


def _read_with_table_fallback(
    conn: sqlite3.Connection,
    query_tpl: str,
    table_candidates: Iterable[str],
    params: tuple,
) -> pd.DataFrame:
    last_exc: Optional[Exception] = None
    for table in table_candidates:
        try:
            return pd.read_sql_query(query_tpl.format(table=table), conn, params=params)
        except Exception as exc:
            last_exc = exc
            continue
    if last_exc is not None:
        raise last_exc
    return pd.DataFrame()


def get_db_last_trade_date(db_path: str) -> str:
    try:
        conn = sqlite3.connect(db_path)
        try:
            for table in ("daily_trading_data", "daily_data"):
                try:
                    df = pd.read_sql_query(f"SELECT MAX(trade_date) AS max_date FROM {table}", conn)
                    if df is not None and not df.empty and df["max_date"].iloc[0]:
                        return str(df["max_date"].iloc[0])
                except Exception:
                    continue
        finally:
            conn.close()
    except Exception:
        pass
    return ""


def get_index_daily_from_db(
    db_path: str,
    start_date: str,
    end_date: str,
    index_code: str = "000001.SH",
    table_candidates: Iterable[str] = ("daily_trading_data", "daily_data"),
) -> pd.DataFrame:
    if not db_path or not os.path.exists(db_path):
        return pd.DataFrame()
    conn = sqlite3.connect(db_path)
    try:
        query_tpl = """
            SELECT trade_date, close_price, vol, pct_chg
            FROM {table}
            WHERE ts_code = ? AND trade_date >= ? AND trade_date <= ?
            ORDER BY trade_date DESC
        """
        try:
            return _read_with_table_fallback(
                conn,
                query_tpl=query_tpl,
                table_candidates=table_candidates,
                params=(index_code, start_date, end_date),
            )
        except Exception:
            return pd.DataFrame()
    finally:
        conn.close()


def load_history_full(
    db_path: str,
    ts_code: str,
    start_date: str,
    end_date: str,
    columns: str,
    table_candidates: Iterable[str] = ("daily_trading_data", "daily_data"),
    normalize_fn: Optional[NormalizeFn] = None,
) -> pd.DataFrame:
    if not db_path or not os.path.exists(db_path):
        return pd.DataFrame()
    conn = sqlite3.connect(db_path)
    try:
        query_tpl = f"""
            SELECT {columns}
            FROM {{table}}
            WHERE ts_code = ? AND trade_date >= ? AND trade_date <= ?
            ORDER BY trade_date
        """
        try:
            df = _read_with_table_fallback(
                conn,
                query_tpl=query_tpl,
                table_candidates=table_candidates,
                params=(ts_code, start_date, end_date),
            )
        except Exception:
            return pd.DataFrame()
    finally:
        conn.close()
    if normalize_fn is not None and df is not None and not df.empty:
        return normalize_fn(df)
    return df


def load_index_recent(
    db_path: str,
    index_code: str = "000001.SH",
    limit: int = 40,
    columns: str = "trade_date, close_price, pct_chg",
    table_candidates: Iterable[str] = ("daily_trading_data", "daily_data"),
) -> pd.DataFrame:
    if not db_path or not os.path.exists(db_path):
        return pd.DataFrame()
    conn = sqlite3.connect(db_path)
    try:
        query_tpl = f"""
            SELECT {columns}
            FROM {{table}}
            WHERE ts_code = ?
            ORDER BY trade_date DESC
            LIMIT {int(limit)}
        """
        try:
            return _read_with_table_fallback(
                conn,
                query_tpl=query_tpl,
                table_candidates=table_candidates,
                params=(index_code,),
            )
        except Exception:
            return pd.DataFrame()
    finally:
        conn.close()


def load_stock_recent(
    db_path: str,
    ts_code: str,
    limit: int = 120,
    columns: str = "trade_date, close_price, vol, pct_chg",
    table_candidates: Iterable[str] = ("daily_trading_history", "daily_trading_data", "daily_data"),
) -> pd.DataFrame:
    if not db_path or not os.path.exists(db_path):
        return pd.DataFrame()
    conn = sqlite3.connect(db_path)
    try:
        query_tpl = f"""
            SELECT {columns}
            FROM {{table}}
            WHERE ts_code = ?
            ORDER BY trade_date DESC
            LIMIT {int(limit)}
        """
        try:
            return _read_with_table_fallback(
                conn,
                query_tpl=query_tpl,
                table_candidates=table_candidates,
                params=(ts_code,),
            )
        except Exception:
            return pd.DataFrame()
    finally:
        conn.close()


def load_stock_history_bulk(
    conn: sqlite3.Connection,
    ts_codes: List[str],
    limit: int,
    columns: str,
    normalize_fn: NormalizeFn,
    bulk_chunk: int = 200,
    table: str = "daily_trading_data",
) -> Dict[str, pd.DataFrame]:
    if not ts_codes:
        return {}
    out: Dict[str, pd.DataFrame] = {}
    cols = columns
    use_window = True
    for i in range(0, len(ts_codes), max(1, bulk_chunk)):
        chunk = ts_codes[i : i + max(1, bulk_chunk)]
        placeholders = ",".join(["?"] * len(chunk))
        if use_window:
            query = f"""
                SELECT {cols} FROM (
                    SELECT {cols},
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
        for ts_code, g in df.groupby("ts_code"):
            out[ts_code] = normalize_fn(g)
    return out


def load_history_range_bulk(
    conn: sqlite3.Connection,
    ts_codes: List[str],
    start_date: str,
    end_date: str,
    columns: str,
    normalize_fn: NormalizeFn,
    bulk_chunk: int = 200,
    table: str = "daily_trading_data",
) -> Dict[str, pd.DataFrame]:
    if not ts_codes:
        return {}
    out: Dict[str, pd.DataFrame] = {}
    cols = columns
    for i in range(0, len(ts_codes), max(1, bulk_chunk)):
        chunk = ts_codes[i : i + max(1, bulk_chunk)]
        placeholders = ",".join(["?"] * len(chunk))
        query = f"""
            SELECT {cols}
            FROM {table}
            WHERE ts_code IN ({placeholders})
              AND trade_date >= ?
              AND trade_date <= ?
            ORDER BY ts_code, trade_date
        """
        df = pd.read_sql_query(query, conn, params=chunk + [start_date, end_date])
        if df is None or df.empty:
            continue
        for ts_code, g in df.groupby("ts_code"):
            out[ts_code] = normalize_fn(g)
    return out
