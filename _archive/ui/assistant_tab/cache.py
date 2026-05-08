from __future__ import annotations

import sqlite3

import pandas as pd
import streamlit as st


@st.cache_data(ttl=30, show_spinner=False)
def cached_assistant_holdings(assistant_db_path: str) -> pd.DataFrame:
    conn = sqlite3.connect(assistant_db_path)
    try:
        return pd.read_sql_query(
            "SELECT * FROM holdings WHERE status = 'holding' ORDER BY buy_date DESC",
            conn,
        )
    finally:
        conn.close()


@st.cache_data(ttl=30, show_spinner=False)
def cached_assistant_trades(assistant_db_path: str, limit: int = 50) -> pd.DataFrame:
    conn = sqlite3.connect(assistant_db_path)
    try:
        return pd.read_sql_query(
            "SELECT * FROM trade_history ORDER BY trade_date DESC, created_at DESC LIMIT ?",
            conn,
            params=(int(limit),),
        )
    finally:
        conn.close()


@st.cache_data(ttl=60, show_spinner=False)
def cached_assistant_daily_recs(assistant_db_path: str, trade_date: str) -> pd.DataFrame:
    conn = sqlite3.connect(assistant_db_path)
    try:
        return pd.read_sql_query(
            """
            SELECT * FROM daily_recommendations
            WHERE recommend_date = ?
            ORDER BY score DESC
            """,
            conn,
            params=(str(trade_date),),
        )
    finally:
        conn.close()


def clear_assistant_ui_cache() -> None:
    try:
        cached_assistant_holdings.clear()
        cached_assistant_trades.clear()
        cached_assistant_daily_recs.clear()
    except Exception:
        pass

