from __future__ import annotations

import sqlite3
from typing import Tuple

import pandas as pd
import streamlit as st


def _period_stats(df: pd.DataFrame) -> Tuple[float, float, float]:
    if df.empty:
        return 0.0, 0.0, 0.0
    profit = df["profit_loss"].sum()
    cost = df["cost_basis"].sum()
    amount = df["amount"].sum()
    pct = profit / cost if cost > 0 else 0.0
    return float(profit), float(pct), float(amount)


def render_assistant_trade_history_page(*, assistant_db: str) -> None:
    st.subheader("交易历史记录")

    conn = sqlite3.connect(assistant_db)
    trades = pd.read_sql_query(
        "SELECT * FROM trade_history ORDER BY trade_date DESC, created_at DESC LIMIT 50",
        conn,
    )
    conn.close()

    if trades.empty:
        st.info("暂无交易记录")
        return

    trades["trade_date"] = pd.to_datetime(trades["trade_date"], errors="coerce")
    sell_trades = trades[trades["action"] == "sell"].copy()
    if not sell_trades.empty:
        sell_trades["amount"] = pd.to_numeric(sell_trades.get("amount", 0), errors="coerce").fillna(0)
        sell_trades["profit_loss"] = pd.to_numeric(sell_trades.get("profit_loss", 0), errors="coerce").fillna(0)
        sell_trades["cost_basis"] = sell_trades["amount"] - sell_trades["profit_loss"]
        sell_trades = sell_trades.dropna(subset=["trade_date"])

    today = pd.Timestamp.now().normalize()
    week_start = today - pd.Timedelta(days=today.weekday())
    month_start = today.replace(day=1)
    daily_profit, daily_pct, daily_amount = _period_stats(sell_trades[sell_trades["trade_date"] >= today] if not sell_trades.empty else sell_trades)
    weekly_profit, weekly_pct, weekly_amount = _period_stats(sell_trades[sell_trades["trade_date"] >= week_start] if not sell_trades.empty else sell_trades)
    monthly_profit, monthly_pct, monthly_amount = _period_stats(sell_trades[sell_trades["trade_date"] >= month_start] if not sell_trades.empty else sell_trades)

    st.markdown("###  已实现盈亏统计（卖出记录）")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("今日盈亏", f"¥{daily_profit:,.2f}", delta=f"{daily_pct*100:.2f}%")
        st.caption(f"成交额：¥{daily_amount:,.2f}")
    with col2:
        st.metric("本周盈亏", f"¥{weekly_profit:,.2f}", delta=f"{weekly_pct*100:.2f}%")
        st.caption(f"成交额：¥{weekly_amount:,.2f}")
    with col3:
        st.metric("本月盈亏", f"¥{monthly_profit:,.2f}", delta=f"{monthly_pct*100:.2f}%")
        st.caption(f"成交额：¥{monthly_amount:,.2f}")

    st.markdown("---")
    buy_trades = trades[trades["action"] == "buy"]
    sell_trades = trades[trades["action"] == "sell"]
    profit_trades = sell_trades[sell_trades["profit_loss"] > 0]
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("总交易", f"{len(trades)}次")
    with col2:
        st.metric("买入", f"{len(buy_trades)}次")
    with col3:
        st.metric("卖出", f"{len(sell_trades)}次")
    with col4:
        win_rate = len(profit_trades) / len(sell_trades) if len(sell_trades) > 0 else 0
        st.metric("胜率", f"{win_rate*100:.1f}%")

    st.markdown("---")
    for _, trade in trades.iterrows():
        action_emoji = "" if trade["action"] == "buy" else ""
        col1, col2, col3, col4, col5 = st.columns([2, 1, 1, 1, 2])
        with col1:
            st.markdown(f"{action_emoji} **{trade['stock_name']}** ({trade['ts_code']})")
        with col2:
            st.markdown(f"{trade['trade_date']}")
        with col3:
            st.markdown(f"¥{trade['price']:.2f}")
        with col4:
            st.markdown(f"{trade['quantity']}股")
        with col5:
            if trade["action"] == "sell" and trade["profit_loss"]:
                profit_text = f"{'' if trade['profit_loss'] > 0 else ''} ¥{trade['profit_loss']:.2f} ({trade['profit_loss_pct']*100:.2f}%)"
                st.markdown(profit_text)
            else:
                st.markdown(f"¥{trade['amount']:.2f}")
