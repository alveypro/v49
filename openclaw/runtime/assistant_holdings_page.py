from __future__ import annotations

import sqlite3
from typing import Any

import pandas as pd
import streamlit as st


def render_assistant_holdings_page(*, assistant: Any) -> None:
    st.subheader("当前持仓管理")

    col1, col2 = st.columns([4, 1])
    with col2:
        if st.button("更新持仓", use_container_width=True):
            with st.spinner("更新中..."):
                assistant.update_holdings()
                st.success("更新完成")
                st.rerun()

    conn = sqlite3.connect(assistant.assistant_db)
    holdings = pd.read_sql_query(
        "SELECT * FROM holdings WHERE status = 'holding' ORDER BY buy_date DESC",
        conn,
    )
    conn.close()

    if holdings.empty:
        st.info("当前无持仓")
        return

    total_cost = holdings["cost_total"].sum()
    total_value = holdings["current_value"].sum()
    total_profit = holdings["profit_loss"].sum()
    total_profit_pct = total_profit / total_cost if total_cost > 0 else 0

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("持仓数量", f"{len(holdings)}只")
    with col2:
        st.metric("总成本", f"¥{total_cost:,.2f}")
    with col3:
        st.metric("总市值", f"¥{total_value:,.2f}")
    with col4:
        st.metric("总盈亏", f"¥{total_profit:,.2f}", delta=f"{total_profit_pct*100:.2f}%")

    st.markdown("---")
    check_alerts = st.button("检查止盈止损", key="assistant_check_stop_conditions")
    alerts = st.session_state.get("assistant_stop_alerts", [])
    if check_alerts:
        alerts = assistant.check_stop_conditions()
        st.session_state["assistant_stop_alerts"] = alerts
    if alerts:
        st.warning("**止盈止损提醒**")
        for alert in alerts:
            if alert["type"] == "take_profit":
                st.success(alert["message"])
            else:
                st.error(alert["message"])

    for _, holding in holdings.iterrows():
        profit_loss = holding.get("profit_loss", 0) or 0
        profit_loss_pct = holding.get("profit_loss_pct", 0) or 0
        buy_price = holding.get("buy_price", 0) or 0
        current_price = holding.get("current_price", 0) or buy_price
        profit_color = "" if profit_loss > 0 else ""

        with st.expander(f"{profit_color} {holding['stock_name']} ({holding['ts_code']}) - {profit_loss_pct*100:.2f}%"):
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("买入价", f"¥{buy_price:.2f}")
            with col2:
                st.metric("当前价", f"¥{current_price:.2f}")
            with col3:
                st.metric("数量", f"{holding['quantity']}股")
            with col4:
                st.metric("盈亏", f"¥{profit_loss:.2f}", delta=f"{profit_loss_pct*100:.2f}%")

            st.markdown(f"**买入日期**: {holding.get('buy_date', 'N/A')}")
            cost_total = holding.get("cost_total", 0) or 0
            current_value = holding.get("current_value", 0) or 0
            st.markdown(f"**成本**: ¥{cost_total:.2f}")
            st.markdown(f"**市值**: ¥{current_value:.2f}")

            st.markdown("---")
            col1, col2, col3 = st.columns([2, 2, 1])
            with col1:
                sell_price = st.number_input("卖出价格", value=float(holding["current_price"]), key=f"sell_price_{holding['id']}")
            with col2:
                sell_reason = st.selectbox("卖出原因", ["止盈", "止损", "手动卖出", "其他"], key=f"sell_reason_{holding['id']}")
            with col3:
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("卖出", key=f"sell_{holding['id']}"):
                    assistant.sell_holding(ts_code=holding["ts_code"], sell_price=sell_price, reason=sell_reason)
                    st.success(f"已记录卖出 {holding['stock_name']}")
                    st.rerun()
