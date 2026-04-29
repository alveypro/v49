from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Callable

import pandas as pd
import streamlit as st


def render_assistant_workbench_page(
    *,
    assistant: Any,
    render_result_overview: Callable[..., None],
) -> None:
    st.subheader("交易工作台")
    st.caption("先看账户状态与交易表现，再做当日选股与执行。")

    try:
        holdings_df = assistant.get_holdings()
        trades_df = assistant.get_trade_history(limit=200)
        daily_cnt = len(st.session_state.get("daily_recommendations", []) or [])
        hold_count = len(holdings_df) if isinstance(holdings_df, pd.DataFrame) else 0
        today_sell = 0
        today_pnl = 0.0
        if isinstance(trades_df, pd.DataFrame) and not trades_df.empty:
            trade_df = trades_df.copy()
            if "action" in trade_df.columns:
                trade_df = trade_df[trade_df["action"] == "sell"]
            if "trade_date" in trade_df.columns:
                today = datetime.now().strftime("%Y-%m-%d")
                trade_df["trade_date"] = trade_df["trade_date"].astype(str)
                today_df = trade_df[trade_df["trade_date"].str.startswith(today)]
            else:
                today_df = trade_df
            today_sell = len(today_df)
            if "profit_loss" in today_df.columns:
                today_pnl = float(pd.to_numeric(today_df["profit_loss"], errors="coerce").fillna(0).sum())

        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.metric("当前持仓", f"{hold_count}只")
        with m2:
            st.metric("今日已卖出", f"{today_sell}笔")
        with m3:
            st.metric("今日已实现盈亏", f"¥{today_pnl:,.2f}")
        with m4:
            st.metric("今日推荐池", f"{daily_cnt}只")
    except Exception:
        pass

    st.markdown("---")
    st.markdown("### 每日智能选股")
    st.info(
        """
 **选股说明**
- 基于**共识策略**（v4/v5/v7/v8/v9）
- 自动扫描全市场股票
- 推荐Top高分标的（一致性筛选）
- 仅供参考，需人工决策
"""
    )

    col1, col2 = st.columns([3, 1])
    with col1:
        top_n = st.slider("推荐数量", 3, 10, 5, key="assistant_daily_scan_top_n")
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("开始选股", type="primary", use_container_width=True):
            with st.spinner("正在扫描全市场...（可能需要2-3分钟）"):
                recommendations = assistant.daily_stock_scan(top_n=top_n)
                st.session_state["daily_recommendations"] = recommendations
                if recommendations:
                    st.success(f"选股完成！找到{len(recommendations)}只标的数量")
                else:
                    st.warning("本次未选出股票，已记录诊断信息")
                st.rerun()

    if "daily_recommendations" in st.session_state and st.session_state["daily_recommendations"]:
        st.markdown("---")
        st.subheader("今日推荐")
        recs = st.session_state["daily_recommendations"]
        recs_df = pd.DataFrame(recs)
        if not recs_df.empty:
            render_result_overview(recs_df, score_col="score", title="今日推荐概览")

        for i, rec in enumerate(recs, 1):
            with st.expander(f"#{i} {rec['stock_name']} ({rec['ts_code']}) - ⭐ {rec['score']:.1f}分", expanded=(i == 1)):
                c1, c2, c3 = st.columns(3)
                with c1:
                    st.metric("评分", f"{rec['score']:.1f}分")
                with c2:
                    st.metric("价格", f"¥{rec['price']:.2f}")
                with c3:
                    st.metric("市值", f"{rec['market_cap']/100000000:.1f}亿")

                st.markdown(f"** 行业**: {rec['industry']}")
                st.markdown(f"** 筛选理由**: {rec['reason'][:150]}...")

                st.markdown("---")
                b1, b2, b3 = st.columns([2, 2, 1])
                with b1:
                    buy_price = st.number_input("买入价格", value=float(rec["price"]), key=f"price_{rec['ts_code']}")
                with b2:
                    quantity = st.number_input("买入数量", value=100, step=100, key=f"qty_{rec['ts_code']}")
                with b3:
                    st.markdown("<br>", unsafe_allow_html=True)
                    if st.button("记录买入", key=f"buy_{rec['ts_code']}"):
                        assistant.add_holding(
                            ts_code=rec["ts_code"],
                            buy_price=buy_price,
                            quantity=quantity,
                            score=rec["score"],
                        )
                        st.success(f"已记录买入 {rec['stock_name']}")
                        st.rerun()
    elif "daily_recommendations" in st.session_state:
        st.warning("本次未选出股票，请查看诊断信息")
        debug_info = getattr(assistant, "last_scan_debug", None)
        if debug_info:
            st.code(json.dumps(debug_info, ensure_ascii=False, indent=2))
