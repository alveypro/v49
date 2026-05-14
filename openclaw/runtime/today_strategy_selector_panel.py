from __future__ import annotations

from typing import Callable, Iterable, Tuple

import streamlit as st


def render_today_strategy_selector_panel(
    *,
    production_strategies: Callable[[], Iterable[str]],
    experimental_strategies: Callable[[], Iterable[str]],
    show_ai_signal_panel: bool,
) -> Tuple[str, bool]:
    st.markdown(
        "<div class='airivo-card' style='margin-bottom:10px;'>"
        "<b>生产策略</b>：v9 / v8 / v5 / combo&nbsp;&nbsp;|&nbsp;&nbsp;"
        "<b>实验策略</b>：v4 / v6 / v7 / stable / ai"
        "</div>",
        unsafe_allow_html=True,
    )

    st.caption("导出请使用下方 CSV 按钮（含策略版本）。")
    st.markdown(
        """
        <style>
        button[title="Download data as CSV"],
        button[title="Download data as csv"],
        button[title="Download as CSV"],
        button[title="Download as csv"],
        button[title="Download data"] { display: none !important; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    prod_label_map = {
        "v9": " v9.0 中线均衡版（生产 / 2-6周）",
        "v8": " v8.0 进阶版（生产 / ATR风控 / 5-15日）",
        "v5": " v5.0 趋势版（生产 / 启动确认 / 5-10日）",
        "combo": " 组合策略（生产 / 共识评分）",
    }
    exp_label_map = {
        "v4": " v4.0 长期稳健版（实验 / 潜伏策略 / 3-7日）",
        "v6": " v6.0 超短线版（实验 / 强势精选 / 2-5日）",
        "v7": " v7.0 智能版（实验 / 动态自适应 / 5-15日）",
        "stable": " stable 稳健实验策略（实验）",
        "ai": " AI 辅助选股（实验）",
    }

    prod_keys = [k for k in production_strategies() if k in prod_label_map]
    if not prod_keys:
        prod_keys = ["v9", "v8", "v5", "combo"]
    modes = [prod_label_map[k] for k in prod_keys]
    strategy_mode = st.radio(
        "选择生产策略（默认运行）",
        [m.strip() for m in modes],
        horizontal=True,
        help="默认仅显示生产策略：v9/v8/v5/combo。实验策略请在下方手动启用。",
    )

    exp_keys = [k for k in experimental_strategies() if k in exp_label_map]
    with st.expander("实验策略入口（手动启用）", expanded=False):
        st.caption("实验策略不进入默认流水线，仅用于研究验证。")
        exp_options = ["不启用实验策略"] + [exp_label_map[k].strip() for k in exp_keys]
        exp_mode = st.selectbox("选择实验策略", exp_options, index=0, key="core_experimental_mode")
        if exp_mode != "不启用实验策略":
            strategy_mode = exp_mode
            st.warning(f"当前正在查看实验策略：{exp_mode}（不会影响默认生产流程）")
            if "AI 辅助选股" in exp_mode:
                show_ai_signal_panel = True

    st.markdown("---")
    return strategy_mode, show_ai_signal_panel
