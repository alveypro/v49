from __future__ import annotations

from typing import Any, Callable

import streamlit as st


def render_today_console_panel(
    *,
    permanent_db_path: str,
    airivo_snapshot: dict[str, Any],
    vp_analyzer: Any,
    status: dict[str, Any] | None,
    set_focus_once: Callable[..., None],
    render_today_execution_queues: Callable[[str, dict[str, Any]], None],
) -> None:
    market_env = "oscillation"
    try:
        market_env = vp_analyzer.get_market_environment()
    except Exception:
        market_env = "oscillation"

    env_text = {"bull": "牛市", "bear": "弱市", "oscillation": "震荡"}.get(market_env, "震荡")
    max_date = status.get("max_date", "N/A") if isinstance(status, dict) else "N/A"
    days_old = int(status.get("days_old", 999)) if isinstance(status, dict) else 999
    freshness = f"{days_old}天前" if days_old < 999 else "未知"

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.metric("市场环境", env_text)
    with k2:
        st.metric("数据日期", str(max_date))
    with k3:
        st.metric("生产策略", "4")
    with k4:
        st.metric("数据新鲜度", freshness)

    a1, a2, a3 = st.columns(3)
    with a1:
        scan_disabled = not bool(st.session_state.get("airivo_trade_actions_enabled", False))
        if st.button("开始扫描", type="primary", use_container_width=True, key="core_console_scan", disabled=scan_disabled):
            st.info("请在下方选择策略参数后，点击对应的“开始扫描”按钮。")
        if scan_disabled:
            st.caption("当前非生产模式：扫描结果只能用于研究回放，不能作为今日生产建议。")
    with a2:
        if st.button("进入执行中心", use_container_width=True, key="core_console_execution"):
            set_focus_once(main_tab="生产后台", production_tab="执行中心")
            st.rerun()
    with a3:
        if st.button("策略演进", use_container_width=True, key="core_console_evolution"):
            set_focus_once(main_tab="生产后台", production_tab="策略演进")
            st.rerun()

    render_today_execution_queues(permanent_db_path, airivo_snapshot)
