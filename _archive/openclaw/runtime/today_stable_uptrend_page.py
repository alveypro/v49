from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

import streamlit as st


def render_today_stable_uptrend_page(
    *,
    stable_uptrend_available: bool,
    permanent_db_path: str,
    db_manager: Any,
    stable_uptrend_context_cls: type[Any],
    render_stable_uptrend_strategy: Callable[..., Any],
    df_to_csv_bytes: Callable[[Any], bytes],
    render_async_scan_status: Callable[[str, str, str], Any],
) -> None:
    exp_uptrend = st.expander("稳定上涨策略说明", expanded=False)
    exp_uptrend.markdown(
        """
        <div style='background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
                    padding: 30px; border-radius: 15px; color: white; margin-bottom: 25px;'>
            <h2 style='margin:0; color: white;'> 稳定上涨策略</h2>
            <p style='margin:10px 0 0 0; font-size:1.05em; opacity:0.95;'>
                目标：筛选“底部启动 / 回撤企稳 / 二次启动”的稳定上涨候选股（非收益保证）
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if "stable_uptrend_results" in st.session_state:
        export_df = st.session_state["stable_uptrend_results"]
        exp_uptrend.download_button(
            " 导出结果（CSV）",
            data=df_to_csv_bytes(export_df),
            file_name=f"稳定上涨策略_扫描结果_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv; charset=utf-8",
        )

    if not stable_uptrend_available:
        st.error("稳定上涨策略模块未找到，请确认 stable_uptrend_strategy.py 已放在系统目录")
    else:
        ctx = stable_uptrend_context_cls(permanent_db_path, db_manager=db_manager)
        render_stable_uptrend_strategy(ctx, pro=getattr(db_manager, "pro", None))

    render_async_scan_status("v4_async_task_id", "v4.0潜伏策略", "综合评分")
