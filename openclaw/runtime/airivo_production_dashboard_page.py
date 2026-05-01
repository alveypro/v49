from __future__ import annotations

from typing import Any, Callable

import pandas as pd
import streamlit as st


def render_airivo_production_dashboard_page(
    *,
    db_path: str,
    safe_file_mtime: Callable[[str], float],
    data_freshness_snapshot_cached: Callable[[str, float], dict[str, Any]],
    latest_candidate_snapshot_cached: Callable[..., tuple[pd.DataFrame | None, str]],
    feedback_snapshot_cached: Callable[[str, float], dict[str, Any]],
) -> dict[str, Any]:
    db_mtime = safe_file_mtime(db_path)
    snap = data_freshness_snapshot_cached(db_path, db_mtime)
    candidates, signal_date = latest_candidate_snapshot_cached(db_path, db_mtime, limit=5)
    feedback = feedback_snapshot_cached(db_path, db_mtime)
    mode_label = {"production": "生产模式", "review": "复核模式", "research": "研究模式"}.get(str(snap.get("mode")), "研究模式")
    risk_level = str(snap.get("risk_level", "RED"))

    st.markdown("### Airivo 生产状态")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("运行模式", mode_label)
    c2.metric("风险门禁", risk_level)
    c3.metric("行情日期", str(snap.get("latest_trade_date") or "N/A"))
    c4.metric("数据新鲜度", f"{int(snap.get('days_old', 999))}天前" if snap.get("days_old", 999) < 999 else "未知")

    if risk_level == "GREEN":
        st.success("数据门禁通过。系统输出仍仅用于研究和决策辅助，不构成投资建议。")
    elif risk_level == "YELLOW":
        st.warning("系统处于复核模式：候选池只能作为人工复核输入，不能直接作为生产建议。")
    else:
        st.error("系统处于研究模式：关键数据过期或缺失，禁止将扫描结果表述为今日生产建议。")

    warnings = [str(x) for x in snap.get("warnings", []) if str(x).strip()]
    if warnings:
        st.caption("；".join(warnings[:4]))

    with st.expander("数据覆盖与新鲜度", expanded=False):
        table_rows = []
        for item in snap.get("tables", []):
            table_rows.append(
                {
                    "维度": item.get("label"),
                    "数据表": item.get("table"),
                    "最新日期": item.get("latest"),
                    "记录数": item.get("rows"),
                    "状态": "OK" if item.get("ok") else str(item.get("reason") or "异常"),
                }
            )
        if table_rows:
            st.dataframe(pd.DataFrame(table_rows), use_container_width=True, hide_index=True)

    left, right = st.columns([1.2, 0.8])
    with left:
        st.markdown("#### 最近候选池快照")
        if candidates is not None and not candidates.empty:
            st.caption(f"来源：strategy_signal_tracking，信号日期 {signal_date}。这里按股票去重聚合，只保留最终候选视图；数据过期时仅作研究回放。")
            st.dataframe(candidates, use_container_width=True, hide_index=True)
        else:
            st.info("暂无候选池快照。请先运行生产扫描或日报流程。")
    with right:
        st.markdown("#### 执行反馈闭环")
        f1, f2 = st.columns(2)
        f1.metric("反馈记录", int(feedback.get("feedback_rows", 0) or 0))
        f2.metric("结果回刷", int(feedback.get("outcome_rows", 0) or 0))
        if feedback.get("execution_rate") is None:
            st.warning("尚未形成真实执行反馈样本。不能用回测结果替代实盘证据。")
        else:
            st.metric("执行率", f"{feedback.get('execution_rate')}%")
        st.caption("下一阶段验收：连续 8 周真实执行记录、周度复盘、失败样本可追溯。")

    st.session_state["airivo_runtime_mode"] = str(snap.get("mode", "research"))
    st.session_state["airivo_risk_level"] = str(snap.get("risk_level", "RED"))
    st.session_state["airivo_trade_actions_enabled"] = bool(snap.get("mode") == "production")
    return snap
