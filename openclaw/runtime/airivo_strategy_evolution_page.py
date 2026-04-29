from __future__ import annotations

from typing import Any, Callable

import pandas as pd
import streamlit as st


def render_airivo_strategy_evolution_page(
    *,
    db_path: str,
    runtime_snapshot: dict[str, Any],
    safe_file_mtime: Callable[[str], float],
    feedback_snapshot_cached: Callable[[str, float], dict[str, Any]],
    bucket_feedback_rows: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    load_feedback_rows: Callable[..., list[dict[str, Any]]],
    feedback_bucket_summary: Callable[[list[dict[str, Any]]], dict[str, int]],
    latest_candidate_snapshot_cached: Callable[..., tuple[pd.DataFrame | None, str]],
) -> None:
    db_mtime = safe_file_mtime(db_path)
    feedback = feedback_snapshot_cached(db_path, db_mtime)
    pending_rows = bucket_feedback_rows(load_feedback_rows(db_path, status_filter="pending", limit=200))
    bucket_counts = feedback_bucket_summary(pending_rows)
    candidates, signal_date = latest_candidate_snapshot_cached(db_path, db_mtime, limit=8)

    st.markdown("## 策略演进")
    st.caption("这里看系统是否真的在变好，而不是继续堆按钮。用真实执行、覆盖分歧和结果回刷来决定下一次升级。")

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("运行模式", {"production": "生产", "review": "复核", "research": "研究"}.get(str(runtime_snapshot.get("mode")), "研究"))
    m2.metric("风险门禁", str(runtime_snapshot.get("risk_level", "RED")))
    m3.metric("执行率", f"{feedback.get('execution_rate')}%" if feedback.get("execution_rate") is not None else "暂无")
    m4.metric("结果回刷", int(feedback.get("outcome_rows", 0) or 0))

    s1, s2, s3 = st.columns(3)
    s1.metric("人工覆盖", int(feedback.get("human_override_rows", 0) or 0))
    s2.metric("缺覆盖原因", int(feedback.get("missing_override_reason_rows", 0) or 0))
    s3.metric("异常待确认", bucket_counts.get("manual_review", 0))

    gates: list[str] = []
    if int(feedback.get("missing_override_reason_rows", 0) or 0) > 0:
        gates.append("人工覆盖原因未补齐，当前样本不能用于严肃评估人工优于系统。")
    if bucket_counts.get("manual_review", 0) > 0:
        gates.append("仍有异常项依赖人工判断，决策代理层尚未闭环。")
    if feedback.get("execution_rate") is None or float(feedback.get("execution_rate") or 0) < 50:
        gates.append("真实执行样本不足，不能仅凭回测推动策略升级。")
    if not gates:
        st.success("当前闭环质量达到第一阶段要求：主流程、执行反馈、结果回刷已经形成连续链路。")
    else:
        for gate in gates:
            st.warning(gate)

    left, right = st.columns([0.55, 0.45])
    with left:
        st.markdown("### 最新生产候选池")
        if candidates is not None and not candidates.empty:
            st.caption(f"信号日期 {signal_date}。这里展示的是系统最近一次真正给出的候选，而不是理论参数。")
            st.dataframe(candidates, use_container_width=True, hide_index=True)
        else:
            st.info("暂无最近候选池快照。")
    with right:
        st.markdown("### 下一阶段升级原则")
        st.markdown(
            "\n".join(
                [
                    "1. 先把 `pending` 变成异常项队列，而不是人工待填总表。",
                    "2. 用真实执行结果反推哪些人工规则应该沉淀成系统规则。",
                    "3. 生产升级只看闭环证据，不看单日漂亮结果。",
                ]
            )
        )
        st.markdown("### 自动分流现状")
        triage_df = pd.DataFrame(
            [
                {"队列": "需要人工确认", "数量": bucket_counts.get("manual_review", 0)},
                {"队列": "系统直接执行", "数量": bucket_counts.get("direct_execute", 0)},
                {"队列": "系统观察等待", "数量": bucket_counts.get("observe", 0)},
                {"队列": "系统自动淘汰", "数量": bucket_counts.get("auto_reject", 0)},
            ]
        )
        st.dataframe(triage_df, use_container_width=True, hide_index=True)
