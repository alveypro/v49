from __future__ import annotations

from typing import Callable, Tuple

import pandas as pd
import streamlit as st


def render_front_scan_summary(*, strategy: str, title: str) -> None:
    summary = st.session_state.get(f"{strategy}_front_scan_summary")
    results_df = st.session_state.get(f"{strategy}_front_scan_results")
    if not isinstance(summary, dict) or not isinstance(results_df, pd.DataFrame) or results_df.empty:
        return
    st.markdown("---")
    st.markdown(f"### 最近一次扫描结果（{title}）")
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.metric("完成时间", str(summary.get("finished_at", "") or "-"))
    with c2:
        st.metric("候选数", int(summary.get("candidate_count", len(results_df)) or len(results_df)))
    with c3:
        st.metric("结果数", int(summary.get("result_count", len(results_df)) or len(results_df)))
    with c4:
        st.metric("过滤淘汰", int(summary.get("filter_failed", 0) or 0))
    with c5:
        elapsed_ms = int(summary.get("elapsed_ms", 0) or 0)
        st.metric("耗时", f"{elapsed_ms/1000:.1f}s" if elapsed_ms > 0 else "-")
    cache_mode = str(summary.get("cache_mode", "") or "")
    lookback_days = int(summary.get("lookback_days", 0) or 0)
    if cache_mode or lookback_days > 0:
        parts = []
        if cache_mode:
            parts.append(f"缓存={cache_mode}")
        if lookback_days > 0:
            parts.append(f"窗口={lookback_days}天")
        st.caption(" | ".join(parts))


def render_cached_scan_results(
    *,
    title: str,
    results_df: pd.DataFrame,
    score_col: str,
    candidate_count: int,
    filter_failed: int,
    select_mode: str,
    threshold: float,
    top_percent: int,
    render_result_overview: Callable[[pd.DataFrame, str, str], None],
    signal_density_hint: Callable[[int, int], Tuple[str, str]],
) -> None:
    st.markdown("---")
    st.markdown(f"###  {title}")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("候选股票", f"{candidate_count}只")
    with col2:
        if candidate_count > 0:
            st.metric("过滤淘汰", f"{filter_failed}只", delta=f"{filter_failed/candidate_count*100:.1f}%")
        else:
            st.metric("过滤淘汰", f"{filter_failed}只")
    with col3:
        if candidate_count > 0:
            st.metric("最终推荐", f"{len(results_df)}只", delta=f"{len(results_df)/candidate_count*100:.2f}%")
        else:
            st.metric("最终推荐", f"{len(results_df)}只")

    if results_df is None or results_df.empty:
        st.warning("未找到符合条件的股票，请降低阈值或放宽筛选条件")
        return

    if select_mode == "阈值筛选":
        st.success(f"找到 {len(results_df)} 只符合条件的股票（≥{threshold}分）")
    elif select_mode == "双重筛选(阈值+Top%)":
        st.success(f"先阈值后Top筛选：≥{threshold}分，Top {top_percent}%（{len(results_df)} 只）")
    else:
        st.success(f"选出 Top {top_percent}%（{len(results_df)} 只）")

    results_df = results_df.reset_index(drop=True)
    render_result_overview(results_df, score_col, "扫描结果概览")
    msg, level = signal_density_hint(len(results_df), candidate_count)
    getattr(st, level)(msg)

    st.markdown("---")
    st.subheader("结果明细（缓存）")
    display_df = results_df.drop(columns=["原始数据"], errors="ignore")
    st.dataframe(display_df, use_container_width=True, hide_index=True)
