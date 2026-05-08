from __future__ import annotations

from typing import Callable, Tuple

import pandas as pd
import streamlit as st


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
