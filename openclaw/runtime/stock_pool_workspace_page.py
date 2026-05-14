from __future__ import annotations

import json
from typing import Any, Callable, Dict

import numpy as np
import pandas as pd
import streamlit as st


def render_stock_pool_workspace_page(
    *,
    save_stock_pool_snapshot: Callable[..., tuple[bool, str]],
    list_stock_pool_meta: Callable[[], list[Dict[str, Any]]],
    load_stock_pool_snapshot: Callable[[str], tuple[pd.DataFrame | None, Dict[str, Any]]],
    parse_pool_base_date: Callable[[Dict[str, Any]], str],
    compute_pool_performance: Callable[..., tuple[pd.DataFrame, Dict[str, Any]]],
    compute_forward_return_buckets: Callable[..., tuple[pd.DataFrame | None, pd.DataFrame | None]],
    df_to_csv_bytes: Callable[[pd.DataFrame], bytes],
    delete_stock_pool_snapshot: Callable[[str], tuple[bool, str]],
) -> None:
    st.header("股票池分析")
    st.caption("保存每次扫描结果，并持续跟踪策略真实表现（胜率/盈亏比/期望收益等）")

    candidate_raw = st.session_state.get("stock_pool_candidate")
    candidate: Dict[str, Any] = candidate_raw if isinstance(candidate_raw, dict) else {}
    candidate_df = candidate.get("df")
    has_candidate_df = isinstance(candidate_df, pd.DataFrame) and not candidate_df.empty

    save_col1, save_col2 = st.columns([2, 1])
    with save_col1:
        note = st.text_input(
            "本次备注（可选）",
            value="",
            key="stock_pool_save_note",
            placeholder="例如：v9放宽Top%到5%，观察信号密度与收益变化",
        )
    with save_col2:
        disabled = not has_candidate_df
        if st.button("保存当前扫描结果", key="save_to_stock_pool", use_container_width=True, disabled=disabled):
            ok, msg = save_stock_pool_snapshot(
                strategy=str(candidate.get("strategy", "unknown")),
                params=candidate.get("params", {}) or {},
                score_col=str(candidate.get("score_col", "综合评分")),
                df=candidate_df,
                note=note,
            )
            if ok:
                st.success(f"保存成功：{msg}")
            else:
                st.error(msg)

    metas = list_stock_pool_meta()
    if not metas:
        st.info("股票池暂无历史记录。先在策略中心扫描一次，然后来这里保存。")
        return

    summary_df = pd.DataFrame(
        [
            {
                "策略": str(m.get("strategy", "")).upper(),
                "时间": m.get("created_at", ""),
                "样本数": int(m.get("row_count", 0) or 0),
                "平均分": m.get("avg_score"),
                "备注": m.get("note", ""),
            }
            for m in metas
        ]
    )
    strategy_agg = summary_df.groupby("策略", as_index=False).agg({"样本数": "sum", "平均分": "mean", "时间": "count"}).rename(columns={"时间": "保存次数"})
    st.markdown("**策略保存概览**")
    st.dataframe(strategy_agg, use_container_width=True, hide_index=True)

    options = [f"{m.get('pool_id')} | {str(m.get('strategy', '')).upper()} | {m.get('created_at', '')} | {m.get('row_count', 0)}条" for m in metas]
    selected = st.selectbox("选择股票池快照", options, key="stock_pool_selected")
    pool_id = selected.split(" | ")[0]
    df, meta = load_stock_pool_snapshot(pool_id)
    if df is None or df.empty:
        st.warning("该记录不存在或为空")
        return

    st.caption(f"策略：{str(meta.get('strategy', '')).upper()}  | 分数字段：{meta.get('score_col', '综合评分')}  | 参数：{json.dumps(meta.get('params', {}), ensure_ascii=False)}")
    base_date = parse_pool_base_date(meta)
    perf_df, perf = compute_pool_performance(df, base_date=base_date)
    if perf:
        m1, m2, m3, m4, m5, m6 = st.columns(6)
        m1.metric("样本", f"{int(perf['count'])}")
        m2.metric("胜率", f"{perf['win_rate']:.1f}%")
        m3.metric("平均收益", f"{perf['avg_ret']:.2f}%")
        m4.metric("盈亏比", "∞" if np.isinf(perf["profit_factor"]) else f"{perf['profit_factor']:.2f}")
        m5.metric("期望收益", f"{perf['expectancy']:.2f}%")
        m6.metric("最大回撤", f"{perf['max_drawdown']:.2f}%")
        if not perf_df.empty:
            st.markdown("**标的浮盈亏跟踪（按当前最新价）**")
            show = perf_df.copy()
            if len(show.columns) == 6:
                show.columns = ["股票代码", "股票名称", "入池价", "最新价", "最新价日期", "浮动收益%"]
            else:
                show.columns = ["股票代码", "入池价", "最新价", "最新价日期", "浮动收益%"]
            latest_trade_date = str(perf.get("latest_trade_date", "") or "")
            if latest_trade_date:
                st.caption(f"最新价来源：日线收盘价，当前可用最新交易日 {latest_trade_date}")
            stale_count = int(perf.get("stale_count", 0) or 0)
            total_count = int(perf.get("count", 0) or 0)
            if total_count > 0 and stale_count >= total_count:
                st.warning("当前最新交易日与入池基准日一致，浮盈亏可能接近0%。若为交易日盘中，请先同步当日数据后再查看。")
            st.dataframe(show.sort_values("浮动收益%", ascending=False), use_container_width=True, hide_index=True)
            chart_df = show[["股票代码", "浮动收益%"]].set_index("股票代码").tail(50)
            st.bar_chart(chart_df)
    else:
        st.info("当前快照缺少可解析的入池价格，暂时无法计算盈亏统计。建议使用包含“最新价格/现价”的扫描结果保存。")

    code_col = "股票代码" if "股票代码" in df.columns else ("ts_code" if "ts_code" in df.columns else "")
    if code_col:
        codes = df[code_col].dropna().astype(str).tolist()
        detail_ret, bucket_df = compute_forward_return_buckets(codes, base_date=base_date, horizons=[1, 3, 5])
        st.markdown("**分桶统计（按入池日期）**")
        st.caption(f"基准日期：{base_date}（按交易日计算 T+1/T+3/T+5）")
        if bucket_df is not None and not bucket_df.empty:
            st.dataframe(bucket_df, use_container_width=True, hide_index=True)
        else:
            st.info("该快照暂时无法计算分桶统计（可能样本日期过新或历史数据缺失）。")

        if detail_ret is not None and not detail_ret.empty:
            show_cols = ["ts_code", "d0", "p0", "d1", "p1", "ret_t1_pct", "d3", "p3", "ret_t3_pct", "d5", "p5", "ret_t5_pct"]
            show_cols = [c for c in show_cols if c in detail_ret.columns]
            det = detail_ret[show_cols].copy()
            det = det.rename(
                columns={
                    "ts_code": "股票代码",
                    "d0": "入池日",
                    "p0": "入池价",
                    "d1": "T+1日期",
                    "p1": "T+1价",
                    "ret_t1_pct": "T+1收益%",
                    "d3": "T+3日期",
                    "p3": "T+3价",
                    "ret_t3_pct": "T+3收益%",
                    "d5": "T+5日期",
                    "p5": "T+5价",
                    "ret_t5_pct": "T+5收益%",
                }
            )
            st.markdown("**分桶明细（逐股）**")
            st.dataframe(det, use_container_width=True, hide_index=True)

    view_df = df.drop(columns=["原始数据"], errors="ignore")
    st.markdown("**快照原始结果**")
    st.dataframe(view_df, use_container_width=True, hide_index=True)
    c3, c4 = st.columns(2)
    with c3:
        st.download_button(
            "导出该快照CSV",
            data=df_to_csv_bytes(view_df),
            file_name=f"股票池_{pool_id}.csv",
            mime="text/csv; charset=utf-8",
            key=f"pool_export_{pool_id}",
            use_container_width=True,
        )
    with c4:
        if st.button("删除该快照", key=f"pool_delete_{pool_id}", use_container_width=True):
            ok, msg = delete_stock_pool_snapshot(pool_id)
            if ok:
                st.success(msg)
                st.rerun()
            else:
                st.error(msg)
