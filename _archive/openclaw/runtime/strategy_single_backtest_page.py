from __future__ import annotations

from datetime import datetime, timedelta
import os
import signal
import traceback
from typing import Any, Callable, Dict, Optional, Tuple

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st


def _num_or_none(val: Any) -> Optional[float]:
    if isinstance(val, (int, float)):
        return float(val)
    return None


def _v8_threshold_from_state() -> Optional[float]:
    raw = st.session_state.get("score_threshold_v8_tab1")
    if isinstance(raw, (list, tuple)) and raw:
        return _num_or_none(raw[0])
    return _num_or_none(raw)


def _single_backtest_defaults(
    strategy_name: str,
    load_evolve_params: Callable[[str], Dict[str, Any]],
) -> Dict[str, Any]:
    hold_default = 10
    thr_default = 65
    sample_default = 800
    hold_source = "evolution"
    thr_source = "evolution"
    sample_source = "default"

    if "v9.0" in strategy_name:
        evo = load_evolve_params("v9_best.json").get("params", {})
        hold_default = int(evo.get("holding_days", 20))
        thr_default = int(evo.get("score_threshold", 65))
        sample_default = int(evo.get("candidate_count", 800))
        hold_state = _num_or_none(st.session_state.get("holding_days_v9"))
        thr_state = _num_or_none(st.session_state.get("score_threshold_v9"))
        sample_state = _num_or_none(st.session_state.get("candidate_count_v9"))
        if hold_state is not None:
            hold_default, hold_source = int(hold_state), "策略中心"
        if thr_state is not None:
            thr_default, thr_source = int(thr_state), "策略中心"
        if sample_state is not None:
            sample_default, sample_source = int(sample_state), "策略中心"
    elif "v8.0" in strategy_name:
        evo = load_evolve_params("v8_best.json").get("params", {})
        hold_default = int(evo.get("holding_days", 10))
        thr_raw = evo.get("score_threshold", [55, 70])
        if isinstance(thr_raw, (list, tuple)) and thr_raw:
            thr_default = int(thr_raw[0])
        elif isinstance(thr_raw, (int, float)):
            thr_default = int(thr_raw)
        thr_state = _v8_threshold_from_state()
        if thr_state is not None:
            thr_default, thr_source = int(thr_state), "策略中心"
    elif "v5.0" in strategy_name:
        evo = load_evolve_params("v5_best.json").get("params", {})
        hold_default = int(evo.get("holding_days", 8))
        thr_default = int(evo.get("score_threshold", 60))
        thr_state = _num_or_none(st.session_state.get("score_threshold_v5"))
        if thr_state is not None:
            thr_default, thr_source = int(thr_state), "策略中心"
    elif "组合策略" in strategy_name:
        evo = load_evolve_params("combo_best.json").get("params", {})
        hold_default = int(evo.get("holding_days", 10))
        thr_default = int(evo.get("combo_threshold", 68))
        sample_default = int(evo.get("candidate_count", 800))
        thr_state = _num_or_none(st.session_state.get("combo_threshold"))
        sample_state = _num_or_none(st.session_state.get("combo_candidate_count"))
        if thr_state is not None:
            thr_default, thr_source = int(thr_state), "策略中心"
        if sample_state is not None:
            sample_default, sample_source = int(sample_state), "策略中心"

    hold_default = max(1, min(30, int(hold_default)))
    thr_default = max(50, min(90, int(thr_default)))
    sample_default = max(100, min(6000, int(sample_default)))
    return {
        "holding_days": hold_default,
        "score_threshold": thr_default,
        "sample_size": sample_default,
        "holding_source": hold_source,
        "threshold_source": thr_source,
        "sample_source": sample_source,
    }


def _load_single_backtest_history(connect_permanent_db: Callable[[], Any]) -> pd.DataFrame:
    from data.dao import DataAccessError, detect_daily_table  # type: ignore

    conn = connect_permanent_db()
    start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
    try:
        daily_table = detect_daily_table(conn)
    except DataAccessError:
        conn.close()
        raise RuntimeError("无法识别日线数据表（daily_trading_data/daily_data）")

    query = f"""
        SELECT dtd.ts_code, sb.name, sb.industry, dtd.trade_date,
               dtd.open_price, dtd.high_price, dtd.low_price,
               dtd.close_price, dtd.vol, dtd.pct_chg, dtd.amount
        FROM {daily_table} dtd
        INNER JOIN stock_basic sb ON dtd.ts_code = sb.ts_code
        WHERE dtd.trade_date >= ?
        ORDER BY dtd.ts_code, dtd.trade_date
    """
    df = pd.read_sql_query(query, conn, params=(start_date,))
    conn.close()
    return df


def _run_single_backtest(
    *,
    vp_analyzer: Any,
    selected_strategy: str,
    input_df: pd.DataFrame,
    sample_size_for_run: int,
    holding_days: int,
    score_threshold: float,
) -> Dict[str, Any]:
    if "v4.0" in selected_strategy:
        return vp_analyzer.backtest_strategy_complete(input_df, sample_size=sample_size_for_run, holding_days=holding_days)
    if "v5.0" in selected_strategy:
        return vp_analyzer.backtest_bottom_breakthrough(input_df, sample_size=sample_size_for_run, holding_days=holding_days)
    if "v8.0" in selected_strategy:
        return vp_analyzer.backtest_v8_ultimate(
            input_df,
            sample_size=sample_size_for_run,
            holding_days=holding_days,
            score_threshold=score_threshold,
        )
    if "v9.0" in selected_strategy:
        return vp_analyzer.backtest_v9_midterm(
            input_df,
            sample_size=sample_size_for_run,
            holding_days=holding_days,
            score_threshold=score_threshold,
        )
    if "组合策略" in selected_strategy:
        return vp_analyzer.backtest_combo_production(
            input_df,
            sample_size=sample_size_for_run,
            holding_days=holding_days,
            combo_threshold=score_threshold,
            min_agree=2,
        )
    return vp_analyzer.backtest_v9_midterm(
        input_df,
        sample_size=sample_size_for_run,
        holding_days=holding_days,
        score_threshold=score_threshold,
    )


def _render_single_backtest_result(
    *,
    result: Dict[str, Any],
    selected_strategy: str,
    build_calibrated_strength_df: Callable[..., pd.DataFrame],
    pick_tradable_segment_from_strength: Callable[..., Dict[str, Any] | None],
    apply_tradable_segment_to_strategy_session: Callable[..., str],
) -> None:
    stats = result.get("stats", {})
    st.markdown("---")
    st.subheader("回测结果详情")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("胜率", f"{stats.get('win_rate', 0):.1f}%")
    with col2:
        st.metric("平均收益", f"{stats.get('avg_return', 0):.2f}%")
    with col3:
        st.metric("夏普比率", f"{stats.get('sharpe_ratio', 0):.2f}")
    with col4:
        st.metric("信号数量", stats.get("total_signals", 0))

    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("最大收益", f"{stats.get('max_return', 0):.2f}%")
    with col2:
        st.metric("最大亏损", f"{stats.get('max_loss', 0):.2f}%")
    with col3:
        st.metric("盈亏比", f"{stats.get('profit_loss_ratio', 0):.2f}")

    st.markdown("---")
    st.subheader("深度分析")
    tab1, tab2, tab3 = st.tabs(["分强度统计", "交易记录", "导出数据"])

    with tab1:
        strength_perf = stats.get("strength_performance")
        if isinstance(strength_perf, dict) and strength_perf:
            st.markdown("###  信号强度表现分析")
            strength_table_df = pd.DataFrame(
                [
                    {
                        "信号强度": f"{strength_range}分",
                        "信号数量": perf["count"],
                        "平均收益": f"{perf['avg_return']:.2f}%",
                        "胜率": f"{perf['win_rate']:.1f}%",
                    }
                    for strength_range, perf in strength_perf.items()
                ]
            )
            st.dataframe(strength_table_df, use_container_width=True, hide_index=True)
            cal_df = build_calibrated_strength_df(strength_perf)
            if not cal_df.empty:
                st.markdown("###  校准后曲线（单调）")
                cal_show = cal_df[["区间", "样本数", "原始胜率%", "校准胜率%", "原始收益%", "校准收益%"]].copy()
                for col in ["原始胜率%", "校准胜率%", "原始收益%", "校准收益%"]:
                    cal_show[col] = pd.to_numeric(cal_show[col], errors="coerce").round(1)
                st.dataframe(cal_show, use_container_width=True, hide_index=True)

                fig_cal = go.Figure()
                fig_cal.add_trace(go.Scatter(x=cal_df["区间"], y=cal_df["原始胜率%"], mode="lines+markers", name="原始胜率%"))
                fig_cal.add_trace(go.Scatter(x=cal_df["区间"], y=cal_df["校准胜率%"], mode="lines+markers", name="校准胜率%"))
                fig_cal.add_trace(go.Scatter(x=cal_df["区间"], y=cal_df["原始收益%"], mode="lines+markers", name="原始收益%"))
                fig_cal.add_trace(go.Scatter(x=cal_df["区间"], y=cal_df["校准收益%"], mode="lines+markers", name="校准收益%"))
                fig_cal.update_layout(height=340, title="分数分段：原始 vs 校准")
                st.plotly_chart(fig_cal, use_container_width=True)

                tradable = cal_df[(cal_df["样本数"] >= 20) & (cal_df["校准收益%"] > 0)].copy()
                if not tradable.empty:
                    tradable = tradable.sort_values(["校准综合分", "样本数"], ascending=[False, False]).head(3)
                    st.success(f"可交易优先分段（样本>=20）：{' / '.join(tradable['区间'].astype(str).tolist())}")
                else:
                    st.warning("当前没有满足样本与收益条件的稳健分段，建议继续累积样本或降低阈值。")

                best_seg = pick_tradable_segment_from_strength(strength_perf)
                if best_seg:
                    st.caption(
                        f"建议分段：{best_seg.get('segment')} | 样本={best_seg.get('samples')} | "
                        f"校准胜率={best_seg.get('cal_win', 0.0):.1f}% | 校准收益={best_seg.get('cal_ret', 0.0):.2f}%"
                    )
                    col_apply1, col_apply2 = st.columns([1, 1])
                    with col_apply1:
                        if st.button("应用该分段到策略参数", key="apply_tradable_segment_now", use_container_width=True):
                            st.success(
                                apply_tradable_segment_to_strategy_session(
                                    str(selected_strategy),
                                    best_seg,
                                    top_percent=1,
                                )
                            )
                            st.rerun()
                    with col_apply2:
                        st.caption("应用后会自动切换为“分位数筛选(Top%)”。")

            st.markdown("###  信号强度可视化")
            labels = list(strength_perf.keys())
            counts = [perf["count"] for perf in strength_perf.values()]
            returns = [perf["avg_return"] for perf in strength_perf.values()]
            win_rates = [perf["win_rate"] for perf in strength_perf.values()]

            fig = make_subplots(
                rows=1,
                cols=2,
                subplot_titles=("信号强度分布", "信号强度 vs 胜率&收益"),
                specs=[[{"type": "bar"}, {"type": "scatter"}]],
            )
            fig.add_trace(go.Bar(x=labels, y=counts, name="信号数量", marker_color="lightblue"), row=1, col=1)
            fig.add_trace(go.Scatter(x=labels, y=win_rates, name="胜率 (%)", mode="lines+markers", marker=dict(size=10)), row=1, col=2)
            fig.add_trace(go.Scatter(x=labels, y=returns, name="平均收益 (%)", mode="lines+markers", marker=dict(size=10), yaxis="y2"), row=1, col=2)
            fig.update_xaxes(title_text="信号强度", row=1, col=1)
            fig.update_xaxes(title_text="信号强度", row=1, col=2)
            fig.update_yaxes(title_text="信号数量", row=1, col=1)
            fig.update_yaxes(title_text="百分比", row=1, col=2)
            fig.update_layout(height=400, showlegend=True)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("暂无分强度统计数据")

    with tab2:
        details = result.get("details", [])
        if details:
            st.markdown("###  详细交易记录（前50条）")
            details_df = details[:50] if isinstance(details, pd.DataFrame) else pd.DataFrame(details[:50])
            st.dataframe(details_df, use_container_width=True, hide_index=True)
            st.markdown("###  交易统计")
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("总交易数", len(details))
            with col2:
                profitable = sum(1 for d in details if float(d.get(f"{stats.get('avg_holding_days', 5)}天收益", "0%").rstrip("%")) > 0)
                st.metric("盈利交易", profitable)
            with col3:
                loss = len(details) - profitable
                st.metric("亏损交易", loss)
            with col4:
                st.metric("盈亏比", f"{profitable/loss:.2f}" if loss > 0 else "∞")
        else:
            st.info("暂无详细交易记录")

    with tab3:
        st.markdown("###  导出回测数据")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("生成回测报告", use_container_width=True, key="single_report"):
                strategy_name = result.get("strategy", "未知策略")
                report_md = f"""#  {strategy_name} 深度回测报告

##  回测概况

**回测时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**回测策略**: {strategy_name}
**持仓天数**: {stats.get('avg_holding_days', 'N/A')}天
**样本数量**: {stats.get('analyzed_stocks', 'N/A')}只

---

##  核心指标

| 指标 | 数值 |
|------|------|
| 总信号数 | {stats.get('total_signals', 0)} |
| 胜率 | {stats.get('win_rate', 0):.1f}% |
| 平均收益 | {stats.get('avg_return', 0):.2f}% |
| 中位数收益 | {stats.get('median_return', 0):.2f}% |
| 最大收益 | {stats.get('max_return', 0):.2f}% |
| 最大亏损 | {stats.get('min_return', 0):.2f}% |
| 夏普比率 | {stats.get('sharpe_ratio', 0):.2f} |
| 盈亏比 | {stats.get('profit_loss_ratio', 0):.2f} |

---
"""
                if isinstance(stats.get("strength_performance"), dict):
                    report_md += "\n##  分强度表现\n"
                    for strength_range, perf in stats["strength_performance"].items():
                        report_md += (
                            f"\n### {strength_range}分\n\n"
                            f"- 信号数量: {perf['count']}\n"
                            f"- 平均收益: {perf['avg_return']:.2f}%\n"
                            f"- 胜率: {perf['win_rate']:.1f}%\n"
                        )
                report_md += f"\n---\n\n*报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n"
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                st.download_button(
                    label=" 下载报告",
                    data=report_md,
                    file_name=f"single_backtest_report_{timestamp}.md",
                    mime="text/markdown",
                    key="download_single_report",
                )
                st.success("报告已生成！")
        with col2:
            if st.button("导出交易记录", use_container_width=True, key="single_export"):
                details = result.get("details", [])
                if details:
                    details_df = details if isinstance(details, pd.DataFrame) else pd.DataFrame(details)
                    timestamp2 = datetime.now().strftime("%Y%m%d_%H%M%S")
                    st.download_button(
                        label=" 下载CSV",
                        data=details_df.to_csv(index=False, encoding="utf-8-sig"),
                        file_name=f"trade_records_{timestamp2}.csv",
                        mime="text/csv",
                        key="download_single_csv",
                    )
                    st.success("交易记录已准备好！")
                else:
                    st.warning("暂无交易记录可导出")


def render_single_backtest_page(
    *,
    vp_analyzer: Any,
    load_evolve_params: Callable[[str], Dict[str, Any]],
    airivo_has_role: Callable[[str], bool],
    airivo_guard_action: Callable[[str, str, str, str], bool],
    airivo_append_action_audit: Callable[..., None],
    start_async_backtest_job: Callable[[str, Dict[str, Any]], Tuple[bool, str, str]],
    set_sim_meta: Callable[[str, str], None],
    auto_backtest_scheduler_tick: Callable[[], Dict[str, Any]],
    connect_permanent_db: Callable[[], Any],
    ensure_price_aliases: Callable[[pd.DataFrame], pd.DataFrame],
    get_async_backtest_job: Callable[[str], Dict[str, Any] | None],
    is_pid_alive: Callable[[int], bool],
    merge_async_backtest_job: Callable[..., Dict[str, Any]],
    now_ts: Callable[[], float],
    now_text: Callable[[], str],
    pick_tradable_segment_from_strength: Callable[..., Dict[str, Any] | None],
    apply_tradable_segment_to_strategy_session: Callable[..., str],
    build_calibrated_strength_df: Callable[..., pd.DataFrame],
) -> None:
    st.subheader("单策略深度回测")

    strategy_options = [
        "v9.0 中线均衡版（生产）",
        "v8.0 进阶版（生产）",
        "v5.0 趋势版（生产）",
        "组合策略（生产共识）",
    ]
    col1, col2 = st.columns(2)
    with col1:
        selected_strategy = st.selectbox(
            "选择策略",
            strategy_options,
            key="single_backtest_strategy_select",
            help="与策略中心生产策略完全对齐：v9/v8/v5/combo。",
        )

    defaults = _single_backtest_defaults(selected_strategy, load_evolve_params)
    prev_strategy = st.session_state.get("_single_backtest_prev_strategy")
    if prev_strategy != selected_strategy:
        st.session_state["single_backtest_holding_days"] = defaults["holding_days"]
        st.session_state["single_backtest_threshold"] = defaults["score_threshold"]
        st.session_state["single_backtest_sample_size"] = defaults["sample_size"]
        st.session_state["_single_backtest_prev_strategy"] = selected_strategy
    else:
        st.session_state.setdefault("single_backtest_holding_days", defaults["holding_days"])
        st.session_state.setdefault("single_backtest_threshold", defaults["score_threshold"])
        st.session_state.setdefault("single_backtest_sample_size", defaults["sample_size"])

    with col2:
        holding_days = st.slider("持仓天数", min_value=1, max_value=30, step=1, key="single_backtest_holding_days")

    col3, col4 = st.columns(2)
    strict_full_market_mode = bool(st.session_state.get("strict_full_market_mode", False))
    st.session_state.setdefault("single_backtest_full_market_mode", strict_full_market_mode)
    if strict_full_market_mode:
        st.session_state["single_backtest_full_market_mode"] = True
    with col3:
        full_market_mode_single = st.checkbox("全量模式（不抽样）", key="single_backtest_full_market_mode", help="开启后按当前历史数据中的全部股票回测。")
        sample_size = st.slider("回测样本数量", min_value=100, max_value=6000, step=100, key="single_backtest_sample_size", disabled=bool(full_market_mode_single))
        if full_market_mode_single:
            st.caption("当前为全量模式：将按历史数据中的全部股票回测。")
    with col4:
        score_threshold = st.slider(
            "评分阈值",
            min_value=50,
            max_value=90,
            step=5,
            key="single_backtest_threshold",
            help="生产策略口径：v5建议60-70，v8建议60-75，v9建议60-70，组合策略建议65-75。",
        )
    async_single_backtest = st.checkbox("后台运行单策略回测（推荐）", value=True, key="single_backtest_async")
    st.caption(f"参数自动对齐：阈值来源={defaults['threshold_source']} | 持仓来源={defaults['holding_source']} | 样本来源={defaults['sample_source']}")

    st.markdown("#### 自动回测")
    ab1, ab2, ab3, ab4 = st.columns(4)
    with ab1:
        st.toggle("启用每日自动回测", value=False, key="auto_backtest_enabled")
    with ab2:
        st.text_input("执行时间(HH:MM)", value="15:35", key="auto_backtest_time")
    with ab3:
        st.number_input("回测历史天数", min_value=120, max_value=720, step=10, value=240, key="auto_backtest_history_days")
    with ab4:
        st.toggle("回测后自动应用分段", value=False, key="auto_backtest_auto_apply_segment")

    st.session_state["auto_backtest_strategy"] = str(selected_strategy)
    st.session_state["auto_backtest_sample_size"] = int(sample_size)
    st.session_state["auto_backtest_full_market_mode"] = bool(full_market_mode_single)
    st.session_state["auto_backtest_holding_days"] = int(holding_days)
    st.session_state["auto_backtest_score_threshold"] = float(score_threshold)

    run_auto_col1, run_auto_col2 = st.columns([1, 2])
    with run_auto_col1:
        if st.button("立即执行自动回测", use_container_width=True, key="run_auto_backtest_now", disabled=not airivo_has_role("admin")):
            if not airivo_guard_action("admin", "run_auto_backtest_now", target=str(selected_strategy), reason="manual_auto_backtest_trigger"):
                st.stop()
            payload = {
                "strategy": str(selected_strategy),
                "sample_size": int(sample_size),
                "full_market_mode": bool(full_market_mode_single),
                "history_days": int(st.session_state.get("auto_backtest_history_days", 240)),
                "holding_days": int(holding_days),
                "score_threshold": float(score_threshold),
            }
            ok, msg, run_id = start_async_backtest_job("single", payload)
            if ok:
                airivo_append_action_audit("run_auto_backtest_now", True, target=str(selected_strategy), detail=msg, extra={"run_id": run_id})
                st.session_state["single_backtest_async_job_id"] = run_id
                set_sim_meta("auto_backtest_last_job_id", run_id)
                st.success(msg)
            else:
                airivo_append_action_audit("run_auto_backtest_now", False, target=str(selected_strategy), detail=msg)
                st.warning(msg)
            st.rerun()
    with run_auto_col2:
        tick_ret = auto_backtest_scheduler_tick()
        st.caption(f"自动回测调度状态：{tick_ret.get('status', 'N/A')}")

    if st.button("开始回测", type="primary", use_container_width=True, key="single_backtest"):
        if async_single_backtest:
            payload = {
                "strategy": selected_strategy,
                "sample_size": int(sample_size),
                "full_market_mode": bool(full_market_mode_single),
                "history_days": 240,
                "holding_days": int(holding_days),
                "score_threshold": float(score_threshold),
            }
            ok, msg, run_id = start_async_backtest_job("single", payload)
            if ok:
                st.session_state["single_backtest_async_job_id"] = run_id
                st.success(msg)
            else:
                st.error(msg)
            st.rerun()
        with st.spinner(f"正在回测 {selected_strategy}..."):
            try:
                df = _load_single_backtest_history(connect_permanent_db)
                if df.empty:
                    st.error("无法获取历史数据")
                else:
                    df = ensure_price_aliases(df)
                    sample_size_for_run = max(1, int(df["ts_code"].nunique())) if full_market_mode_single and "ts_code" in df.columns else int(sample_size)
                    result = _run_single_backtest(
                        vp_analyzer=vp_analyzer,
                        selected_strategy=selected_strategy,
                        input_df=df,
                        sample_size_for_run=sample_size_for_run,
                        holding_days=int(holding_days),
                        score_threshold=float(score_threshold),
                    )
                    err_text = str(result.get("error", "")) if isinstance(result, dict) else ""
                    if (not result.get("success")) and ("close_price" in err_text):
                        retry_df = ensure_price_aliases(df)
                        result = _run_single_backtest(
                            vp_analyzer=vp_analyzer,
                            selected_strategy=selected_strategy,
                            input_df=retry_df,
                            sample_size_for_run=sample_size_for_run,
                            holding_days=int(holding_days),
                            score_threshold=float(score_threshold),
                        )
                    if result.get("success"):
                        st.session_state["single_backtest_result"] = result
                        st.success("回测完成！")
                        st.rerun()
                    else:
                        st.error(f"回测失败：{result.get('error', '未知错误')}")
                        if result.get("traceback"):
                            with st.expander("查看回测错误详情", expanded=False):
                                st.code(str(result.get("traceback")))
            except Exception as exc:
                st.error(f"回测失败: {exc}")
                st.code(traceback.format_exc())

    single_job_id = str(st.session_state.get("single_backtest_async_job_id", "") or "")
    if single_job_id:
        job = get_async_backtest_job(single_job_id)
        if job:
            st.info(f"后台单策略回测状态：{job.get('status')}（ID={single_job_id}）")
            sj1, sj2 = st.columns([1, 1])
            with sj1:
                if st.button("刷新单策略任务状态", key=f"refresh_single_job_{single_job_id}"):
                    st.rerun()
            with sj2:
                if str(job.get("status")) == "running" and st.button("取消单策略任务", key=f"cancel_single_job_{single_job_id}"):
                    pid = int(job.get("pid", 0) or 0)
                    if pid > 0 and is_pid_alive(pid):
                        try:
                            os.killpg(pid, signal.SIGTERM)
                        except Exception:
                            try:
                                os.kill(pid, signal.SIGTERM)
                            except Exception:
                                pass
                    merge_async_backtest_job(single_job_id, job, status="failed", error="任务已手动取消", ended_at=now_ts())
                    st.session_state["single_backtest_async_job_id"] = ""
                    st.rerun()
            if str(job.get("status")) == "success":
                out = job.get("result") or {}
                result = out.get("result") if isinstance(out, dict) else None
                if isinstance(result, dict) and result.get("success"):
                    st.session_state["single_backtest_result"] = result
                    set_sim_meta("auto_backtest_last_ok", "1")
                    set_sim_meta("auto_backtest_last_finished_at", now_text())
                    set_sim_meta("auto_backtest_last_strategy", str(selected_strategy))
                    if bool(st.session_state.get("auto_backtest_auto_apply_segment", False)):
                        stats_auto = result.get("stats", {}) if isinstance(result, dict) else {}
                        strength_auto = stats_auto.get("strength_performance", {}) if isinstance(stats_auto, dict) else {}
                        seg_auto = pick_tradable_segment_from_strength(strength_auto) if isinstance(strength_auto, dict) else {}
                        if seg_auto:
                            st.success(f"自动应用完成：{apply_tradable_segment_to_strategy_session(str(selected_strategy), seg_auto, top_percent=1)}")
                    st.success("后台单策略回测已完成，结果已更新。")
                    st.session_state["single_backtest_async_job_id"] = ""
                    st.rerun()
                else:
                    st.error(f"后台单策略回测失败：{(result or {}).get('error', '未知错误') if isinstance(result, dict) else '未知错误'}")
                    set_sim_meta("auto_backtest_last_ok", "0")
                    if isinstance(result, dict) and str(result.get("traceback", "")).strip():
                        with st.expander("查看后台任务错误详情", expanded=False):
                            st.code(str(result.get("traceback", "")))
                    st.session_state["single_backtest_async_job_id"] = ""
            elif str(job.get("status")) == "failed":
                st.error(f"后台单策略回测失败：{job.get('error', '未知错误')}")
                set_sim_meta("auto_backtest_last_ok", "0")
                if str(job.get("traceback", "")).strip():
                    with st.expander("查看后台任务错误详情", expanded=False):
                        st.code(str(job.get("traceback", "")))
                st.session_state["single_backtest_async_job_id"] = ""

    if "single_backtest_result" in st.session_state:
        _render_single_backtest_result(
            result=st.session_state["single_backtest_result"],
            selected_strategy=selected_strategy,
            build_calibrated_strength_df=build_calibrated_strength_df,
            pick_tradable_segment_from_strength=pick_tradable_segment_from_strength,
            apply_tradable_segment_to_strategy_session=apply_tradable_segment_to_strategy_session,
        )
