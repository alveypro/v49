from __future__ import annotations

from datetime import datetime, timedelta
import traceback
from typing import Any, Callable, Dict, Tuple

import pandas as pd
import streamlit as st


def _load_ai_history(connect_permanent_db: Callable[[], Any]) -> pd.DataFrame:
    from data.dao import DataAccessError, detect_daily_table  # type: ignore

    conn = connect_permanent_db()
    start_date = (datetime.now() - timedelta(days=150)).strftime("%Y%m%d")
    try:
        daily_table = detect_daily_table(conn)
    except DataAccessError:
        conn.close()
        raise RuntimeError("无法识别日线数据表（daily_trading_data/daily_data）")

    query = f"""
        SELECT dtd.ts_code, sb.name, sb.industry, sb.circ_mv,
               dtd.trade_date, dtd.close_price, dtd.vol, dtd.amount, dtd.pct_chg
        FROM {daily_table} dtd
        INNER JOIN stock_basic sb ON dtd.ts_code = sb.ts_code
        WHERE dtd.trade_date >= ?
        ORDER BY dtd.ts_code, dtd.trade_date
    """
    df = pd.read_sql_query(query, conn, params=(start_date,))
    conn.close()
    return df


def render_ai_signal_page(
    *,
    show_ai_signal_panel: bool,
    render_page_header: Callable[..., None],
    load_evolve_params: Callable[[str], Dict[str, Any]],
    vp_analyzer: Any,
    connect_permanent_db: Callable[[], Any],
    apply_filter_mode: Callable[..., pd.DataFrame],
    apply_multi_period_filter: Callable[..., pd.DataFrame],
    permanent_db_path: str,
    add_reason_summary: Callable[..., pd.DataFrame],
    get_sim_account: Callable[[], Dict[str, Any]],
    auto_buy_ai_stocks: Callable[..., Tuple[int, str]],
    render_result_overview: Callable[..., None],
    signal_density_hint: Callable[..., Tuple[str, str]],
    standardize_result_df: Callable[..., pd.DataFrame],
    df_to_csv_bytes: Callable[[pd.DataFrame], bytes],
) -> None:
    if not show_ai_signal_panel:
        st.caption("AI智能选股已并入实验策略入口：请在上方“实验策略入口（手动启用）”中选择「AI 辅助选股（实验）」。")
        return

    st.markdown("---")
    render_page_header(" AI 智能选股", "共识与量化结合 · 高效选股 · 稳健风控", tag="AI Signal")

    evolve_v5 = load_evolve_params("ai_v5_best.json")
    evolve_v2 = load_evolve_params("ai_v2_best.json")

    strategy_version = st.radio(
        "选择策略版本",
        ["V5.0 稳健月度目标版（推荐）", "V2.0 追涨版"],
        horizontal=True,
        help="V5.0 稳健 / V2.0 动量",
    )
    use_v3 = "V5.0" in strategy_version

    if use_v3 and evolve_v5.get("params"):
        st.success(f"已应用自动进化参数（V5.0，{evolve_v5.get('run_at', 'unknown')}）")
    elif (not use_v3) and evolve_v2.get("params"):
        st.success(f"已应用自动进化参数（V2.0，{evolve_v2.get('run_at', 'unknown')}）")

    st.caption("V5.0：偏稳健，强调回撤控制。" if use_v3 else "V2.0：偏动量，信号更激进。")
    st.divider()

    st.markdown("###  策略参数设置")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        evo_target = (evolve_v5 if use_v3 else evolve_v2).get("params", {}).get("target_return")
        target_default = int(round(float(evo_target) * 100)) if isinstance(evo_target, (int, float)) else (18 if use_v3 else 20)
        target_return = st.slider(
            "目标月收益阈值（%）",
            min_value=10,
            max_value=50,
            value=target_default,
            step=1,
            help="预测未来20天可能达到的收益目标" if use_v3 else "筛选近 20 个交易日涨幅达标的标的",
        )
    with col2:
        evo_min_amount = (evolve_v5 if use_v3 else evolve_v2).get("params", {}).get("min_amount")
        min_amount_default = float(evo_min_amount) if isinstance(evo_min_amount, (int, float)) else (2.5 if use_v3 else 2.0)
        min_amount = st.slider("最低成交活跃度（亿元）", min_value=0.5, max_value=15.0, value=min_amount_default, step=0.5)
    with col3:
        evo_vol = (evolve_v5 if use_v3 else evolve_v2).get("params", {}).get("max_volatility")
        max_volatility_default = (float(evo_vol) * 100) if isinstance(evo_vol, (int, float)) else (14.0 if use_v3 else 12.0)
        max_volatility = st.slider("最大波动容忍度（%）", min_value=5.0, max_value=25.0, value=max_volatility_default, step=0.5)
    with col4:
        top_n = st.slider("优选推荐数量", 5, 100, 25 if use_v3 else 30, 5, key="ai_top_n_v3")

    filter_ai_col1, filter_ai_col2, filter_ai_col3 = st.columns(3)
    with filter_ai_col1:
        select_mode_ai = st.selectbox("筛选模式", ["双重筛选(阈值+Top%)", "分位数筛选(Top%)", "阈值筛选"], index=0, key="ai_select_mode")
    with filter_ai_col2:
        score_threshold_ai = st.slider("评分阈值", 30, 90, 60, 5, key="ai_score_threshold")
    with filter_ai_col3:
        top_percent_ai = st.slider("Top百分比", 1, 10, 2, 1, key="ai_top_percent")

    adj_ai_col1, adj_ai_col2 = st.columns(2)
    with adj_ai_col1:
        market_adjust_strength_ai = st.slider("市场状态调节强度", 0.0, 1.0, 0.5, 0.05, key="ai_market_strength")
    with adj_ai_col2:
        disagree_std_weight_ai = st.slider("分歧惩罚强度", 0.0, 1.5, 0.35, 0.05, key="ai_disagree_weight")

    adj_ai_col3, adj_ai_col4 = st.columns(2)
    with adj_ai_col3:
        enable_consistency_ai = st.checkbox("启用多周期一致性过滤", value=True, key="ai_consistency")
    with adj_ai_col4:
        min_align_ai = st.slider("一致性要求（2/3或3/3）", 2, 3, 2, 1, key="ai_consistency_min")

    with st.expander("市值筛选（可选）", expanded=False):
        if use_v3:
            evo_min_mc = evolve_v5.get("params", {}).get("min_market_cap")
            evo_max_mc = evolve_v5.get("params", {}).get("max_market_cap")
            default_mcap = (int(evo_min_mc), int(evo_max_mc)) if isinstance(evo_min_mc, (int, float)) and isinstance(evo_max_mc, (int, float)) else (100, 5000)
        else:
            default_mcap = (0, 5000)
        min_market_cap, max_market_cap = st.slider("流通市值范围（亿）", min_value=0, max_value=5000, value=default_mcap, step=10)

    button_text = " 开启 AI 稳健月度目标 (V5.0)" if use_v3 else " 开启 AI 高效选股 (V2.0)"
    if st.button(button_text, type="primary", use_container_width=True):
        with st.spinner(f"AI 正在全市场扫描 {'V5.0 稳健月度目标' if use_v3 else 'V2.0 高收益标的'}..."):
            try:
                df = _load_ai_history(connect_permanent_db)
                if df.empty:
                    st.error("数据库为空，请先在'数据中心'更新数据")
                else:
                    if use_v3:
                        stocks = vp_analyzer.select_monthly_target_stocks_v3(
                            df,
                            target_return=target_return / 100,
                            min_amount=min_amount,
                            max_volatility=max_volatility / 100,
                            min_market_cap=min_market_cap,
                            max_market_cap=max_market_cap,
                        )
                        session_key = "ai_monthly_stocks_v3"
                        version_name = "V5.0"
                    else:
                        stocks = vp_analyzer.select_monthly_target_stocks(
                            df,
                            target_return=target_return / 100,
                            min_amount=min_amount,
                            max_volatility=max_volatility / 100,
                        )
                        session_key = "ai_monthly_stocks_v2"
                        version_name = "V2.0"

                    if not stocks.empty:
                        candidate_count = len(stocks)
                        stocks = stocks.copy()
                        if "评分" in stocks.columns:
                            stocks["评分"] = pd.to_numeric(stocks["评分"], errors="coerce")
                            stocks = stocks.dropna(subset=["评分"])

                        market_env_ai = "oscillation"
                        try:
                            market_env_ai = vp_analyzer.get_market_environment()
                        except Exception:
                            pass
                        env_multiplier = 1.02 if market_env_ai == "bull" else 0.95 if market_env_ai == "bear" else 0.98
                        adj_factor = 1.0 - market_adjust_strength_ai + (market_adjust_strength_ai * env_multiplier)
                        if "评分" in stocks.columns:
                            stocks["评分"] = stocks["评分"] * adj_factor

                        penalty_cols = ["20日涨幅%", "5日涨幅%", "回撤%", "波动率%", "放量倍数"]
                        present_cols = [c for c in penalty_cols if c in stocks.columns]
                        if present_cols:
                            penalty = stocks[present_cols].apply(pd.to_numeric, errors="coerce").std(axis=1, ddof=0).fillna(0)
                            stocks["分歧惩罚"] = (penalty * disagree_std_weight_ai).round(2)
                        else:
                            stocks["分歧惩罚"] = 0.0
                        stocks["市场因子"] = round(adj_factor, 2)
                        if "评分" in stocks.columns:
                            stocks["评分"] = (stocks["评分"] - stocks["分歧惩罚"]).round(2)
                            stocks = apply_filter_mode(
                                stocks,
                                score_col="评分",
                                mode=select_mode_ai,
                                threshold=score_threshold_ai,
                                top_percent=top_percent_ai,
                            )
                        if enable_consistency_ai and not stocks.empty:
                            stocks = apply_multi_period_filter(stocks, permanent_db_path, min_align=min_align_ai)
                        stocks = add_reason_summary(stocks, score_col="评分")
                        if stocks.empty:
                            st.error("AI 未找到符合筛选条件的标的，请放宽阈值或筛选比例")
                            st.stop()

                        st.session_state[session_key] = stocks
                        st.session_state["ai_candidate_count"] = candidate_count
                        st.session_state["ai_strategy_version"] = version_name
                        st.success(f"{version_name} 扫描完成：找到 {len(stocks)} 只{'综合潜力' if use_v3 else '高收益潜力'}标的")
                        sim_account = get_sim_account()
                        buy_count, buy_status = auto_buy_ai_stocks(stocks, sim_account["per_buy_amount"], sim_account["auto_buy_top_n"])
                        st.session_state["last_ai_auto_buy"] = {
                            "count": buy_count,
                            "status": buy_status,
                            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }
                        st.rerun()
                    else:
                        if use_v3:
                            st.error("V5.0 未找到股票，可能的原因：\n1. 数据库数据不足（请先到「数据中心」更新数据）\n2. 数据查询出错（请查看系统日志）\n3. 当前市场偏弱或稳健过滤过严")
                            st.info("提示：V5.0已自动从“严格稳健”→“稳健放宽”→“救援筛选”仍未命中。\n可尝试：降低目标收益阈值、提高最大波动容忍度、或暂时放宽回撤/新股过滤。")
                            debug_runs = getattr(vp_analyzer, "last_v5_debug", None)
                            if debug_runs:
                                st.code(
                                    "\n".join(
                                        [
                                            f"[{s['stage']}] total={s['total_stocks']} cand={s['candidates']} res={s['results']} | "
                                            f"history={s['skip_history']} st={s['skip_st']} data={s['skip_len_data']} "
                                            f"limitup={s['skip_limitup']} amount={s['skip_amount']} mcap={s['skip_mcap']} turnover={s['skip_turnover']} ret20={s['skip_ret20_gate']} "
                                            f"ind_weak={s['skip_industry_weak']} vol_pct={s['skip_vol_percentile']} dd={s['skip_drawdown']} vol={s['skip_volatility']} "
                                            f"pull={s['skip_pullback']} bias={s['skip_bias']} score={s['skip_score']}"
                                            for s in debug_runs
                                        ]
                                    )
                                )
                        else:
                            st.warning("当前市场环境下未发现符合 V2.0 标准的标的，建议：\n1. 切换到V5.0稳健月度目标版（推荐）\n2. 降低门槛或等待大盘企稳")
            except Exception as exc:
                st.error(f"运行失败: {exc}")
                st.code(traceback.format_exc())

    result_key = "ai_monthly_stocks_v3" if use_v3 else "ai_monthly_stocks_v2"
    if result_key in st.session_state:
        stocks = st.session_state[result_key].head(top_n)
        version_name = st.session_state.get("ai_strategy_version", "V5.0" if use_v3 else "V2.0")
        st.divider()
        st.subheader(f"AI 优选名单 ({version_name} {'稳健月度目标版' if use_v3 else '追涨版'})")
        render_result_overview(stocks, score_col="评分", title="AI 结果概览")
        candidate_count = st.session_state.get("ai_candidate_count", len(stocks))
        msg, level = signal_density_hint(len(stocks), candidate_count)
        getattr(st, level)(msg)
        auto_buy_info = st.session_state.get("last_ai_auto_buy")
        if auto_buy_info:
            if auto_buy_info.get("status") == "duplicate":
                st.info("本次 AI 优选名单已自动买入过，无需重复买入。")
            elif auto_buy_info.get("status") == "disabled":
                st.warning("自动买入已关闭，本次未执行买入。")
            elif auto_buy_info.get("status") in ("empty", "skipped"):
                st.info("本次无可买标的，未执行买入。")
            else:
                st.info(f"已自动买入 {auto_buy_info.get('count', 0)} 只标的（{auto_buy_info.get('time', '')}）")

        col_m1, col_m2, col_m3, col_m4 = st.columns(4)
        with col_m1:
            st.metric("标的数量", f"{len(stocks)} 只")
        with col_m2:
            avg_ret20 = pd.to_numeric(stocks["20日涨幅%"], errors="coerce").mean()
            avg_ret5 = pd.to_numeric(stocks["5日涨幅%"], errors="coerce").mean() if "5日涨幅%" in stocks.columns else 0
            st.metric("平均20日涨幅", f"{avg_ret20:.1f}%", delta=f"5日: {avg_ret5:.1f}%")
        with col_m3:
            if "放量倍数" in stocks.columns:
                avg_vol_ratio = pd.to_numeric(stocks["放量倍数"], errors="coerce").mean()
                st.metric("平均放量倍数", f"{avg_vol_ratio:.2f}x")
            else:
                st.metric("平均放量倍数", "—")
        with col_m4:
            if "近20日成交额(亿)" in stocks.columns:
                avg_amt = pd.to_numeric(stocks["近20日成交额(亿)"], errors="coerce").mean()
                st.metric("平均活跃度", f"{avg_amt:.1f} 亿")
            else:
                st.metric("平均活跃度", "—")

        display_ai = standardize_result_df(stocks, score_col="评分")
        st.dataframe(
            display_ai,
            use_container_width=True,
            hide_index=True,
            column_config={
                "评分": st.column_config.NumberColumn(format="%.1f "),
                "筛选理由": st.column_config.TextColumn(width="large"),
                "核心理由": st.column_config.TextColumn(width="large"),
            },
        )
        st.markdown("---")
        st.download_button(
            label=f" 导出 {version_name} 结果 (Excel 兼容)",
            data=df_to_csv_bytes(stocks),
            file_name=f"AI_稳健月度目标{version_name}_结果_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv; charset=utf-8",
        )
