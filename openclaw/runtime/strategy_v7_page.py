from __future__ import annotations

from datetime import datetime
import traceback
from typing import Any, Callable, Dict, Optional, Tuple

import pandas as pd
import streamlit as st


def render_v7_strategy_page(
    *,
    vp_analyzer: Any,
    logger: Any,
    permanent_db_path: str,
    bulk_history_limit: int,
    v7_evaluator_available: bool,
    load_evolve_params: Callable[[str], Dict[str, Any]],
    sync_scan_task_with_params: Callable[[str, Dict[str, Any], str], None],
    render_scan_param_hint: Callable[[str], None],
    detect_heavy_background_job: Callable[[], Tuple[bool, str]],
    start_async_scan_task: Callable[..., Tuple[bool, str, str]],
    mark_scan_submitted: Callable[[str, Dict[str, Any]], None],
    get_db_last_trade_date: Callable[[str], str],
    load_v7_cache: Callable[[Dict[str, Any], str], Tuple[Optional[pd.DataFrame], Dict[str, Any]]],
    save_v7_cache: Callable[[Dict[str, Any], str, pd.DataFrame, Dict[str, Any]], None],
    connect_permanent_db: Callable[[], Any],
    load_candidate_stocks: Callable[..., pd.DataFrame],
    load_external_bonus_maps: Callable[[Any], Any],
    load_stock_history_bulk: Callable[..., Dict[str, pd.DataFrame]],
    load_stock_history: Callable[..., pd.DataFrame],
    calc_external_bonus: Callable[..., float],
    apply_filter_mode: Callable[..., pd.DataFrame],
    apply_multi_period_filter: Callable[..., pd.DataFrame],
    add_reason_summary: Callable[..., pd.DataFrame],
    render_v7_results: Callable[..., None],
    set_stock_pool_candidate: Callable[[str, Dict[str, Any], str, pd.DataFrame], None],
    append_reason_col: Callable[..., Any],
    standardize_result_df: Callable[..., pd.DataFrame],
    df_to_csv_bytes: Callable[[pd.DataFrame], bytes],
    render_async_scan_status: Callable[[str, str, str], Any],
) -> None:
    evolve_v7_core = load_evolve_params("v7_best.json")

    exp_v7 = st.expander("v7.0 策略说明", expanded=False)
    exp_v7.markdown(
        """
        <div style='background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    padding: 32px 28px; border-radius: 15px; color: white;
                    margin-bottom: 24px; box-shadow: 0 10px 30px rgba(0,0,0,0.3);'>
            <h1 style='margin:0; color: white; font-size: 2.2em; font-weight: 700; text-align: center;'>
                 v7.0 智能选股系统
            </h1>
            <p style='margin: 12px 0 0 0; font-size: 1.05em; text-align: center; opacity: 0.95;'>
                多因子协同，聚焦环境识别与行业轮动，强调稳定信号与一致性
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if v7_evaluator_available:
        exp_v7.success(
            """
            **当前使用 v7.0 智能版**

            **体系要点：**
            - 环境识别：趋势、波动与风险偏好
            - 情绪与资金：强弱扩散与成交结构
            - 行业轮动：热度与相对强度跟踪
            - 动态权重：随环境调整因子贡献
            - 分层过滤：市场 → 行业 → 个股

            **适用场景：**
            - 需要随市自适应的稳定选股
            - 重视行业轮动与结构一致性

            **说明：**
            - 回测指标以最新数据库与参数为准
            """
        )
    else:
        exp_v7.error(
            """
             **v7.0智能选股系统评分器未找到**
            - 请确保 `comprehensive_stock_evaluator_v7_ultimate.py` 文件存在
            - 建议重启应用后重试
            """
        )
        st.stop()

    st.markdown("###  扫描参数设置")
    col1, col2, col3 = st.columns(3)

    with col1:
        evo_thr = evolve_v7_core.get("params", {}).get("score_threshold")
        v7_default = int(round(evo_thr)) if isinstance(evo_thr, (int, float)) else 60
        score_threshold_v7 = st.slider(
            "评分阈值",
            min_value=50,
            max_value=90,
            value=v7_default,
            step=5,
            help="推荐70分起步，适应性强",
            key="score_threshold_v7_tab1",
        )
    evo_hold_v7 = evolve_v7_core.get("params", {}).get("holding_days")
    if isinstance(evo_hold_v7, (int, float)):
        st.caption(f"自动进化建议持仓周期：{int(evo_hold_v7)} 天（来源：自动进化）")

    with col2:
        scan_all_v7 = st.checkbox(" 全市场扫描", value=True, help="扫描所有A股（推荐）", key="scan_all_v7_tab1")

    with col3:
        show_details = st.checkbox(" 显示详细信息", value=True, help="显示市场环境、行业轮动等信息", key="show_details_v7_tab1")

    filter_col1_v7, filter_col2_v7 = st.columns(2)
    with filter_col1_v7:
        select_mode_v7 = st.selectbox(
            "筛选模式",
            ["双重筛选(阈值+Top%)", "分位数筛选(Top%)", "阈值筛选"],
            index=0,
            key="v7_select_mode_tab1",
        )
    with filter_col2_v7:
        top_percent_v7 = st.slider("Top百分比", 1, 10, 2, 1, key="v7_top_percent_tab1")

    filter_col3_v7, filter_col4_v7 = st.columns(2)
    with filter_col3_v7:
        enable_consistency_v7 = st.checkbox("启用多周期一致性过滤", value=True, key="v7_consistency_tab1")
    with filter_col4_v7:
        min_align_v7 = st.slider("一致性要求（2/3或3/3）", 2, 3, 2, 1, key="v7_consistency_min_tab1")
    use_cache_v7 = st.checkbox("优先使用离线缓存结果", value=True, key="v7_cache_tab1")
    async_scan_v7 = st.checkbox("后台运行扫描（推荐）", value=True, key="v7_async_scan")
    st.caption("开启后扫描会进入后台队列，页面刷新或短暂断线后仍可继续查看结果。")

    with st.expander("高级筛选选项（可选）"):
        col1, col2 = st.columns(2)
        with col1:
            cap_min_v7 = st.number_input(
                "最小市值（亿元）",
                min_value=0,
                max_value=5000,
                value=0,
                step=10,
                help="0表示不限制",
                key="cap_min_v7_tab1",
            )
        with col2:
            cap_max_v7 = st.number_input(
                "最大市值（亿元）",
                min_value=0,
                max_value=50000,
                value=0,
                step=50,
                help="0表示不限制",
                key="cap_max_v7_tab1",
            )

    v7_live_params = {
        "score_threshold": int(score_threshold_v7),
        "top_percent": int(top_percent_v7),
        "select_mode": select_mode_v7,
        "scan_all": bool(scan_all_v7),
        "cap_min": float(cap_min_v7),
        "cap_max": float(cap_max_v7),
        "enable_consistency": bool(enable_consistency_v7),
        "min_align": int(min_align_v7),
    }
    sync_scan_task_with_params("v7_async_task_id", v7_live_params, "v7.0策略")
    render_scan_param_hint("v7_async_task_id")
    if st.button("开始智能扫描（v7.0）", type="primary", use_container_width=True, key="scan_v7_tab1"):
        heavy_running, heavy_reason = detect_heavy_background_job()
        if heavy_running:
            st.warning(f"{heavy_reason}，系统将继续执行扫描（可能稍慢）。")
        if async_scan_v7:
            cache_params = dict(v7_live_params)
            ok, msg, run_id = start_async_scan_task("v7", cache_params, score_col="综合评分")
            if ok:
                st.session_state["v7_async_task_id"] = run_id
                mark_scan_submitted("v7_async_task_id", cache_params)
                st.success(msg)
            else:
                st.warning(msg)
            st.rerun()
        with st.spinner("v7.0智能系统扫描中...（识别环境→计算情绪→分析行业→动态评分→三层过滤）"):
            try:
                cache_params = dict(v7_live_params)
                db_last = get_db_last_trade_date(permanent_db_path)
                if use_cache_v7:
                    cached_df, cached_meta = load_v7_cache(cache_params, db_last)
                    if cached_df is not None and not cached_df.empty:
                        created_at = cached_meta.get("created_at", "未知")
                        st.info(f"已加载离线缓存（数据日期 {db_last}，生成时间 {created_at}）")
                        candidate_count = int(cached_meta.get("candidate_count", len(cached_df)))
                        filter_failed = int(cached_meta.get("filter_failed", 0))
                        render_v7_results(
                            cached_df,
                            candidate_count=candidate_count,
                            filter_failed=filter_failed,
                            score_threshold_v7=score_threshold_v7,
                            select_mode_v7=select_mode_v7,
                            top_percent_v7=top_percent_v7,
                        )
                        st.markdown("---")
                        st.subheader("智能结果列表（v7.0·缓存）")
                        v7_cached_display_df = cached_df.drop(columns=["原始数据"], errors="ignore")
                        st.dataframe(v7_cached_display_df, use_container_width=True, hide_index=True)
                        st.session_state["v7_scan_results_tab1"] = cached_df
                        set_stock_pool_candidate("v7", cache_params, "综合评分", cached_df)
                        st.stop()

                if hasattr(vp_analyzer, "evaluator_v7") and vp_analyzer.evaluator_v7:
                    vp_analyzer.evaluator_v7.reset_cache()

                conn = connect_permanent_db()
                if scan_all_v7 and cap_min_v7 == 0 and cap_max_v7 == 0:
                    stocks_df = load_candidate_stocks(conn, scan_all=True, require_industry=True)
                    st.info(f"全市场扫描模式：共{len(stocks_df)}只A股")
                else:
                    stocks_df = load_candidate_stocks(
                        conn,
                        scan_all=False,
                        cap_min_yi=cap_min_v7,
                        cap_max_yi=cap_max_v7,
                        require_industry=True,
                    )

                if len(stocks_df) == 0:
                    st.error("未找到符合条件的股票")
                    conn.close()
                else:
                    st.info(f"找到 {len(stocks_df)} 只候选股票，开始智能评分...")
                    bonus_global = 0.0
                    bonus_stock_map: Dict[str, float] = {}
                    top_list_set = set()
                    top_inst_set = set()
                    bonus_industry_map: Dict[str, float] = {}
                    try:
                        (
                            bonus_global,
                            bonus_stock_map,
                            top_list_set,
                            top_inst_set,
                            bonus_industry_map,
                        ) = load_external_bonus_maps(conn)
                    except Exception:
                        pass

                    if show_details and hasattr(vp_analyzer, "evaluator_v7") and vp_analyzer.evaluator_v7:
                        market_regime = vp_analyzer.evaluator_v7.market_analyzer.identify_market_regime()
                        market_sentiment = vp_analyzer.evaluator_v7.market_analyzer.calculate_market_sentiment()
                        hot_industries = vp_analyzer.evaluator_v7.industry_analyzer.get_hot_industries(top_n=5)

                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("市场环境", market_regime)
                        with col2:
                            sentiment_emoji = "" if market_sentiment > 0.3 else "" if market_sentiment > -0.3 else ""
                            st.metric(f"{sentiment_emoji} 市场情绪", f"{market_sentiment:.2f}")
                        with col3:
                            st.metric("热门行业", f"Top{len(hot_industries)}")

                        with st.expander("查看热门行业详情"):
                            for i, ind in enumerate(hot_industries, 1):
                                heat = vp_analyzer.evaluator_v7.industry_analyzer.sector_performance.get(ind, {}).get("heat", 0)
                                st.text(f"{i}. {ind} (热度: {heat:.2f})")

                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    results = []
                    filter_failed = 0
                    history_cache: Dict[str, pd.DataFrame] = {}
                    if len(stocks_df) <= bulk_history_limit:
                        history_cache = load_stock_history_bulk(
                            conn,
                            stocks_df["ts_code"].tolist(),
                            120,
                            "ts_code, trade_date, close_price, vol, pct_chg",
                        )

                    for idx, row in stocks_df.iterrows():
                        ts_code = row["ts_code"]
                        stock_name = row["name"]
                        industry = row["industry"]
                        progress_bar.progress((idx + 1) / len(stocks_df))
                        status_text.text(f"正在评分: {stock_name} ({ts_code}) - {idx+1}/{len(stocks_df)}")

                        try:
                            stock_data = history_cache.get(ts_code)
                            if stock_data is None:
                                stock_data = load_stock_history(
                                    conn,
                                    ts_code,
                                    120,
                                    "trade_date, close_price, vol, pct_chg",
                                )

                            if len(stock_data) >= 60:
                                stock_data["name"] = stock_name
                                score_result = vp_analyzer.evaluator_v7.evaluate_stock_v7(
                                    stock_data=stock_data,
                                    ts_code=ts_code,
                                    industry=industry,
                                )

                                if not score_result["success"]:
                                    filter_failed += 1
                                    continue

                                extra = calc_external_bonus(
                                    ts_code,
                                    industry,
                                    bonus_global,
                                    bonus_stock_map,
                                    top_list_set,
                                    top_inst_set,
                                    bonus_industry_map,
                                )
                                final_score = float(score_result["final_score"]) + extra
                                results.append(
                                    {
                                        "股票代码": ts_code,
                                        "股票名称": stock_name,
                                        "行业": industry,
                                        "流通市值": f"{row['circ_mv']/10000:.1f}亿",
                                        "综合评分": f"{final_score:.1f}",
                                        "评级": score_result.get("grade", "-"),
                                        "资金加分": f"{extra:.1f}",
                                        "市场环境": score_result.get("market_regime", "-"),
                                        "行业热度": f"{score_result.get('industry_heat', 0):.2f}",
                                        "行业排名": f"#{score_result.get('industry_rank', 0)}" if score_result.get("industry_rank", 0) > 0 else "未进Top8",
                                        "行业加分": f"+{score_result.get('bonus_score', 0)}分",
                                        "最新价格": f"{stock_data['close_price'].iloc[-1]:.2f}元",
                                        "智能止损": f"{score_result.get('stop_loss', 0):.2f}元",
                                        "智能止盈": f"{score_result.get('take_profit', 0):.2f}元",
                                        "筛选理由": score_result.get("signal_reasons", ""),
                                        "原始数据": score_result,
                                    }
                                )
                        except Exception as e:
                            logger.warning(f"评分失败 {ts_code}: {e}")
                            continue

                    progress_bar.empty()
                    status_text.empty()
                    conn.close()

                    if results:
                        results_df = pd.DataFrame(results)
                        results_df = apply_filter_mode(
                            results_df,
                            score_col="综合评分",
                            mode=select_mode_v7,
                            threshold=score_threshold_v7,
                            top_percent=top_percent_v7,
                        )
                        if enable_consistency_v7 and not results_df.empty:
                            results_df = apply_multi_period_filter(results_df, permanent_db_path, min_align=min_align_v7)
                        results_df = add_reason_summary(results_df, score_col="综合评分")
                        if results_df.empty:
                            st.warning("未找到符合条件的股票，请降低阈值或放宽筛选条件")
                            st.stop()

                        render_v7_results(
                            results_df,
                            candidate_count=len(stocks_df),
                            filter_failed=filter_failed,
                            score_threshold_v7=score_threshold_v7,
                            select_mode_v7=select_mode_v7,
                            top_percent_v7=top_percent_v7,
                        )
                        save_v7_cache(
                            cache_params,
                            db_last,
                            results_df,
                            {"candidate_count": len(stocks_df), "filter_failed": filter_failed},
                        )

                        st.session_state["v7_scan_results_tab1"] = results_df
                        set_stock_pool_candidate("v7", cache_params, "综合评分", results_df)

                        st.markdown("---")
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            avg_score = results_df["综合评分"].astype(float).mean()
                            st.metric("平均评分", f"{avg_score:.1f}分")
                        with col2:
                            max_score = results_df["综合评分"].astype(float).max()
                            st.metric("最高评分", f"{max_score:.1f}分")
                        with col3:
                            grade_high = sum(1 for g in results_df["评级"] if str(g).strip() in ("S", "A", "A+"))
                            st.metric("高评级", f"{grade_high}只")
                        with col4:
                            hot_count = sum(1 for r in results_df["行业排名"] if "#" in str(r) and int(str(r).replace("#", "")) <= 5)
                            st.metric("热门行业", f"{hot_count}只")

                        st.markdown("---")
                        st.subheader("智能结果列表（v7.0·动态权重）")
                        view_mode = st.radio("显示模式", ["完整信息", "核心指标", "简洁模式"], horizontal=True, key="view_mode_v7_tab1")

                        if view_mode == "完整信息":
                            display_cols = [
                                "股票代码",
                                "股票名称",
                                "行业",
                                "流通市值",
                                "综合评分",
                                "评级",
                                "资金加分",
                                "市场环境",
                                "行业热度",
                                "行业排名",
                                "行业加分",
                                "最新价格",
                                "智能止损",
                                "智能止盈",
                                "筛选理由",
                            ]
                        elif view_mode == "核心指标":
                            display_cols = [
                                "股票代码",
                                "股票名称",
                                "行业",
                                "流通市值",
                                "综合评分",
                                "评级",
                                "资金加分",
                                "行业热度",
                                "行业排名",
                                "最新价格",
                                "智能止损",
                                "智能止盈",
                            ]
                        else:
                            display_cols = ["股票代码", "股票名称", "行业", "流通市值", "综合评分", "资金加分", "评级", "最新价格", "筛选理由"]

                        display_cols = append_reason_col(display_cols, results_df)
                        display_df = results_df[display_cols]
                        display_df = standardize_result_df(display_df, score_col="综合评分")

                        st.dataframe(
                            display_df,
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                "综合评分": st.column_config.NumberColumn("综合评分", help="v7.0动态评分（100分制）", format="%.1f分"),
                                "评级": st.column_config.TextColumn("评级", help="评级：S/A/B/C（优秀/良好/中性/谨慎)", width="medium"),
                                "筛选理由": st.column_config.TextColumn("筛选理由", help="智能分析推荐原因", width="large"),
                            },
                        )

                        st.markdown("---")
                        export_df = results_df.drop(columns=["原始数据"], errors="ignore")
                        csv = df_to_csv_bytes(export_df)
                        st.download_button(
                            label=" 导出结果（CSV）",
                            data=csv,
                            file_name=f"核心策略_V7_智能选股_扫描结果_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv; charset=utf-8",
                        )
                    else:
                        st.warning(
                            f"未找到≥{score_threshold_v7}分的股票\n\n**说明：**\n"
                            "v7.0使用动态权重+三层过滤，门槛会根据市场环境自动调整。\n\n"
                            "**建议：**\n1. 降低评分阈值到60分\n2. 查看市场环境信息，了解当前市场状态\n"
                            "3. 当前可能不是最佳入场时机"
                        )
            except Exception as e:
                st.error(f"扫描失败: {e}")
                st.code(traceback.format_exc())

    if "v7_scan_results_tab1" in st.session_state:
        st.markdown("---")
        st.markdown("###  上次扫描结果")
        results_df = st.session_state["v7_scan_results_tab1"]
        display_df = results_df.drop(columns=["原始数据"], errors="ignore")
        st.dataframe(display_df, use_container_width=True, hide_index=True)
    render_async_scan_status("v7_async_task_id", "v7.0智能版", "综合评分")
