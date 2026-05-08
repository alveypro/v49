from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Callable

import pandas as pd
import streamlit as st


def render_today_v5_strategy_page(
    *,
    v5_evaluator_available: bool,
    vp_analyzer: Any,
    logger: Any,
    permanent_db_path: str,
    strict_full_market_mode: bool,
    evolve_v5_core: dict[str, Any],
    center_v5_params: dict[str, Any],
    center_v5_src: str,
    airivo_has_role: Callable[[str], bool],
    airivo_guard_action: Callable[[str, str, str, str], bool],
    airivo_append_action_audit: Callable[..., None],
    sync_scan_task_with_params: Callable[[str, dict[str, Any], str], None],
    render_scan_param_hint: Callable[[str], None],
    detect_heavy_background_job: Callable[[], tuple[bool, str]],
    start_async_scan_task: Callable[..., tuple[bool, str, str]],
    mark_scan_submitted: Callable[[str, dict[str, Any]], None],
    get_db_last_trade_date: Callable[[str], str],
    load_scan_cache: Callable[..., tuple[pd.DataFrame | None, dict[str, Any]]],
    render_cached_scan_results: Callable[..., None],
    set_stock_pool_candidate: Callable[[str, dict[str, Any], str, pd.DataFrame], None],
    connect_permanent_db: Callable[[], Any],
    load_candidate_stocks: Callable[..., pd.DataFrame],
    load_external_bonus_maps: Callable[[Any], tuple[Any, Any, Any, Any, Any]],
    load_history_range_bulk: Callable[..., dict[str, pd.DataFrame]],
    load_stock_history: Callable[..., pd.DataFrame],
    update_scan_progress_ui: Callable[..., None],
    calc_external_bonus: Callable[..., float],
    apply_filter_mode: Callable[..., pd.DataFrame],
    apply_multi_period_filter: Callable[..., pd.DataFrame],
    add_reason_summary: Callable[..., pd.DataFrame],
    signal_density_hint: Callable[..., tuple[str, str]],
    save_scan_cache: Callable[..., None],
    append_reason_col: Callable[[list[str], pd.DataFrame], list[str]],
    standardize_result_df: Callable[..., pd.DataFrame],
    df_to_csv_bytes: Callable[[pd.DataFrame], bytes],
    render_async_scan_status: Callable[[str, str, str], Any],
    bulk_history_limit: int,
) -> None:
    exp_v5 = st.expander("v5.0 策略说明", expanded=False)
    exp_v5.markdown(
        """
        <div style='background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
                    padding: 40px 30px; border-radius: 15px; color: white;
                    margin-bottom: 30px; box-shadow: 0 10px 30px rgba(0,0,0,0.3);'>
            <h1 style='margin:0; color: white; font-size: 2.5em; font-weight: 700; text-align: center;'>
                 启动确认型选股 - 趋势趋势捕手 v5.0
            </h1>
            <p style='margin: 15px 0 0 0; font-size: 1.2em; text-align: center; opacity: 0.95;'>
                启动确认版 · 8维度100分评分体系 · 重视趋势确认 · 追求趋势延续能力
            </p>
            <div style='display: flex; justify-content: center; gap: 30px; margin-top: 25px; flex-wrap: wrap;'>
                <div style='text-align: center;'><div style='font-size: 2em; font-weight: 700;'>20分</div><div style='font-size: 0.9em; opacity: 0.9;'>启动确认（翻倍）</div></div>
                <div style='text-align: center;'><div style='font-size: 2em; font-weight: 700;'>18分</div><div style='font-size: 0.9em; opacity: 0.9;'>主力行为（提权）</div></div>
                <div style='text-align: center;'><div style='font-size: 2em; font-weight: 700;'>8分</div><div style='font-size: 0.9em; opacity: 0.9;'>涨停基因（提权）</div></div>
                <div style='text-align: center;'><div style='font-size: 2em; font-weight: 700;'>中短期</div><div style='font-size: 0.9em; opacity: 0.9;'>持仓周期</div></div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if v5_evaluator_available:
        exp_v5.success(
            """
             **当前使用 v5.0 趋势版**

            ** 核心差异（对比v4.0潜伏策略版）：**
            -  **启动确认**：10分 → 20分（翻倍！）
            -  **主力行为**：15分 → 18分（提权！）
            -  **涨停基因**：5分 → 8分（提权！）
            -  **潜伏价值**：20分 → 10分（降权）
            -  **底部特征**：20分 → 10分（降权）

            ** 适用场景：**
            -  想要确认趋势后买入
            -  追求短期趋势延续能力
            -  不想等待潜伏期
            -  愿意承担适度追高风险

            ** 注意：**
            - 启动确认型买入点相对较高
            - 适合短期操作，需及时止盈
            - 建议配合技术面分析
            """
        )
    else:
        exp_v5.error(
            """
             **v5.0启动确认版评分器未找到**
            - 请确保 `comprehensive_stock_evaluator_v5.py` 文件存在
            - 建议重启应用后重试
            """
        )
        st.stop()

    st.markdown("###  选择扫描模式")
    exp_v5.info(
        """
        ** 当前市场环境说明：**

        v5.0 关注趋势确认信号（均线、量能、走势一致性），评分相对严格。
        评分分布偏低属正常现象，信号更集中。

        ** 建议：**
        - 当前市场环境下，建议使用**50-60分**作为筛选标准
        - 如果想要更保守的潜伏策略，建议使用**v4.0"潜伏策略"**
        - v5.0适合追求"确认趋势后买入"的投资者
        """
    )

    scan_mode_v5 = st.radio(
        "选择模式（v5.0）",
        [" 强势启动（≥60分）- 趋势明确", " 即将趋势（55-59分）- 蓄势待发", " 潜在机会（50-54分）- 提前关注"],
        help=" 强势启动：60分起，趋势已确认 | 即将趋势：准备启动 | 潜在机会：提前布局",
        horizontal=True,
        key="scan_mode_v5",
    )

    st.markdown("---")
    st.markdown("###  参数设置")

    param_col1_v5, param_col2_v5, param_col3_v5 = st.columns(3)
    with param_col1_v5:
        if "强势启动" in scan_mode_v5:
            default_threshold_v5, min_threshold_v5 = 60, 55
        elif "即将趋势" in scan_mode_v5:
            default_threshold_v5, min_threshold_v5 = 55, 50
        else:
            default_threshold_v5, min_threshold_v5 = 50, 45
        evo_thr = evolve_v5_core.get("params", {}).get("score_threshold")
        if isinstance(evo_thr, (int, float)):
            default_threshold_v5 = int(round(evo_thr))
            default_threshold_v5 = max(min_threshold_v5, min(90, default_threshold_v5))
        center_thr_v5 = center_v5_params.get("score_threshold")
        if isinstance(center_thr_v5, (int, float)):
            default_threshold_v5 = max(min_threshold_v5, min(90, int(round(center_thr_v5))))
        score_threshold_v5 = st.slider(
            "评分阈值",
            min_value=min_threshold_v5,
            max_value=90,
            value=default_threshold_v5,
            step=1,
            help="建议：强势启动60+，即将趋势55+，潜在机会50+",
            key="score_threshold_v5",
        )
        if center_v5_src == "strategy_center":
            st.caption(f"当前参数来源：策略中心（v5阈值 {default_threshold_v5}）")

    with param_col2_v5:
        cap_min_v5 = st.number_input(
            "最小市值（亿元）",
            min_value=0.0,
            max_value=5000.0,
            value=100.0,
            step=10.0,
            help="建议100亿以上，流动性好",
            key="cap_min_v5",
        )
    with param_col3_v5:
        cap_max_v5 = st.number_input(
            "最大市值（亿元）",
            min_value=float(cap_min_v5),
            max_value=50000.0,
            value=max(15000.0, float(cap_min_v5)),
            step=50.0,
            help="建议100-15000亿，覆盖大中小盘并保留流动性",
            key="cap_max_v5",
        )
    if strict_full_market_mode:
        cap_min_v5 = 0.0
        cap_max_v5 = 0.0
        st.caption("全市场严格口径：v5 市值已切换为 0~0。")

    filter_col1_v5, filter_col2_v5 = st.columns(2)
    with filter_col1_v5:
        modes_v5 = ["双重筛选(阈值+Top%)", "分位数筛选(Top%)", "阈值筛选"]
        if "v5_select_mode" not in st.session_state or st.session_state.get("v5_select_mode") not in modes_v5:
            st.session_state["v5_select_mode"] = "分位数筛选(Top%)"
        select_mode_v5 = st.selectbox("筛选模式", modes_v5, key="v5_select_mode")
    with filter_col2_v5:
        top_percent_v5 = st.slider("Top百分比", 1, 10, 1, 1, key="v5_top_percent")

    filter_col3_v5, filter_col4_v5 = st.columns(2)
    with filter_col3_v5:
        enable_consistency_v5 = st.checkbox("启用多周期一致性过滤", value=True, key="v5_consistency")
    with filter_col4_v5:
        min_align_v5 = st.slider("一致性要求（2/3或3/3）", 2, 3, 2, 1, key="v5_consistency_min")
    if "v5_cache" not in st.session_state:
        st.session_state["v5_cache"] = True
    st.session_state["v5_async_scan"] = True
    use_cache_v5 = st.checkbox("优先使用离线缓存结果", value=True, key="v5_cache")
    async_scan_v5 = st.checkbox("后台运行（生产默认，已强制开启）", value=True, key="v5_async_scan", disabled=True)
    st.caption("生产策略扫描已强制后台化：任务进入后台队列，页面刷新或短暂断线后仍可继续查看结果。")
    st.info("v5.0策略将扫描所有符合市值条件的股票（无数量限制）")
    evo_hold = evolve_v5_core.get("params", {}).get("holding_days")
    if isinstance(evo_hold, (int, float)):
        st.caption(f"自动进化建议持仓周期：{int(evo_hold)} 天（来源：自动进化）")

    st.markdown("---")
    v5_live_params = {
        "score_threshold": int(score_threshold_v5),
        "top_percent": int(top_percent_v5),
        "select_mode": select_mode_v5,
        "cap_min": float(cap_min_v5),
        "cap_max": float(cap_max_v5),
        "enable_consistency": bool(enable_consistency_v5),
        "min_align": int(min_align_v5),
    }
    sync_scan_task_with_params("v5_async_task_id", v5_live_params, "v5.0策略")
    render_scan_param_hint("v5_async_task_id")

    if st.button("开始扫描（v5.0启动确认型）", type="primary", use_container_width=True, key="scan_btn_v5", disabled=not airivo_has_role("operator")):
        if not airivo_guard_action("operator", "scan_v5", target="v5", reason="start_scan_v5"):
            st.stop()
        airivo_append_action_audit("scan_v5", True, target="v5", detail="scan_requested")
        heavy_running, heavy_reason = detect_heavy_background_job()
        if heavy_running:
            st.warning(f"{heavy_reason}，系统将继续执行扫描（可能稍慢）。")

        cache_params = {
            "score_threshold": score_threshold_v5,
            "top_percent": top_percent_v5,
            "select_mode": select_mode_v5,
            "cap_min": float(cap_min_v5),
            "cap_max": float(cap_max_v5),
            "enable_consistency": bool(enable_consistency_v5),
            "min_align": int(min_align_v5),
        }
        if async_scan_v5:
            ok, msg, run_id = start_async_scan_task("v5", cache_params, score_col="综合评分")
            if ok:
                st.session_state["v5_async_task_id"] = run_id
                mark_scan_submitted("v5_async_task_id", cache_params)
                st.success(msg)
            else:
                st.warning(msg)
            st.rerun()

        with st.spinner("正在扫描..."):
            try:
                db_last = get_db_last_trade_date(permanent_db_path)
                if use_cache_v5:
                    cached_df, cached_meta = load_scan_cache("v5_scan", cache_params, db_last)
                    if cached_df is not None and not cached_df.empty:
                        created_at = cached_meta.get("created_at", "未知")
                        st.info(f"已加载离线缓存（数据日期 {db_last}，生成时间 {created_at}）")
                        render_cached_scan_results(
                            "扫描结果（v5.0启动确认型）",
                            cached_df,
                            score_col="综合评分",
                            candidate_count=int(cached_meta.get("candidate_count", len(cached_df))),
                            filter_failed=int(cached_meta.get("filter_failed", 0)),
                            select_mode=select_mode_v5,
                            threshold=score_threshold_v5,
                            top_percent=top_percent_v5,
                        )
                        st.session_state["v5_scan_results"] = cached_df
                        set_stock_pool_candidate("v5", cache_params, "综合评分", cached_df)
                        st.stop()

                conn = connect_permanent_db()
                stocks_df = load_candidate_stocks(
                    conn,
                    scan_all=False,
                    cap_min_yi=cap_min_v5,
                    cap_max_yi=cap_max_v5,
                    random_order=True,
                )
                if stocks_df.empty:
                    st.error(f"未找到符合市值条件（{cap_min_v5}-{cap_max_v5}亿）的股票，请检查是否已更新市值数据")
                    st.info("提示：请先到Tab5（数据中心）点击「更新市值数据」")
                    conn.close()
                else:
                    st.success(f"找到 {len(stocks_df)} 只符合市值条件（{cap_min_v5}-{cap_max_v5}亿）的股票，开始评分...")
                    bonus_global = 0.0
                    bonus_stock_map = {}
                    top_list_set = set()
                    top_inst_set = set()
                    bonus_industry_map = {}
                    try:
                        bonus_global, bonus_stock_map, top_list_set, top_inst_set, bonus_industry_map = load_external_bonus_maps(conn)
                    except Exception:
                        pass
                    actual_min_mv = stocks_df["circ_mv"].min() / 10000
                    actual_max_mv = stocks_df["circ_mv"].max() / 10000
                    st.info(f"实际市值范围: {actual_min_mv:.1f} - {actual_max_mv:.1f} 亿元")

                    history_cache = {}
                    if len(stocks_df) <= bulk_history_limit:
                        end_date = datetime.now().strftime("%Y%m%d")
                        start_date = (datetime.now() - timedelta(days=180)).strftime("%Y%m%d")
                        history_cache = load_history_range_bulk(
                            conn,
                            stocks_df["ts_code"].tolist(),
                            start_date,
                            end_date,
                            "ts_code, trade_date, close_price, vol, pct_chg",
                        )

                    results: list[dict[str, Any]] = []
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    for idx, row in stocks_df.iterrows():
                        ts_code = row["ts_code"]
                        stock_name = row["name"]
                        update_scan_progress_ui(progress_bar, status_text, idx, len(stocks_df), f"正在评分: {stock_name} ({idx+1}/{len(stocks_df)})")
                        try:
                            stock_data = history_cache.get(ts_code)
                            if stock_data is None:
                                stock_data = load_stock_history(conn, ts_code, 120, "trade_date, close_price, vol, pct_chg")
                            if len(stock_data) >= 60:
                                stock_data["name"] = stock_name
                                score_result = vp_analyzer.evaluator_v5.evaluate_stock_v4(stock_data)
                                if score_result:
                                    extra = calc_external_bonus(
                                        ts_code,
                                        row["industry"],
                                        bonus_global,
                                        bonus_stock_map,
                                        top_list_set,
                                        top_inst_set,
                                        bonus_industry_map,
                                    )
                                    final_score = float(score_result.get("final_score", 0)) + extra
                                else:
                                    extra = 0.0
                                    final_score = 0.0
                                if score_result:
                                    dim_scores = score_result.get("dimension_scores", {})
                                    results.append(
                                        {
                                            "股票代码": ts_code,
                                            "股票名称": stock_name,
                                            "行业": row["industry"],
                                            "流通市值": f"{row['circ_mv']/10000:.1f}亿",
                                            "综合评分": f"{final_score:.1f}",
                                            "评级": score_result.get("grade", "-"),
                                            "资金加分": f"{extra:.1f}",
                                            "启动确认": f"{dim_scores.get('启动确认', 0):.1f}",
                                            "主力行为": f"{dim_scores.get('主力行为', 0):.1f}",
                                            "涨停基因": f"{dim_scores.get('涨停基因', 0):.1f}",
                                            "MACD趋势": f"{dim_scores.get('MACD趋势', 0):.1f}",
                                            "量价配合": f"{dim_scores.get('量价配合', 0):.1f}",
                                            "均线多头": f"{dim_scores.get('均线多头', 0):.1f}",
                                            "潜伏价值": f"{dim_scores.get('潜伏价值', 0):.1f}",
                                            "底部特征": f"{dim_scores.get('底部特征', 0):.1f}",
                                            "最新价格": f"{stock_data['close_price'].iloc[-1]:.2f}元",
                                            "止损价": f"{score_result.get('stop_loss', 0):.2f}元",
                                            "止盈价": f"{score_result.get('take_profit', 0):.2f}元",
                                            "筛选理由": score_result.get("description", ""),
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
                        results_df = apply_filter_mode(results_df, score_col="综合评分", mode=select_mode_v5, threshold=score_threshold_v5, top_percent=top_percent_v5)
                        if enable_consistency_v5 and not results_df.empty:
                            results_df = apply_multi_period_filter(results_df, permanent_db_path, min_align=min_align_v5)
                        results_df = add_reason_summary(results_df, score_col="综合评分")
                        if results_df.empty:
                            st.warning("未找到符合条件的股票，请降低阈值或放宽筛选条件")
                            st.stop()
                        if select_mode_v5 == "阈值筛选":
                            st.success(f"找到 {len(results_df)} 只符合条件的股票（≥{score_threshold_v5}分）")
                        elif select_mode_v5 == "双重筛选(阈值+Top%)":
                            st.success(f"先阈值后Top筛选：≥{score_threshold_v5}分，Top {top_percent_v5}%（{len(results_df)} 只）")
                        else:
                            st.success(f"选出 Top {top_percent_v5}%（{len(results_df)} 只）")
                        results_df = results_df.reset_index(drop=True)
                        msg, level = signal_density_hint(len(results_df), len(stocks_df))
                        getattr(st, level)(msg)
                        save_scan_cache("v5_scan", cache_params, db_last, results_df, {"candidate_count": len(stocks_df), "filter_failed": 0})
                        st.session_state["v5_scan_results"] = results_df
                        set_stock_pool_candidate("v5", cache_params, "综合评分", results_df)

                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("标的数量", f"{len(results)}只")
                        with col2:
                            st.metric("平均评分", f"{results_df['综合评分'].astype(float).mean():.1f}分")
                        with col3:
                            st.metric("最高评分", f"{results_df['综合评分'].astype(float).max():.1f}分")
                        with col4:
                            grade_s = sum(1 for g in results_df["评级"] if g == "S")
                            grade_a = sum(1 for g in results_df["评级"] if g == "A")
                            st.metric("S+A级", f"{grade_s+grade_a}只")

                        st.markdown("---")
                        st.subheader("结果列表（v5.0启动确认·8维评分）")
                        view_mode = st.radio("显示模式", ["完整评分", "核心指标", "简洁模式"], horizontal=True, key="v5_view_mode")
                        if view_mode == "完整评分":
                            display_cols = ["股票代码", "股票名称", "行业", "流通市值", "综合评分", "评级", "资金加分", "启动确认", "主力行为", "涨停基因", "MACD趋势", "量价配合", "均线多头", "潜伏价值", "底部特征", "最新价格", "止损价", "止盈价", "筛选理由"]
                        elif view_mode == "核心指标":
                            display_cols = ["股票代码", "股票名称", "行业", "流通市值", "综合评分", "评级", "资金加分", "启动确认", "主力行为", "最新价格", "止损价", "止盈价", "筛选理由"]
                        else:
                            display_cols = ["股票代码", "股票名称", "行业", "流通市值", "综合评分", "资金加分", "评级", "最新价格", "筛选理由"]
                        display_cols = append_reason_col(display_cols, results_df)
                        display_df = standardize_result_df(results_df[display_cols], score_col="综合评分")
                        st.dataframe(
                            display_df,
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                "综合评分": st.column_config.NumberColumn("综合评分", help="v5.0启动确认评分（100分制）", format="%.1f分"),
                                "评级": st.column_config.TextColumn("评级", help="S:优秀 A:良好 B:中性 C:谨慎", width="small"),
                                "筛选理由": st.column_config.TextColumn("筛选理由", help="智能分析推荐原因", width="large"),
                            },
                        )
                        st.markdown("---")
                        export_df = results_df.drop("原始数据", axis=1)
                        st.download_button(
                            label=" 导出结果（CSV）",
                            data=df_to_csv_bytes(export_df),
                            file_name=f"核心策略_V5_启动确认_扫描结果_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv; charset=utf-8",
                        )
                    else:
                        st.warning(f"未找到≥{score_threshold_v5}分的股票\n\n**建议：**\n1. 降低评分阈值到50-55分\n2. 扩大市值范围")
            except Exception as e:
                import traceback

                st.error(f"扫描失败: {e}")
                st.code(traceback.format_exc())

    if "v5_scan_results" in st.session_state:
        st.markdown("---")
        st.markdown("###  上次扫描结果")
        results_df = st.session_state["v5_scan_results"]
        display_df = results_df.drop(columns=["原始数据"], errors="ignore")
        st.dataframe(display_df, use_container_width=True, hide_index=True)

    render_async_scan_status("v5_async_task_id", "v5.0启动确认", "综合评分")
