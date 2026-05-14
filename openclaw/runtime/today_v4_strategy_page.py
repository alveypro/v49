from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

import pandas as pd
import streamlit as st


def render_today_v4_strategy_page(
    *,
    v4_evaluator_available: bool,
    vp_analyzer: Any,
    permanent_db_path: str,
    logger: Any,
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
    load_external_bonus_maps: Callable[[Any], tuple[Any, Any, Any, Any, Any]],
    load_candidate_stocks: Callable[..., pd.DataFrame],
    batch_load_stock_histories: Callable[..., dict[str, pd.DataFrame]],
    load_stock_history: Callable[..., pd.DataFrame],
    calc_external_bonus: Callable[..., float],
    apply_filter_mode: Callable[..., pd.DataFrame],
    apply_multi_period_filter: Callable[..., pd.DataFrame],
    add_reason_summary: Callable[..., pd.DataFrame],
    render_result_overview: Callable[..., None],
    signal_density_hint: Callable[..., tuple[str, str]],
    save_scan_cache: Callable[..., None],
    append_reason_col: Callable[[list[str], pd.DataFrame], list[str]],
    standardize_result_df: Callable[..., pd.DataFrame],
    df_to_csv_bytes: Callable[[pd.DataFrame], bytes],
) -> None:
    exp_v4 = st.expander("v4.0 策略说明", expanded=False)
    exp_v4.markdown(
        """
        <div style='background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    padding: 40px 30px; border-radius: 15px; color: white;
                    margin-bottom: 30px; box-shadow: 0 10px 30px rgba(0,0,0,0.3);'>
            <h1 style='margin:0; color: white; font-size: 2.2em; font-weight: 700; text-align: center;'>
                 v4.0 长期稳健版 - 潜伏策略
            </h1>
            <p style='margin: 12px 0 0 0; font-size: 1.05em; text-align: center; opacity: 0.95;'>
                定位：在趋势确认前完成布局，强调安全边际与稳定性
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if v4_evaluator_available:
        exp_v4.success(
            """
            **当前使用 v4.0 潜伏策略版**

            **核心定位：**
            - 以安全边际为优先
            - 在趋势确认前完成布局

            **评分结构（100分制）：**
            - 潜伏价值：20
            - 底部特征：20
            - 量价配合：15
            - MACD趋势：15
            - 均线结构：10
            - 主力行为：10
            - 启动确认：5
            - 历史强势：5

            **适用场景：**
            - 稳健型交易
            - 愿意等待确认前布局
            - 关注买入成本与风险控制

            **回测说明：**
            - 结果以最新数据库与参数为准
            """
        )
    else:
        exp_v4.error(
            """
             **v4.0潜伏策略版评分器未找到**
            - 请确保 `comprehensive_stock_evaluator_v4.py` 文件存在
            - 建议重启应用后重试
            """
        )
        st.stop()

    st.markdown("###  扫描模式")
    scan_mode = st.radio(
        "扫描模式（v4.0）",
        ["市值优选（100-500亿）", "底部蓄势监控"],
        horizontal=True,
        key="v4_scan_mode",
        help="市值优选：流动性更好 | 底部蓄势：监控低位启动阶段",
    )

    st.markdown("---")
    st.markdown("###  参数设置")
    st.info(
        """
        v4.0 说明（回测样本约2000只）：
        - 评分60：信号相对充足
        - 评分65：筛选更严格，信号减少
        - 评分70：偏保守，仅少量标的

        建议：
        - 初始阈值 60
        - 市值区间 100-500 亿
        - 持仓周期约 5 天
        """
    )

    param_col1_v4, param_col2_v4 = st.columns(2)
    with param_col1_v4:
        score_threshold_v4 = st.slider(
            "评分阈值",
            min_value=50,
            max_value=90,
            value=60,
            step=1,
            help="建议从60起，视信号密度调整",
            key="score_threshold_v4",
        )
    with param_col2_v4:
        scan_all_v4 = st.checkbox(
            "全市场扫描",
            value=True,
            help="扫描所有A股，不限制市值范围",
            key="scan_all_v4",
        )

    filter_col1_v4, filter_col2_v4 = st.columns(2)
    with filter_col1_v4:
        select_mode_v4 = st.selectbox(
            "筛选模式",
            ["双重筛选(阈值+Top%)", "分位数筛选(Top%)", "阈值筛选"],
            index=0,
            key="v4_select_mode",
        )
    with filter_col2_v4:
        top_percent_v4 = st.slider("Top百分比", 1, 10, 2, 1, key="v4_top_percent")

    filter_col3_v4, filter_col4_v4 = st.columns(2)
    with filter_col3_v4:
        enable_consistency_v4 = st.checkbox("启用多周期一致性过滤", value=True, key="v4_consistency")
    with filter_col4_v4:
        min_align_v4 = st.slider("一致性要求（2/3或3/3）", 2, 3, 2, 1, key="v4_consistency_min")
    use_cache_v4 = st.checkbox("优先使用离线缓存结果", value=True, key="v4_cache")
    async_scan_v4 = st.checkbox("后台运行扫描（推荐）", value=True, key="v4_async_scan")
    st.caption("开启后扫描会进入后台队列，页面刷新或短暂断线后仍可继续查看结果。")

    with st.expander("高级筛选选项（可选）"):
        col1, col2 = st.columns(2)
        with col1:
            cap_min_v4 = st.number_input(
                "最小市值（亿元）",
                min_value=0,
                max_value=5000,
                value=0,
                step=10,
                help="0表示不限制。建议50亿以上",
                key="cap_min_v4",
            )
        with col2:
            cap_max_v4 = st.number_input(
                "最大市值（亿元）",
                min_value=0,
                max_value=50000,
                value=0,
                step=50,
                help="0表示不限制。建议5000亿以内",
                key="cap_max_v4",
            )
        st.info("提示：勾选「全市场扫描」且市值都为0时，将扫描所有A股（约3000-5000只）")

    st.markdown("---")
    v4_live_params = {
        "score_threshold": int(score_threshold_v4),
        "top_percent": int(top_percent_v4),
        "select_mode": select_mode_v4,
        "scan_all": bool(scan_all_v4),
        "cap_min": float(cap_min_v4),
        "cap_max": float(cap_max_v4),
        "enable_consistency": bool(enable_consistency_v4),
        "min_align": int(min_align_v4),
    }
    sync_scan_task_with_params("v4_async_task_id", v4_live_params, "v4.0策略")
    render_scan_param_hint("v4_async_task_id")

    if st.button("开始扫描（v4.0潜伏策略）", type="primary", use_container_width=True, key="scan_btn_v4"):
        heavy_running, heavy_reason = detect_heavy_background_job()
        if heavy_running:
            st.warning(f"{heavy_reason}，系统将继续执行扫描（可能稍慢）。")

        cache_params = {
            "score_threshold": score_threshold_v4,
            "top_percent": top_percent_v4,
            "select_mode": select_mode_v4,
            "scan_all": bool(scan_all_v4),
            "cap_min": float(cap_min_v4),
            "cap_max": float(cap_max_v4),
            "enable_consistency": bool(enable_consistency_v4),
            "min_align": int(min_align_v4),
        }

        if async_scan_v4:
            ok, msg, run_id = start_async_scan_task("v4", cache_params, score_col="综合评分")
            if ok:
                st.session_state["v4_async_task_id"] = run_id
                mark_scan_submitted("v4_async_task_id", cache_params)
                st.success(msg)
            else:
                st.warning(msg)
            st.rerun()

        with st.spinner("正在扫描全市场股票..."):
            try:
                db_last = get_db_last_trade_date(permanent_db_path)
                if use_cache_v4:
                    cached_df, cached_meta = load_scan_cache("v4_scan", cache_params, db_last)
                    if cached_df is not None and not cached_df.empty:
                        created_at = cached_meta.get("created_at", "未知")
                        st.info(f"已加载离线缓存（数据日期 {db_last}，生成时间 {created_at}）")
                        render_cached_scan_results(
                            "扫描结果（v4.0潜伏策略）",
                            cached_df,
                            score_col="综合评分",
                            candidate_count=int(cached_meta.get("candidate_count", len(cached_df))),
                            filter_failed=int(cached_meta.get("filter_failed", 0)),
                            select_mode=select_mode_v4,
                            threshold=score_threshold_v4,
                            top_percent=top_percent_v4,
                        )
                        st.session_state["v4_scan_results"] = cached_df
                        set_stock_pool_candidate("v4", cache_params, "综合评分", cached_df)
                        st.stop()

                conn = connect_permanent_db()
                bonus_global = 0.0
                bonus_stock_map = {}
                top_list_set = set()
                top_inst_set = set()
                bonus_industry_map = {}
                try:
                    bonus_global, bonus_stock_map, top_list_set, top_inst_set, bonus_industry_map = load_external_bonus_maps(conn)
                except Exception:
                    pass

                if scan_all_v4 and cap_min_v4 == 0 and cap_max_v4 == 0:
                    stocks_df = load_candidate_stocks(conn, scan_all=True, cap_min_yi=0, cap_max_yi=0)
                    st.info(f"全市场扫描模式：共{len(stocks_df)}只A股")
                else:
                    cap_min_wan = cap_min_v4 * 10000 if cap_min_v4 > 0 else 0
                    cap_max_wan = cap_max_v4 * 10000 if cap_max_v4 > 0 else 999999999
                    total_query = """
                        SELECT
                            COUNT(*) as total,
                            COUNT(CASE WHEN circ_mv IS NOT NULL AND circ_mv > 0 THEN 1 END) as has_mv,
                            MIN(circ_mv)/10000 as min_mv,
                            MAX(circ_mv)/10000 as max_mv
                        FROM stock_basic
                    """
                    total_stats = pd.read_sql_query(total_query, conn)
                    stocks_df = load_candidate_stocks(
                        conn,
                        scan_all=False,
                        cap_min_yi=cap_min_v4,
                        cap_max_yi=cap_max_v4,
                    )
                    with st.expander("数据库统计信息", expanded=False):
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("数据库总股票数", f"{total_stats['total'].iloc[0]}只")
                        with col2:
                            st.metric("有市值数据", f"{total_stats['has_mv'].iloc[0]}只")
                        with col3:
                            st.metric("市值范围", f"{total_stats['min_mv'].iloc[0]:.1f}-{total_stats['max_mv'].iloc[0]:.1f}亿")
                        st.info(f"查询条件：{cap_min_wan}万元 ≤ 市值 ≤ {cap_max_wan}万元（即{cap_min_v4}亿-{cap_max_v4}亿）")
                    st.info(f"市值筛选模式：找到{len(stocks_df)}只股票（{cap_min_v4 if cap_min_v4 > 0 else 0}-{cap_max_v4 if cap_max_v4 > 0 else '不限'}亿）")

                if stocks_df.empty:
                    st.error("未找到符合条件的股票，请检查是否已更新市值数据")
                    st.info("提示：请先到Tab1（数据中心）点击「更新市值数据」")
                    conn.close()
                else:
                    actual_min_mv = stocks_df["circ_mv"].min() / 10000
                    actual_max_mv = stocks_df["circ_mv"].max() / 10000
                    st.success(f"实际市值范围: {actual_min_mv:.1f} - {actual_max_mv:.1f} 亿元，开始八维评分...")

                    results: list[dict[str, Any]] = []
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    status_text.text("正在批量加载历史数据...")
                    all_codes = stocks_df["ts_code"].tolist()
                    histories = batch_load_stock_histories(
                        conn,
                        all_codes,
                        limit=120,
                        columns="ts_code, trade_date, close_price, vol, pct_chg",
                    )
                    status_text.text(f"数据加载完成，开始评分 {len(stocks_df)} 只股票...")

                    for idx, row in stocks_df.iterrows():
                        ts_code = row["ts_code"]
                        stock_name = row["name"]
                        progress_bar.progress((idx + 1) / len(stocks_df))
                        if (idx + 1) % 50 == 0 or idx == 0:
                            status_text.text(f"正在评分: {stock_name} ({idx+1}/{len(stocks_df)})")
                        try:
                            stock_data = histories.get(ts_code)
                            if stock_data is None or stock_data.empty:
                                stock_data = load_stock_history(conn, ts_code, 120, "trade_date, close_price, vol, pct_chg")
                            if len(stock_data) >= 60:
                                stock_data["name"] = stock_name
                                score_result = vp_analyzer.evaluator_v4.evaluate_stock_v4(stock_data)
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
                                            "潜伏价值": f"{dim_scores.get('潜伏价值', 0):.1f}",
                                            "底部特征": f"{dim_scores.get('底部特征', 0):.1f}",
                                            "量价配合": f"{dim_scores.get('量价配合', 0):.1f}",
                                            "MACD趋势": f"{dim_scores.get('MACD趋势', 0):.1f}",
                                            "均线多头": f"{dim_scores.get('均线多头', 0):.1f}",
                                            "主力行为": f"{dim_scores.get('主力行为', 0):.1f}",
                                            "启动确认": f"{dim_scores.get('启动确认', 0):.1f}",
                                            "涨停基因": f"{dim_scores.get('涨停基因', 0):.1f}",
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
                        results_df = apply_filter_mode(
                            results_df,
                            score_col="综合评分",
                            mode=select_mode_v4,
                            threshold=score_threshold_v4,
                            top_percent=top_percent_v4,
                        )
                        if enable_consistency_v4 and not results_df.empty:
                            results_df = apply_multi_period_filter(results_df, permanent_db_path, min_align=min_align_v4)
                        results_df = add_reason_summary(results_df, score_col="综合评分")
                        if results_df.empty:
                            st.warning("未找到符合条件的股票，请降低阈值或放宽筛选条件")
                            st.stop()

                        if select_mode_v4 == "阈值筛选":
                            st.success(f"找到 {len(results_df)} 只符合条件的股票（≥{score_threshold_v4}分）")
                        elif select_mode_v4 == "双重筛选(阈值+Top%)":
                            st.success(f"先阈值后Top筛选：≥{score_threshold_v4}分，Top {top_percent_v4}%（{len(results_df)} 只）")
                        else:
                            st.success(f"选出 Top {top_percent_v4}%（{len(results_df)} 只）")

                        render_result_overview(results_df, score_col="综合评分", title="扫描结果概览")
                        msg, level = signal_density_hint(len(results_df), len(stocks_df))
                        getattr(st, level)(msg)
                        save_scan_cache("v4_scan", cache_params, db_last, results_df, {"candidate_count": len(stocks_df), "filter_failed": 0})
                        st.session_state["v4_scan_results"] = results_df
                        set_stock_pool_candidate("v4", cache_params, "综合评分", results_df)

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
                        st.subheader("结果列表（v4.0潜伏策略·8维评分）")
                        view_mode = st.radio("显示模式", ["完整评分", "核心指标", "简洁模式"], horizontal=True, key="v4_view_mode")
                        if view_mode == "完整评分":
                            display_cols = ["股票代码", "股票名称", "行业", "流通市值", "综合评分", "资金加分", "评级", "潜伏价值", "底部特征", "量价配合", "MACD趋势", "均线多头", "主力行为", "启动确认", "涨停基因", "最新价格", "止损价", "止盈价", "筛选理由"]
                        elif view_mode == "核心指标":
                            display_cols = ["股票代码", "股票名称", "行业", "流通市值", "综合评分", "资金加分", "评级", "潜伏价值", "底部特征", "最新价格", "止损价", "止盈价", "筛选理由"]
                        else:
                            display_cols = ["股票代码", "股票名称", "行业", "流通市值", "综合评分", "资金加分", "评级", "最新价格", "筛选理由"]
                        display_cols = append_reason_col(display_cols, results_df)
                        display_df = standardize_result_df(results_df[display_cols], score_col="综合评分")
                        st.dataframe(
                            display_df,
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                "综合评分": st.column_config.NumberColumn("综合评分", help="v4.0潜伏策略评分（100分制）", format="%.1f分"),
                                "评级": st.column_config.TextColumn("评级", help="S:优秀 A:良好 B:中性 C:谨慎", width="small"),
                                "筛选理由": st.column_config.TextColumn("筛选理由", help="智能分析推荐原因", width="large"),
                            },
                        )

                        st.markdown("---")
                        st.info(
                            """
                            ###  v4.0策略操作建议（潜伏策略）

                            ** 核心理念**: 在启动前潜伏，而不是启动后追高

                            ** 评级说明**:
                            - **S级(≥80分)**:  完美潜伏机会，重点关注，建议仓位18-20%
                            - **A级(70-79分)**: ⭐ 优质潜伏标的，积极关注，建议仓位15-18%
                            - **B级(60-69分)**:  良好机会，谨慎关注，建议仓位10-15%
                            - **C级(50-59分)**:  合格标的，保持观察，建议仓位5-10%

                            ** 持仓周期**: 5天（数据验证的平均持仓约5天）

                            ** 止盈止损**:
                            - 止损：严格执行-3%止损，或跌破止损价
                            - 止盈：达到+4%或止盈价时分批止盈

                            ** 仓位管理**:
                            - 单只股票：不超过20%仓位
                            - 总仓位：最多持有3-5只
                            - 分批建仓：首次50%，确认后加仓50%

                            ** 风险提示**:
                            - 本策略经2000只股票、274个真实信号验证，胜率56.6%
                            - 严格执行纪律，不追涨不抄底
                            - 设置好止损，控制单笔亏损<3%
                            """
                        )

                        st.markdown("---")
                        export_df = results_df.drop(columns=["原始数据"], errors="ignore")
                        st.download_button(
                            label=" 导出结果（CSV）",
                            data=df_to_csv_bytes(export_df),
                            file_name=f"核心策略_V4_潜伏策略_扫描结果_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv; charset=utf-8",
                        )
                    else:
                        st.warning("未找到符合条件的股票，请适当放宽筛选条件")
            except Exception as e:
                import traceback

                st.error(f"扫描失败: {e}")
                st.code(traceback.format_exc())

    if "v4_scan_results" in st.session_state:
        st.markdown("---")
        st.markdown("###  上次扫描结果")
        results_df = st.session_state["v4_scan_results"]
        display_df = results_df.drop(columns=["原始数据"], errors="ignore")
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        st.download_button(
            label="导出最近一次结果（CSV）",
            data=df_to_csv_bytes(display_df),
            file_name=f"核心策略_V8_最近结果_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv; charset=utf-8",
            key="v8_recent_export_csv",
        )
