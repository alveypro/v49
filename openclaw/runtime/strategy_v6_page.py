from __future__ import annotations

from datetime import datetime
import traceback
from typing import Any, Callable, Dict

import pandas as pd
import streamlit as st


def render_v6_strategy_page(
    *,
    vp_analyzer: Any,
    logger: Any,
    permanent_db_path: str,
    v6_evaluator_available: bool,
    load_evolve_params: Callable[[str], Dict[str, Any]],
    sync_scan_task_with_params: Callable[[str, Dict[str, Any], str], None],
    render_scan_param_hint: Callable[[str], None],
    detect_heavy_background_job: Callable[[], Any],
    start_async_scan_task: Callable[..., Any],
    mark_scan_submitted: Callable[[str, Dict[str, Any]], None],
    connect_permanent_db: Callable[[], Any],
    load_candidate_stocks: Callable[..., pd.DataFrame],
    load_stock_history: Callable[..., pd.DataFrame],
    load_external_bonus_maps: Callable[[Any], Any],
    calc_external_bonus: Callable[..., float],
    update_scan_progress_ui: Callable[..., None],
    apply_filter_mode: Callable[..., pd.DataFrame],
    apply_multi_period_filter: Callable[..., pd.DataFrame],
    add_reason_summary: Callable[..., pd.DataFrame],
    render_result_overview: Callable[..., None],
    signal_density_hint: Callable[..., Any],
    set_stock_pool_candidate: Callable[[str, Dict[str, Any], str, pd.DataFrame], None],
    append_reason_col: Callable[..., Any],
    standardize_result_df: Callable[..., pd.DataFrame],
    df_to_csv_bytes: Callable[[pd.DataFrame], bytes],
    render_async_scan_status: Callable[[str, str, str], Any],
) -> None:
    evolve_v6_core = load_evolve_params("v6_best.json")

    exp_v6 = st.expander("v6.0 策略说明", expanded=False)
    exp_v6.markdown(
        """
        <div style='background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
                    padding: 40px 30px; border-radius: 15px; color: white;
                    margin-bottom: 30px; box-shadow: 0 10px 30px rgba(0,0,0,0.3);'>
            <h1 style='margin:0; color: white; font-size: 2.5em; font-weight: 700; text-align: center;'>
                 v6.0 超短线狙击·专业版 - 只选市场高质量1-3%
            </h1>
            <p style='margin: 15px 0 0 0; font-size: 1.2em; text-align: center; opacity: 0.95;'>
                三级过滤·七维严格评分·精英筛选·胜率80-90%·单次8-15%
            </p>
            <div style='display: flex; justify-content: center; gap: 30px; margin-top: 25px; flex-wrap: wrap;'>
                <div style='text-align: center;'>
                    <div style='font-size: 2em; font-weight: 700;'>80-90%</div>
                    <div style='font-size: 0.9em; opacity: 0.9;'>超高胜率</div>
                </div>
                <div style='text-align: center;'>
                    <div style='font-size: 2em; font-weight: 700;'>8-15%</div>
                    <div style='font-size: 0.9em; opacity: 0.9;'>单次收益</div>
                </div>
                <div style='text-align: center;'>
                    <div style='font-size: 2em; font-weight: 700;'>1-3%</div>
                    <div style='font-size: 0.9em; opacity: 0.9;'>市场占比</div>
                </div>
                <div style='text-align: center;'>
                    <div style='font-size: 2em; font-weight: 700;'>2-5天</div>
                    <div style='font-size: 0.9em; opacity: 0.9;'>持仓周期</div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if v6_evaluator_available:
        exp_v6.success(
            """
             **当前使用 v6.0 超短线版**

            ** 核心理念：三级过滤，只选市场高质量的1-3%！**

            **【第一级】必要条件过滤（硬性淘汰）：**
            -  板块3日涨幅 > 1%（板块必须走强）
            -  资金净流入 > 0（必须有资金）
            -  股票3日涨幅 > 0（必须上涨）
            -  板块内排名 ≤ 30%（必须是板块前列）
            -  价格位置 < 85%（不追高）
            -  放量 > 0.8倍（不能严重缩量）

            **【第二级】七维严格评分（极度严格）：**
            -  **资金流向**：30分（连续3天+20000万才给15分）
            -  **板块热度**：25分（涨幅>8%才给12分）
            -  **短期动量**：20分（涨幅>15%才给12分）
            -  **龙头属性**：10分（板块前3名才给4分以上）
            -  **相对强度**：8分（跑赢>10%才给8分）
            -  **技术突破**：5分（放量>2.5倍才给5分）
            -  **安全边际**：2分

            **【第三级】精英筛选：**
            - 协同加分（0-30分）：板块总龙头+15分，资金趋势+12分
            - 风险扣分（0-60分）：追高-25分，强势-20分，连续涨停-15分

            ** 适用场景：**
            -  超短线高手
            -  只做板块龙头
            -  追求极致精准
            -  宁缺毋滥

            ** 预期效果：**
            - 85分门槛：10-50只精选标的，胜率80-85%
            - 90分门槛：3-10只精选标的，胜率85-90%
            - 95分门槛：1-3只高级标的，胜率90%+
            """
        )
    else:
        exp_v6.error(
            """
             **v6.0 超短线版评分器未找到**
            - 请确保 `comprehensive_stock_evaluator_v6_ultimate.py` 文件存在
            - 建议重启应用后重试
            """
        )
        st.stop()

    st.markdown("###  选择扫描模式")

    scan_mode_v6 = st.radio(
        "选择模式（v6.0专业版）",
        [
            " 核心龙头（≥90分）- 精选标的3-10只",
            " 精选龙头（≥85分）- 精选标的10-50只",
            " 候选池（≥80分）- 候选标的50-100只",
        ],
        horizontal=True,
        help=" 90分：精选，胜率85-90% |  85分：精选，胜率80-85% |  80分：候选，胜率75-80%",
        key="scan_mode_v6_tab1",
    )

    col_v6_a, col_v6_b = st.columns(2)
    with col_v6_a:
        if "90分" in scan_mode_v6:
            score_threshold_v6_tab1 = 90
        elif "85分" in scan_mode_v6:
            score_threshold_v6_tab1 = 85
        else:
            score_threshold_v6_tab1 = 80

        evo_thr = evolve_v6_core.get("params", {}).get("score_threshold")
        if isinstance(evo_thr, (int, float)):
            score_threshold_v6_tab1 = int(round(evo_thr))

        st.metric("评分阈值", f"{score_threshold_v6_tab1}分", help="自动根据模式设置")
    evo_hold_v6 = evolve_v6_core.get("params", {}).get("holding_days")
    if isinstance(evo_hold_v6, (int, float)):
        st.caption(f"自动进化建议持仓周期：{int(evo_hold_v6)} 天（来源：自动进化）")

    with col_v6_b:
        scan_all_stocks = st.checkbox(
            " 全市场扫描（推荐）",
            value=True,
            help="扫描所有A股，不限制市值范围",
            key="scan_all_v6_tab1",
        )

    filter_col1_v6, filter_col2_v6 = st.columns(2)
    with filter_col1_v6:
        select_mode_v6 = st.selectbox(
            "筛选模式",
            ["双重筛选(阈值+Top%)", "分位数筛选(Top%)", "阈值筛选"],
            index=0,
            key="v6_select_mode_tab1",
        )
    with filter_col2_v6:
        top_percent_v6 = st.slider("Top百分比", 1, 10, 2, 1, key="v6_top_percent_tab1")

    filter_col3_v6, filter_col4_v6 = st.columns(2)
    with filter_col3_v6:
        enable_consistency_v6 = st.checkbox("启用多周期一致性过滤", value=True, key="v6_consistency_tab1")
    with filter_col4_v6:
        min_align_v6 = st.slider("一致性要求（2/3或3/3）", 2, 3, 2, 1, key="v6_consistency_min_tab1")
    async_scan_v6 = st.checkbox("后台运行扫描（推荐）", value=True, key="v6_async_scan")
    st.caption("开启后扫描会进入后台队列，页面刷新或短暂断线后仍可继续查看结果。")

    with st.expander("高级筛选选项（可选）"):
        col1, col2 = st.columns(2)
        with col1:
            cap_min_v6_tab1 = st.number_input(
                "最小市值（亿元）",
                min_value=0,
                max_value=5000,
                value=0,
                step=10,
                help="0表示不限制。建议50亿以上",
                key="cap_min_v6_tab1",
            )
        with col2:
            cap_max_v6_tab1 = st.number_input(
                "最大市值（亿元）",
                min_value=0,
                max_value=50000,
                value=0,
                step=50,
                help="0表示不限制。建议5000亿以内",
                key="cap_max_v6_tab1",
            )

    v6_live_params = {
        "score_threshold": int(score_threshold_v6_tab1),
        "top_percent": int(top_percent_v6),
        "select_mode": select_mode_v6,
        "cap_min": float(cap_min_v6_tab1 if not scan_all_stocks else 0),
        "cap_max": float(cap_max_v6_tab1 if not scan_all_stocks else 0),
        "enable_consistency": bool(enable_consistency_v6),
        "min_align": int(min_align_v6),
        "scan_all": bool(scan_all_stocks),
    }
    sync_scan_task_with_params("v6_async_task_id", v6_live_params, "v6.0策略")
    render_scan_param_hint("v6_async_task_id")
    if st.button("开始扫描（v6.0专业版）", type="primary", use_container_width=True, key="scan_v6_tab1"):
        heavy_running, heavy_reason = detect_heavy_background_job()
        if heavy_running:
            st.warning(f"{heavy_reason}，系统将继续执行扫描（可能稍慢）。")
        if async_scan_v6:
            cap_min_async_v6 = float(cap_min_v6_tab1 if not scan_all_stocks else 0)
            cap_max_async_v6 = float(cap_max_v6_tab1 if not scan_all_stocks else 0)
            cache_params = {
                "score_threshold": int(score_threshold_v6_tab1),
                "top_percent": int(top_percent_v6),
                "select_mode": select_mode_v6,
                "cap_min": cap_min_async_v6,
                "cap_max": cap_max_async_v6,
                "enable_consistency": bool(enable_consistency_v6),
                "min_align": int(min_align_v6),
            }
            ok, msg, run_id = start_async_scan_task("v6", cache_params, score_col="综合评分")
            if ok:
                st.session_state["v6_async_task_id"] = run_id
                mark_scan_submitted("v6_async_task_id", cache_params)
                st.success(msg)
            else:
                st.warning(msg)
            st.rerun()
        with st.spinner("v6.0专业版全市场扫描中...（三级过滤+严格评分）"):
            try:
                conn = connect_permanent_db()

                if scan_all_stocks:
                    stocks_df = load_candidate_stocks(conn, scan_all=True)
                    st.info(f"全市场扫描模式：共{len(stocks_df)}只A股")
                else:
                    stocks_df = load_candidate_stocks(
                        conn,
                        scan_all=False,
                        cap_min_yi=cap_min_v6_tab1,
                        cap_max_yi=cap_max_v6_tab1,
                    )

                if len(stocks_df) == 0:
                    st.error(f"未找到符合市值条件（{cap_min_v6_tab1}-{cap_max_v6_tab1}亿）的股票")
                    conn.close()
                else:
                    st.info(f"找到 {len(stocks_df)} 只符合市值条件的股票，开始三级过滤...")
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

                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    results = []
                    filter_failed_count = 0

                    for idx, row in stocks_df.iterrows():
                        ts_code = row["ts_code"]
                        stock_name = row["name"]
                        update_scan_progress_ui(
                            progress_bar,
                            status_text,
                            idx,
                            len(stocks_df),
                            f"正在评分: {stock_name} ({ts_code}) - {idx+1}/{len(stocks_df)}",
                        )

                        try:
                            stock_data = load_stock_history(
                                conn,
                                ts_code,
                                120,
                                "trade_date, close_price, vol, pct_chg",
                            )

                            if len(stock_data) >= 60:
                                stock_data["name"] = stock_name
                                score_result = vp_analyzer.evaluator_v6.evaluate_stock_v6(stock_data, ts_code)

                                if score_result.get("filter_failed", False):
                                    filter_failed_count += 1
                                    continue

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
                                            "资金流向": f"{dim_scores.get('资金流向', 0):.1f}",
                                            "板块热度": f"{dim_scores.get('板块热度', 0):.1f}",
                                            "短期动量": f"{dim_scores.get('短期动量', 0):.1f}",
                                            "龙头属性": f"{dim_scores.get('龙头属性', 0):.1f}",
                                            "相对强度": f"{dim_scores.get('相对强度', 0):.1f}",
                                            "技术突破": f"{dim_scores.get('技术突破', 0):.1f}",
                                            "安全边际": f"{dim_scores.get('安全边际', 0):.1f}",
                                            "最新价格": f"{stock_data['close_price'].iloc[-1]:.2f}元",
                                            "止损价": f"{score_result.get('stop_loss', 0):.2f}元",
                                            "止盈价": f"{score_result.get('take_profit', 0):.2f}元",
                                            "筛选理由": score_result.get("description", ""),
                                            "协同组合": score_result.get("synergy_combo", "无"),
                                            "原始数据": score_result,
                                        }
                                    )
                        except Exception as e:
                            logger.warning(f"评分失败 {ts_code}: {e}")
                            continue

                    progress_bar.empty()
                    status_text.empty()
                    conn.close()

                    st.markdown("---")
                    st.markdown("###  三级过滤结果")

                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("候选股票", f"{len(stocks_df)}只")
                    with col2:
                        st.metric("必要条件淘汰", f"{filter_failed_count}只", delta=f"{filter_failed_count/len(stocks_df)*100:.1f}%")
                    with col3:
                        passed_count = len(stocks_df) - filter_failed_count
                        st.metric("进入评分", f"{passed_count}只", delta=f"{passed_count/len(stocks_df)*100:.1f}%")
                    with col4:
                        st.metric("最终筛选", f"{len(results)}只", delta=f"{len(results)/len(stocks_df)*100:.2f}%")

                    if results:
                        results_df = pd.DataFrame(results)
                        results_df = apply_filter_mode(
                            results_df,
                            score_col="综合评分",
                            mode=select_mode_v6,
                            threshold=score_threshold_v6_tab1,
                            top_percent=top_percent_v6,
                        )
                        if enable_consistency_v6 and not results_df.empty:
                            results_df = apply_multi_period_filter(
                                results_df,
                                permanent_db_path,
                                min_align=min_align_v6,
                            )
                        results_df = add_reason_summary(results_df, score_col="综合评分")
                        if results_df.empty:
                            st.warning("未找到符合条件的股票，请降低阈值或放宽筛选条件")
                            st.stop()

                        if select_mode_v6 == "阈值筛选":
                            st.success(f"找到 {len(results_df)} 只符合条件的股票（≥{score_threshold_v6_tab1}分）")
                        elif select_mode_v6 == "双重筛选(阈值+Top%)":
                            st.success(f"先阈值后Top筛选：≥{score_threshold_v6_tab1}分，Top {top_percent_v6}%（{len(results_df)} 只）")
                        else:
                            st.success(f"选出 Top {top_percent_v6}%（{len(results_df)} 只）")

                        results_df = results_df.reset_index(drop=True)
                        msg, level = signal_density_hint(len(results_df), len(stocks_df))
                        getattr(st, level)(msg)
                        render_result_overview(results_df, score_col="综合评分", title="扫描结果概览")

                        st.session_state["v6_scan_results_tab1"] = results_df
                        set_stock_pool_candidate(
                            "v6",
                            {
                                "score_threshold": int(score_threshold_v6_tab1),
                                "top_percent": int(top_percent_v6),
                                "select_mode": select_mode_v6,
                                "scan_all": bool(scan_all_stocks),
                                "cap_min": float(cap_min_v6_tab1),
                                "cap_max": float(cap_max_v6_tab1),
                                "enable_consistency": bool(enable_consistency_v6),
                                "min_align": int(min_align_v6),
                            },
                            "综合评分",
                            results_df,
                        )

                        st.markdown("---")
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("标的数量", f"{len(results)}只")
                        with col2:
                            avg_score = results_df["综合评分"].astype(float).mean()
                            st.metric("平均评分", f"{avg_score:.1f}分")
                        with col3:
                            max_score = results_df["综合评分"].astype(float).max()
                            st.metric("最高评分", f"{max_score:.1f}分")
                        with col4:
                            grade_s = sum(1 for g in results_df["评级"] if g == "S")
                            grade_a = sum(1 for g in results_df["评级"] if g == "A")
                            st.metric("S+A级", f"{grade_s+grade_a}只")

                        st.markdown("---")
                        st.subheader("结果列表（v6.0专业版·七维评分）")

                        view_mode = st.radio(
                            "显示模式",
                            ["完整评分", "核心指标", "简洁模式"],
                            horizontal=True,
                            key="view_mode_v6_tab1",
                        )

                        if view_mode == "完整评分":
                            display_cols = [
                                "股票代码",
                                "股票名称",
                                "行业",
                                "流通市值",
                                "综合评分",
                                "评级",
                                "资金加分",
                                "资金流向",
                                "板块热度",
                                "短期动量",
                                "龙头属性",
                                "相对强度",
                                "技术突破",
                                "安全边际",
                                "最新价格",
                                "止损价",
                                "止盈价",
                                "筛选理由",
                                "协同组合",
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
                                "资金流向",
                                "板块热度",
                                "龙头属性",
                                "最新价格",
                                "止损价",
                                "止盈价",
                                "筛选理由",
                            ]
                        else:
                            display_cols = [
                                "股票代码",
                                "股票名称",
                                "行业",
                                "流通市值",
                                "综合评分",
                                "资金加分",
                                "评级",
                                "最新价格",
                                "筛选理由",
                                "协同组合",
                            ]

                        display_cols = append_reason_col(display_cols, results_df)
                        display_df = results_df[display_cols]
                        display_df = standardize_result_df(display_df, score_col="综合评分")

                        st.dataframe(
                            display_df,
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                "综合评分": st.column_config.NumberColumn(
                                    "综合评分",
                                    help="v6.0专业版评分（100分制）",
                                    format="%.1f分",
                                ),
                                "评级": st.column_config.TextColumn(
                                    "评级",
                                    help="S:优秀 A:良好 B:中性 C:谨慎",
                                    width="small",
                                ),
                                "筛选理由": st.column_config.TextColumn(
                                    "筛选理由",
                                    help="智能分析推荐原因",
                                    width="large",
                                ),
                            },
                        )

                        st.markdown("---")
                        export_df = results_df.drop(columns=["原始数据"], errors="ignore")
                        csv = df_to_csv_bytes(export_df)
                        st.download_button(
                            label=" 导出结果（CSV）",
                            data=csv,
                            file_name=f"v6.0_专业版_扫描结果_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv; charset=utf-8",
                        )
                    else:
                        st.warning(
                            f"未找到≥{score_threshold_v6_tab1}分的股票\n\n**说明：**\n"
                            "v6.0专业版使用极度严格的三级过滤标准，只选市场高质量的1-3%。\n\n"
                            "**建议：**\n1. 降低评分阈值到80分\n2. 扩大市值范围到50-2000亿\n"
                            "3. 这是正常现象，说明当前市场没有符合高级标准的股票"
                        )
            except Exception as e:
                st.error(f"扫描失败: {e}")
                st.code(traceback.format_exc())

    if "v6_scan_results_tab1" in st.session_state:
        st.markdown("---")
        st.markdown("###  上次扫描结果")
        results_df = st.session_state["v6_scan_results_tab1"]
        display_df = results_df.drop(columns=["原始数据"], errors="ignore")
        st.dataframe(display_df, use_container_width=True, hide_index=True)
    render_async_scan_status("v6_async_task_id", "v6.0超短线专业版", "综合评分")
