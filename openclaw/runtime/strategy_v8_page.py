from __future__ import annotations

from datetime import datetime, timedelta
import traceback
from typing import Any, Callable, Dict, Optional, Tuple

import pandas as pd
import streamlit as st


def render_v8_strategy_page(
    *,
    vp_analyzer: Any,
    logger: Any,
    permanent_db_path: str,
    v8_evaluator_available: bool,
    strict_full_market_mode: bool,
    load_evolve_params: Callable[[str], Dict[str, Any]],
    load_strategy_center_scan_defaults: Callable[[str], Tuple[Dict[str, Any], str]],
    sync_scan_task_with_params: Callable[[str, Dict[str, Any], str], None],
    render_scan_param_hint: Callable[[str], None],
    render_front_scan_summary: Callable[[str, str], None],
    has_role: Callable[[str], bool],
    guard_action: Callable[[str, str, str, str], bool],
    append_action_audit: Callable[..., None],
    detect_heavy_background_job: Callable[[], Tuple[bool, str]],
    start_async_scan_task: Callable[..., Tuple[bool, str, str]],
    mark_scan_submitted: Callable[[str, Dict[str, Any]], None],
    run_front_scan_via_offline_pipeline: Callable[..., Tuple[Optional[pd.DataFrame], Dict[str, Any]]],
    mark_front_scan_completed: Callable[..., None],
    get_db_last_trade_date: Callable[[str], str],
    load_scan_cache: Callable[[str, Dict[str, Any], str], Tuple[Optional[pd.DataFrame], Dict[str, Any]]],
    save_scan_cache: Callable[[str, Dict[str, Any], str, pd.DataFrame, Dict[str, Any]], None],
    connect_permanent_db: Callable[[], Any],
    load_candidate_stocks: Callable[..., pd.DataFrame],
    load_stock_history_fallback: Callable[..., pd.DataFrame],
    load_external_bonus_maps: Callable[[Any], Any],
    calc_external_bonus: Callable[..., float],
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
    evolve_v8_core = load_evolve_params("v8_best.json")
    center_v8_params, center_v8_src = load_strategy_center_scan_defaults("v8")

    exp_v8 = st.expander("v8.0 策略说明", expanded=False)
    exp_v8.markdown(
        """
        <div style='background: linear-gradient(135deg, #f093fb 0%, #f5576c 50%, #ffd700 100%);
                    padding: 40px 30px; border-radius: 15px; color: white;
                    margin-bottom: 30px; box-shadow: 0 10px 30px rgba(0,0,0,0.3);'>
            <h1 style='margin:0; color: white; font-size: 2.5em; font-weight: 700; text-align: center;'>
                 v8.0 进阶版 · 量化风控体系
            </h1>
            <p style='margin: 15px 0 0 0; font-size: 1.2em; text-align: center; opacity: 0.95;'>
                ATR 风控 + 市场过滤 + 仓位管理 + 多因子评分
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if v8_evaluator_available:
        exp_v8.success(
            """
            **当前使用 v8.0 进阶版**

            **体系要点：**
            - ATR 风控：按波动自动调节止损/止盈
            - 市场过滤：趋势、情绪与成交热度
            - 仓位管理：基于胜率与盈亏比的自适应仓位
            - 多因子评分：结构、资金、动量、波动综合评估
            - 动态再平衡：持仓持续复核与替换

            **适用场景：**
            - 强调风控与执行纪律
            - 追求中短周期的稳定性与一致性

            **说明：**
            - 回测指标以最新数据库与参数为准
            """
        )
    else:
        exp_v8.error(
            """
             **v8.0 进阶版评分器未找到**
            - 请确保 `comprehensive_stock_evaluator_v8_ultimate.py` 文件存在
            - 建议重启应用后重试
            """
        )
        st.stop()

    st.markdown("###  扫描参数设置")
    col1, col2, col3 = st.columns(3)
    with col1:
        evo_thr = evolve_v8_core.get("params", {}).get("score_threshold")
        if isinstance(evo_thr, (int, float)):
            default_range = (int(round(evo_thr)), 90)
        else:
            default_range = (55, 70)
        center_thr_v8 = center_v8_params.get("score_threshold")
        if isinstance(center_thr_v8, (int, float)):
            center_thr_int_v8 = max(45, min(90, int(round(center_thr_v8))))
            default_range = (center_thr_int_v8, 90)
        score_threshold_v8 = st.slider(
            "评分阈值区间",
            min_value=45,
            max_value=90,
            value=default_range,
            step=5,
            help="可选最小和最大阈值：55-70建议，60-65稳健，75极致。仅落在区间内的股票会展示。",
            key="score_threshold_v8_tab1",
        )
        if isinstance(score_threshold_v8, (tuple, list)):
            v8_thr_min = float(score_threshold_v8[0])
            v8_thr_max = float(score_threshold_v8[1] if len(score_threshold_v8) > 1 else score_threshold_v8[0])
        else:
            v8_thr_min = float(score_threshold_v8)
            v8_thr_max = float(score_threshold_v8)
        score_threshold_v8_payload = [v8_thr_min, v8_thr_max]
        if center_v8_src == "strategy_center":
            st.caption(f"当前参数来源：策略中心（v8最小阈值 {default_range[0]}）")
    evo_hold_v8 = evolve_v8_core.get("params", {}).get("holding_days")
    if isinstance(evo_hold_v8, (int, float)):
        st.caption(f"自动进化建议持仓周期：{int(evo_hold_v8)} 天（来源：自动进化）")

    with col2:
        scan_all_v8 = st.checkbox(
            " 全市场扫描",
            value=bool(st.session_state.get("scan_all_v8_tab1", False)),
            help="扫描所有A股，耗时显著更高；生产默认关闭",
            key="scan_all_v8_tab1",
        )

    with col3:
        enable_kelly = st.checkbox(" 显示凯利仓位", value=True, help="显示凯利公式计算的最优仓位", key="enable_kelly_v8_tab1")

    filter_col1_v8, filter_col2_v8 = st.columns(2)
    with filter_col1_v8:
        modes_v8 = ["双重筛选(阈值+Top%)", "分位数筛选(Top%)", "阈值筛选"]
        if "v8_select_mode_tab1" not in st.session_state or st.session_state.get("v8_select_mode_tab1") not in modes_v8:
            st.session_state["v8_select_mode_tab1"] = "分位数筛选(Top%)"
        select_mode_v8 = st.selectbox("筛选模式", modes_v8, key="v8_select_mode_tab1")
    with filter_col2_v8:
        top_percent_v8 = st.slider("Top百分比", 1, 10, 1, 1, key="v8_top_percent_tab1")

    filter_col3_v8, filter_col4_v8 = st.columns(2)
    with filter_col3_v8:
        enable_consistency_v8 = st.checkbox("启用多周期一致性过滤", value=True, key="v8_consistency_tab1")
    with filter_col4_v8:
        min_align_v8 = st.slider("一致性要求（2/3或3/3）", 2, 3, 2, 1, key="v8_consistency_min_tab1")
    if "v8_cache_tab1" not in st.session_state:
        st.session_state["v8_cache_tab1"] = True
    st.session_state["v8_async_scan"] = True
    use_cache_v8 = st.checkbox("优先使用离线缓存结果", value=True, key="v8_cache_tab1")
    async_scan_v8 = st.checkbox(
        "后台运行（生产默认，已强制开启）",
        value=True,
        key="v8_async_scan",
        disabled=True,
    )
    st.caption("生产策略扫描已强制后台化：任务进入后台队列，页面刷新或短暂断线后仍可继续查看结果。")

    with st.expander("高级筛选选项（可选）"):
        col1, col2 = st.columns(2)
        with col1:
            cap_min_v8 = st.number_input("最小市值（亿元）", min_value=0, max_value=5000, value=100, step=10, help="0表示不限制", key="cap_min_v8_tab1")
        with col2:
            cap_max_v8 = st.number_input("最大市值（亿元）", min_value=0, max_value=50000, value=15000, step=50, help="0表示不限制", key="cap_max_v8_tab1")
    if strict_full_market_mode:
        scan_all_v8 = True
        cap_min_v8 = 0.0
        cap_max_v8 = 0.0
        st.caption("全市场严格口径：v8 市值已切换为 0~0。")

    v8_live_params = {
        "score_threshold": score_threshold_v8_payload,
        "top_percent": int(top_percent_v8),
        "select_mode": select_mode_v8,
        "scan_all": bool(scan_all_v8),
        "cap_min": float(cap_min_v8),
        "cap_max": float(cap_max_v8),
        "enable_consistency": bool(enable_consistency_v8),
        "min_align": int(min_align_v8),
    }
    sync_scan_task_with_params("v8_async_task_id", v8_live_params, "v8.0策略")
    render_scan_param_hint("v8_async_task_id")
    render_front_scan_summary("v8", "v8.0进阶版")
    if "v8_scan_results_tab1" in st.session_state:
        st.markdown("### 最近一次v8结果")
        recent_v8_df = st.session_state["v8_scan_results_tab1"]
        st.dataframe(
            standardize_result_df(recent_v8_df.drop(columns=["原始数据"], errors="ignore"), score_col="综合评分"),
            use_container_width=True,
            hide_index=True,
        )

    if st.button("开始扫描（v8.0）", type="primary", use_container_width=True, key="scan_v8_tab1", disabled=not has_role("operator")):
        if not guard_action("operator", "scan_v8", "v8", "start_scan_v8"):
            st.stop()
        append_action_audit("scan_v8", True, target="v8", detail="scan_requested")
        heavy_running, heavy_reason = detect_heavy_background_job()
        if heavy_running:
            st.warning(f"{heavy_reason}，系统将继续执行扫描（可能稍慢）。")
        if async_scan_v8:
            cache_params = {
                "score_threshold": score_threshold_v8_payload,
                "top_percent": int(top_percent_v8),
                "select_mode": select_mode_v8,
                "scan_all": bool(scan_all_v8),
                "cap_min": float(cap_min_v8),
                "cap_max": float(cap_max_v8),
                "enable_consistency": bool(enable_consistency_v8),
                "min_align": int(min_align_v8),
            }
            ok, msg, run_id = start_async_scan_task("v8", cache_params, score_col="综合评分")
            if ok:
                st.session_state["v8_async_task_id"] = run_id
                mark_scan_submitted("v8_async_task_id", cache_params)
                st.success(msg)
            else:
                if run_id:
                    st.session_state["v8_async_task_id"] = run_id
                st.warning(msg)
            st.rerun()

        with st.spinner("v8.0进阶版扫描中...（三级市场过滤→18维度评分→ATR风控→凯利仓位）"):
            try:
                cache_params = {
                    "score_threshold": score_threshold_v8_payload,
                    "top_percent": top_percent_v8,
                    "select_mode": select_mode_v8,
                    "scan_all": bool(scan_all_v8),
                    "cap_min": float(cap_min_v8),
                    "cap_max": float(cap_max_v8),
                    "enable_consistency": bool(enable_consistency_v8),
                    "min_align": int(min_align_v8),
                    "enable_kelly": bool(enable_kelly),
                }
                results_df, meta = run_front_scan_via_offline_pipeline(strategy="v8", params=cache_params, analyzer=vp_analyzer)
                if results_df is None or results_df.empty:
                    st.warning("未找到符合条件的股票，请降低阈值或放宽筛选条件")
                    st.stop()
                st.session_state["v8_scan_results_tab1"] = results_df
                mark_front_scan_completed(strategy="v8", results_df=results_df, meta=meta, score_col="综合评分")
                set_stock_pool_candidate("v8", cache_params, "综合评分", results_df)
                st.session_state["v8_front_scan_notice"] = f"v8.0扫描完成，返回 {len(results_df)} 条"
                st.rerun()

                db_last = get_db_last_trade_date(permanent_db_path)
                if use_cache_v8:
                    cached_df, cached_meta = load_scan_cache("v8_scan", cache_params, db_last)
                    if cached_df is not None and not cached_df.empty:
                        created_at = cached_meta.get("created_at", "未知")
                        st.info(f"已加载离线缓存（数据日期 {db_last}，生成时间 {created_at}）")
                        from openclaw.runtime.scan_ui import render_cached_scan_results
                        render_cached_scan_results(
                            title="扫描结果（v8.0进阶版）",
                            results_df=cached_df,
                            score_col="综合评分",
                            candidate_count=int(cached_meta.get("candidate_count", len(cached_df))),
                            filter_failed=int(cached_meta.get("filter_failed", 0)),
                            select_mode=select_mode_v8,
                            threshold=v8_thr_min,
                            top_percent=top_percent_v8,
                            render_result_overview=render_result_overview,
                            signal_density_hint=signal_density_hint,
                        )
                        st.session_state["v8_scan_results_tab1"] = cached_df
                        set_stock_pool_candidate("v8", cache_params, "综合评分", cached_df)
                        st.stop()

                if hasattr(vp_analyzer, "evaluator_v8") and vp_analyzer.evaluator_v8:
                    vp_analyzer.evaluator_v8.reset_cache()

                conn = connect_permanent_db()
                st.info("正在进行三级市场过滤（择时系统）...")
                try:
                    from data.history import load_history_full as load_history_full_v2  # type: ignore

                    index_data = load_history_full_v2(
                        db_path=permanent_db_path,
                        ts_code="000001.SH",
                        start_date=(datetime.now() - timedelta(days=420)).strftime("%Y%m%d"),
                        end_date=datetime.now().strftime("%Y%m%d"),
                        columns="trade_date, close_price AS close, vol AS volume",
                        table_candidates=("daily_trading_history", "daily_trading_data", "daily_data"),
                    )
                    if index_data is not None and len(index_data) > 120:
                        index_data = index_data.tail(120).reset_index(drop=True)
                except Exception:
                    index_data = pd.DataFrame()

                if len(index_data) >= 60:
                    market_filter = vp_analyzer.evaluator_v8.market_filter
                    market_status = market_filter.comprehensive_filter(index_data)
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        trend_status = market_status.get("trend", {})
                        st.metric("市场趋势", f"{trend_status.get('trend', '未知')}")
                    with col2:
                        sentiment_status = market_status.get("sentiment", {})
                        sentiment_val = sentiment_status.get("sentiment_score", 0)
                        st.metric("市场情绪", f"{sentiment_val:.2f}", delta="健康" if sentiment_val > -0.2 else "警告")
                    with col3:
                        volume_status = market_status.get("volume", {})
                        st.metric("市场热度", f"{volume_status.get('volume_status', '未知')}")
                else:
                    st.warning("大盘数据不足，跳过市场过滤")
                    market_status = {"can_trade": True, "position_multiplier": 1.0, "reason": "数据不足，默认可交易"}

                if not market_status["can_trade"]:
                    st.warning(
                        f"""
                         **市场环境不佳，建议观望！**

                        **未通过原因：**
                        {market_status.get('reason', '综合评估不通过')}

                        **v8.0择时系统建议：**
                        当前市场环境不适合激进操作，建议：
                        1. 空仓观望，等待更好时机
                        2. 关注市场转势信号
                        3. 可以小仓位试探（不超过20%）

                         强行扫描请继续，但风险自负！
                        """
                    )
                    if not st.checkbox("我理解风险，继续扫描", key="force_scan_v8"):
                        st.stop()
                else:
                    st.success("市场环境通过三级过滤，可以安全选股！")

                if scan_all_v8 and cap_min_v8 == 0 and cap_max_v8 == 0:
                    stocks_df = load_candidate_stocks(conn, scan_all=True, require_industry=True)
                    st.info(f"全市场扫描模式：共{len(stocks_df)}只A股")
                else:
                    stocks_df = load_candidate_stocks(
                        conn,
                        scan_all=False,
                        cap_min_yi=cap_min_v8,
                        cap_max_yi=cap_max_v8,
                        require_industry=True,
                    )

                if len(stocks_df) == 0:
                    st.error("未找到符合条件的股票")
                    conn.close()
                else:
                    st.info(f"找到 {len(stocks_df)} 只候选股票，开始18维度智能评分...")
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
                    filter_failed = 0

                    for idx, row in stocks_df.iterrows():
                        ts_code = row["ts_code"]
                        stock_name = row["name"]
                        industry = row["industry"]
                        progress_bar.progress((idx + 1) / len(stocks_df))
                        status_text.text(f"正在评分: {stock_name} ({ts_code}) - {idx+1}/{len(stocks_df)}")
                        try:
                            stock_data = load_stock_history_fallback(
                                conn,
                                ts_code,
                                120,
                                "trade_date, close_price, high_price, low_price, vol, pct_chg",
                            )
                            if len(stock_data) >= 60:
                                stock_data["name"] = stock_name
                                score_result = vp_analyzer.evaluator_v8.evaluate_stock_v8(
                                    stock_data=stock_data,
                                    ts_code=ts_code,
                                    index_data=index_data if len(index_data) else None,
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
                                kelly_position = ""
                                if enable_kelly and "win_rate" in score_result and "win_loss_ratio" in score_result:
                                    kelly_pct = vp_analyzer.evaluator_v8._calculate_kelly_position(
                                        score_result["win_rate"], score_result["win_loss_ratio"]
                                    )
                                    kelly_position = f"{kelly_pct*100:.1f}%"
                                close_col = "close_price" if "close_price" in stock_data.columns else "close"
                                latest_price = stock_data[close_col].iloc[-1]
                                results.append(
                                    {
                                        "股票代码": ts_code,
                                        "股票名称": stock_name,
                                        "行业": industry,
                                        "流通市值": f"{row['circ_mv']/10000:.1f}亿",
                                        "综合评分": f"{final_score:.1f}",
                                        "评级": score_result.get("grade", "-"),
                                        "资金加分": f"{extra:.1f}",
                                        "星级": f"{score_result.get('star_rating', 0)}⭐" if score_result.get("star_rating", 0) else "-",
                                        "建议仓位": f"{score_result.get('position_suggestion', 0)*100:.0f}%" if score_result.get("position_suggestion") else "-",
                                        "预期胜率": f"{score_result.get('win_rate', 0)*100:.1f}%" if "win_rate" in score_result else "-",
                                        "盈亏比": f"{score_result.get('win_loss_ratio', 0):.2f}" if "win_loss_ratio" in score_result else "-",
                                        "凯利仓位": kelly_position if enable_kelly else "-",
                                        "最新价格": f"{latest_price:.2f}元",
                                        "ATR值": f"{score_result.get('atr_stops', {}).get('atr_value', 0):.2f}" if score_result.get("atr_stops") else "-",
                                        "ATR止损": (
                                            f"{score_result.get('atr_stops', {}).get('stop_loss', 0):.2f}元"
                                            if score_result.get("atr_stops") and score_result["atr_stops"].get("stop_loss") is not None
                                            else "-"
                                        ),
                                        "ATR止盈": (
                                            f"{score_result.get('atr_stops', {}).get('take_profit', 0):.2f}元"
                                            if score_result.get("atr_stops") and score_result["atr_stops"].get("take_profit") is not None
                                            else "-"
                                        ),
                                        "ATR移动止损": (
                                            f"{score_result.get('atr_stops', {}).get('trailing_stop', 0):.2f}元"
                                            if score_result.get("atr_stops") and score_result["atr_stops"].get("trailing_stop") is not None
                                            else "-"
                                        ),
                                        "止损幅度%": (
                                            f"{score_result.get('atr_stops', {}).get('stop_loss_pct', 0):.2f}%"
                                            if score_result.get("atr_stops") and score_result["atr_stops"].get("stop_loss_pct") is not None
                                            else "-"
                                        ),
                                        "止盈幅度%": (
                                            f"{score_result.get('atr_stops', {}).get('take_profit_pct', 0):.2f}%"
                                            if score_result.get("atr_stops") and score_result["atr_stops"].get("take_profit_pct") is not None
                                            else "-"
                                        ),
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

                    st.markdown("---")
                    st.markdown("###  扫描结果（v8.0）")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("候选股票", f"{len(stocks_df)}只")
                    with col2:
                        st.metric("过滤淘汰", f"{filter_failed}只", delta=f"{filter_failed/len(stocks_df)*100:.1f}%")
                    with col3:
                        st.metric("最终推荐", f"{len(results)}只", delta=f"{len(results)/len(stocks_df)*100:.2f}%")

                    results_df = pd.DataFrame(results) if results else pd.DataFrame()
                    filter_counts: Dict[str, int] = {}
                    if not results_df.empty:
                        min_thr, _ = score_threshold_v8 if isinstance(score_threshold_v8, tuple) else (score_threshold_v8, 100)
                        filter_counts["raw"] = len(results_df)
                        preview = results_df.copy()
                        preview["score_val"] = pd.to_numeric(preview["综合评分"], errors="coerce")
                        preview = preview.dropna(subset=["score_val"])
                        if select_mode_v8 in ("阈值筛选", "双重筛选(阈值+Top%)"):
                            preview = preview[preview["score_val"] >= min_thr]
                        filter_counts["after_threshold"] = len(preview)
                        if select_mode_v8 in ("分位数筛选(Top%)", "双重筛选(阈值+Top%)") and len(preview) > 0:
                            preview = preview.sort_values("score_val", ascending=False)
                            keep_n = max(1, int(len(preview) * top_percent_v8 / 100))
                            preview = preview.head(keep_n)
                        filter_counts["after_top"] = len(preview)
                        if enable_consistency_v8 and not preview.empty:
                            preview = apply_multi_period_filter(preview, permanent_db_path, min_align=min_align_v8)
                        filter_counts["after_consistency"] = len(preview)

                        results_df = apply_filter_mode(
                            results_df,
                            score_col="综合评分",
                            mode=select_mode_v8,
                            threshold=min_thr,
                            top_percent=top_percent_v8,
                        )
                        if enable_consistency_v8 and not results_df.empty:
                            results_df = apply_multi_period_filter(results_df, permanent_db_path, min_align=min_align_v8)
                        results_df = add_reason_summary(results_df, score_col="综合评分")

                    if results and results_df.empty:
                        st.warning("未找到符合条件的股票，请降低阈值或放宽筛选条件")
                        if filter_counts:
                            st.info(
                                f"过滤分布：原始{filter_counts.get('raw', 0)} → "
                                f"阈值后{filter_counts.get('after_threshold', 0)} → "
                                f"Top后{filter_counts.get('after_top', 0)} → "
                                f"一致性后{filter_counts.get('after_consistency', 0)}"
                            )
                        st.stop()

                    if len(results) > 0 and not results_df.empty:
                        try:
                            dist_scores = results_df["综合评分"].astype(float)
                            avg_score = dist_scores.mean()
                            median_score = dist_scores.median()
                            pct70 = (dist_scores >= 70).sum()
                            pct65 = (dist_scores >= 65).sum()
                            pct60 = (dist_scores >= 60).sum()
                            st.info(
                                f"""
                                **分布提示：**
                                - 平均分：{avg_score:.1f}，中位数：{median_score:.1f}
                                - ≥70分：{pct70} 只，≥65分：{pct65} 只，≥60分：{pct60} 只

                                **推荐阈值：** {max(55, min(70, round(median_score)))} 分 （取中位数附近，范围[55,70]）
                                """
                            )
                        except Exception:
                            pass

                    if results and not results_df.empty:
                        min_thr, _ = score_threshold_v8 if isinstance(score_threshold_v8, tuple) else (score_threshold_v8, 100)
                        if select_mode_v8 == "阈值筛选":
                            st.success(f"找到 {len(results_df)} 只符合条件的股票（≥{min_thr}分）")
                        elif select_mode_v8 == "双重筛选(阈值+Top%)":
                            st.success(f"先阈值后Top筛选：≥{min_thr}分，Top {top_percent_v8}%（{len(results_df)} 只）")
                        else:
                            st.success(f"选出 Top {top_percent_v8}%（{len(results_df)} 只）")
                        render_result_overview(results_df, score_col="综合评分", title="扫描结果概览")
                        msg, level = signal_density_hint(len(results_df), len(stocks_df))
                        getattr(st, level)(msg)

                        save_scan_cache(
                            "v8_scan",
                            cache_params,
                            get_db_last_trade_date(permanent_db_path),
                            results_df,
                            {"candidate_count": len(stocks_df), "filter_failed": filter_failed},
                        )
                        st.session_state["v8_scan_results_tab1"] = results_df
                        set_stock_pool_candidate("v8", cache_params, "综合评分", results_df)

                        st.markdown("---")
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            avg_score = results_df["综合评分"].astype(float).mean()
                            st.metric("平均评分", f"{avg_score:.1f}分")
                        with col2:
                            max_score = results_df["综合评分"].astype(float).max()
                            st.metric("最高评分", f"{max_score:.1f}分")
                        with col3:
                            grade_high = sum(1 for g in results_df["评级"] if str(g) in ("S", "A", "A+"))
                            st.metric("高评级", f"{grade_high}只")
                        with col4:
                            if enable_kelly:
                                kelly_series = results_df["凯利仓位"] if "凯利仓位" in results_df else pd.Series(dtype=float)
                                numeric_kelly = pd.to_numeric(kelly_series.str.rstrip("%"), errors="coerce").dropna()
                                st.metric("平均凯利仓位", f"{numeric_kelly.mean():.1f}%" if len(numeric_kelly) > 0 else "-")
                            else:
                                st.metric("平均凯利仓位", "-")

                        st.markdown("---")
                        st.subheader("结果列表（v8.0·18维度）")
                        view_mode = st.radio("显示模式", ["完整信息", "核心指标", "简洁模式"], horizontal=True, key="view_mode_v8_tab1")
                        if view_mode == "完整信息":
                            display_cols = [
                                "股票代码", "股票名称", "行业", "流通市值", "综合评分", "评级", "资金加分", "星级",
                                "建议仓位", "预期胜率", "盈亏比", "凯利仓位", "最新价格", "ATR值", "ATR止损",
                                "ATR止盈", "ATR移动止损", "止损幅度%", "止盈幅度%", "筛选理由",
                            ]
                        elif view_mode == "核心指标":
                            display_cols = [
                                "股票代码", "股票名称", "行业", "综合评分", "评级", "资金加分", "星级",
                                "建议仓位", "预期胜率", "凯利仓位", "最新价格", "ATR值", "ATR止损", "ATR止盈", "ATR移动止损",
                            ]
                        else:
                            display_cols = ["股票代码", "股票名称", "行业", "综合评分", "资金加分", "评级", "星级", "建议仓位", "最新价格", "筛选理由"]
                        display_cols = append_reason_col(display_cols, results_df)
                        display_df = standardize_result_df(results_df[display_cols], score_col="综合评分")
                        st.dataframe(
                            display_df,
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                "综合评分": st.column_config.NumberColumn("综合评分", help="v8.0评分（18维度·100分制）", format="%.1f分"),
                                "评级": st.column_config.TextColumn("评级", help="评级：S/A/B/C（优秀/良好/中性/谨慎)", width="medium"),
                                "星级": st.column_config.TextColumn("星级", help="星级用于仓位建议", width="small"),
                                "建议仓位": st.column_config.TextColumn("建议仓位", help="根据星级/评分建议的单票仓位", width="small"),
                                "凯利仓位": st.column_config.TextColumn("凯利仓位", help="凯利公式计算的最优仓位比例", width="small"),
                                "筛选理由": st.column_config.TextColumn("筛选理由", help="v8.0智能分析推荐原因", width="large"),
                            },
                        )
                        st.markdown("---")
                        export_df = results_df.drop(columns=["原始数据"], errors="ignore")
                        csv = df_to_csv_bytes(export_df)
                        st.download_button(
                            label=" 导出结果（CSV）",
                            data=csv,
                            file_name=f"核心策略_V8_扫描结果_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv; charset=utf-8",
                        )
                    else:
                        min_thr, _ = score_threshold_v8 if isinstance(score_threshold_v8, tuple) else (score_threshold_v8, 100)
                        st.warning(
                            f"未找到≥{min_thr}分的股票\n\n**说明：**\n"
                            "v8.0使用18维度评分+三级市场过滤，标准较严格。\n\n"
                            "**建议：**\n1. 降低评分阈值到60-65分\n2. 放宽Top比例或关闭一致性过滤\n3. 当前可能不是最佳入场时机"
                        )
                        if filter_counts:
                            st.info(
                                f"过滤分布：原始{filter_counts.get('raw', 0)} → "
                                f"阈值后{filter_counts.get('after_threshold', 0)} → "
                                f"Top后{filter_counts.get('after_top', 0)} → "
                                f"一致性后{filter_counts.get('after_consistency', 0)}"
                            )
            except Exception as e:
                st.error(f"扫描失败: {e}")
                st.code(traceback.format_exc())

    notice_v8 = str(st.session_state.pop("v8_front_scan_notice", "") or "")
    if notice_v8:
        st.success(notice_v8)
    if "v8_scan_results_tab1" in st.session_state:
        render_front_scan_summary("v8", "v8.0进阶版")
        st.markdown("---")
        st.markdown("###  上次扫描结果")
        results_df = st.session_state["v8_scan_results_tab1"]
        display_df = results_df.drop(columns=["原始数据"], errors="ignore")
        st.dataframe(display_df, use_container_width=True, hide_index=True)
    render_async_scan_status("v8_async_task_id", "v8.0进阶版", "综合评分")
