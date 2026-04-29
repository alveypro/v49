from __future__ import annotations

from datetime import datetime, timedelta
import traceback
from typing import Any, Callable, Dict, Optional, Tuple

import pandas as pd
import streamlit as st

from openclaw.runtime.scan_ui import render_cached_scan_results


def render_v9_strategy_page(
    *,
    vp_analyzer: Any,
    permanent_db_path: str,
    bulk_history_limit: int,
    strict_full_market_mode: bool,
    load_evolve_params: Callable[[str], Dict[str, Any]],
    load_strategy_center_scan_defaults: Callable[[str], Tuple[Dict[str, Any], str]],
    has_role: Callable[[str], bool],
    guard_action: Callable[[str, str, str, str], bool],
    append_action_audit: Callable[..., None],
    detect_heavy_background_job: Callable[[], Tuple[bool, str]],
    start_async_scan_task: Callable[..., Tuple[bool, str, str]],
    mark_scan_submitted: Callable[[str, Dict[str, Any]], None],
    sync_scan_task_with_params: Callable[[str, Dict[str, Any], str], None],
    render_scan_param_hint: Callable[[str], None],
    get_db_last_trade_date: Callable[[str], str],
    load_scan_cache: Callable[[str, Dict[str, Any], str], Tuple[Optional[pd.DataFrame], Dict[str, Any]]],
    save_scan_cache: Callable[[str, Dict[str, Any], str, pd.DataFrame, Dict[str, Any]], None],
    connect_permanent_db: Callable[[], Any],
    load_candidate_stocks: Callable[..., pd.DataFrame],
    load_external_bonus_maps: Callable[[Any], Any],
    load_history_range_bulk: Callable[..., Dict[str, pd.DataFrame]],
    apply_multi_period_filter: Callable[..., pd.DataFrame],
    add_reason_summary: Callable[..., pd.DataFrame],
    render_result_overview: Callable[..., None],
    signal_density_hint: Callable[..., Any],
    set_stock_pool_candidate: Callable[[str, Dict[str, Any], str, pd.DataFrame], None],
    standardize_result_df: Callable[..., pd.DataFrame],
    df_to_csv_bytes: Callable[[pd.DataFrame], bytes],
    update_scan_progress_ui: Callable[..., None],
    render_async_scan_status: Callable[[str, str, str], Any],
) -> None:
    exp_v9 = st.expander("v9.0 策略说明", expanded=False)
    exp_v9.markdown(
        """
        <div style='background: linear-gradient(135deg, #0f2027 0%, #203a43 50%, #2c5364 100%);
                    padding: 35px 30px; border-radius: 15px; color: white; margin-bottom: 25px;'>
            <h1 style='margin:0; color: white; font-size: 2.2em; font-weight: 700; text-align: center;'>
                 v9.0 中线均衡版 - 资金流·动量·趋势·波动·板块强度
            </h1>
            <p style='margin: 12px 0 0 0; font-size: 1.1em; text-align: center; opacity: 0.9;'>
                中线周期 2-6 周 · 平衡风格 · 适合稳健进取型
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    evolve_v9 = load_evolve_params("v9_best.json")
    center_v9_params, center_v9_src = load_strategy_center_scan_defaults("v9")
    evo_params_v9 = evolve_v9.get("params", {}) if isinstance(evolve_v9, dict) else {}
    if evo_params_v9:
        exp_v9.success(f"已应用自动进化参数（v9.0，{evolve_v9.get('run_at', 'unknown')}）")
        exp_v9.caption(
            f"推荐阈值: {evo_params_v9.get('score_threshold')} | 持仓: {evo_params_v9.get('holding_days')} | "
            f"窗口: {evo_params_v9.get('lookback_days')} | 最低成交额(亿): {evo_params_v9.get('min_turnover')}"
        )
    if center_v9_src == "strategy_center":
        exp_v9.info(
            f"策略中心默认参数已接管：阈值 {center_v9_params.get('score_threshold')} | "
            f"持仓 {center_v9_params.get('holding_days')} 天"
        )

    def load_history_full(ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        try:
            from data.history import load_history_full as load_history_full_v2  # type: ignore

            return load_history_full_v2(
                db_path=permanent_db_path,
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                columns="trade_date, close_price, vol, amount, pct_chg, turnover_rate",
            )
        except Exception:
            return pd.DataFrame()

    exp_v9.info(
        """
        **v9.0 评分结构：**
        - 资金流：成交额方向性
        - 动量：20/60 日趋势一致
        - 趋势：均线结构向上
        - 波动：中等波动区间
        - 板块：行业动量加权
        """
    )

    market_env = "oscillation"
    try:
        market_env = vp_analyzer.get_market_environment()
    except Exception:
        market_env = "oscillation"
    env_map = {"bull": " 牛市", "bear": " 熊市", "oscillation": " 震荡"}
    env_label = env_map.get(market_env, " 震荡")
    st.caption(f"当前市场环境：{env_label}")

    evo_thr_v9 = int(evo_params_v9.get("score_threshold", 65))
    evo_hold_v9 = int(evo_params_v9.get("holding_days", 20))
    evo_lookback_v9 = int(evo_params_v9.get("lookback_days", 120))
    evo_min_turnover_v9 = float(evo_params_v9.get("min_turnover", 5.0))
    center_thr_v9 = center_v9_params.get("score_threshold")
    center_hold_v9 = center_v9_params.get("holding_days")
    if isinstance(center_thr_v9, (int, float)):
        evo_thr_v9 = int(round(center_thr_v9))
    if isinstance(center_hold_v9, (int, float)):
        evo_hold_v9 = int(round(center_hold_v9))

    evo_thr_v9 = max(50, min(90, evo_thr_v9))
    evo_hold_v9 = max(3, min(30, evo_hold_v9))
    evo_lookback_v9 = max(80, min(200, evo_lookback_v9))
    evo_min_turnover_v9 = max(1.0, min(50.0, evo_min_turnover_v9))
    sync_thr_v9 = evo_thr_v9
    sync_hold_v9 = evo_hold_v9
    v9_locked_by_unified = bool(st.session_state.get("v9_params_locked_by_unified", False))
    if v9_locked_by_unified:
        st.caption(
            "当前参数来源：统一口径（一键同步）"
            f"（阈值 {int(st.session_state.get('score_threshold_v9', sync_thr_v9))}，"
            f"持仓 {int(st.session_state.get('holding_days_v9', sync_hold_v9))} 天）"
        )
    elif center_v9_src == "strategy_center":
        st.caption(f"当前参数来源：策略中心（阈值 {sync_thr_v9}，持仓 {sync_hold_v9} 天）")
        if not st.session_state.get("v9_center_defaults_initialized", False):
            st.session_state["score_threshold_v9"] = sync_thr_v9
            st.session_state["holding_days_v9"] = sync_hold_v9
            st.session_state["v9_center_defaults_initialized"] = True
        if st.button("同步策略中心默认参数", key="sync_v9_center_defaults", disabled=not has_role("admin")):
            if not guard_action("admin", "sync_v9_center_defaults", "v9", "sync_strategy_center_defaults"):
                st.stop()
            st.session_state["score_threshold_v9"] = sync_thr_v9
            st.session_state["holding_days_v9"] = sync_hold_v9
            st.session_state["v9_params_locked_by_unified"] = False
            append_action_audit("sync_v9_center_defaults", True, target="v9", detail=f"thr={sync_thr_v9},hold={sync_hold_v9}")
            st.rerun()
    else:
        if "score_threshold_v9" not in st.session_state:
            st.session_state["score_threshold_v9"] = evo_thr_v9
        if "holding_days_v9" not in st.session_state:
            st.session_state["holding_days_v9"] = evo_hold_v9

    if "lookback_days_v9" not in st.session_state:
        st.session_state["lookback_days_v9"] = int(evo_lookback_v9)
    st.session_state.setdefault("min_turnover_v9", float(evo_min_turnover_v9))
    st.session_state.setdefault("top_percent_v9", 1)
    if "scan_all_v9" not in st.session_state:
        st.session_state["scan_all_v9"] = False
    st.session_state.setdefault("cap_min_v9", 100.0)
    st.session_state.setdefault("cap_max_v9", 15000.0)
    if "candidate_count_v9" not in st.session_state:
        st.session_state["candidate_count_v9"] = 800

    col1, col2, col3 = st.columns(3)
    with col1:
        score_threshold_v9 = st.slider("评分阈值（v9.0）", 50, 90, step=5, key="score_threshold_v9")
    with col2:
        holding_days_v9 = st.slider("建议持仓天数", 3, 30, step=1, key="holding_days_v9")
    with col3:
        lookback_days_v9 = st.slider("评分窗口（天）", 80, 200, int(st.session_state.get("lookback_days_v9", evo_lookback_v9)), 10)

    col4, col5, col6 = st.columns(3)
    with col4:
        min_turnover_v9 = st.slider("最低成交额（亿）", 1.0, 50.0, step=1.0, key="min_turnover_v9")
    with col5:
        candidate_count_v9 = st.slider("候选数量（按市值）", 200, 3000, int(st.session_state.get("candidate_count_v9", 800)), 100)
    with col6:
        scan_all_v9 = st.checkbox("全市场扫描", value=bool(st.session_state.get("scan_all_v9", False)))

    col_mode1, col_mode2, col_mode3 = st.columns(3)
    with col_mode1:
        select_mode_v9 = st.selectbox("选股模式", ["分位数筛选(Top%)", "阈值筛选"], index=0, key="select_mode_v9")
    with col_mode2:
        top_percent_v9 = st.slider("Top百分比", 1, 10, step=1, key="top_percent_v9")
    with col_mode3:
        weak_market_filter_v9 = st.checkbox("弱市空仓保护", value=True, key="weak_market_filter_v9")

    col_mode4, col_mode5 = st.columns(2)
    with col_mode4:
        enable_consistency_v9 = st.checkbox("启用多周期一致性过滤", value=True, key="v9_consistency")
    with col_mode5:
        min_align_v9 = st.slider("一致性要求（2/3或3/3）", 2, 3, 2, 1, key="v9_consistency_min")
    if "v9_cache" not in st.session_state:
        st.session_state["v9_cache"] = True
    st.session_state["v9_async_scan"] = True
    use_cache_v9 = st.checkbox("优先使用离线缓存结果", value=True, key="v9_cache")
    async_scan_v9 = st.checkbox("后台运行（生产默认，已强制开启）", value=True, key="v9_async_scan", disabled=True)
    st.caption("生产策略扫描已强制后台化：任务进入后台队列，页面刷新或短暂断线后仍可继续查看结果。")

    col7, col8 = st.columns(2)
    with col7:
        cap_min_v9 = st.number_input("最小市值（亿元）", min_value=0, max_value=5000, step=10, key="cap_min_v9")
    with col8:
        cap_max_v9 = st.number_input("最大市值（亿元）", min_value=0, max_value=50000, step=50, key="cap_max_v9")
    if strict_full_market_mode:
        scan_all_v9 = True
        cap_min_v9 = 0.0
        cap_max_v9 = 0.0
        candidate_count_v9 = 3000
        st.caption("全市场严格口径：v9 市值=0~0，候选数=3000。")

    v9_live_params = {
        "score_threshold": int(score_threshold_v9),
        "top_percent": int(top_percent_v9),
        "select_mode": select_mode_v9,
        "scan_all": bool(scan_all_v9),
        "cap_min": float(cap_min_v9),
        "cap_max": float(cap_max_v9),
        "enable_consistency": bool(enable_consistency_v9),
        "min_align": int(min_align_v9),
        "holding_days": int(holding_days_v9),
        "lookback_days": int(lookback_days_v9),
        "min_turnover": float(min_turnover_v9),
        "candidate_count": int(candidate_count_v9),
    }
    st.session_state["lookback_days_v9"] = int(lookback_days_v9)
    st.session_state["scan_all_v9"] = bool(scan_all_v9)
    st.session_state["candidate_count_v9"] = int(candidate_count_v9)
    sync_scan_task_with_params("v9_async_task_id", v9_live_params, "v9.0策略")
    render_scan_param_hint("v9_async_task_id")

    if st.button("开始扫描（v9.0中线均衡版）", type="primary", use_container_width=True, key="scan_v9", disabled=not has_role("operator")):
        if not guard_action("operator", "scan_v9", "v9", "start_scan_v9"):
            st.stop()
        append_action_audit("scan_v9", True, target="v9", detail="scan_requested")
        heavy_running, heavy_reason = detect_heavy_background_job()
        if heavy_running:
            st.warning(f"{heavy_reason}，系统将继续执行扫描（可能稍慢）。")
        if async_scan_v9:
            cache_params = dict(v9_live_params)
            ok, msg, run_id = start_async_scan_task("v9", cache_params, score_col="综合评分")
            if ok:
                st.session_state["v9_async_task_id"] = run_id
                mark_scan_submitted("v9_async_task_id", cache_params)
                st.success(msg)
            else:
                st.warning(msg)
            st.rerun()

        with st.spinner("v9.0 中线均衡版扫描中..."):
            try:
                cache_params = dict(v9_live_params)
                db_last = get_db_last_trade_date(permanent_db_path)
                if use_cache_v9:
                    cached_df, cached_meta = load_scan_cache("v9_scan", cache_params, db_last)
                    if cached_df is not None and not cached_df.empty:
                        created_at = cached_meta.get("created_at", "未知")
                        st.info(f"已加载离线缓存（数据日期 {db_last}，生成时间 {created_at}）")
                        render_cached_scan_results(
                            title="扫描结果（v9.0中线均衡版）",
                            results_df=cached_df,
                            score_col="综合评分",
                            candidate_count=int(cached_meta.get("candidate_count", len(cached_df))),
                            filter_failed=int(cached_meta.get("filter_failed", 0)),
                            select_mode=select_mode_v9,
                            threshold=score_threshold_v9,
                            top_percent=top_percent_v9,
                            render_result_overview=render_result_overview,
                            signal_density_hint=signal_density_hint,
                        )
                        st.session_state["v9_scan_results_tab1"] = cached_df
                        set_stock_pool_candidate("v9", cache_params, "综合评分", cached_df)
                        st.stop()

                if weak_market_filter_v9 and market_env == "bear":
                    st.warning(f"当前市场环境：{env_label}，建议空仓观望。")
                    if not st.checkbox("我理解风险，仍要继续扫描", key="force_scan_v9"):
                        st.stop()

                conn = connect_permanent_db()
                stocks_df = load_candidate_stocks(
                    conn,
                    scan_all=bool(scan_all_v9),
                    cap_min_yi=cap_min_v9,
                    cap_max_yi=cap_max_v9,
                    require_industry=True,
                )
                if stocks_df.empty:
                    st.error("未找到符合条件的股票")
                    conn.close()
                    st.stop()

                stocks_df = stocks_df.head(candidate_count_v9)
                try:
                    bonus_global, bonus_stock_map, top_list_set, top_inst_set, bonus_industry_map = load_external_bonus_maps(conn)
                except Exception:
                    bonus_global, bonus_stock_map, top_list_set, top_inst_set, bonus_industry_map = (0.0, {}, set(), set(), {})

                industry_scores: Dict[str, float] = {}
                ind_vals: Dict[str, list[float]] = {}
                end_date = datetime.now().strftime("%Y%m%d")
                start_date = (datetime.now() - timedelta(days=lookback_days_v9 + 30)).strftime("%Y%m%d")

                history_cache = {}
                if len(stocks_df) <= bulk_history_limit:
                    history_cache = load_history_range_bulk(
                        conn,
                        stocks_df["ts_code"].tolist(),
                        start_date,
                        end_date,
                        "ts_code, trade_date, close_price, vol, amount, pct_chg, turnover_rate",
                    )

                for _, row in stocks_df.iterrows():
                    ts_code = row["ts_code"]
                    hist = history_cache.get(ts_code)
                    if hist is None:
                        hist = load_history_full(ts_code, start_date, end_date)
                    if hist is None or len(hist) < 21:
                        continue
                    close = pd.to_numeric(hist["close_price"], errors="coerce").ffill()
                    r20 = (close.iloc[-1] / close.iloc[-21] - 1.0) * 100 if len(close) > 21 else 0.0
                    ind_vals.setdefault(row["industry"], []).append(r20)
                for ind, vals in ind_vals.items():
                    if vals:
                        industry_scores[ind] = float(sum(vals) / len(vals))

                results = []
                progress_bar = st.progress(0)
                status_text = st.empty()

                for idx, row in stocks_df.iterrows():
                    ts_code = row["ts_code"]
                    update_scan_progress_ui(progress_bar, status_text, idx, len(stocks_df), f"正在评分: {row['name']} ({idx+1}/{len(stocks_df)})")
                    hist = history_cache.get(ts_code)
                    if hist is None:
                        hist = load_history_full(ts_code, start_date, end_date)
                    if hist is None or len(hist) < 80:
                        continue

                    avg_amount = pd.to_numeric(hist["amount"], errors="coerce").tail(20).mean()
                    avg_amount_yi = avg_amount / 1e5
                    if avg_amount_yi < min_turnover_v9:
                        continue

                    ind_strength = industry_scores.get(row["industry"], 0.0)
                    score_info = vp_analyzer._calc_v9_score_from_hist(hist, industry_strength=ind_strength)
                    base_score = float(score_info["score"])

                    extra = float(bonus_global)
                    mf_net = bonus_stock_map.get(ts_code, 0.0)
                    if mf_net > 1e8:
                        extra += 2.0
                    elif mf_net > 0:
                        extra += 1.0
                    elif mf_net < 0:
                        extra -= 1.0
                    if ts_code in top_list_set:
                        extra += 1.5
                    if ts_code in top_inst_set:
                        extra += 1.0
                    ind_flow = bonus_industry_map.get(row["industry"], 0.0)
                    if ind_flow > 0:
                        extra += 1.0
                    elif ind_flow < 0:
                        extra -= 1.0

                    score = base_score + extra
                    latest_col = "close_price" if "close_price" in hist.columns else ("close" if "close" in hist.columns else "")
                    latest_price_val = float(pd.to_numeric(hist[latest_col], errors="coerce").iloc[-1]) if latest_col else 0.0
                    row_item = {
                        "股票代码": ts_code,
                        "股票名称": row["name"],
                        "行业": row["industry"],
                        "流通市值": f"{row['circ_mv']/10000:.1f}亿",
                        "综合评分": f"{score:.1f}",
                        "最新价格": f"{latest_price_val:.2f}元" if latest_price_val > 0 else "-",
                        "资金流": score_info["details"].get("fund_score"),
                        "动量": score_info["details"].get("momentum_score"),
                        "趋势": score_info["details"].get("trend_score"),
                        "波动": score_info["details"].get("volatility_score"),
                        "板块强度": score_info["details"].get("sector_score"),
                        "资金加分": f"{extra:.1f}",
                        "建议持仓": f"{holding_days_v9}天",
                    }
                    if select_mode_v9 == "阈值筛选":
                        if score >= score_threshold_v9:
                            results.append(row_item)
                    else:
                        results.append(row_item)

                progress_bar.empty()
                status_text.empty()
                conn.close()

                if results:
                    results_df = pd.DataFrame(results)
                    if select_mode_v9 != "阈值筛选":
                        results_df["score_val"] = pd.to_numeric(results_df["综合评分"], errors="coerce")
                        results_df = results_df.sort_values("score_val", ascending=False)
                        keep_n = max(1, int(len(results_df) * top_percent_v9 / 100))
                        results_df = results_df.head(keep_n).drop(columns=["score_val"])
                    if enable_consistency_v9 and not results_df.empty:
                        results_df = apply_multi_period_filter(results_df, permanent_db_path, min_align=min_align_v9)
                    results_df = add_reason_summary(results_df, score_col="综合评分")

                    save_scan_cache("v9_scan", cache_params, db_last, results_df, {"candidate_count": len(stocks_df), "filter_failed": 0})
                    st.session_state["v9_scan_results_tab1"] = results_df
                    set_stock_pool_candidate("v9", cache_params, "综合评分", results_df)
                    if select_mode_v9 == "阈值筛选":
                        st.success(f"找到 {len(results_df)} 只符合条件的股票（≥{score_threshold_v9}分）")
                    else:
                        st.success(f"选出 Top {top_percent_v9}%（{len(results_df)} 只）")
                    render_result_overview(results_df, score_col="综合评分", title="扫描结果概览")
                    msg, level = signal_density_hint(len(results_df), len(stocks_df))
                    getattr(st, level)(msg)
                    results_df = standardize_result_df(results_df, score_col="综合评分")
                    st.dataframe(results_df, use_container_width=True, hide_index=True)
                    st.download_button(
                        " 导出结果（CSV）",
                        data=df_to_csv_bytes(results_df),
                        file_name=f"核心策略_V9_中线均衡_扫描结果_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv; charset=utf-8",
                    )
                else:
                    st.warning("未找到符合条件的股票，请适当降低阈值或放宽筛选条件")
            except Exception as e:
                st.error(f"扫描失败: {e}")
                st.code(traceback.format_exc())

    if "v9_scan_results_tab1" in st.session_state:
        st.markdown("---")
        st.markdown("###  上次扫描结果")
        results_df = st.session_state["v9_scan_results_tab1"]
        st.dataframe(results_df, use_container_width=True, hide_index=True)
    render_async_scan_status("v9_async_task_id", "v9.0中线均衡版", "综合评分")
