from __future__ import annotations

from datetime import datetime, timedelta
import traceback
from typing import Any, Callable, Dict, Tuple

import numpy as np
import pandas as pd
import streamlit as st

from openclaw.runtime.scan_ui import render_cached_scan_results


def render_combo_strategy_page(
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
    run_front_scan_via_offline_pipeline: Callable[..., Tuple[pd.DataFrame | None, Dict[str, Any]]],
    mark_front_scan_completed: Callable[..., None],
    get_db_last_trade_date: Callable[[str], str],
    load_scan_cache: Callable[[str, Dict[str, Any], str], Tuple[pd.DataFrame | None, Dict[str, Any]]],
    save_scan_cache: Callable[[str, Dict[str, Any], str, pd.DataFrame, Dict[str, Any]], None],
    connect_permanent_db: Callable[[], Any],
    load_candidate_stocks: Callable[..., pd.DataFrame],
    load_external_bonus_maps: Callable[[Any], Any],
    load_history_range_bulk: Callable[..., Dict[str, pd.DataFrame]],
    normalize_stock_df: Callable[[pd.DataFrame], pd.DataFrame],
    sync_scan_task_with_params: Callable[[str, Dict[str, Any], str], None],
    render_scan_param_hint: Callable[[str], None],
    render_front_scan_summary: Callable[[str, str], None],
    apply_multi_period_filter: Callable[..., pd.DataFrame],
    add_reason_summary: Callable[..., pd.DataFrame],
    render_result_overview: Callable[..., None],
    signal_density_hint: Callable[..., Any],
    set_stock_pool_candidate: Callable[[str, Dict[str, Any], str, pd.DataFrame], None],
    standardize_result_df: Callable[..., pd.DataFrame],
    df_to_csv_bytes: Callable[[pd.DataFrame], bytes],
    update_scan_progress_ui: Callable[..., None],
    render_async_scan_status: Callable[[str, str, str], Any],
    calc_external_bonus: Callable[..., float],
) -> None:
    center_combo_params, center_combo_src = load_strategy_center_scan_defaults("combo")
    exp_combo = st.expander("组合策略说明", expanded=False)
    exp_combo.markdown(
        """
        <div style='background: linear-gradient(135deg, #1f4037 0%, #99f2c8 100%);
                    padding: 35px 30px; border-radius: 15px; color: #0b1f17; margin-bottom: 25px;'>
            <h1 style='margin:0; color: #0b1f17; font-size: 2.1em; font-weight: 700; text-align: center;'>
                 组合策略共识评分（v4/v5/v7/v8/v9）
            </h1>
            <p style='margin: 12px 0 0 0; font-size: 1.05em; text-align: center; opacity: 0.9;'>
                多策略协同共识 · 过滤噪音 · 提升稳定性 · 强调胜率与一致性
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    exp_combo.info(
        """
        **共识逻辑：**
        - v4/v5/v7/v8/v9 五大策略同时评分
        - 评分按权重融合为“共识分”
        - 满足最小一致数量（agree_count）后进入候选
        - 叠加资金加分（北向/龙虎榜/机构/行业资金）
        """
    )

    evolve_combo = load_evolve_params("combo_best.json")
    evo_params_combo = evolve_combo.get("params", {}) if isinstance(evolve_combo, dict) else {}
    if evo_params_combo:
        exp_combo.success(f"已应用自动进化参数（COMBO，{evolve_combo.get('run_at', 'unknown')}）")
    center_thr_combo = center_combo_params.get("score_threshold")
    combo_threshold_default = int(evo_params_combo.get("combo_threshold", 68))
    if isinstance(center_thr_combo, (int, float)):
        combo_threshold_default = max(50, min(90, int(round(center_thr_combo))))
    if center_combo_src == "strategy_center":
        st.caption(f"当前参数来源：策略中心（组合阈值 {combo_threshold_default}）")

    col_a, col_b, col_c = st.columns(3)
    with col_a:
        candidate_count = st.slider("候选数量（按市值）", 200, 3000, int(st.session_state.get("combo_candidate_count", evo_params_combo.get("candidate_count", 800))), 100, key="combo_candidate_count")
    with col_b:
        min_turnover = st.slider("最低成交额（亿）", 1.0, 50.0, float(evo_params_combo.get("min_turnover", 5.0)), 1.0, key="combo_min_turnover")
    with col_c:
        min_agree = st.slider("最小一致数量（策略数）", 2, 5, int(evo_params_combo.get("min_agree", 3)), 1, key="combo_min_agree")

    col_d, col_e, col_f = st.columns(3)
    with col_d:
        cap_min_combo = st.number_input("最小市值（亿元）", min_value=0, max_value=5000, value=100, step=10, key="combo_cap_min")
    with col_e:
        cap_max_combo = st.number_input("最大市值（亿元）", min_value=0, max_value=50000, value=15000, step=50, key="combo_cap_max")
    with col_f:
        modes_combo = ["双重筛选(阈值+Top%)", "分位数筛选(Top%)", "阈值筛选"]
        if "combo_select_mode" not in st.session_state or st.session_state.get("combo_select_mode") not in modes_combo:
            st.session_state["combo_select_mode"] = str(evo_params_combo.get("select_mode", "分位数筛选(Top%)"))
        select_mode_combo = st.selectbox("筛选模式", modes_combo, key="combo_select_mode")
    if strict_full_market_mode:
        cap_min_combo = 0.0
        cap_max_combo = 0.0
        candidate_count = 3000
        st.caption("全市场严格口径：组合策略市值=0~0，候选数=3000。")

    col_g, col_h, col_i = st.columns(3)
    with col_g:
        combo_threshold = st.slider("共识阈值", 50, 90, combo_threshold_default, 5, key="combo_threshold")
    with col_h:
        top_percent_combo = st.slider("Top百分比", 1, 10, int(evo_params_combo.get("top_percent", 1)), 1, key="combo_top_percent")
    with col_i:
        lookback_days_combo = st.slider("评分窗口（天）", 80, 200, int(st.session_state.get("combo_lookback_days", evo_params_combo.get("lookback_days", 90))), 10, key="combo_lookback_days")

    col_j, col_k, col_l = st.columns(3)
    with col_j:
        disagree_std_weight = st.slider("分歧惩罚强度", 0.0, 1.5, 0.35, 0.05, key="combo_disagree_std")
    with col_k:
        disagree_count_weight = st.slider("分歧惩罚/项", 0.0, 5.0, 1.0, 0.5, key="combo_disagree_count")
    with col_l:
        market_adjust_strength = st.slider("市场状态调节强度", 0.0, 1.0, 0.5, 0.05, key="combo_market_strength")

    col_m, col_n = st.columns(2)
    with col_m:
        enable_consistency_combo = st.checkbox("启用多周期一致性过滤", value=bool(evo_params_combo.get("enable_consistency", True)), key="combo_consistency")
    with col_n:
        min_align_combo = st.slider("一致性要求（2/3或3/3）", 2, 3, int(evo_params_combo.get("min_align", 2)), 1, key="combo_consistency_min")
    if "combo_cache" not in st.session_state:
        st.session_state["combo_cache"] = True
    st.session_state["combo_lightweight_mode"] = True
    st.session_state["combo_async_scan"] = True
    use_cache_combo = st.checkbox("优先使用离线缓存结果", value=True, key="combo_cache")
    async_scan_combo = st.checkbox("后台运行（生产默认，已强制开启）", value=True, key="combo_async_scan", disabled=True)
    st.caption("生产策略扫描已强制后台化：任务进入后台队列，页面刷新或短暂断线后仍可继续查看结果。")

    st.markdown("---")
    st.subheader("权重设置（总和自动归一化）")
    market_env_combo = "oscillation"
    try:
        market_env_combo = vp_analyzer.get_market_environment()
    except Exception:
        market_env_combo = "oscillation"
    env_label_combo = "震荡市" if market_env_combo == "oscillation" else ("牛市" if market_env_combo == "bull" else "弱市")
    auto_weights = st.checkbox("根据市场环境自动调整权重", value=bool(evo_params_combo.get("auto_weights", True)), key="combo_auto_weights")
    st.caption(f"当前市场环境判断：{env_label_combo}")

    weight_presets = {
        "bull": {"v4": 0.10, "v5": 0.20, "v7": 0.30, "v8": 0.30, "v9": 0.10},
        "oscillation": {"v4": 0.15, "v5": 0.15, "v7": 0.30, "v8": 0.25, "v9": 0.15},
        "bear": {"v4": 0.25, "v5": 0.15, "v7": 0.20, "v8": 0.15, "v9": 0.25},
    }
    preset = weight_presets.get(market_env_combo, weight_presets["oscillation"])
    w1, w2, w3, w4, w5 = st.columns(5)
    with w1:
        w_v4 = st.slider("v4权重", 0.0, 1.0, float(evo_params_combo.get("w_v4", preset["v4"])), 0.05, key="w_v4", disabled=auto_weights)
    with w2:
        w_v5 = st.slider("v5权重", 0.0, 1.0, float(evo_params_combo.get("w_v5", preset["v5"])), 0.05, key="w_v5", disabled=auto_weights)
    with w3:
        w_v7 = st.slider("v7权重", 0.0, 1.0, float(evo_params_combo.get("w_v7", preset["v7"])), 0.05, key="w_v7", disabled=auto_weights)
    with w4:
        w_v8 = st.slider("v8权重", 0.0, 1.0, float(evo_params_combo.get("w_v8", preset["v8"])), 0.05, key="w_v8", disabled=auto_weights)
    with w5:
        w_v9 = st.slider("v9权重", 0.0, 1.0, float(evo_params_combo.get("w_v9", preset["v9"])), 0.05, key="w_v9", disabled=auto_weights)
    if auto_weights:
        w_v4, w_v5, w_v7, w_v8, w_v9 = preset["v4"], preset["v5"], preset["v7"], preset["v8"], preset["v9"]
        st.info(f"已应用动态权重（{env_label_combo}）：v4={w_v4} v5={w_v5} v7={w_v7} v8={w_v8} v9={w_v9}")

    st.markdown("---")
    st.subheader("各策略阈值（用于一致性判断）")
    t1, t2, t3, t4, t5 = st.columns(5)
    with t1:
        thr_v4 = st.slider("v4阈值", 50, 90, int(evo_params_combo.get("thr_v4", 60)), 5, key="thr_v4")
    with t2:
        thr_v5 = st.slider("v5阈值", 50, 90, int(evo_params_combo.get("thr_v5", 60)), 5, key="thr_v5")
    with t3:
        thr_v7 = st.slider("v7阈值", 50, 90, int(evo_params_combo.get("thr_v7", 65)), 5, key="thr_v7")
    with t4:
        thr_v8 = st.slider("v8阈值", 50, 90, int(evo_params_combo.get("thr_v8", 65)), 5, key="thr_v8")
    with t5:
        thr_v9 = st.slider("v9阈值", 50, 90, int(evo_params_combo.get("thr_v9", 60)), 5, key="thr_v9")

    def load_history_full_combo(ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        try:
            from data.history import load_history_full as load_history_full_v2  # type: ignore
            return load_history_full_v2(
                db_path=permanent_db_path,
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                columns="trade_date, close_price, high_price, low_price, vol, amount, pct_chg, turnover_rate",
                normalize_fn=normalize_stock_df,
            )
        except Exception:
            return pd.DataFrame()

    combo_live_params = {
        "candidate_count": int(candidate_count),
        "min_turnover": float(min_turnover),
        "min_agree": int(min_agree),
        "cap_min": float(cap_min_combo),
        "cap_max": float(cap_max_combo),
        "select_mode": select_mode_combo,
        "combo_threshold": float(combo_threshold),
        "top_percent": int(top_percent_combo),
        "lookback_days": int(lookback_days_combo),
        "disagree_std_weight": float(disagree_std_weight),
        "disagree_count_weight": float(disagree_count_weight),
        "market_adjust_strength": float(market_adjust_strength),
        "enable_consistency": bool(enable_consistency_combo),
        "min_align": int(min_align_combo),
        "auto_weights": bool(auto_weights),
        "lightweight_mode": bool(st.session_state.get("combo_lightweight_mode", True)),
        "w_v4": float(w_v4),
        "w_v5": float(w_v5),
        "w_v7": float(w_v7),
        "w_v8": float(w_v8),
        "w_v9": float(w_v9),
        "thr_v4": float(thr_v4),
        "thr_v5": float(thr_v5),
        "thr_v7": float(thr_v7),
        "thr_v8": float(thr_v8),
        "thr_v9": float(thr_v9),
    }
    sync_scan_task_with_params("combo_async_task_id", combo_live_params, "组合策略")
    render_scan_param_hint("combo_async_task_id")
    render_front_scan_summary("combo", "组合共识策略")
    if "combo_scan_results" in st.session_state:
        st.markdown("### 最近一次组合结果")
        recent_combo_df = st.session_state["combo_scan_results"]
        st.dataframe(standardize_result_df(recent_combo_df.drop(columns=["原始数据"], errors="ignore"), score_col="共识评分"), use_container_width=True, hide_index=True)

    if st.button("开始扫描（组合共识）", type="primary", use_container_width=True, key="scan_combo", disabled=not has_role("operator")):
        if not guard_action("operator", "scan_combo", "combo", "start_scan_combo"):
            st.stop()
        append_action_audit("scan_combo", True, target="combo", detail="scan_requested")
        heavy_running, heavy_reason = detect_heavy_background_job()
        if heavy_running:
            st.warning(f"{heavy_reason}，系统将继续执行扫描（可能稍慢）。")
        if async_scan_combo:
            cache_params = dict(combo_live_params)
            ok, msg, run_id = start_async_scan_task("combo", cache_params, score_col="共识评分")
            if ok:
                st.session_state["combo_async_task_id"] = run_id
                mark_scan_submitted("combo_async_task_id", cache_params)
                st.success(msg)
            else:
                st.warning(msg)
            st.rerun()

        with st.spinner("组合共识评分计算中..."):
            try:
                cache_params = dict(combo_live_params)
                results_df, meta = run_front_scan_via_offline_pipeline(strategy="combo", params=cache_params, analyzer=vp_analyzer)
                if results_df is None or results_df.empty:
                    st.warning("未找到符合条件的股票，请降低阈值或减少一致数量")
                    st.stop()
                st.session_state["combo_scan_results"] = results_df
                mark_front_scan_completed(strategy="combo", results_df=results_df, meta=meta, score_col="共识评分")
                set_stock_pool_candidate("combo", cache_params, "共识评分", results_df)
                st.session_state["combo_front_scan_notice"] = f"组合策略扫描完成，返回 {len(results_df)} 条"
                st.rerun()

                db_last = get_db_last_trade_date(permanent_db_path)
                if use_cache_combo:
                    cached_df, cached_meta = load_scan_cache("combo_scan", cache_params, db_last)
                    if cached_df is not None and not cached_df.empty:
                        created_at = cached_meta.get("created_at", "未知")
                        st.info(f"已加载离线缓存（数据日期 {db_last}，生成时间 {created_at}）")
                        render_cached_scan_results(
                            title="组合策略结果概览",
                            results_df=cached_df,
                            score_col="共识评分",
                            candidate_count=int(cached_meta.get("candidate_count", len(cached_df))),
                            filter_failed=int(cached_meta.get("filter_failed", 0)),
                            select_mode=select_mode_combo,
                            threshold=combo_threshold,
                            top_percent=top_percent_combo,
                            render_result_overview=render_result_overview,
                            signal_density_hint=signal_density_hint,
                        )
                        st.session_state["combo_scan_results"] = cached_df
                        set_stock_pool_candidate("combo", cache_params, "共识评分", cached_df)
                        st.stop()

                conn = connect_permanent_db()
                stocks_df = load_candidate_stocks(conn, scan_all=True, cap_min_yi=cap_min_combo, cap_max_yi=cap_max_combo, require_industry=True)
                if stocks_df.empty:
                    st.error("未找到符合条件的股票")
                    conn.close()
                    st.stop()
                stocks_df = stocks_df.head(candidate_count)

                bonus_global, bonus_stock_map, top_list_set, top_inst_set, bonus_industry_map = load_external_bonus_maps(conn)
                try:
                    from data.history import load_history_full as load_history_full_v2  # type: ignore
                    index_data = load_history_full_v2(
                        db_path=permanent_db_path,
                        ts_code="000001.SH",
                        start_date=(datetime.now() - timedelta(days=420)).strftime("%Y%m%d"),
                        end_date=datetime.now().strftime("%Y%m%d"),
                        columns="trade_date, close_price AS close, vol AS volume",
                        table_candidates=("daily_trading_history", "daily_trading_data", "daily_data"),
                        normalize_fn=normalize_stock_df,
                    )
                    if index_data is not None and len(index_data) > 120:
                        index_data = index_data.tail(120).reset_index(drop=True)
                except Exception:
                    index_data = pd.DataFrame()
                if not (len(index_data) >= 60 and "trade_date" in index_data.columns):
                    index_data = None

                end_date = datetime.now().strftime("%Y%m%d")
                start_date = (datetime.now() - timedelta(days=lookback_days_combo + 30)).strftime("%Y%m%d")
                history_cache = {}
                if len(stocks_df) <= bulk_history_limit:
                    history_cache = load_history_range_bulk(conn, stocks_df["ts_code"].tolist(), start_date, end_date, "ts_code, trade_date, close_price, high_price, low_price, vol, amount, pct_chg, turnover_rate")
                conn.close()

                ind_vals: Dict[str, list[float]] = {}
                for _, row in stocks_df.iterrows():
                    hist = history_cache.get(row["ts_code"])
                    if hist is None:
                        hist = load_history_full_combo(row["ts_code"], start_date, end_date)
                    if hist is None or len(hist) < 21:
                        continue
                    close = pd.to_numeric(hist["close_price"], errors="coerce").ffill()
                    if len(close) > 21:
                        r20 = (close.iloc[-1] / close.iloc[-21] - 1.0) * 100
                        ind_vals.setdefault(row["industry"], []).append(r20)
                industry_scores = {ind: float(np.mean(vals)) for ind, vals in ind_vals.items() if vals}

                results = []
                progress_bar = st.progress(0)
                status_text = st.empty()
                weights = {"v4": w_v4, "v5": w_v5, "v7": w_v7, "v8": w_v8, "v9": w_v9}

                for idx, row in stocks_df.iterrows():
                    ts_code = row["ts_code"]
                    stock_name = row["name"]
                    industry = row["industry"]
                    update_scan_progress_ui(progress_bar, status_text, idx, len(stocks_df), f"正在评分: {stock_name} ({idx+1}/{len(stocks_df)})")
                    hist = history_cache.get(ts_code)
                    if hist is None:
                        hist = load_history_full_combo(ts_code, start_date, end_date)
                    if hist is None or len(hist) < 80:
                        continue
                    avg_amount = pd.to_numeric(hist["amount"], errors="coerce").tail(20).mean()
                    avg_amount_yi = avg_amount / 1e5
                    if avg_amount_yi < min_turnover:
                        continue
                    stock_data = hist.copy()
                    stock_data["name"] = stock_name
                    v4_res = vp_analyzer.evaluator_v4.evaluate_stock_v4(stock_data)
                    v4_score = float(v4_res.get("final_score", 0)) if v4_res else None
                    v5_res = vp_analyzer.evaluator_v5.evaluate_stock_v4(stock_data)
                    v5_score = float(v5_res.get("final_score", 0)) if v5_res else None
                    v7_res = vp_analyzer.evaluator_v7.evaluate_stock_v7(stock_data=stock_data, ts_code=ts_code, industry=industry)
                    v7_score = float(v7_res.get("final_score", 0)) if v7_res and v7_res.get("success") else None
                    v8_res = vp_analyzer.evaluator_v8.evaluate_stock_v8(stock_data=stock_data, ts_code=ts_code, index_data=index_data if index_data is not None else None)
                    v8_score = float(v8_res.get("final_score", 0)) if v8_res and v8_res.get("success") else None
                    v9_info = vp_analyzer._calc_v9_score_from_hist(hist, industry_strength=industry_scores.get(industry, 0.0))
                    v9_score = float(v9_info.get("score", 0)) if v9_info else None
                    scores = {"v4": v4_score, "v5": v5_score, "v7": v7_score, "v8": v8_score, "v9": v9_score}

                    agree_count = 0
                    if v4_score is not None and v4_score >= thr_v4:
                        agree_count += 1
                    if v5_score is not None and v5_score >= thr_v5:
                        agree_count += 1
                    if v7_score is not None and v7_score >= thr_v7:
                        agree_count += 1
                    if v8_score is not None and v8_score >= thr_v8:
                        agree_count += 1
                    if v9_score is not None and v9_score >= thr_v9:
                        agree_count += 1
                    if agree_count < min_agree:
                        continue

                    weight_sum = sum(weights[k] for k, v in scores.items() if v is not None)
                    if weight_sum <= 0:
                        continue
                    weighted_score = sum((scores[k] * weights[k]) for k in scores if scores[k] is not None) / weight_sum
                    score_list = [v for v in scores.values() if v is not None]
                    score_std = float(np.std(score_list)) if len(score_list) > 1 else 0.0
                    disagree_count = 0
                    if v4_score is not None and v4_score < thr_v4:
                        disagree_count += 1
                    if v5_score is not None and v5_score < thr_v5:
                        disagree_count += 1
                    if v7_score is not None and v7_score < thr_v7:
                        disagree_count += 1
                    if v8_score is not None and v8_score < thr_v8:
                        disagree_count += 1
                    if v9_score is not None and v9_score < thr_v9:
                        disagree_count += 1
                    penalty = (score_std * disagree_std_weight) + (disagree_count * disagree_count_weight)
                    env_multiplier = 1.02 if market_env_combo == "bull" else (0.95 if market_env_combo == "bear" else 0.98)
                    adj_factor = 1.0 - market_adjust_strength + (market_adjust_strength * env_multiplier)
                    contrib = {
                        "v4贡献": (scores["v4"] * weights["v4"] / weight_sum) if scores["v4"] is not None else 0.0,
                        "v5贡献": (scores["v5"] * weights["v5"] / weight_sum) if scores["v5"] is not None else 0.0,
                        "v7贡献": (scores["v7"] * weights["v7"] / weight_sum) if scores["v7"] is not None else 0.0,
                        "v8贡献": (scores["v8"] * weights["v8"] / weight_sum) if scores["v8"] is not None else 0.0,
                        "v9贡献": (scores["v9"] * weights["v9"] / weight_sum) if scores["v9"] is not None else 0.0,
                    }
                    extra = calc_external_bonus(ts_code, industry, bonus_global, bonus_stock_map, top_list_set, top_inst_set, bonus_industry_map)
                    final_score = (weighted_score * adj_factor) + extra - penalty
                    row_item = {
                        "股票代码": ts_code,
                        "股票名称": stock_name,
                        "行业": industry,
                        "流通市值": f"{row['circ_mv']/10000:.1f}亿",
                        "共识评分": f"{final_score:.1f}",
                        "共识基础分": f"{weighted_score:.1f}",
                        "资金加分": f"{extra:.1f}",
                        "分歧惩罚": f"{penalty:.2f}",
                        "市场因子": f"{adj_factor:.2f}",
                        "一致数": agree_count,
                        "v4": f"{v4_score:.1f}" if v4_score is not None else "-",
                        "v5": f"{v5_score:.1f}" if v5_score is not None else "-",
                        "v7": f"{v7_score:.1f}" if v7_score is not None else "-",
                        "v8": f"{v8_score:.1f}" if v8_score is not None else "-",
                        "v9": f"{v9_score:.1f}" if v9_score is not None else "-",
                        **{k: f"{v:.1f}" for k, v in contrib.items()},
                        "建议持仓": "5-15天",
                    }
                    if select_mode_combo == "阈值筛选":
                        if final_score >= combo_threshold:
                            results.append(row_item)
                    elif select_mode_combo == "双重筛选(阈值+Top%)":
                        if final_score >= combo_threshold:
                            results.append(row_item)
                    else:
                        results.append(row_item)

                progress_bar.empty()
                status_text.empty()
                if results:
                    results_df = pd.DataFrame(results)
                    if select_mode_combo != "阈值筛选":
                        results_df["score_val"] = pd.to_numeric(results_df["共识评分"], errors="coerce")
                        results_df = results_df.sort_values("score_val", ascending=False)
                        keep_n = max(1, int(len(results_df) * top_percent_combo / 100))
                        results_df = results_df.head(keep_n).drop(columns=["score_val"])
                    if enable_consistency_combo and not results_df.empty:
                        results_df = apply_multi_period_filter(results_df, permanent_db_path, min_align=min_align_combo)
                    results_df = add_reason_summary(results_df, score_col="共识评分")
                    save_scan_cache("combo_scan", cache_params, db_last, results_df, {"candidate_count": len(stocks_df), "filter_failed": 0})
                    st.session_state["combo_scan_results"] = results_df
                    set_stock_pool_candidate("combo", cache_params, "共识评分", results_df)
                    if select_mode_combo == "阈值筛选":
                        st.success(f"找到 {len(results_df)} 只符合条件的股票（≥{combo_threshold}分）")
                    elif select_mode_combo == "双重筛选(阈值+Top%)":
                        st.success(f"先阈值后Top筛选：≥{combo_threshold}分，Top {top_percent_combo}%（{len(results_df)} 只）")
                    else:
                        st.success(f"选出 Top {top_percent_combo}%（{len(results_df)} 只）")
                    render_result_overview(results_df, score_col="共识评分", title="组合策略结果概览")
                    msg, level = signal_density_hint(len(results_df), len(stocks_df))
                    getattr(st, level)(msg)
                    results_df = standardize_result_df(results_df, score_col="共识评分")
                    st.dataframe(results_df, use_container_width=True, hide_index=True)
                    with st.expander("共识贡献拆解", expanded=False):
                        cols = ["股票代码", "股票名称", "共识评分", "共识基础分", "资金加分", "一致数", "v4贡献", "v5贡献", "v7贡献", "v8贡献", "v9贡献", "v4", "v5", "v7", "v8", "v9"]
                        show_cols = [c for c in cols if c in results_df.columns]
                        st.dataframe(results_df[show_cols], use_container_width=True, hide_index=True)
                    st.download_button(" 导出结果（CSV）", data=df_to_csv_bytes(results_df), file_name=f"组合策略_共识评分_扫描结果_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", mime="text/csv; charset=utf-8")
                else:
                    st.warning("未找到符合条件的股票，请降低阈值或减少一致数量")
            except Exception as e:
                st.error(f"扫描失败: {e}")
                st.code(traceback.format_exc())

    notice_combo = str(st.session_state.pop("combo_front_scan_notice", "") or "")
    if notice_combo:
        st.success(notice_combo)
    if "combo_scan_results" in st.session_state:
        render_front_scan_summary("combo", "组合共识策略")
        st.markdown("---")
        st.markdown("###  上次扫描结果")
        results_df = st.session_state["combo_scan_results"]
        display_df = results_df.drop(columns=["原始数据"], errors="ignore")
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        st.download_button(
            label="导出最近一次结果（CSV）",
            data=df_to_csv_bytes(display_df),
            file_name=f"组合策略_最近结果_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv; charset=utf-8",
            key="combo_recent_export_csv",
        )
    render_async_scan_status("combo_async_task_id", "组合共识策略", "共识评分")
