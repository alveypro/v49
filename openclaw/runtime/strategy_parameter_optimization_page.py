from __future__ import annotations

from datetime import datetime, timedelta
import traceback
from typing import Any, Callable, Dict, List, Tuple

import pandas as pd
import streamlit as st


def _load_optimization_history(connect_permanent_db: Callable[[], Any]) -> pd.DataFrame:
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


def _run_optimization_case(
    *,
    vp_analyzer: Any,
    optimize_strategy: str,
    df: pd.DataFrame,
    sample_size: int,
    hold: int,
    thr: int,
) -> Dict[str, Any]:
    if optimize_strategy == "v5":
        return vp_analyzer.backtest_bottom_breakthrough(df, sample_size=int(sample_size), holding_days=int(hold))
    if optimize_strategy == "v8":
        return vp_analyzer.backtest_v8_ultimate(
            df,
            sample_size=int(sample_size),
            holding_days=int(hold),
            score_threshold=float(thr),
        )
    if optimize_strategy == "combo":
        return vp_analyzer.backtest_combo_production(
            df,
            sample_size=int(sample_size),
            holding_days=int(hold),
            combo_threshold=float(thr),
            min_agree=2,
        )
    return vp_analyzer.backtest_v9_midterm(
        df,
        sample_size=int(sample_size),
        holding_days=int(hold),
        score_threshold=float(thr),
    )


def render_parameter_optimization_page(
    *,
    vp_analyzer: Any,
    connect_permanent_db: Callable[[], Any],
    ensure_price_aliases: Callable[[pd.DataFrame], pd.DataFrame],
    airivo_has_role: Callable[[str], bool],
    airivo_guard_action: Callable[[str, str, str, str], bool],
    airivo_append_action_audit: Callable[..., None],
    production_baseline_params: Callable[..., Dict[str, Dict[str, Any]]],
    get_production_compare_params: Callable[[], Dict[str, Any]],
    apply_production_baseline_to_session: Callable[[Dict[str, Dict[str, Any]]], None],
    save_production_unified_profile: Callable[[str, bool, Dict[str, Dict[str, Any]]], Tuple[bool, str]],
) -> None:
    st.subheader("参数优化")
    st.caption("生产策略专用优化（v9 / v8 / v5 / combo）。晋升门槛：样本>=50，胜率>=52%，最大回撤<=25%。")

    optimize_strategy = st.selectbox("优化目标策略", ["v9", "v8", "v5", "combo"], index=0, key="prod_opt_strategy")
    optimize_profile = st.selectbox("优化空间模板", ["稳健收敛（推荐）", "进攻试探"], index=0, key="prod_opt_profile")

    profile_defaults: Dict[str, Dict[str, Any]] = {
        "稳健收敛（推荐）": {
            "v5": {"thresholds": [60, 65, 70], "holds": [3, 5, 8], "sample": 1200},
            "v8": {"thresholds": [55, 60, 65, 70], "holds": [3, 5, 8], "sample": 1200},
            "v9": {"thresholds": [60, 65, 70], "holds": [3, 5, 8, 10], "sample": 1500},
            "combo": {"thresholds": [60, 65, 70], "holds": [6, 8, 10], "sample": 1500},
        },
        "进攻试探": {
            "v5": {"thresholds": [55, 60, 65], "holds": [3, 5, 8], "sample": 800},
            "v8": {"thresholds": [50, 55, 60, 65], "holds": [3, 5, 8], "sample": 800},
            "v9": {"thresholds": [55, 60, 65], "holds": [3, 5, 8], "sample": 1000},
            "combo": {"thresholds": [58, 60, 65], "holds": [6, 8], "sample": 1000},
        },
    }
    preset = profile_defaults.get(optimize_profile, profile_defaults["稳健收敛（推荐）"]).get(optimize_strategy, {})
    default_thresholds = [int(x) for x in preset.get("thresholds", [60, 65])]
    default_holds = [int(x) for x in preset.get("holds", [5, 8])]
    default_sample = int(preset.get("sample", 1200))

    if st.button("一键应用推荐优化空间", key="apply_prod_opt_profile"):
        st.session_state["prod_opt_thresholds"] = list(default_thresholds)
        st.session_state["prod_opt_holds"] = list(default_holds)
        st.session_state["prod_opt_sample"] = int(default_sample)
        st.rerun()

    oc1, oc2, oc3 = st.columns(3)
    with oc1:
        sample_size = st.slider("优化样本数量", 100, 6000, int(st.session_state.get("prod_opt_sample", default_sample)), 100)
    with oc2:
        threshold_grid = st.multiselect(
            "阈值候选",
            options=[45, 50, 55, 60, 65, 70, 75, 80],
            default=[int(x) for x in st.session_state.get("prod_opt_thresholds", default_thresholds)],
        )
    with oc3:
        hold_grid = st.multiselect(
            "持仓候选(天)",
            options=[3, 5, 6, 8, 10, 12, 15],
            default=[int(x) for x in st.session_state.get("prod_opt_holds", default_holds)],
        )
    st.session_state["prod_opt_sample"] = int(sample_size)
    st.session_state["prod_opt_thresholds"] = [int(x) for x in threshold_grid]
    st.session_state["prod_opt_holds"] = [int(x) for x in hold_grid]

    if not threshold_grid:
        st.warning("请至少选择一个阈值候选。")
    if not hold_grid:
        st.warning("请至少选择一个持仓候选。")

    if st.button("开始优化", type="primary", use_container_width=True, key="start_optimization"):
        if not threshold_grid or not hold_grid:
            st.stop()
        with st.spinner(f"正在优化 {optimize_strategy} 参数空间..."):
            try:
                df = ensure_price_aliases(_load_optimization_history(connect_permanent_db))
                if df.empty:
                    st.error("无法获取历史数据")
                    st.stop()

                all_rows: List[Dict[str, Any]] = []
                total_cases = len(threshold_grid) * len(hold_grid)
                progress = st.progress(0.0)
                case_idx = 0
                for thr in sorted(set(int(x) for x in threshold_grid)):
                    for hold in sorted(set(int(x) for x in hold_grid)):
                        case_idx += 1
                        out = _run_optimization_case(
                            vp_analyzer=vp_analyzer,
                            optimize_strategy=optimize_strategy,
                            df=df,
                            sample_size=int(sample_size),
                            hold=int(hold),
                            thr=int(thr),
                        )
                        stats = (out or {}).get("stats", {}) if isinstance(out, dict) else {}
                        ok = bool((out or {}).get("success", False))
                        win_rate = float(stats.get("win_rate", 0.0) or 0.0)
                        if abs(win_rate) <= 1.0:
                            win_rate *= 100.0
                        max_dd = float(stats.get("max_drawdown", 0.0) or 0.0)
                        if abs(max_dd) <= 1.0:
                            max_dd *= 100.0
                        signals = int(stats.get("total_signals", 0) or 0)
                        avg_ret = float(stats.get("avg_return", 0.0) or 0.0)
                        sharpe = float(stats.get("sharpe_ratio", 0.0) or 0.0)
                        objective = (0.45 * win_rate) + (0.35 * avg_ret) + (0.15 * sharpe * 10.0) - (0.25 * max_dd)
                        gate_pass = bool(signals >= 50 and win_rate >= 52.0 and max_dd <= 25.0)
                        all_rows.append(
                            {
                                "strategy": optimize_strategy,
                                "threshold": int(thr),
                                "holding_days": int(hold),
                                "success": ok,
                                "signals": signals,
                                "win_rate_pct": win_rate,
                                "avg_return_pct": avg_ret,
                                "max_drawdown_pct": max_dd,
                                "sharpe_ratio": sharpe,
                                "objective": objective,
                                "gate_pass": gate_pass,
                            }
                        )
                        progress.progress(case_idx / max(1, total_cases))

                result_df = pd.DataFrame(all_rows)
                if result_df.empty:
                    st.error("优化没有产出有效结果。")
                    st.stop()
                result_df = result_df.sort_values(["objective", "win_rate_pct"], ascending=[False, False]).reset_index(drop=True)
                st.session_state["prod_optimization_result"] = {
                    "strategy": optimize_strategy,
                    "sample_size": int(sample_size),
                    "threshold_grid": sorted(set(int(x) for x in threshold_grid)),
                    "hold_grid": sorted(set(int(x) for x in hold_grid)),
                    "result_df": result_df.to_dict("records"),
                    "best": result_df.iloc[0].to_dict(),
                }
                st.success(f"{optimize_strategy} 参数优化完成，共评估 {len(result_df)} 组。")
                st.rerun()
            except Exception as exc:
                st.error(f"优化失败: {exc}")
                st.code(traceback.format_exc())

    if "prod_optimization_result" in st.session_state:
        opt = st.session_state["prod_optimization_result"] or {}
        result_df = pd.DataFrame(opt.get("result_df", []))
        best = opt.get("best", {}) if isinstance(opt.get("best", {}), dict) else {}
        if not result_df.empty:
            st.markdown("---")
            pass_count = int(result_df["gate_pass"].fillna(False).astype(bool).sum())
            fail_count = int(len(result_df) - pass_count)
            r1, r2, r3, r4 = st.columns(4)
            r1.metric("优化策略", str(opt.get("strategy", "N/A")))
            r2.metric("总组合", int(len(result_df)))
            r3.metric("满足晋升门槛", pass_count)
            r4.metric("未达标", fail_count)

            st.success(
                f"最佳参数：阈值 {int(best.get('threshold', 0))} / 持仓 {int(best.get('holding_days', 0))}天 | "
                f"胜率 {float(best.get('win_rate_pct', 0.0)):.1f}% | 回撤 {float(best.get('max_drawdown_pct', 0.0)):.1f}% | "
                f"样本 {int(best.get('signals', 0))} | 晋升建议 {'YES' if bool(best.get('gate_pass', False)) else 'NO'}"
            )
            c_apply1, c_apply2 = st.columns([1, 2])
            with c_apply1:
                apply_best = st.button("一键回写最佳参数到策略中心", type="primary", key="apply_best_opt_to_center", disabled=not airivo_has_role("admin"))
            with c_apply2:
                st.caption("回写后会同步当前会话参数，并写入统一口径文件（重启后保持）。")
            if apply_best:
                if not airivo_guard_action("admin", "apply_best_opt_to_center", target=str(opt.get("strategy", "")).strip().lower(), reason="optimization_best_writeback"):
                    st.stop()
                try:
                    strict_mode = bool(st.session_state.get("strict_full_market_mode", False))
                    unified_cap_min = float(st.session_state.get("unified_cap_min", 100.0) or 100.0)
                    unified_cap_max = float(st.session_state.get("unified_cap_max", 15000.0) or 15000.0)
                    params = production_baseline_params("稳健标准", strict_full_market=strict_mode)
                    current = get_production_compare_params()
                    for sk in ("v5", "v8", "v9", "combo"):
                        params.setdefault(sk, {})
                        params[sk]["score_threshold"] = int((current.get(sk) or {}).get("score_threshold", params[sk].get("score_threshold", 60)))
                        params[sk]["holding_days"] = int((current.get(sk) or {}).get("holding_days", params[sk].get("holding_days", 8)))
                        params[sk]["cap_min"] = float(unified_cap_min)
                        params[sk]["cap_max"] = float(unified_cap_max)

                    target_sk = str(opt.get("strategy", "")).strip().lower()
                    if target_sk in {"v5", "v8", "v9", "combo"}:
                        params[target_sk]["score_threshold"] = int(best.get("threshold", params[target_sk].get("score_threshold", 60)))
                        params[target_sk]["holding_days"] = int(best.get("holding_days", params[target_sk].get("holding_days", 8)))

                    apply_production_baseline_to_session(params)
                    ok_save, save_msg = save_production_unified_profile("优化回写", strict_mode, params)
                    if ok_save:
                        airivo_append_action_audit("apply_best_opt_to_center", True, target=target_sk, detail=save_msg)
                        st.success("已回写最佳参数到策略中心，并保存到统一口径文件。")
                        st.caption(f"统一口径文件：{save_msg}")
                    else:
                        airivo_append_action_audit("apply_best_opt_to_center", False, target=target_sk, detail=save_msg)
                        st.warning(f"已回写到会话参数，但保存统一口径文件失败：{save_msg}")
                    st.rerun()
                except Exception as exc:
                    airivo_append_action_audit("apply_best_opt_to_center", False, target=str(opt.get("strategy", "")).strip().lower(), detail=str(exc))
                    st.error(f"回写失败：{exc}")

            show_df = result_df.rename(
                columns={
                    "strategy": "策略",
                    "threshold": "阈值",
                    "holding_days": "持仓天数",
                    "signals": "样本数",
                    "win_rate_pct": "胜率(%)",
                    "avg_return_pct": "平均收益(%)",
                    "max_drawdown_pct": "最大回撤(%)",
                    "sharpe_ratio": "夏普比率",
                    "objective": "综合评分",
                    "gate_pass": "晋升门槛通过",
                }
            )
            show_df = show_df.sort_values(["综合评分", "胜率(%)"], ascending=[False, False])
            st.dataframe(show_df, use_container_width=True, hide_index=True)
