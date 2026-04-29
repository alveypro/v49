from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Any, Callable, Dict, Tuple

import pandas as pd
import streamlit as st


def render_data_ops_core_page(
    *,
    render_page_header: Callable[..., None],
    get_auto_evolve_status: Callable[[], Dict[str, Any]],
    load_production_report_by_strategy: Callable[[str], Tuple[Dict[str, Any] | None, str, str]],
    safe_parse_dt: Callable[[str], Any],
    airivo_has_role: Callable[[str], bool],
    airivo_guard_action: Callable[[str, str, str, str], bool],
    airivo_append_action_audit: Callable[..., None],
    trigger_auto_evolve_optimize: Callable[..., Tuple[bool, str]],
    load_portfolio_risk_budget: Callable[[], Dict[str, Any]],
    evaluate_production_rollback_trigger: Callable[[], Dict[str, Any]],
    load_production_rollback_state: Callable[[], Dict[str, Any]],
    execute_production_auto_rollback: Callable[..., Tuple[bool, str, Any]],
    compute_production_allocation_plan: Callable[..., Dict[str, Any]],
    write_production_allocation_report: Callable[[Dict[str, Any]], Tuple[bool, str, str]],
    build_production_rebalance_orders: Callable[[Dict[str, Any]], Dict[str, Any]],
    write_production_rebalance_report: Callable[[Dict[str, Any], Dict[str, Any]], Tuple[bool, str, str]],
    precheck_production_rebalance_orders: Callable[..., Dict[str, Any]],
    execute_production_rebalance_orders: Callable[..., Dict[str, Any]],
    load_latest_production_rebalance_audit: Callable[[], Dict[str, Any]],
    build_weekly_rebalance_quality_dashboard: Callable[..., Dict[str, Any]],
    load_latest_auto_rebalance_log: Callable[[], Dict[str, Any] | None],
    db_manager: Any,
) -> None:
    render_page_header(" 数据与系统", "数据更新 · 系统健康 · 运维动作", tag="Data Ops")
    st.caption("仅保留关键运维动作。")

    with st.expander("运行与回测摘要", expanded=True):
        evolve_status = get_auto_evolve_status()
        source_options = ["自动(最新)", "V9", "V8", "V5", "COMBO"]
        selected_source = st.selectbox("摘要来源", source_options, index=0, key="summary_source_selector_data")
        evolve_latest = evolve_status.get("data", {}) or {}
        display_run_source = str(evolve_status.get("run_source", "N/A"))
        display_run_at = str(evolve_status.get("run_at", "") or "")
        display_mtime = str(evolve_status.get("last_run_mtime", "") or "")
        if selected_source != "自动(最新)":
            display_run_source = selected_source
            picked, picked_mtime, picked_type = load_production_report_by_strategy(selected_source)
            if picked:
                evolve_latest = picked
                display_run_at = str(picked.get("run_at", "") or "")
                display_mtime = picked_mtime
                if picked_type and picked_type != "best":
                    display_run_source = f"{selected_source}({picked_type})"
            else:
                evolve_latest = {}
                display_run_at = ""
                display_mtime = ""
                evo_dir = os.path.join(os.path.dirname(__file__), "..", "..", "evolution")
                file_map = {"V9": "v9_best.json", "V8": "v8_best.json", "V5": "v5_best.json", "COMBO": "combo_best.json"}
                target_file = file_map.get(str(selected_source).upper(), "")
                target_path = os.path.join(evo_dir, target_file) if target_file else ""
                if target_path and (not os.path.exists(target_path)):
                    st.warning(f"未找到 {target_file}；该策略最近可能未通过晋升。请检查 {selected_source.lower()}_last_attempt.json。")
                else:
                    st.warning(f"{selected_source} 摘要文件读取失败，当前无法显示该策略摘要。")

        run_dt = safe_parse_dt(display_run_at)
        run_age_mins = int(max(0.0, (datetime.now() - run_dt).total_seconds() // 60)) if run_dt else None
        rs1, rs2, rs3, rs4 = st.columns(4)
        with rs1:
            st.metric("自动进化状态", str(evolve_status.get("runtime_state", "未知")))
        with rs2:
            st.metric("最近回测时间", display_run_at or "N/A")
        with rs3:
            st.metric("结果文件更新时间", display_mtime or "N/A")
        with rs4:
            risk_raw = str(evolve_status.get("risk_level", "unknown")).lower()
            risk_effective = str(evolve_status.get("risk_level_effective", risk_raw)).lower()
            risk_stale = bool(evolve_status.get("risk_stale", False))
            risk_metric_text = str(risk_effective).upper() if not risk_stale else f"HISTORY-{str(risk_raw).upper()}"
            st.metric("风险等级", risk_metric_text)
        st.caption(f"数据口径：摘要来源 {display_run_source} | 回测时间 {display_run_at or 'N/A'} | 日志时间 {str(evolve_status.get('log_mtime', '') or 'N/A')}")

        c1, c2 = st.columns([1, 3])
        with c1:
            trigger_disabled = str(evolve_status.get("runtime_state", "")).startswith("运行中")
            if st.button("更新摘要", key="trigger_auto_evolve_optimize_data", disabled=trigger_disabled or (not airivo_has_role("admin"))):
                if not airivo_guard_action("admin", "trigger_auto_evolve_optimize_data", target="auto_evolve_optimize", reason="manual_optimize_summary_refresh"):
                    st.stop()
                ok, msg = trigger_auto_evolve_optimize(force_now=True)
                if ok:
                    airivo_append_action_audit("trigger_auto_evolve_optimize_data", True, target="auto_evolve_optimize", detail=msg)
                    st.success(msg)
                    st.rerun()
                else:
                    airivo_append_action_audit("trigger_auto_evolve_optimize_data", False, target="auto_evolve_optimize", detail=msg)
                    st.error(msg)
        with c2:
            if isinstance(run_age_mins, int):
                st.caption(f"数据新鲜度：{run_age_mins} 分钟")

        if isinstance(evolve_latest, dict) and evolve_latest.get("stats"):
            stats = evolve_latest.get("stats", {})
            summary_parts = []
            if isinstance(stats.get("win_rate"), (int, float)):
                summary_parts.append(f"胜率{stats.get('win_rate'):.1f}%")
            if isinstance(stats.get("avg_holding_days"), (int, float)):
                summary_parts.append(f"平均持仓{stats.get('avg_holding_days'):.1f}天")
            if isinstance(stats.get("avg_return"), (int, float)):
                summary_parts.append(f"平均收益{stats.get('avg_return'):.2f}%")
            if summary_parts:
                st.markdown("**摘要：" + " · ".join(summary_parts) + "**")

        st.markdown("---")
        st.markdown("**组合级风险预算（扫描+回测同口径）**")
        rb_defaults = load_portfolio_risk_budget()
        rb_cfg1, rb_cfg2, rb_cfg3 = st.columns(3)
        with rb_cfg1:
            st.toggle("启用风险预算", value=bool(rb_defaults.get("enabled", True)), key="prod_rb_enable")
        with rb_cfg2:
            st.slider("最大持仓数", min_value=5, max_value=80, value=int(rb_defaults.get("max_positions", 20)), step=1, key="prod_rb_max_positions")
        with rb_cfg3:
            st.slider("行业集中度上限", min_value=0.10, max_value=0.80, value=float(rb_defaults.get("max_industry_ratio", 0.35)), step=0.01, key="prod_rb_max_ind_ratio")
        st.caption("说明：按共识评分从高到低选股，超出行业上限后跳过该行业信号。")

        st.markdown("---")
        st.markdown("**生产安全回滚触发器**")
        rb_eval = evaluate_production_rollback_trigger()
        rb_targets = list(rb_eval.get("targets") or [])
        rb_state = load_production_rollback_state()
        rc1, rc2, rc3, rc4 = st.columns(4)
        with rc1:
            st.metric("触发状态", "TRIGGERED" if bool(rb_eval.get("triggered", False)) else "NORMAL")
        with rc2:
            st.metric("RED策略数", int(rb_eval.get("red_count", 0)))
        with rc3:
            st.metric("未晋升策略数", int(rb_eval.get("no_count", 0)))
        with rc4:
            st.metric("回滚目标数", len(rb_targets))
        st.caption(f"触发原因：{rb_eval.get('reason', 'N/A')} | 目标：{','.join(rb_targets) if rb_targets else '无'} | 签名：{rb_eval.get('signature', '')}")
        if rb_state:
            st.caption(f"上次执行：{rb_state.get('last_run_at', 'N/A')} | 目标：{','.join(rb_state.get('last_targets', []) or []) or '无'} | 成功：{','.join(rb_state.get('last_done', []) or []) or '无'}")
        auto_rb_enabled = st.toggle("启用自动回滚（有触发才执行）", value=False, key="enable_prod_auto_rollback")
        st.toggle("回滚后自动触发重训", value=False, key="prod_retrain_after_rollback")
        rb_col1, rb_col2 = st.columns([1, 3])
        with rb_col1:
            if st.button("手动执行一次回滚", key="run_prod_rollback_once", use_container_width=True, disabled=not airivo_has_role("admin")):
                if not airivo_guard_action("admin", "run_prod_rollback_once", target="production_rollback", reason="manual_production_rollback"):
                    st.stop()
                ok_rb, msg_rb, _ = execute_production_auto_rollback(force=True)
                if ok_rb:
                    airivo_append_action_audit("run_prod_rollback_once", True, target="production_rollback", detail=msg_rb)
                    st.success(msg_rb)
                else:
                    airivo_append_action_audit("run_prod_rollback_once", False, target="production_rollback", detail=msg_rb)
                    st.warning(msg_rb)
                st.rerun()
        with rb_col2:
            if auto_rb_enabled:
                ok_rb, msg_rb, _ = execute_production_auto_rollback(force=False)
                if ok_rb:
                    st.warning(f"自动回滚已执行：{msg_rb}")
                else:
                    st.caption(f"自动回滚检查：{msg_rb}")

        st.markdown("---")
        st.markdown("**策略级仓位分配建议（v5/v8/v9/combo）**")
        alloc_col1, alloc_col2, alloc_col3 = st.columns(3)
        with alloc_col1:
            alloc_capital = st.number_input("组合总资金", min_value=10000.0, max_value=1_000_000_000.0, value=float(st.session_state.get("prod_alloc_total_capital", 1_000_000.0)), step=10000.0, key="prod_alloc_total_capital")
        with alloc_col2:
            alloc_regime = st.selectbox("市场状态口径", ["auto", "bull", "oscillation", "bear"], index=0, key="prod_alloc_regime_choice")
        with alloc_col3:
            if st.button("生成仓位建议", key="run_prod_allocation_plan", use_container_width=True, disabled=not airivo_has_role("admin")):
                if not airivo_guard_action("admin", "run_prod_allocation_plan", target="allocation_plan", reason=str(alloc_regime)):
                    st.stop()
                alloc_plan = compute_production_allocation_plan(capital_total=float(alloc_capital), regime_choice=str(alloc_regime))
                airivo_append_action_audit("run_prod_allocation_plan", bool(alloc_plan.get("ok")), target="allocation_plan", detail=str(alloc_plan.get("error") or "ok"), extra={"regime": str(alloc_regime), "capital_total": float(alloc_capital)})
                st.session_state["prod_allocation_plan"] = alloc_plan
                st.rerun()

        alloc_plan_show = st.session_state.get("prod_allocation_plan")
        if isinstance(alloc_plan_show, dict) and alloc_plan_show.get("ok"):
            plan_df = pd.DataFrame(alloc_plan_show.get("plan_df", []))
            if not plan_df.empty:
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("市场状态", str(alloc_plan_show.get("regime", "N/A")))
                m2.metric("策略数", int(len(plan_df)))
                m3.metric("最大策略权重", f"{float(plan_df['target_weight'].max())*100:.1f}%")
                m4.metric("平均单票上限", f"{float(plan_df['single_ticket_cap'].mean()):.0f}")
                show_df = plan_df.rename(columns={"strategy": "策略", "win_rate_pct": "胜率(%)", "max_drawdown_pct": "最大回撤(%)", "risk": "风险级别", "promo": "晋升通过", "target_weight": "目标权重", "capital_alloc": "分配资金", "slot_count": "建议仓位数", "single_ticket_cap": "单票上限"})
                show_df["目标权重"] = show_df["目标权重"].apply(lambda x: round(float(x) * 100.0, 2))
                st.dataframe(show_df, use_container_width=True, hide_index=True)
                if st.button("生成今日生产简报（md+csv）", key="gen_prod_alloc_report", disabled=not airivo_has_role("admin")):
                    if not airivo_guard_action("admin", "gen_prod_alloc_report", target="allocation_report", reason="allocation_report_generate"):
                        st.stop()
                    ok_rp, md_path, csv_path = write_production_allocation_report(alloc_plan_show)
                    if ok_rp:
                        airivo_append_action_audit("gen_prod_alloc_report", True, target="allocation_report", detail=str(md_path), extra={"csv_path": str(csv_path)})
                        st.success("生产简报已生成。")
                        st.caption(f"Markdown: {md_path}")
                        st.caption(f"CSV: {csv_path}")
                    else:
                        airivo_append_action_audit("gen_prod_alloc_report", False, target="allocation_report", detail="report_generation_failed")
                        st.warning("生产简报生成失败。")

                reb_col1, reb_col2 = st.columns([1, 1])
                with reb_col1:
                    if st.button("生成调仓指令清单", key="gen_prod_rebalance_orders", use_container_width=True, disabled=not airivo_has_role("admin")):
                        if not airivo_guard_action("admin", "gen_prod_rebalance_orders", target="rebalance_orders", reason="rebalance_order_generate"):
                            st.stop()
                        reb = build_production_rebalance_orders(alloc_plan_show)
                        airivo_append_action_audit("gen_prod_rebalance_orders", bool(reb.get("ok")), target="rebalance_orders", detail=str(reb.get("error") or "ok"))
                        st.session_state["prod_rebalance_orders"] = reb
                        st.rerun()
                with reb_col2:
                    if st.button("导出调仓简报（md+csv）", key="export_prod_rebalance_report", use_container_width=True, disabled=not airivo_has_role("admin")):
                        if not airivo_guard_action("admin", "export_prod_rebalance_report", target="rebalance_report", reason="rebalance_report_export"):
                            st.stop()
                        reb = st.session_state.get("prod_rebalance_orders", {})
                        ok_rb, md_rb, csv_rb = write_production_rebalance_report(alloc_plan_show, reb if isinstance(reb, dict) else {})
                        if ok_rb:
                            airivo_append_action_audit("export_prod_rebalance_report", True, target="rebalance_report", detail=str(md_rb), extra={"csv_path": str(csv_rb)})
                            st.success("调仓简报已生成。")
                            st.caption(f"Markdown: {md_rb}")
                            st.caption(f"CSV: {csv_rb}")
                        else:
                            airivo_append_action_audit("export_prod_rebalance_report", False, target="rebalance_report", detail="report_generation_failed")
                            st.warning("调仓简报生成失败，请先生成调仓指令清单。")

                reb_show = st.session_state.get("prod_rebalance_orders", {})
                if isinstance(reb_show, dict) and reb_show.get("ok"):
                    odf = pd.DataFrame(reb_show.get("orders_df", []))
                    sm = reb_show.get("summary", {}) or {}
                    rr1, rr2, rr3, rr4 = st.columns(4)
                    rr1.metric("总指令", int(sm.get("total_orders", 0)))
                    rr2.metric("BUY", int(sm.get("buy_count", 0)))
                    rr3.metric("HOLD", int(sm.get("hold_count", 0)))
                    rr4.metric("REDUCE", int(sm.get("reduce_count", 0)))
                    if not odf.empty:
                        show_cols = [c for c in ["action", "strategy", "ts_code", "name", "industry", "score", "target_amount", "reason"] if c in odf.columns]
                        st.dataframe(odf[show_cols], use_container_width=True, hide_index=True)
                    ex1, ex2 = st.columns([1, 3])
                    with ex1:
                        execute_reduce = st.toggle("执行REDUCE卖出", value=False, key="prod_exec_reduce_toggle")
                        precheck = precheck_production_rebalance_orders(reb_show, execute_reduce=bool(execute_reduce))
                        can_execute = bool(precheck.get("ok")) and str(precheck.get("status", "RED")).upper() != "RED"
                        if st.button("执行到模拟账户", key="exec_prod_rebalance_to_sim", use_container_width=True, disabled=(not can_execute) or (not airivo_has_role("admin"))):
                            if not airivo_guard_action("admin", "exec_prod_rebalance_to_sim", target="rebalance_sim", reason=f"execute_reduce={bool(execute_reduce)}"):
                                st.stop()
                            exec_ret = execute_production_rebalance_orders(reb_show, execute_reduce=bool(execute_reduce))
                            airivo_append_action_audit("exec_prod_rebalance_to_sim", bool(exec_ret.get("ok")), target="rebalance_sim", detail=str(exec_ret.get("error") or exec_ret.get("batch_id") or "ok"))
                            st.session_state["prod_rebalance_exec_result"] = exec_ret
                            st.rerun()
                    with ex2:
                        st.caption("说明：执行 BUY 到模拟账户；REDUCE 默认不执行，开启后才会按最新价卖出。")
                elif isinstance(reb_show, dict) and reb_show.get("error"):
                    st.info(f"调仓指令暂不可用：{reb_show.get('error')}")

                exec_show = st.session_state.get("prod_rebalance_exec_result", {})
                if isinstance(exec_show, dict) and exec_show.get("ok"):
                    esm = exec_show.get("summary", {}) or {}
                    st.success(f"模拟执行完成（batch={exec_show.get('batch_id','N/A')}）：BUY={int(esm.get('buy_done', 0))} | REDUCE={int(esm.get('reduce_done', 0))} | SKIP={int(esm.get('skipped', 0))} | 现金余额={float(esm.get('cash_after', 0.0)):.2f}")
                    edf = pd.DataFrame(exec_show.get("executed_df", []))
                    if not edf.empty:
                        st.dataframe(edf, use_container_width=True, hide_index=True)
                elif isinstance(exec_show, dict) and exec_show.get("error"):
                    st.warning(f"模拟执行失败：{exec_show.get('error')}")

                latest_audit = load_latest_production_rebalance_audit()
                if latest_audit:
                    q = latest_audit.get("quality", {}) or {}
                    st.caption(f"最近执行审计：{latest_audit.get('run_at', 'N/A')} | batch={latest_audit.get('batch_id', 'N/A')}")
                    qa1, qa2, qa3, qa4 = st.columns(4)
                    qa1.metric("执行质量分", f"{float(q.get('score', 0.0)):.2f}")
                    qa2.metric("质量等级", str(q.get("level", "N/A")))
                    qa3.metric("执行成功率", f"{float(q.get('success_rate_pct', 0.0)):.1f}%")
                    qa4.metric("执行换手额", f"{float(q.get('turnover', 0.0)):.0f}")
                weekly = build_weekly_rebalance_quality_dashboard(days=7)
                if weekly.get("ok"):
                    st.markdown("**周度执行质量看板（近7天）**")
                    w1, w2, w3, w4 = st.columns(4)
                    w1.metric("执行批次", int(weekly.get("runs", 0)))
                    w2.metric("平均质量分", f"{float(weekly.get('avg_score', 0.0)):.2f}")
                    w3.metric("平均成功率", f"{float(weekly.get('avg_success_rate', 0.0)):.1f}%")
                    w4.metric("累计换手额", f"{float(weekly.get('turnover_total', 0.0)):.0f}")
                    daily_df = pd.DataFrame(weekly.get("daily_df", []))
                    if not daily_df.empty:
                        st.line_chart(daily_df.set_index("trade_date")[["score", "success_rate_pct"]], height=220)
                        st.bar_chart(daily_df.set_index("trade_date")[["turnover"]], height=180)
                    skip_top = weekly.get("skip_top", []) or []
                    if skip_top:
                        st.dataframe(pd.DataFrame(skip_top, columns=["跳过原因", "次数"]), use_container_width=True, hide_index=True)

                    st.markdown("---")
                    st.markdown("**自动调仓任务调度（每日）**")
                    auto_log = load_latest_auto_rebalance_log()
                    if auto_log:
                        st.caption(f"最近自动调仓日志：{auto_log.get('run_at', 'N/A')} | ok={auto_log.get('ok', False)} | stage={auto_log.get('stage', 'N/A')}")

    st.markdown("---")
    update_mode = st.radio("更新模式", ["快速（5天）", "标准（30天）", "深度（90天）"], horizontal=True)
    days = 5 if update_mode == "快速（5天）" else 30 if update_mode == "标准（30天）" else 90
    st.caption(f"更新窗口：最近 {days} 天")
    if st.button("开始更新数据", type="primary", use_container_width=True, disabled=not airivo_has_role("admin")):
        if not airivo_guard_action("admin", "start_data_update", target="stock_data", reason=f"days={days}"):
            st.stop()
        with st.spinner(f"正在更新{days}天数据..."):
            try:
                result = db_manager.update_stock_data_from_tushare(days=days)
                if result["success"]:
                    airivo_append_action_audit("start_data_update", True, target="stock_data", detail=f"updated_days={result['updated_days']},total_records={result['total_records']}", extra={"days": days})
                    st.success(f"更新成功！\n- 更新天数：{result['updated_days']}天\n- 失败天数：{result.get('failed_days', 0)}天\n- 总记录数：{result['total_records']:,}条")
                    if result.get("calendar_warning"):
                        st.warning(result.get("calendar_warning"))
                    time.sleep(1)
                    st.rerun()
                else:
                    airivo_append_action_audit("start_data_update", False, target="stock_data", detail=str(result.get("error")), extra={"days": days})
                    st.error(f"更新失败：{result.get('error')}")
            except Exception as exc:
                airivo_append_action_audit("start_data_update", False, target="stock_data", detail=str(exc), extra={"days": days})
                st.error(f"更新失败：{exc}")

    st.markdown("---")
    st.subheader("流通市值数据更新")
    st.caption("市值筛选异常时先执行本操作。")
    if st.button("更新流通市值数据", use_container_width=True, type="primary", disabled=not airivo_has_role("admin")):
        if not airivo_guard_action("admin", "update_market_cap", target="market_cap", reason="market_cap_refresh"):
            st.stop()
        with st.spinner("正在从Tushare获取最新市值数据..."):
            result = db_manager.update_market_cap()
            if result.get("success"):
                airivo_append_action_audit("update_market_cap", True, target="market_cap", detail=f"updated_count={result.get('updated_count', 0)}")
                stats = result.get("stats", {})
                st.success(f"市值数据更新成功！\n- 更新股票数：{result.get('updated_count', 0):,}只\n- 100-500亿：{stats.get('count_100_500', 0)}只 黄金区间\n- 50-100亿：{stats.get('count_50_100', 0)}只\n- <50亿：{stats.get('count_below_50', 0)}只\n- >500亿：{stats.get('count_above_500', 0)}只")
                time.sleep(1)
                st.rerun()
            else:
                airivo_append_action_audit("update_market_cap", False, target="market_cap", detail=str(result.get("error")))
                st.error(f"更新失败：{result.get('error')}")

    st.markdown("---")
    st.subheader("数据库优化与维护")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("数据库健康检查", use_container_width=True, disabled=not airivo_has_role("admin")):
            if not airivo_guard_action("admin", "database_health_check", target="database", reason="manual_database_health_check"):
                st.stop()
            with st.spinner("正在检查数据库健康状态..."):
                health = db_manager.check_database_health()
                airivo_append_action_audit("database_health_check", "error" not in health, target="database", detail=str(health.get("error") or "ok"))
                if "error" in health:
                    st.error(f"检查失败: {health['error']}")
                else:
                    if health.get("has_stock_basic") and health.get("has_daily_data"):
                        st.success("数据库结构正常")
                        col_a, col_b, col_c = st.columns(3)
                        with col_a:
                            st.metric("股票数量", f"{health.get('stock_count', 0):,}")
                        with col_b:
                            st.metric("数据记录", f"{health.get('data_count', 0):,}")
                        with col_c:
                            days_old = health.get("days_since_update", 999)
                            is_fresh = health.get("is_fresh", False)
                            st.metric("数据新鲜度", f"{days_old}天前" if days_old < 999 else "未知", delta="新鲜" if is_fresh else "需更新", delta_color="normal" if is_fresh else "inverse")
                    else:
                        st.warning("数据库结构不完整，建议重新初始化")
    with col2:
        if st.button("优化数据库", use_container_width=True, type="secondary", disabled=not airivo_has_role("admin")):
            if not airivo_guard_action("admin", "optimize_database", target="database", reason="manual_database_optimize"):
                st.stop()
            with st.spinner("正在优化数据库（清理重复数据、重建索引）..."):
                result = db_manager.optimize_database()
                if result.get("success"):
                    airivo_append_action_audit("optimize_database", True, target="database", detail=str(result.get("message") or "ok"))
                    st.success(f"{result.get('message')}")
                    time.sleep(1)
                    st.rerun()
                else:
                    airivo_append_action_audit("optimize_database", False, target="database", detail=str(result.get("error")))
                    st.error(f"优化失败: {result.get('error')}")
    with st.expander("数据库维护说明"):
        st.markdown("建议：每周检查健康、每月优化一次、更新前保留备份。")
