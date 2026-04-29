from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

import streamlit as st


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _read_database_status(connect_permanent_db: Callable[[], sqlite3.Connection]) -> Dict[str, Any]:
    from data.dao import DataAccessError, detect_daily_table  # type: ignore

    conn = connect_permanent_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM stock_basic")
        stock_count = int(cursor.fetchone()[0])
        try:
            daily_table = detect_daily_table(conn)
        except DataAccessError:
            daily_table = ""
        if daily_table:
            cursor.execute(f"SELECT COUNT(DISTINCT ts_code) FROM {daily_table}")
            data_stock_count = int(cursor.fetchone()[0])
            cursor.execute(f"SELECT COUNT(*) FROM {daily_table}")
            total_records = int(cursor.fetchone()[0])
            cursor.execute(f"SELECT MIN(trade_date), MAX(trade_date) FROM {daily_table}")
            min_date, max_date = cursor.fetchone()
        else:
            data_stock_count = 0
            total_records = 0
            min_date, max_date = None, None
        return {
            "stock_count": stock_count,
            "data_stock_count": data_stock_count,
            "total_records": total_records,
            "min_date": min_date,
            "max_date": max_date,
        }
    finally:
        conn.close()


def render_data_ops_status_page(
    *,
    connect_permanent_db: Callable[[], sqlite3.Connection],
    rollback_latest_promoted_params: Callable[[str], Tuple[bool, str]],
    fund_bonus_enabled: Callable[[], bool],
    get_last_trade_date_from_tushare: Callable[[], Optional[str]],
    compute_health_report: Callable[[str], Dict[str, Any]],
    run_funding_repair: Callable[[str], Dict[str, Dict[str, Any]]],
    airivo_has_role: Callable[[str], bool],
    airivo_guard_action: Callable[[str, str, str, str], bool],
    airivo_append_action_audit: Callable[..., None],
    db_manager: Any,
    permanent_db_path: str,
) -> None:
    repo_root = _repo_root()
    evolution_dir = repo_root / "evolution"
    log_path = repo_root / "auto_evolve.log"
    report_path = evolution_dir / "health_report.json"
    database_status: Dict[str, Any] = {}

    with st.expander("数据库状态", expanded=True):
        try:
            database_status = _read_database_status(connect_permanent_db)
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("股票总数", f"{int(database_status.get('stock_count', 0)):,}")
            with col2:
                st.metric("有数据股票", f"{int(database_status.get('data_stock_count', 0)):,}")
            with col3:
                st.metric("交易记录", f"{int(database_status.get('total_records', 0)):,}")
            with col4:
                min_date = database_status.get("min_date")
                max_date = database_status.get("max_date")
                if min_date and max_date:
                    st.metric("数据范围", f"{min_date}~{max_date}")
                else:
                    st.metric("数据范围", "无数据")
        except Exception as e:
            st.error(f"无法读取数据库状态: {e}")

    with st.expander("自动进化状态", expanded=False):
        try:
            status_cols = st.columns(3)
            lock_path = "/tmp/auto_evolve.lock"
            is_running = os.path.exists(lock_path)
            with status_cols[0]:
                st.metric("运行状态", "运行中" if is_running else "空闲")
            with status_cols[1]:
                st.metric("锁文件", "存在" if os.path.exists(lock_path) else "无")
            with status_cols[2]:
                st.metric("日志文件", "存在" if log_path.exists() else "无")

            show_logs = st.checkbox("显示最新日志", value=False, key="auto_evolve_show_logs")
            if show_logs:
                if log_path.exists():
                    try:
                        with log_path.open("r", encoding="utf-8") as f:
                            lines = f.readlines()[-120:]
                        st.code("".join(lines))
                    except Exception as e:
                        st.warning(f"无法读取日志: {e}")
                else:
                    st.info("未找到自动进化日志文件。")

            evolve_path = evolution_dir / "last_run.json"
            if evolve_path.exists():
                with evolve_path.open("r", encoding="utf-8") as f:
                    evolve = json.load(f)
                st.markdown(f"**最近运行时间**：{evolve.get('run_at', 'N/A')}")
                st.markdown(f"**综合评分**：{evolve.get('score', 0):.2f}")
                params = evolve.get("params", {})
                stats = evolve.get("stats", {})
                col_a, col_b, col_c, col_d = st.columns(4)
                with col_a:
                    st.metric("阈值", params.get("score_threshold", "—"))
                with col_b:
                    st.metric("持仓天数", params.get("max_holding_days", "—"))
                with col_c:
                    st.metric("止损%", params.get("stop_loss_pct", "—"))
                with col_d:
                    st.metric("止盈%", params.get("take_profit_pct", "—"))
                st.caption("自动进化仅更新后台参数。")
                if stats:
                    st.markdown("**回测摘要**")
                    st.write(
                        {
                            "总信号": stats.get("total_signals"),
                            "胜率(%)": stats.get("win_rate"),
                            "加权平均收益(%)": stats.get("weighted_avg_return"),
                            "夏普比率": stats.get("sharpe_ratio"),
                            "最大回撤(%)": stats.get("max_drawdown"),
                        }
                    )
                st.markdown("**参数回滚（最近一次备份）**")
                rb1, rb2 = st.columns([2, 1])
                with rb1:
                    rollback_strategy = st.selectbox(
                        "选择回滚策略",
                        ["V4", "V5", "V6", "V7", "V8", "V9", "COMBO", "STABLE_UPTREND", "AI_V5", "AI_V2"],
                        key="auto_evolve_rollback_strategy",
                    )
                with rb2:
                    if st.button("回滚到上一个稳定版本", key="auto_evolve_rollback_btn", use_container_width=True):
                        ok, msg = rollback_latest_promoted_params(rollback_strategy)
                        if ok:
                            st.success(msg)
                            st.rerun()
                        else:
                            st.error(msg)
            else:
                st.info("未发现自动进化结果文件。后台任务未运行或尚未生成。")
        except Exception as e:
            st.error(f"读取自动进化结果失败: {e}")

    with st.expander("资金类数据开关", expanded=False):
        st.caption("当资金接口延迟严重时，可关闭资金加分与相关健康告警。")
        enable_funds = st.checkbox("启用资金类加分", value=fund_bonus_enabled(), key="enable_fund_bonus")
        if enable_funds:
            st.success("资金类加分已启用")
        else:
            st.warning("资金类加分已关闭（健康检测将忽略资金表）")

    with st.expander("自动健康检测", expanded=False):
        col_h1, col_h2, col_h3 = st.columns([1, 2, 2])
        with col_h1:
            run_now = st.button("立即检测", use_container_width=True, key="health_check_now")
        with col_h2:
            st.caption("手动检测会立即刷新报告。")

    with st.expander("交易日历检测", expanded=False):
        cal_date = get_last_trade_date_from_tushare()
        if cal_date:
            st.metric("交易日历最新交易日", cal_date)
            db_max = database_status.get("max_date")
            if db_max and db_max != "N/A":
                st.caption(f"数据库最新交易日：{db_max}")
                if str(db_max) < str(cal_date):
                    st.warning("数据库落后于交易日历，建议更新数据")
                else:
                    st.success("数据库与交易日历一致或领先")
        else:
            st.warning("交易日历获取失败（trade_cal），请检查 Tushare 连接")
        with col_h3:
            repair_now = st.button("一键修复资金表", use_container_width=True, key="health_repair_now", disabled=not airivo_has_role("admin"))

        report = None
        if run_now:
            report = compute_health_report(getattr(db_manager, "db_path", permanent_db_path))
            try:
                report_path.parent.mkdir(parents=True, exist_ok=True)
                with report_path.open("w", encoding="utf-8") as f:
                    json.dump(report, f, ensure_ascii=False, indent=2)
                st.success("健康报告已刷新")
            except Exception as e:
                st.error(f"写入健康报告失败: {e}")
        elif repair_now:
            if not airivo_guard_action("admin", "health_repair_now", target="funding_table", reason="manual_funding_repair"):
                st.stop()
            with st.spinner("正在修复资金表数据（可能需要1-3分钟）..."):
                repair = run_funding_repair(permanent_db_path)
                if "error" in repair:
                    airivo_append_action_audit("health_repair_now", False, target="funding_table", detail=str(repair["error"].get("error")))
                    st.error(f"修复失败: {repair['error'].get('error')}")
                else:
                    airivo_append_action_audit(
                        "health_repair_now",
                        True,
                        target="funding_table",
                        detail=f"success_count={sum(1 for r in repair.values() if r and r.get('success'))}",
                    )
                    ok_count = sum(1 for r in repair.values() if r and r.get("success"))
                    st.success(f"修复完成：成功 {ok_count}/{len(repair)}")
                    st.json(repair)
            report = compute_health_report(getattr(db_manager, "db_path", permanent_db_path))
            try:
                report_path.parent.mkdir(parents=True, exist_ok=True)
                with report_path.open("w", encoding="utf-8") as f:
                    json.dump(report, f, ensure_ascii=False, indent=2)
            except Exception:
                pass
        elif report_path.exists():
            try:
                with report_path.open("r", encoding="utf-8") as f:
                    report = json.load(f)
            except Exception as e:
                st.error(f"读取健康报告失败: {e}")

        if report:
            st.markdown(f"**最近检测时间**：{report.get('run_at', 'N/A')}")
            if report.get("ok"):
                st.success("系统健康：未发现明显异常")
            else:
                st.warning("发现异常，请根据提示处理")

            warnings = report.get("warnings", [])
            risk_stale_report = bool(((report.get("stats") or {}).get("risk_stale", False)))
            if warnings:
                st.markdown("**异常提示**")
                for warning in warnings:
                    if risk_stale_report and str(warning).startswith("risk sentinel="):
                        continue
                    st.markdown(f"- {warning}")

            stats = report.get("stats", {})
            if stats:
                col_s1, col_s2, col_s3 = st.columns(3)
                with col_s1:
                    st.metric("最新交易日", stats.get("last_trade_date", "N/A"))
                with col_s2:
                    st.metric("最新日记录数", stats.get("records_last_trade_date", "N/A"))
                with col_s3:
                    recent = stats.get("recent_trade_dates", [])
                    st.metric("近10交易日", f"{len(recent)}天")
                risk_run_at = stats.get("risk_run_at")
                risk_age_mins = stats.get("risk_age_mins")
                if risk_run_at:
                    if isinstance(risk_age_mins, int):
                        stale_note = "（历史状态，仅参考）" if risk_stale_report else ""
                        st.caption(f"风险哨兵时间：{risk_run_at} · 约 {risk_age_mins} 分钟前 {stale_note}")
                    else:
                        st.caption(f"风险哨兵时间：{risk_run_at}")

            if warnings:
                st.markdown("**建议处理**")
                tips = []
                for warning in warnings:
                    if risk_stale_report and str(warning).startswith("risk sentinel="):
                        continue
                    if "table missing" in warning or "not updated" in warning or "lagging" in warning:
                        tips.append("到「数据与系统」执行一次数据更新，并确保自动任务在收盘后运行。")
                    elif "records low" in warning:
                        tips.append("检查交易日是否完整，必要时执行深度更新（90天）。")
                    elif "win_rate low" in warning:
                        tips.append("检查评分阈值是否过低或市场环境偏弱，建议提高阈值或减少策略一致数。")
                    elif "max_drawdown high" in warning:
                        tips.append("考虑开启弱市空仓或提高止损严格度。")
                for tip in sorted(set(tips)):
                    st.markdown(f"- {tip}")
