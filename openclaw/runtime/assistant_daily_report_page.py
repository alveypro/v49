from __future__ import annotations

import ast
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional

import pandas as pd
import streamlit as st


def _oc_abs_path(path_value: Any) -> Optional[Path]:
    if not path_value:
        return None
    path_obj = Path(str(path_value))
    if path_obj.is_absolute():
        return path_obj
    return Path.cwd() / path_obj


def _normalize_result_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    if "strategies" in out.columns and "strategy" not in out.columns:
        out["strategy"] = out["strategies"].apply(lambda x: ",".join(x) if isinstance(x, list) else str(x or ""))
    if "reasons" in out.columns and "reason" not in out.columns:
        out["reason"] = out["reasons"].apply(lambda x: ",".join(x) if isinstance(x, list) else str(x or ""))
    return out


def _has_rich_detail(df: pd.DataFrame) -> bool:
    if df is None or df.empty:
        return False
    detail_cols = ["strategy", "reason", "weighted_score", "score", "strategies", "reasons"]
    for col in detail_cols:
        if col not in df.columns:
            continue
        series = df[col].astype(str).str.strip()
        series = series.replace({"": "nan", "None": "nan", "none": "nan", "nan": "nan", "NaN": "nan"})
        if (series != "nan").any():
            return True
    return False


def _extract_non_empty_result(exec_obj: dict) -> Optional[dict]:
    artifacts = exec_obj.get("artifacts") or {}
    csv_candidates: List[str] = []
    report_csv_paths = artifacts.get("report_csv_paths") or []
    if isinstance(report_csv_paths, list):
        csv_candidates.extend(report_csv_paths)
    elif isinstance(report_csv_paths, str) and report_csv_paths:
        csv_candidates.append(report_csv_paths)

    for csv_file in csv_candidates:
        csv_path = _oc_abs_path(csv_file)
        if not csv_path or not csv_path.exists():
            continue
        try:
            df_csv = pd.read_csv(csv_path)
            if not df_csv.empty and "ts_code" in df_csv.columns and _has_rich_detail(df_csv):
                return {"kind": "csv", "artifact": str(csv_path), "df": _normalize_result_df(df_csv)}
        except Exception:
            pass

    md_path = _oc_abs_path(artifacts.get("report_markdown"))
    if md_path and md_path.exists():
        try:
            rows = []
            in_opp = False
            for raw_line in md_path.read_text(encoding="utf-8").splitlines():
                line = raw_line.strip()
                if line.startswith("## Opportunities"):
                    in_opp = True
                    continue
                if in_opp and line.startswith("## "):
                    break
                if in_opp and line.startswith("- {"):
                    obj = ast.literal_eval(line[2:].strip())
                    if isinstance(obj, dict):
                        rows.append(obj)
            if rows:
                return {"kind": "markdown", "artifact": str(md_path), "df": _normalize_result_df(pd.DataFrame(rows))}
        except Exception:
            pass

    for csv_file in csv_candidates:
        csv_path = _oc_abs_path(csv_file)
        if not csv_path or not csv_path.exists():
            continue
        try:
            df_csv = pd.read_csv(csv_path)
            if not df_csv.empty and "ts_code" in df_csv.columns:
                return {"kind": "csv", "artifact": str(csv_path), "df": _normalize_result_df(df_csv)}
        except Exception:
            pass
    return None


def render_assistant_daily_report_page(*, assistant: Any) -> None:
    st.subheader("每日交易报告")

    if st.button("生成报告", type="primary"):
        with st.spinner("生成中..."):
            report = assistant.generate_daily_report()
            st.session_state["daily_report"] = report
            st.success("报告生成完成")

    if "daily_report" in st.session_state:
        st.code(st.session_state["daily_report"], language="text")
        filename = f"trading_report_{datetime.now().strftime('%Y%m%d')}.txt"
        st.download_button(label=" 下载报告", data=st.session_state["daily_report"], file_name=filename, mime="text/plain")

    st.markdown("---")
    st.markdown("### OC 执行追踪（partner_execution）")
    st.caption("展示 oc daily 产物中的 tracking.record / tracking.refresh / tracking.scoreboard")

    try:
        oc_log_dir = Path("logs/openclaw")
        sum_files = sorted(oc_log_dir.glob("run_summary_*.json"), reverse=True)
        col_a, col_b = st.columns([1, 2])
        with col_a:
            if st.button("补跑追踪（非交易日可用）", key="assistant_tracking_rerun"):
                if not sum_files:
                    st.warning("未找到 run_summary 文件，无法补跑。请先执行一次 oc daily。")
                else:
                    latest_sum = str(sum_files[0])
                    for spth in sum_files[:20]:
                        try:
                            sobj = json.loads(spth.read_text(encoding="utf-8"))
                            scan_picks = (((sobj.get("scan") or {}).get("result") or {}).get("picks") or [])
                            opps = sobj.get("opportunities") or []
                            if (isinstance(scan_picks, list) and len(scan_picks) > 0) or (isinstance(opps, list) and len(opps) > 0):
                                latest_sum = str(spth)
                                break
                        except Exception:
                            pass
                    cmd = [
                        sys.executable,
                        "openclaw/strategy_tracking_cli.py",
                        "run-all",
                        "--run-summary",
                        latest_sum,
                        "--output-dir",
                        str(oc_log_dir),
                    ]
                    try:
                        res = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
                        if res.returncode == 0:
                            obj = json.loads((res.stdout or "").strip() or "{}")
                            st.session_state["assistant_tracking_rerun_result"] = obj
                            st.success("补跑完成。已刷新 tracking 看板。")
                        else:
                            st.warning(f"补跑失败：{(res.stderr or res.stdout or '').strip()[:500]}")
                    except Exception as exc:
                        st.warning(f"补跑异常：{exc}")
        with col_b:
            latest_sum_text = str(sum_files[0]) if sum_files else "N/A"
            st.caption(f"最新 run_summary：{latest_sum_text}")

        if "assistant_tracking_rerun_result" in st.session_state:
            with st.expander("最近一次补跑结果", expanded=False):
                st.json(st.session_state["assistant_tracking_rerun_result"])

        exec_files = sorted(oc_log_dir.glob("partner_execution_*.json"), reverse=True)
        if not exec_files:
            st.info("未找到 partner_execution 日志。请先执行一次 oc daily。")
            return

        options = [str(p) for p in exec_files[:30]]
        default_idx = 0
        exec_obj_cache = {}
        latest_non_empty = None
        for idx, opt in enumerate(options):
            try:
                obj = json.loads(Path(opt).read_text(encoding="utf-8"))
                exec_obj_cache[opt] = obj
                non_empty = _extract_non_empty_result(obj)
                if latest_non_empty is None and non_empty is not None:
                    default_idx = idx
                    latest_non_empty = {"exec": opt, "kind": non_empty["kind"], "artifact": non_empty["artifact"], "df": non_empty["df"]}
            except Exception:
                continue

        selected_exec = st.selectbox("选择执行文件", options=options, index=default_idx, key="assistant_partner_exec_select")
        exec_obj = exec_obj_cache.get(selected_exec)
        if exec_obj is None:
            exec_obj = json.loads(Path(selected_exec).read_text(encoding="utf-8"))
        tracking = exec_obj.get("tracking") or {}
        tracking_by_strategy = exec_obj.get("tracking_by_strategy") or []
        record = tracking.get("record") or {}
        refresh = tracking.get("refresh") or {}
        scoreboard = tracking.get("scoreboard") or {}

        record_reason = str(record.get("reason") or "")
        refresh_reason = str(refresh.get("reason") or "")
        score_reason = str(scoreboard.get("reason") or "")
        inserted = int(record.get("inserted") or 0)
        evaluated = int(refresh.get("evaluated") or 0)
        rows = int(scoreboard.get("rows") or 0)

        st.markdown("#### 人话结论")
        summary_lines: List[str] = []
        next_action = "继续正常使用，等待下一次交易日数据刷新。"
        if not tracking:
            summary_lines.append("当前文件没有追踪数据。")
            next_action = "换一个更新的执行文件，或点击上方“补跑追踪”。"
        else:
            if inserted > 0:
                summary_lines.append(f"本次已记录新信号 {inserted} 条。")
            elif record_reason in {"no_picks", "no_run_summary"}:
                summary_lines.append("本次没有可记录新信号（常见于当日无新标的）。")
            else:
                summary_lines.append("本次信号记录量为 0。")

            if evaluated > 0:
                summary_lines.append(f"已完成 {evaluated} 条信号评估。")
            elif refresh_reason == "no_signals":
                summary_lines.append("当前没有可评估信号。")
            else:
                summary_lines.append("评估阶段暂无新增结果。")

            if rows > 0:
                summary_lines.append(f"看板已生成，共 {rows} 行。")
                next_action = "可直接展开下方 Markdown/CSV 看板查看策略表现。"
            elif score_reason == "no_performance_rows":
                summary_lines.append("看板暂为空：样本还没走完 T+N 窗口（例如 T+5/T+10）。")
                next_action = "等待 1-2 个交易日后再看，或继续积累信号样本。"
            else:
                summary_lines.append("看板暂未生成。")

        for line in summary_lines:
            st.markdown(f"- {line}")
        st.caption(f"下一步建议：{next_action}")

        st.markdown("#### 按策略跟踪状态")
        rows_tracking = []
        if isinstance(tracking_by_strategy, list) and tracking_by_strategy:
            for item in tracking_by_strategy:
                if not isinstance(item, dict):
                    continue
                sname = str(item.get("strategy") or "").strip() or "unknown"
                result = item.get("result") or {}
                rec = (result.get("record") or {}) if isinstance(result, dict) else {}
                ref = (result.get("refresh") or {}) if isinstance(result, dict) else {}
                rows_tracking.append(
                    {
                        "策略": sname,
                        "新增信号": int(rec.get("inserted") or 0),
                        "待结算": int(ref.get("skipped_no_price") or 0),
                        "已结算": int(ref.get("skipped_existing") or 0) + int(ref.get("evaluated") or 0),
                    }
                )
        else:
            rows_tracking.append(
                {
                    "策略": str(exec_obj.get("strategy") or "latest"),
                    "新增信号": inserted,
                    "待结算": int(refresh.get("skipped_no_price") or 0),
                    "已结算": int(refresh.get("skipped_existing") or 0) + evaluated,
                }
            )
        try:
            df_track = pd.DataFrame(rows_tracking)
            st.dataframe(df_track, use_container_width=True, hide_index=True)
        except Exception:
            st.table(rows_tracking)

        st.markdown("#### 最近一次非空选股结果（自动）")
        if latest_non_empty is None:
            st.info("最近 30 次执行里暂未找到非空选股结果。")
        else:
            st.caption(f"来源执行文件：{latest_non_empty['exec']}")
            st.caption(f"来源产物：{latest_non_empty['artifact']}")
            result_df = latest_non_empty["df"]
            if result_df is None or result_df.empty:
                st.info("该结果无可展示行。")
            else:
                show_cols = [c for c in ["ts_code", "weighted_score", "score", "strategy", "reason", "reasons"] if c in result_df.columns]
                if not show_cols:
                    show_cols = list(result_df.columns)[:8]
                st.dataframe(result_df[show_cols], use_container_width=True)

        if not tracking:
            st.warning("该执行文件没有 tracking 字段。请选更新的 partner_execution 文件，或点击上方“补跑追踪”。")
        elif inserted == 0 and record_reason in {"no_picks", "no_run_summary"}:
            st.info("本次没有新信号可记录（no_picks），这是正常现象。")
        elif inserted > 0:
            st.success(f"本次新记录信号：{inserted} 条。")

        if evaluated == 0 and refresh_reason == "no_signals":
            st.info("当前没有可评估信号（no_signals）。先累计几次选股后会自动出现。")
        elif evaluated == 0 and (score_reason == "no_performance_rows" or rows == 0):
            st.info("样本还没走完 T+N（例如 T+5/T+10），因此看板暂时为空。")
        elif rows > 0:
            st.success(f"策略看板已生成：{rows} 行。")

        with st.expander("技术明细（tracking JSON）", expanded=False):
            col_tr1, col_tr2, col_tr3 = st.columns(3)
            with col_tr1:
                st.markdown("**tracking.record**")
                st.json(record)
            with col_tr2:
                st.markdown("**tracking.refresh**")
                st.json(refresh)
            with col_tr3:
                st.markdown("**tracking.scoreboard**")
                st.json(scoreboard)

        sb_md = scoreboard.get("markdown") or (exec_obj.get("artifacts") or {}).get("strategy_scoreboard_markdown") or ""
        sb_csv = scoreboard.get("csv") or (exec_obj.get("artifacts") or {}).get("strategy_scoreboard_csv") or ""
        st.markdown("#### Scoreboard 文件")
        c_sb1, c_sb2 = st.columns(2)
        with c_sb1:
            st.code(str(sb_md or "N/A"), language="text")
            if sb_md and Path(sb_md).exists():
                try:
                    md_content = Path(sb_md).read_text(encoding="utf-8")
                    md_content = (
                        md_content.replace("Strategy Scoreboard", "策略评分看板")
                        .replace("lookback_days", "回看天数")
                        .replace("rows", "行数")
                        .replace("strategy", "策略")
                        .replace("horizon_days", "期限(天)")
                        .replace("samples", "样本数")
                        .replace("win_rate_pct", "胜率(%)")
                        .replace("avg_ret_pct", "平均收益(%)")
                        .replace("median_ret_pct", "中位收益(%)")
                        .replace("avg_excess_ret_pct", "平均超额收益(%)")
                    )
                    with st.expander("预览 Markdown 看板", expanded=False):
                        st.markdown(md_content)
                except Exception as exc:
                    st.warning(f"读取 markdown 失败：{exc}")
        with c_sb2:
            st.code(str(sb_csv or "N/A"), language="text")
            if sb_csv and Path(sb_csv).exists():
                try:
                    df_sb = pd.read_csv(sb_csv)
                    with st.expander("预览 CSV 看板", expanded=False):
                        if df_sb.empty:
                            st.info("当前看板为空（通常是样本还未走完 T+N）。")
                        else:
                            rename = {
                                "strategy": "策略",
                                "horizon_days": "期限(天)",
                                "samples": "样本数",
                                "win_rate_pct": "胜率(%)",
                                "avg_ret_pct": "平均收益(%)",
                                "median_ret_pct": "中位收益(%)",
                                "avg_excess_ret_pct": "平均超额收益(%)",
                            }
                            st.dataframe(df_sb.rename(columns=rename), use_container_width=True)
                except Exception as exc:
                    st.warning(f"读取 csv 失败：{exc}")
    except Exception as e:
        st.warning(f"执行追踪展示异常：{e}")
