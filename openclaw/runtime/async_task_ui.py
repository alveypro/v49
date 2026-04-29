from __future__ import annotations

from datetime import datetime
import json
import hashlib
import os
import re
import signal
from typing import Any, Callable, Dict, Optional

import pandas as pd
import streamlit as st


def render_async_scan_status(
    *,
    task_key: str,
    title: str,
    score_col: str,
    get_async_scan_task: Callable[[str], Optional[Dict[str, Any]]],
    recover_async_scan_task: Callable[[str], Optional[Dict[str, Any]]],
    is_pid_alive: Callable[[int], bool],
    update_async_scan_task: Callable[..., None],
    now_ts: Callable[[], float],
    read_async_scan_df: Callable[[Dict[str, Any]], Optional[pd.DataFrame]],
    standardize_result_df: Callable[[pd.DataFrame, str], pd.DataFrame],
    df_to_csv_bytes: Callable[[pd.DataFrame], bytes],
    set_stock_pool_candidate: Callable[[str, Dict[str, Any], str, pd.DataFrame], None],
) -> Optional[pd.DataFrame]:
    run_id = st.session_state.get(task_key, "")
    if not run_id:
        return None
    task = get_async_scan_task(run_id)
    if not task:
        task = recover_async_scan_task(run_id)
        if not task:
            st.warning(f"后台任务 {run_id} 不存在，可能未成功提交或已清理。请重新发起扫描。")
            st.session_state[task_key] = ""
            return None
    else:
        if str(task.get("status", "")) in {"failed", "cancelled"}:
            recovered = recover_async_scan_task(run_id)
            if recovered and str(recovered.get("status", "")) == "success":
                task = recovered
        elif str(task.get("status", "")) in {"queued", "running"}:
            pid = int(task.get("pid", 0) or 0)
            if pid > 0 and not is_pid_alive(pid):
                recovered = recover_async_scan_task(run_id)
                if recovered:
                    task = recovered

    status_now = str(task.get("status", ""))
    msg_now = str(task.get("message", ""))
    if status_now == "failed" and "服务重启前中断" in msg_now:
        st.session_state[task_key] = ""
        return None

    st.markdown("---")
    st.markdown(f"### 后台扫描任务（{title}）")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("任务ID", run_id)
    with c2:
        st.metric("状态", str(task.get("status", "-")))
    with c3:
        st.metric("进度", f"{int(task.get('progress', 0))}%")
    with c4:
        st.metric("结果数", f"{int(task.get('row_count', 0))} 条")
    st.progress(max(0, min(100, int(task.get("progress", 0)))) / 100.0)
    status = str(task.get("status", ""))
    if status in {"queued", "running"}:
        cancel_key = f"cancel_{task_key}_{run_id}"
        if st.button("取消当前后台任务", key=cancel_key, use_container_width=False):
            pid = int(task.get("pid", 0) or 0)
            if pid > 0 and is_pid_alive(pid):
                try:
                    os.killpg(pid, signal.SIGTERM)
                except Exception:
                    try:
                        os.kill(pid, signal.SIGTERM)
                    except Exception:
                        pass
            update_async_scan_task(
                run_id,
                status="cancelled",
                stage="cancelled",
                progress=100,
                message="任务已手动取消",
                ended_at=now_ts(),
            )
            st.session_state[task_key] = ""
            st.warning("已取消当前任务。请调整参数后重新点击“开始扫描”。")
            st.rerun()

    if status in {"failed", "cancelled"}:
        recovered = recover_async_scan_task(run_id)
        if recovered and str(recovered.get("status", "")) == "success":
            task = recovered
            status = "success"
        else:
            maybe_df = read_async_scan_df(task)
            if maybe_df is not None and not maybe_df.empty:
                task = dict(task)
                task["status"] = "success"
                task["message"] = "已检测到结果文件，自动恢复成功状态"
                update_async_scan_task(run_id, status="success", message=task["message"])
                status = "success"

    if status not in {"failed", "cancelled"}:
        st.caption(str(task.get("message", "")))

    if status in {"queued", "running"}:
        if hasattr(st, "autorefresh"):
            st.autorefresh(interval=5000, key=f"refresh_{run_id}")
        else:
            st.info("任务运行中，请手动刷新页面查看进度。")
        return None
    if status == "failed":
        msg = str(task.get("message", "后台任务失败"))
        st.error(msg)
        err = str(task.get("error", "") or "")
        if err:
            with st.expander("查看错误详情", expanded=False):
                st.code(err)
        return None
    if status != "success":
        return None

    result_df = read_async_scan_df(task)
    if result_df is None or result_df.empty:
        st.warning("后台扫描完成，但结果为空")
        return pd.DataFrame()

    st.success(f"后台扫描完成，返回 {len(result_df)} 条")
    show_df = result_df.drop(columns=["原始数据"], errors="ignore")
    show_df = standardize_result_df(show_df, score_col=score_col)
    export_cols = [c for c in show_df.columns if not str(c).startswith("_")]
    export_df = show_df[export_cols].copy() if export_cols else show_df.copy()
    safe_title = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff_-]+", "_", str(title or "async_scan")).strip("_")
    safe_run_id = re.sub(r"[^0-9A-Za-z_-]+", "_", str(run_id or "latest")).strip("_")
    st.download_button(
        "下载本次扫描结果（CSV）",
        data=df_to_csv_bytes(export_df),
        file_name=f"{safe_title}_后台扫描结果_{safe_run_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv; charset=utf-8",
        key=f"download_async_scan_{task_key}_{safe_run_id}",
        use_container_width=True,
    )
    result_csv = str(task.get("result_csv", "") or "")
    if result_csv:
        st.caption(f"服务器结果文件：{result_csv}")
    st.dataframe(show_df, use_container_width=True, hide_index=True)
    set_stock_pool_candidate(
        strategy=str(task.get("strategy", "") or "").lower(),
        params=task.get("params", {}) or {},
        score_col=str(task.get("score_col", score_col) or score_col),
        df=result_df,
    )
    return result_df


def render_async_task_dashboard(
    *,
    limit: int,
    cleanup_async_backtest_jobs: Callable[[], None],
    list_recent_async_scan_tasks: Callable[[int], pd.DataFrame],
    list_recent_async_backtest_jobs: Callable[[int], pd.DataFrame],
    prune_all_finished_async_tasks: Callable[[], Dict[str, int]],
) -> None:
    cleanup_async_backtest_jobs()
    scan_df = list_recent_async_scan_tasks(limit=limit)
    backtest_df = list_recent_async_backtest_jobs(limit=limit)
    merged = pd.concat([scan_df, backtest_df], ignore_index=True) if (not scan_df.empty or not backtest_df.empty) else pd.DataFrame()
    with st.expander("最近后台任务", expanded=False):
        action_col1, action_col2 = st.columns([1, 3])
        with action_col1:
            if st.button("清理已完成任务", key="prune_finished_async_tasks", use_container_width=True):
                removed = prune_all_finished_async_tasks()
                st.success(f"已清理扫描 {removed['scan']} 个，回测 {removed['backtest']} 个。")
                st.rerun()
        with action_col2:
            st.caption("仅清理 success / failed / cancelled 任务，不影响运行中的后台任务。")
        if merged.empty:
            st.caption("暂无后台扫描/回测任务记录。")
            return
        show_df = merged.copy()
        show_df["错误"] = show_df["错误"].astype(str).str.slice(0, 120)
        total = len(show_df)
        running_count = int(show_df["状态"].astype(str).isin(["queued", "running"]).sum())
        success_count = int(show_df["状态"].astype(str).eq("success").sum())
        failed_count = int(show_df["状态"].astype(str).eq("failed").sum())
        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.metric("任务总数", total)
        with m2:
            st.metric("运行中", running_count)
        with m3:
            st.metric("成功", success_count)
        with m4:
            st.metric("失败", failed_count)
        st.dataframe(show_df, use_container_width=True, hide_index=True)
        running_df = show_df[show_df["状态"].astype(str).isin(["queued", "running"])]
        if not running_df.empty:
            st.caption(f"仍在运行任务数：{len(running_df)}")
            if hasattr(st, "autorefresh"):
                st.autorefresh(interval=5000, key="async_task_dashboard_refresh")


def scan_params_fingerprint(params: Dict[str, Any]) -> str:
    try:
        text = json.dumps(params or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    except Exception:
        text = str(params or {})
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def sync_scan_task_with_params(
    *,
    task_key: str,
    params: Dict[str, Any],
    strategy_label: str,
    get_async_scan_task: Callable[[str], Optional[Dict[str, Any]]],
    recover_async_scan_task: Callable[[str], Optional[Dict[str, Any]]],
) -> None:
    fp_key = f"{task_key}__params_fp"
    dirty_key = f"{task_key}__params_dirty"
    msg_key = f"{task_key}__params_msg"
    current_fp = scan_params_fingerprint(params)
    prev_fp = st.session_state.get(fp_key)
    st.session_state[fp_key] = current_fp

    if prev_fp is None:
        st.session_state.setdefault(dirty_key, False)
        return
    if prev_fp == current_fp:
        return

    st.session_state[dirty_key] = True
    run_id = str(st.session_state.get(task_key, "") or "")
    if run_id:
        task = get_async_scan_task(run_id)
        if not task:
            task = recover_async_scan_task(run_id)
        if task and str(task.get("status", "")) in {"queued", "running"}:
            st.session_state[msg_key] = (
                f"{strategy_label}参数已变更：当前任务继续按旧参数运行；新参数将在你下次点击“开始扫描”时生效。"
            )
            return
        st.session_state[task_key] = ""
    st.session_state[msg_key] = f"{strategy_label}参数已变更：旧任务状态已清除，点击“开始扫描”后才会提交新任务。"


def render_scan_param_hint(*, task_key: str) -> None:
    msg_key = f"{task_key}__params_msg"
    dirty_key = f"{task_key}__params_dirty"
    msg = str(st.session_state.get(msg_key, "") or "")
    if msg:
        st.info(msg)
        st.session_state[msg_key] = ""
    elif st.session_state.get(dirty_key, False):
        st.caption("参数已变更，尚未开始扫描。")


def mark_scan_submitted(*, task_key: str, params: Dict[str, Any]) -> None:
    st.session_state[f"{task_key}__params_fp"] = scan_params_fingerprint(params)
    st.session_state[f"{task_key}__params_dirty"] = False
    st.session_state[f"{task_key}__params_msg"] = ""
