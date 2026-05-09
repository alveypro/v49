from __future__ import annotations

from datetime import datetime
import json
import os
import shutil
import subprocess
import sys
import traceback
import uuid
from typing import Any, Callable, Dict, List, Tuple

import pandas as pd


def launch_async_scan_process(
    *,
    app_root: str,
    run_id: str,
    strategy: str,
    params: Dict[str, Any],
    score_col: str,
    async_scan_log_paths: Callable[[str], Tuple[str, str]],
    build_async_scan_env: Callable[[str, Dict[str, Any]], Dict[str, Any]],
    python_executable: str | None = None,
) -> Dict[str, Any]:
    py_bin = python_executable or os.path.join(app_root, ".venv", "bin", "python")
    if not os.path.exists(py_bin):
        py_bin = shutil.which("python3") or "/usr/bin/python3"
    stdout_log, stderr_log = async_scan_log_paths(run_id)
    env = os.environ.copy()
    env.update(
        {
            "OPENCLAW_ASYNC_SCAN_WORKER": "1",
            "OPENCLAW_ASYNC_SCAN_RUN_ID": str(run_id),
            "OPENCLAW_ASYNC_SCAN_STRATEGY": str(strategy),
            "OPENCLAW_ASYNC_SCAN_SCORE_COL": str(score_col),
        }
    )
    env.update({k: str(v) for k, v in build_async_scan_env(strategy, params).items()})
    with open(stdout_log, "ab") as out_f, open(stderr_log, "ab") as err_f:
        proc = subprocess.Popen(
            [py_bin, os.path.join(app_root, "v49_app.py")],
            cwd=app_root,
            env=env,
            stdout=out_f,
            stderr=err_f,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
    return {"pid": int(proc.pid), "stdout_log": stdout_log, "stderr_log": stderr_log}


def start_async_scan_task(
    *,
    strategy: str,
    params: Dict[str, Any],
    score_col: str,
    async_scan_tasks: Dict[str, Dict[str, Any]],
    async_scan_lock: Any,
    cleanup_async_scan_tasks: Callable[[], None],
    scan_params_fingerprint: Callable[[Dict[str, Any]], str],
    now_ts: Callable[[], float],
    now_text: Callable[[], str],
    async_scan_state_path: Callable[[str], str],
    persist_async_scan_task: Callable[[str], None],
    launch_async_scan_process: Callable[[str, str, Dict[str, Any], str], Dict[str, Any]],
    merge_async_scan_task: Callable[..., Dict[str, Any]],
    run_id_factory: Callable[[str], str] | None = None,
) -> Tuple[bool, str, str]:
    cleanup_async_scan_tasks()
    for rid, task in list(async_scan_tasks.items()):
        if task.get("strategy") != strategy:
            continue
        if task.get("status") not in {"queued", "running"}:
            continue
        old_params = task.get("params", {}) or {}
        if scan_params_fingerprint(old_params) == scan_params_fingerprint(params or {}):
            return False, f"同参数任务已在运行：{rid}", str(rid)
    for rid, task in list(async_scan_tasks.items()):
        if task.get("strategy") == strategy and task.get("status") == "running":
            return False, f"已有运行中任务：{rid}。请先等待完成或点击“取消当前后台任务”。", str(rid)

    run_id = run_id_factory(strategy) if run_id_factory is not None else f"{strategy}_{datetime.now().strftime('%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    invalidated_ids: List[str] = []
    task_data: Dict[str, Any] = {}
    with async_scan_lock:
        for rid, task in list(async_scan_tasks.items()):
            if task.get("strategy") == strategy and task.get("status") in {"queued"}:
                async_scan_tasks[rid]["status"] = "cancelled"
                async_scan_tasks[rid]["stage"] = "cancelled"
                async_scan_tasks[rid]["progress"] = 100
                async_scan_tasks[rid]["message"] = f"已被新任务替代：{run_id}"
                async_scan_tasks[rid]["ended_at"] = now_ts()
                invalidated_ids.append(rid)
        task_data = {
            "run_id": run_id,
            "strategy": strategy,
            "status": "queued",
            "stage": "queued",
            "progress": 0,
            "message": "排队中",
            "created_at": now_ts(),
            "created_at_text": now_text(),
            "ended_at": 0,
            "params": params or {},
            "score_col": score_col,
            "result_csv": "",
            "meta_json": "",
            "state_json": async_scan_state_path(run_id),
            "meta": {},
            "error": "",
            "row_count": 0,
        }
        async_scan_tasks[run_id] = dict(task_data)
    for rid in invalidated_ids:
        persist_async_scan_task(rid)
    persist_async_scan_task(run_id)
    try:
        launch_info = launch_async_scan_process(run_id, strategy, params or {}, score_col)
        merge_async_scan_task(
            run_id,
            task_data,
            status="running",
            stage="scan",
            progress=1,
            message="任务已启动",
            pid=int(launch_info["pid"]),
            worker_started_at=now_text(),
            stdout_log=str(launch_info["stdout_log"]),
            stderr_log=str(launch_info["stderr_log"]),
        )
    except Exception as exc:
        merge_async_scan_task(
            run_id,
            task_data,
            status="failed",
            stage="error",
            progress=100,
            message=f"任务启动失败: {exc}",
            error=traceback.format_exc(limit=12),
            ended_at=now_ts(),
        )
        return False, f"后台任务启动失败：{exc}", run_id
    return True, f"已提交后台扫描任务：{run_id}", run_id


def run_async_scan_job(
    *,
    run_id: str,
    strategy: str,
    params: Dict[str, Any],
    score_col: str,
    result_dir: str,
    cancelled_error: str,
    get_async_scan_task: Callable[[str], Dict[str, Any] | None],
    update_async_scan_task: Callable[..., None],
    build_async_scan_env: Callable[[str, Dict[str, Any]], Dict[str, Any]],
    run_scan: Callable[[str, Dict[str, Any], Dict[str, Any]], Tuple[pd.DataFrame | None, Dict[str, Any]]],
    now_text: Callable[[], str],
    now_ts: Callable[[], float],
    set_current_run_id: Callable[[str], None] | None = None,
    record_signal_chain: Callable[..., None] | None = None,
) -> None:
    current = get_async_scan_task(run_id)
    if current and str(current.get("status", "")) == "cancelled":
        return
    update_async_scan_task(run_id, status="running", stage="scan", progress=1, message="任务已启动")
    try:
        os.makedirs(result_dir, exist_ok=True)
        env_overrides = build_async_scan_env(strategy, params)
        if set_current_run_id is not None:
            set_current_run_id(run_id)
        result_df, meta = run_scan(strategy, params, env_overrides)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        result_csv = os.path.join(result_dir, f"{strategy}_{run_id}_{ts}.csv")
        meta_json = os.path.join(result_dir, f"{strategy}_{run_id}_{ts}.meta.json")
        row_count = int(len(result_df)) if isinstance(result_df, pd.DataFrame) else 0
        if isinstance(result_df, pd.DataFrame):
            result_df.to_csv(result_csv, index=False)
        else:
            result_csv = ""
        meta_out = {
            "run_id": run_id,
            "strategy": strategy,
            "status": "success",
            "score_col": score_col,
            "row_count": row_count,
            "finished_at": now_text(),
            "params": params,
            "meta": meta or {},
            "result_csv": result_csv,
            "meta_json": meta_json,
        }
        with open(meta_json, "w", encoding="utf-8") as f:
            json.dump(meta_out, f, ensure_ascii=False, indent=2)
        if record_signal_chain is not None:
            record_signal_chain(
                run_id=run_id,
                strategy=strategy,
                params=params,
                score_col=score_col,
                result_df=result_df,
                meta=meta or {},
                result_csv=result_csv,
                meta_json=meta_json,
                row_count=row_count,
            )
        update_async_scan_task(
            run_id,
            status="success",
            stage="done",
            progress=100,
            message=f"扫描完成：{row_count} 条结果",
            ended_at=now_ts(),
            result_csv=result_csv,
            meta_json=meta_json,
            row_count=row_count,
            meta=meta or {},
        )
    except Exception as exc:
        if cancelled_error in str(exc):
            update_async_scan_task(
                run_id,
                status="cancelled",
                stage="cancelled",
                progress=100,
                message="任务已取消（参数变更或新任务替代）",
                ended_at=now_ts(),
            )
            return
        update_async_scan_task(
            run_id,
            status="failed",
            stage="error",
            progress=100,
            message=f"任务失败: {exc}",
            error=traceback.format_exc(limit=12),
            ended_at=now_ts(),
        )
    finally:
        if set_current_run_id is not None:
            set_current_run_id("")


def run_async_scan_worker_main(
    *,
    load_async_scan_state: Callable[[str], Dict[str, Any]],
    update_async_scan_task: Callable[..., None],
    run_async_scan_job: Callable[[str, str, Dict[str, Any], str], None],
    now_text: Callable[[], str],
    now_ts: Callable[[], float],
    process_id: Callable[[], int] = os.getpid,
    stderr_write: Callable[[str], Any] = sys.stderr.write,
    set_current_run_id: Callable[[str], None] | None = None,
) -> int:
    run_id = str(os.getenv("OPENCLAW_ASYNC_SCAN_RUN_ID", "") or "").strip()
    strategy = str(os.getenv("OPENCLAW_ASYNC_SCAN_STRATEGY", "") or "").strip().lower()
    score_col = str(os.getenv("OPENCLAW_ASYNC_SCAN_SCORE_COL", "综合评分") or "综合评分")
    if not run_id or not strategy:
        stderr_write("missing async scan worker env\n")
        return 2
    if set_current_run_id is not None:
        set_current_run_id(run_id)
    try:
        update_async_scan_task(
            run_id,
            status="running",
            stage="scan",
            progress=1,
            message="任务已启动",
            pid=process_id(),
            worker_started_at=now_text(),
        )
        state = load_async_scan_state(run_id)
        run_async_scan_job(run_id, strategy, dict(state.get("params") or {}), score_col)
        return 0
    except Exception:
        update_async_scan_task(
            run_id,
            status="failed",
            stage="error",
            progress=100,
            message="任务失败: worker crashed",
            error=traceback.format_exc(limit=20),
            ended_at=now_ts(),
            pid=process_id(),
        )
        return 1
    finally:
        if set_current_run_id is not None:
            set_current_run_id("")


def launch_async_backtest_process(
    *,
    app_root: str,
    run_id: str,
    job_kind: str,
    async_backtest_log_paths: Callable[[str], Tuple[str, str]],
    python_executable: str | None = None,
) -> Dict[str, Any]:
    py_bin = python_executable or os.path.join(app_root, ".venv", "bin", "python")
    if not os.path.exists(py_bin):
        py_bin = shutil.which("python3") or "/usr/bin/python3"
    stdout_log, stderr_log = async_backtest_log_paths(run_id)
    env = os.environ.copy()
    env.update(
        {
            "OPENCLAW_ASYNC_BACKTEST_WORKER": "1",
            "OPENCLAW_ASYNC_BACKTEST_RUN_ID": str(run_id),
            "OPENCLAW_ASYNC_BACKTEST_JOB_KIND": str(job_kind),
        }
    )
    with open(stdout_log, "ab") as out_f, open(stderr_log, "ab") as err_f:
        proc = subprocess.Popen(
            [py_bin, os.path.join(app_root, "v49_app.py")],
            cwd=app_root,
            env=env,
            stdout=out_f,
            stderr=err_f,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
    return {"pid": int(proc.pid), "stdout_log": stdout_log, "stderr_log": stderr_log}


def start_async_backtest_job(
    *,
    job_kind: str,
    payload: Dict[str, Any],
    async_backtest_jobs: Dict[str, Dict[str, Any]],
    async_backtest_lock: Any,
    now_ts: Callable[[], float],
    now_text: Callable[[], str],
    merge_async_backtest_job: Callable[..., Dict[str, Any]],
    launch_async_backtest_process: Callable[[str, str, Dict[str, Any]], Dict[str, Any]],
    run_id_factory: Callable[[str], str] | None = None,
) -> Tuple[bool, str, str]:
    with async_backtest_lock:
        for rid, job in list(async_backtest_jobs.items()):
            if str(job.get("status")) == "running":
                return False, "已有回测任务在运行，请等待完成", ""
        run_id = run_id_factory(str(job_kind)) if run_id_factory is not None else datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
        job_data = {
            "run_id": run_id,
            "job_kind": str(job_kind),
            "payload": dict(payload or {}),
            "status": "queued",
            "created_at": now_ts(),
            "ended_at": 0.0,
            "result": None,
            "error": "",
            "traceback": "",
        }
        async_backtest_jobs[run_id] = dict(job_data)
    merge_async_backtest_job(run_id, job_data)
    try:
        launch_info = launch_async_backtest_process(run_id, str(job_kind), payload or {})
        merge_async_backtest_job(
            run_id,
            job_data,
            status="running",
            pid=int(launch_info["pid"]),
            worker_started_at=now_text(),
            stdout_log=str(launch_info["stdout_log"]),
            stderr_log=str(launch_info["stderr_log"]),
        )
    except Exception as exc:
        merge_async_backtest_job(
            run_id,
            job_data,
            status="failed",
            error=str(exc),
            traceback=traceback.format_exc(),
            ended_at=now_ts(),
        )
        return False, f"后台回测启动失败：{exc}", run_id
    return True, f"后台回测已启动（任务ID={run_id}）", run_id


def run_async_backtest_worker_main(
    *,
    load_async_backtest_state: Callable[[str], Dict[str, Any]],
    merge_async_backtest_job: Callable[..., Dict[str, Any]],
    run_single_backtest_worker: Callable[[Dict[str, Any]], Dict[str, Any]],
    run_comparison_backtest_worker: Callable[[Dict[str, Any]], Dict[str, Any]],
    now_text: Callable[[], str],
    now_ts: Callable[[], float],
    process_id: Callable[[], int] = os.getpid,
    stderr_write: Callable[[str], Any] = sys.stderr.write,
    record_backtest_chain: Callable[..., None] | None = None,
) -> int:
    run_id = str(os.getenv("OPENCLAW_ASYNC_BACKTEST_RUN_ID", "") or "").strip()
    job_kind = str(os.getenv("OPENCLAW_ASYNC_BACKTEST_JOB_KIND", "") or "").strip().lower()
    if not run_id or not job_kind:
        stderr_write("missing async backtest worker env\n")
        return 2
    job = load_async_backtest_state(run_id)
    payload = dict(job.get("payload") or {})
    try:
        merge_async_backtest_job(
            run_id,
            job,
            status="running",
            pid=process_id(),
            worker_started_at=now_text(),
        )
        out = run_single_backtest_worker(payload) if job_kind == "single" else run_comparison_backtest_worker(payload)
        out_error = str(out.get("error", "") or "")
        if not out_error:
            nested = out.get("result")
            if isinstance(nested, dict):
                out_error = str(nested.get("error", "") or "")
        success = bool(out.get("success"))
        merge_async_backtest_job(
            run_id,
            job,
            status="success" if success else "failed",
            result=out,
            error=out_error if not success else "",
            traceback=str(out.get("traceback", "")) if not success else "",
            ended_at=now_ts(),
            pid=process_id(),
        )
        if record_backtest_chain is not None:
            record_backtest_chain(run_id=run_id, job_kind=job_kind, payload=payload, result=out)
        return 0 if success else 1
    except Exception:
        failed_result = {
            "success": False,
            "error": "后台回测 worker crashed",
            "traceback": traceback.format_exc(),
        }
        merge_async_backtest_job(
            run_id,
            job,
            status="failed",
            error=str(failed_result["error"]),
            traceback=str(failed_result["traceback"]),
            ended_at=now_ts(),
            pid=process_id(),
        )
        if record_backtest_chain is not None:
            try:
                record_backtest_chain(run_id=run_id, job_kind=job_kind, payload=payload, result=failed_result)
            except Exception:
                pass
        return 1


def restore_recent_async_task_refs(
    *,
    session_state: Any,
    latest_async_scan_run_id: Callable[[str], str],
    latest_async_backtest_run_id: Callable[[str], str],
) -> None:
    scan_key_map = {
        "v4_async_task_id": "v4",
        "v5_async_task_id": "v5",
        "v6_async_task_id": "v6",
        "v7_async_task_id": "v7",
        "v8_async_task_id": "v8",
        "v9_async_task_id": "v9",
        "combo_async_task_id": "combo",
    }
    restore_allowed = {
        "v4": True,
        "v5": bool(session_state.get("v5_async_scan", False)),
        "v6": True,
        "v7": True,
        "v8": bool(session_state.get("v8_async_scan", False)),
        "v9": bool(session_state.get("v9_async_scan", False)),
        "combo": bool(session_state.get("combo_async_scan", False)),
    }
    for task_key, strategy in scan_key_map.items():
        if not restore_allowed.get(strategy, True):
            continue
        if str(session_state.get(task_key, "") or "").strip():
            continue
        run_id = latest_async_scan_run_id(strategy)
        if run_id:
            session_state[task_key] = run_id
    if not str(session_state.get("single_backtest_async_job_id", "") or "").strip():
        run_id = latest_async_backtest_run_id("single")
        if run_id:
            session_state["single_backtest_async_job_id"] = run_id
    if not str(session_state.get("comparison_async_job_id", "") or "").strip():
        run_id = latest_async_backtest_run_id("comparison")
        if run_id:
            session_state["comparison_async_job_id"] = run_id
