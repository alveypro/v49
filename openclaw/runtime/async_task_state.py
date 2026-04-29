from __future__ import annotations

import glob
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


def is_pid_alive(pid: Any) -> bool:
    try:
        pid_int = int(pid or 0)
    except Exception:
        return False
    if pid_int <= 0:
        return False
    try:
        os.kill(pid_int, 0)
        return True
    except OSError:
        return False


def async_scan_state_path(run_id: str, result_dir: str) -> str:
    os.makedirs(result_dir, exist_ok=True)
    return os.path.join(result_dir, f"{run_id}.state.json")


def async_backtest_state_path(run_id: str, result_dir: str) -> str:
    os.makedirs(result_dir, exist_ok=True)
    return os.path.join(result_dir, f"{run_id}.state.json")


def async_scan_log_paths(run_id: str, result_dir: str) -> Tuple[str, str]:
    os.makedirs(result_dir, exist_ok=True)
    return (
        os.path.join(result_dir, f"{run_id}.stdout.log"),
        os.path.join(result_dir, f"{run_id}.stderr.log"),
    )


def async_backtest_log_paths(run_id: str, result_dir: str) -> Tuple[str, str]:
    os.makedirs(result_dir, exist_ok=True)
    return (
        os.path.join(result_dir, f"{run_id}.stdout.log"),
        os.path.join(result_dir, f"{run_id}.stderr.log"),
    )


def load_async_scan_state(run_id: str, result_dir: str) -> Dict[str, Any]:
    state_path = async_scan_state_path(run_id, result_dir)
    if not os.path.exists(state_path):
        return {}
    try:
        with open(state_path, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def write_async_scan_state(run_id: str, state: Dict[str, Any], result_dir: str) -> Dict[str, Any]:
    state_path = async_scan_state_path(run_id, result_dir)
    tmp_path = f"{state_path}.tmp"
    payload = dict(state or {})
    payload["run_id"] = run_id
    payload["state_json"] = state_path
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, state_path)
    return payload


def load_async_backtest_state(run_id: str, result_dir: str) -> Dict[str, Any]:
    state_path = async_backtest_state_path(run_id, result_dir)
    if not os.path.exists(state_path):
        return {}
    try:
        with open(state_path, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def write_async_backtest_state(run_id: str, state: Dict[str, Any], result_dir: str) -> Dict[str, Any]:
    state_path = async_backtest_state_path(run_id, result_dir)
    tmp_path = f"{state_path}.tmp"
    payload = dict(state or {})
    payload["run_id"] = run_id
    payload["state_json"] = state_path
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, state_path)
    return payload


def cleanup_async_scan_tasks(
    *,
    async_scan_tasks: Dict[str, Dict[str, Any]],
    async_scan_lock: Any,
    keep_seconds: int,
    now_ts: float,
) -> None:
    with async_scan_lock:
        stale_ids: List[str] = []
        for run_id, item in async_scan_tasks.items():
            status = str(item.get("status", ""))
            ended_at = float(item.get("ended_at", 0) or 0)
            if status in {"success", "failed", "cancelled"} and ended_at > 0 and (now_ts - ended_at) > keep_seconds:
                stale_ids.append(run_id)
        for run_id in stale_ids:
            task = async_scan_tasks.pop(run_id, {})
            for k in ("result_csv", "meta_json", "state_json"):
                p = task.get(k)
                if isinstance(p, str) and p and os.path.exists(p):
                    try:
                        os.remove(p)
                    except Exception:
                        pass


def get_async_scan_task(
    run_id: str,
    *,
    async_scan_tasks: Dict[str, Dict[str, Any]],
    async_scan_lock: Any,
    keep_seconds: int,
    now_ts: float,
) -> Optional[Dict[str, Any]]:
    if not run_id:
        return None
    cleanup_async_scan_tasks(
        async_scan_tasks=async_scan_tasks,
        async_scan_lock=async_scan_lock,
        keep_seconds=keep_seconds,
        now_ts=now_ts,
    )
    with async_scan_lock:
        item = async_scan_tasks.get(run_id)
        return dict(item) if item else None


def merge_async_scan_task(
    run_id: str,
    *,
    base: Optional[Dict[str, Any]],
    updates: Dict[str, Any],
    async_scan_tasks: Dict[str, Dict[str, Any]],
    async_scan_lock: Any,
    result_dir: str,
) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    if base:
        merged.update(base)
    else:
        merged.update(load_async_scan_state(run_id, result_dir))
    merged.update(updates)
    merged = write_async_scan_state(run_id, merged, result_dir)
    with async_scan_lock:
        current = dict(async_scan_tasks.get(run_id) or {})
        current.update(merged)
        async_scan_tasks[run_id] = current
        return dict(current)


def persist_async_scan_task(
    run_id: str,
    *,
    async_scan_tasks: Dict[str, Dict[str, Any]],
    async_scan_lock: Any,
    result_dir: str,
) -> None:
    try:
        with async_scan_lock:
            item = dict(async_scan_tasks.get(run_id) or {})
        if item:
            write_async_scan_state(run_id, item, result_dir)
    except Exception:
        pass


def merge_async_backtest_job(
    run_id: str,
    *,
    base: Optional[Dict[str, Any]],
    updates: Dict[str, Any],
    async_backtest_jobs: Dict[str, Dict[str, Any]],
    async_backtest_lock: Any,
    result_dir: str,
) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    if base:
        merged.update(base)
    else:
        merged.update(load_async_backtest_state(run_id, result_dir))
    merged.update(updates)
    merged = write_async_backtest_state(run_id, merged, result_dir)
    with async_backtest_lock:
        current = dict(async_backtest_jobs.get(run_id) or {})
        current.update(merged)
        async_backtest_jobs[run_id] = current
        return dict(current)


def cleanup_async_backtest_jobs(
    *,
    async_backtest_jobs: Dict[str, Dict[str, Any]],
    async_backtest_lock: Any,
    result_dir: str,
    keep_seconds: int,
    now_ts: float,
) -> None:
    with async_backtest_lock:
        stale_ids: List[str] = []
        for run_id, job in async_backtest_jobs.items():
            status = str(job.get("status", "") or "")
            ended_at = float(job.get("ended_at", 0) or 0)
            if status in {"success", "failed", "cancelled"} and ended_at > 0 and (now_ts - ended_at) > keep_seconds:
                stale_ids.append(run_id)
        for run_id in stale_ids:
            async_backtest_jobs.pop(run_id, None)
    for p in glob.glob(os.path.join(result_dir, "*.state.json")):
        try:
            with open(p, "r", encoding="utf-8") as f:
                obj = json.load(f) or {}
        except Exception:
            continue
        status = str(obj.get("status", "") or "")
        ended_at = float(obj.get("ended_at", 0) or 0)
        if status not in {"success", "failed", "cancelled"} or ended_at <= 0:
            continue
        if (now_ts - ended_at) <= keep_seconds:
            continue
        base = p[: -len(".state.json")]
        for suffix in (".state.json", ".stdout.log", ".stderr.log"):
            fp = f"{base}{suffix}"
            if os.path.exists(fp):
                try:
                    os.remove(fp)
                except Exception:
                    pass


def prune_all_finished_async_tasks(
    *,
    async_scan_tasks: Dict[str, Dict[str, Any]],
    async_scan_lock: Any,
    async_scan_result_dir: str,
    async_backtest_jobs: Dict[str, Dict[str, Any]],
    async_backtest_lock: Any,
    async_backtest_result_dir: str,
) -> Dict[str, int]:
    removed = {"scan": 0, "backtest": 0}
    with async_scan_lock:
        finished_scan_ids = [
            run_id for run_id, task in async_scan_tasks.items()
            if str(task.get("status", "") or "") in {"success", "failed", "cancelled"}
        ]
        for run_id in finished_scan_ids:
            task = async_scan_tasks.pop(run_id, {})
            for key in ("result_csv", "meta_json", "state_json", "stdout_log", "stderr_log"):
                path = str(task.get(key, "") or "")
                if path and os.path.exists(path):
                    try:
                        os.remove(path)
                    except Exception:
                        pass
            removed["scan"] += 1
    for p in glob.glob(os.path.join(async_scan_result_dir, "*.state.json")):
        try:
            with open(p, "r", encoding="utf-8") as f:
                obj = json.load(f) or {}
        except Exception:
            continue
        if str(obj.get("status", "") or "") not in {"success", "failed", "cancelled"}:
            continue
        base = p[: -len(".state.json")]
        for suffix in (".state.json", ".stdout.log", ".stderr.log"):
            fp = f"{base}{suffix}"
            if os.path.exists(fp):
                try:
                    os.remove(fp)
                except Exception:
                    pass
        for key in ("result_csv", "meta_json"):
            fp = str(obj.get(key, "") or "")
            if fp and os.path.exists(fp):
                try:
                    os.remove(fp)
                except Exception:
                    pass

    with async_backtest_lock:
        finished_backtest_ids = [
            run_id for run_id, job in async_backtest_jobs.items()
            if str(job.get("status", "") or "") in {"success", "failed", "cancelled"}
        ]
        for run_id in finished_backtest_ids:
            async_backtest_jobs.pop(run_id, None)
            removed["backtest"] += 1
    for p in glob.glob(os.path.join(async_backtest_result_dir, "*.state.json")):
        try:
            with open(p, "r", encoding="utf-8") as f:
                obj = json.load(f) or {}
        except Exception:
            continue
        if str(obj.get("status", "") or "") not in {"success", "failed", "cancelled"}:
            continue
        base = p[: -len(".state.json")]
        for suffix in (".state.json", ".stdout.log", ".stderr.log"):
            fp = f"{base}{suffix}"
            if os.path.exists(fp):
                try:
                    os.remove(fp)
                except Exception:
                    pass
    return removed


def read_async_scan_df(task: Dict[str, Any]) -> Optional[pd.DataFrame]:
    p = task.get("result_csv")
    if not isinstance(p, str) or not p or not os.path.exists(p):
        return None
    try:
        return pd.read_csv(p)
    except Exception:
        return None


def recover_async_scan_task(
    run_id: str,
    *,
    result_dir: str,
    async_scan_tasks: Dict[str, Dict[str, Any]],
    async_scan_lock: Any,
    now_ts: float,
) -> Optional[Dict[str, Any]]:
    if not run_id:
        return None
    try:
        state_path = async_scan_state_path(run_id, result_dir)
        state_obj: Dict[str, Any] = load_async_scan_state(run_id, result_dir)
        pattern = os.path.join(result_dir, f"*_{run_id}_*.meta.json")
        meta_candidates = glob.glob(pattern)
        meta: Dict[str, Any] = {}
        meta_path = ""
        if meta_candidates:
            meta_path = sorted(meta_candidates, key=lambda p: os.path.getmtime(p), reverse=True)[0]
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f) or {}
        if not meta and not state_obj:
            return None

        merged: Dict[str, Any] = {}
        merged.update(state_obj)
        merged.update(meta)
        strategy = str(merged.get("strategy", "") or run_id.split("_", 1)[0])
        score_col = str(merged.get("score_col", "综合评分"))
        row_count = int(merged.get("row_count", 0) or 0)
        status = str(merged.get("status", "success" if meta else "failed"))
        csv_from_meta = str(merged.get("result_csv", "") or "")
        csv_guess = meta_path.replace(".meta.json", ".csv") if meta_path else ""
        result_csv = csv_from_meta if (csv_from_meta and os.path.exists(csv_from_meta)) else (csv_guess if csv_guess and os.path.exists(csv_guess) else "")

        pid = int(merged.get("pid", 0) or 0)
        if status in {"queued", "running"} and pid > 0 and is_pid_alive(pid):
            status = "running"
            msg = f"后台任务仍在运行（PID={pid}）"
        elif status in {"queued", "running"}:
            status = "failed"
            msg = "任务在服务重启前中断，请重新发起扫描"
        else:
            msg = f"任务结果已从磁盘恢复（{merged.get('finished_at', '未知时间')}）"

        item = {
            "run_id": run_id,
            "strategy": strategy,
            "status": status,
            "stage": "done" if status == "success" else ("scan" if status == "running" else "error"),
            "progress": int(merged.get("progress", 100 if status != "running" else 1) or 0),
            "message": msg,
            "created_at": 0,
            "created_at_text": "",
            "ended_at": float(merged.get("ended_at", now_ts) or 0),
            "params": merged.get("params", {}) or {},
            "score_col": score_col,
            "result_csv": result_csv,
            "meta_json": meta_path,
            "state_json": state_path,
            "meta": merged.get("meta", {}) or {},
            "error": str(merged.get("error", "") or ""),
            "row_count": row_count,
            "pid": pid,
            "stdout_log": str(merged.get("stdout_log", "") or ""),
            "stderr_log": str(merged.get("stderr_log", "") or ""),
        }
        with async_scan_lock:
            async_scan_tasks[run_id] = item
        return dict(item)
    except Exception:
        return None


def get_async_backtest_job(
    run_id: str,
    *,
    async_backtest_jobs: Dict[str, Dict[str, Any]],
    async_backtest_lock: Any,
    result_dir: str,
    keep_seconds: int,
    now_ts: float,
) -> Optional[Dict[str, Any]]:
    run_id = str(run_id or "")
    if not run_id:
        return None
    cleanup_async_backtest_jobs(
        async_backtest_jobs=async_backtest_jobs,
        async_backtest_lock=async_backtest_lock,
        result_dir=result_dir,
        keep_seconds=keep_seconds,
        now_ts=now_ts,
    )
    with async_backtest_lock:
        item = async_backtest_jobs.get(run_id)
        job = dict(item) if item else {}
    if not job:
        state = load_async_backtest_state(run_id, result_dir)
        if not state:
            return None
        job = dict(state)
        with async_backtest_lock:
            async_backtest_jobs[run_id] = dict(job)
    pid = int(job.get("pid", 0) or 0)
    status = str(job.get("status", "") or "")
    if status == "running" and pid > 0 and not is_pid_alive(pid):
        state = load_async_backtest_state(run_id, result_dir)
        if state:
            job = dict(state)
        if str(job.get("status", "")) == "running":
            job = merge_async_backtest_job(
                run_id,
                base=job,
                updates={
                    "status": "failed",
                    "error": str(job.get("error", "") or "回测任务在服务重启前中断"),
                    "ended_at": float(job.get("ended_at", 0.0) or now_ts),
                },
                async_backtest_jobs=async_backtest_jobs,
                async_backtest_lock=async_backtest_lock,
                result_dir=result_dir,
            )
    return dict(job)


def list_recent_async_scan_tasks(result_dir: str, limit: int = 12) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for p in sorted(glob.glob(os.path.join(result_dir, "*.state.json")), key=os.path.getmtime, reverse=True)[: max(1, int(limit))]:
        try:
            with open(p, "r", encoding="utf-8") as f:
                obj = json.load(f) or {}
        except Exception:
            continue
        ended_at = float(obj.get("ended_at", 0) or 0)
        rows.append(
            {
                "任务ID": str(obj.get("run_id", "") or ""),
                "类型": "扫描",
                "策略": str(obj.get("strategy", "") or "").upper(),
                "状态": str(obj.get("status", "") or ""),
                "结果数": int(obj.get("row_count", 0) or 0),
                "开始时间": str(obj.get("created_at_text", "") or ""),
                "结束时间": datetime.fromtimestamp(ended_at).strftime("%Y-%m-%d %H:%M:%S") if ended_at > 0 else "",
                "结果文件": str(obj.get("result_csv", "") or ""),
                "错误": str(obj.get("error", "") or ""),
            }
        )
    return pd.DataFrame(rows)


def list_recent_async_backtest_jobs(result_dir: str, limit: int = 12) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for p in sorted(glob.glob(os.path.join(result_dir, "*.state.json")), key=os.path.getmtime, reverse=True)[: max(1, int(limit))]:
        try:
            with open(p, "r", encoding="utf-8") as f:
                obj = json.load(f) or {}
        except Exception:
            continue
        payload = obj.get("payload") or {}
        ended_at = float(obj.get("ended_at", 0) or 0)
        created_at = float(obj.get("created_at", 0) or 0)
        rows.append(
            {
                "任务ID": str(obj.get("run_id", "") or ""),
                "类型": "回测",
                "策略": str(payload.get("strategy", obj.get("job_kind", "")) or ""),
                "状态": str(obj.get("status", "") or ""),
                "结果数": "",
                "开始时间": datetime.fromtimestamp(created_at).strftime("%Y-%m-%d %H:%M:%S") if created_at > 0 else "",
                "结束时间": datetime.fromtimestamp(ended_at).strftime("%Y-%m-%d %H:%M:%S") if ended_at > 0 else "",
                "结果文件": str(obj.get("state_json", "") or ""),
                "错误": str(obj.get("error", "") or ""),
            }
        )
    return pd.DataFrame(rows)


def latest_async_scan_run_id(strategy: str, result_dir: str, statuses: Optional[set[str]] = None) -> str:
    target = str(strategy or "").lower()
    allowed = statuses or {"queued", "running", "success"}
    best_path = ""
    best_mtime = -1.0
    for p in glob.glob(os.path.join(result_dir, "*.state.json")):
        try:
            with open(p, "r", encoding="utf-8") as f:
                obj = json.load(f) or {}
        except Exception:
            continue
        if str(obj.get("strategy", "") or "").lower() != target:
            continue
        if str(obj.get("status", "") or "") not in allowed:
            continue
        try:
            mt = os.path.getmtime(p)
        except Exception:
            mt = 0.0
        if mt > best_mtime:
            best_mtime = mt
            best_path = p
    if not best_path:
        return ""
    try:
        with open(best_path, "r", encoding="utf-8") as f:
            obj = json.load(f) or {}
        return str(obj.get("run_id", "") or "")
    except Exception:
        return ""


def latest_async_backtest_run_id(job_kind: str, result_dir: str, statuses: Optional[set[str]] = None) -> str:
    target = str(job_kind or "").lower()
    allowed = statuses or {"queued", "running", "success"}
    best_path = ""
    best_mtime = -1.0
    for p in glob.glob(os.path.join(result_dir, "*.state.json")):
        try:
            with open(p, "r", encoding="utf-8") as f:
                obj = json.load(f) or {}
        except Exception:
            continue
        if str(obj.get("job_kind", "") or "").lower() != target:
            continue
        if str(obj.get("status", "") or "") not in allowed:
            continue
        try:
            mt = os.path.getmtime(p)
        except Exception:
            mt = 0.0
        if mt > best_mtime:
            best_mtime = mt
            best_path = p
    if not best_path:
        return ""
    try:
        with open(best_path, "r", encoding="utf-8") as f:
            obj = json.load(f) or {}
        return str(obj.get("run_id", "") or "")
    except Exception:
        return ""
