from __future__ import annotations

from datetime import datetime
import glob
import json
import os
from typing import Any, Callable, Dict, List, Tuple

import pandas as pd


def load_latest_production_report(app_root: str) -> Tuple[Dict[str, Any], str, str]:
    evo_dir = os.path.join(app_root, "evolution")
    file_map = {
        "V9": "v9_best.json",
        "V8": "v8_best.json",
        "V5": "v5_best.json",
        "COMBO": "combo_best.json",
    }
    best_payload: Dict[str, Any] = {}
    best_strategy = ""
    best_mtime = 0.0
    best_mtime_text = ""
    for strategy, fname in file_map.items():
        path = os.path.join(evo_dir, fname)
        try:
            if not os.path.exists(path):
                continue
            mtime = os.path.getmtime(path)
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f) or {}
            if mtime > best_mtime and isinstance(payload, dict):
                best_mtime = mtime
                best_payload = payload
                best_strategy = strategy
                best_mtime_text = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            continue
    return best_payload, best_strategy, best_mtime_text


def load_production_report_by_strategy(app_root: str, strategy: str) -> Tuple[Dict[str, Any], str, str]:
    evo_dir = os.path.join(app_root, "evolution")
    file_map: Dict[str, List[Tuple[str, str]]] = {
        "V9": [("best", "v9_best.json"), ("last_attempt", "v9_last_attempt.json"), ("candidate", "v9_candidate.json")],
        "V8": [("best", "v8_best.json"), ("last_attempt", "v8_last_attempt.json"), ("candidate", "v8_candidate.json")],
        "V5": [("best", "v5_best.json"), ("last_attempt", "v5_last_attempt.json"), ("candidate", "v5_candidate.json")],
        "COMBO": [("best", "combo_best.json"), ("last_attempt", "combo_last_attempt.json"), ("candidate", "combo_candidate.json")],
    }
    s = str(strategy or "").strip().upper()
    for source_type, filename in file_map.get(s, []):
        path = os.path.join(evo_dir, filename)
        if not os.path.exists(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f) or {}
            mtime = datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%d %H:%M:%S")
            return (payload if isinstance(payload, dict) else {}), mtime, source_type
        except Exception:
            continue
    return {}, "", "missing"


def load_latest_production_promotion(app_root: str, strategy_filter: str = "") -> Dict[str, Any]:
    promo_path = os.path.join(app_root, "evolution", "promotion_history.jsonl")
    allow = {"V9", "V8", "V5", "COMBO"}
    target = str(strategy_filter or "").strip().upper()
    if not os.path.exists(promo_path):
        return {}
    try:
        with open(promo_path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            pos = max(0, size - 65536)
            f.seek(pos)
            tail = f.read().decode("utf-8", errors="ignore")
        lines = [ln.strip() for ln in tail.splitlines() if ln.strip()]
        for ln in reversed(lines):
            try:
                obj = json.loads(ln)
                decision = (obj.get("decision") or {}) if isinstance(obj, dict) else {}
                strategy = str(decision.get("strategy", "")).upper()
                if strategy in allow and (not target or strategy == target):
                    return decision
            except Exception:
                continue
    except Exception:
        pass
    return {}


def get_auto_evolve_status(
    *,
    app_root: str,
    auto_evolve_lock_path: str,
    is_pid_running: Callable[[int], bool],
    load_latest_production_report: Callable[[], Tuple[Dict[str, Any], str, str]],
    load_evolve_params: Callable[[str], Dict[str, Any]],
    fmt_file_mtime: Callable[[str], str],
    safe_parse_dt: Callable[[str], Any],
    load_latest_production_promotion: Callable[[], Dict[str, Any]],
) -> Dict[str, Any]:
    lock_path = auto_evolve_lock_path
    log_path = os.path.join(app_root, "auto_evolve.log")
    risk_path = os.path.join(app_root, "evolution", "risk_sentinel.json")
    legacy_evolve_path = os.path.join(app_root, "evolution", "last_run.json")

    lock_exists = os.path.exists(lock_path)
    pid = ""
    pid_alive = False
    if lock_exists:
        try:
            with open(lock_path, "r", encoding="utf-8") as f:
                pid = (f.read() or "").strip()
            if pid.isdigit():
                pid_alive = is_pid_running(int(pid))
        except Exception:
            pid = ""
            pid_alive = False

    if pid_alive:
        runtime_state = "运行中"
    elif lock_exists:
        try:
            os.remove(lock_path)
            lock_exists = False
        except Exception:
            pass
        runtime_state = "空闲（检测到历史锁）"
    else:
        runtime_state = "空闲"

    data, data_strategy, data_mtime = load_latest_production_report()
    if not data:
        data = load_evolve_params("last_run.json")
        data_strategy = "V4"
        data_mtime = fmt_file_mtime(legacy_evolve_path)
    run_at = str(data.get("run_at", "") or "")
    run_dt = safe_parse_dt(run_at)
    age_mins = None
    if run_dt is not None:
        age_mins = int(max(0.0, (datetime.now() - run_dt).total_seconds() // 60))

    risk_level = "unknown"
    risk_level_effective = "unknown"
    risk_rules: List[str] = []
    risk_run_at = ""
    risk_age_mins = None
    risk_stale = False
    try:
        if os.path.exists(risk_path):
            with open(risk_path, "r", encoding="utf-8") as f:
                risk_data = json.load(f) or {}
            risk_level = str(risk_data.get("risk_level", "unknown") or "unknown")
            risk_rules = [str(x) for x in (risk_data.get("triggered_rules") or [])]
            risk_run_at = str(risk_data.get("run_at", "") or "")
            risk_dt = safe_parse_dt(risk_run_at)
            stale_mins = int(os.getenv("OPENCLAW_RISK_SENTINEL_STALE_MINUTES", "180"))
            if risk_dt is not None:
                risk_age_mins = int(max(0.0, (datetime.now() - risk_dt).total_seconds() // 60))
                risk_stale = bool(risk_age_mins > stale_mins)
            risk_level_effective = "unknown" if risk_stale else risk_level
    except Exception:
        pass

    last_promo = load_latest_production_promotion()
    return {
        "runtime_state": runtime_state,
        "pid": pid,
        "pid_alive": pid_alive,
        "run_at": run_at,
        "run_age_mins": age_mins,
        "last_run_mtime": data_mtime,
        "run_source": data_strategy or "N/A",
        "log_mtime": fmt_file_mtime(log_path),
        "risk_level": risk_level,
        "risk_level_effective": risk_level_effective,
        "risk_rules": risk_rules,
        "risk_run_at": risk_run_at,
        "risk_age_mins": risk_age_mins,
        "risk_stale": risk_stale,
        "last_promotion": last_promo,
        "data": data,
    }


def resolve_app_path(app_root: str, path_text: str) -> str:
    p = str(path_text or "").strip()
    if not p:
        return ""
    if os.path.isabs(p):
        return p
    p1 = os.path.join(app_root, p)
    if os.path.exists(p1):
        return p1
    p2 = os.path.join(os.getcwd(), p)
    if os.path.exists(p2):
        return p2
    return p1


def load_latest_tracking_scoreboard(
    app_root: str,
    *,
    max_files: int = 60,
    resolve_app_path_fn: Callable[[str], str],
) -> Tuple[pd.DataFrame, str, str]:
    log_dirs = [
        os.path.join(app_root, "logs", "openclaw"),
        os.path.join(os.getcwd(), "logs", "openclaw"),
    ]
    log_dir = ""
    for d in log_dirs:
        if os.path.isdir(d):
            log_dir = d
            break
    if not log_dir:
        return pd.DataFrame(), "", ""

    exec_files = sorted(glob.glob(os.path.join(log_dir, "partner_execution_*.json")), reverse=True)[: max(1, int(max_files))]
    for fp in exec_files:
        try:
            with open(fp, "r", encoding="utf-8") as f:
                obj = json.load(f) or {}
            tracking = obj.get("tracking") or {}
            scoreboard = tracking.get("scoreboard") or {}
            artifacts = obj.get("artifacts") or {}
            csv_path_raw = str(scoreboard.get("csv") or artifacts.get("strategy_scoreboard_csv") or "").strip()
            if not csv_path_raw:
                continue
            csv_path = resolve_app_path_fn(csv_path_raw)
            if not csv_path or not os.path.exists(csv_path):
                continue
            df = pd.read_csv(csv_path)
            if df is None or df.empty or "strategy" not in df.columns:
                continue
            return df, fp, csv_path
        except Exception:
            continue
    return pd.DataFrame(), "", ""
