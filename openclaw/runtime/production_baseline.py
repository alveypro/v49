from __future__ import annotations

from datetime import datetime
import glob
import json
import os
import subprocess
import sys
from typing import Any, Callable, Dict, List, Tuple


def production_baseline_params(profile: str = "稳健标准", strict_full_market: bool = False) -> Dict[str, Dict[str, Any]]:
    base = {
        "v5": {"score_threshold": 60, "holding_days": 8, "top_percent": 1, "cap_min": 100.0, "cap_max": 15000.0, "candidate_count": 800},
        "v8": {"score_threshold": 65, "holding_days": 10, "top_percent": 1, "cap_min": 100.0, "cap_max": 15000.0, "candidate_count": 800, "scan_all": False},
        "v9": {
            "score_threshold": 65,
            "holding_days": 20,
            "top_percent": 1,
            "cap_min": 100.0,
            "cap_max": 15000.0,
            "candidate_count": 800,
            "scan_all": False,
            "lookback_days": 120,
            "min_turnover": 5.0,
        },
        "combo": {
            "score_threshold": 68,
            "holding_days": 10,
            "top_percent": 1,
            "cap_min": 100.0,
            "cap_max": 15000.0,
            "candidate_count": 800,
            "lookback_days": 90,
            "min_turnover": 5.0,
        },
    }
    if str(profile) == "进攻增强":
        base["v5"]["score_threshold"] = 58
        base["v8"]["score_threshold"] = 62
        base["v9"]["score_threshold"] = 62
        base["combo"]["score_threshold"] = 66
        base["v5"]["top_percent"] = 1
        base["v8"]["top_percent"] = 1
        base["v9"]["top_percent"] = 1
        base["combo"]["top_percent"] = 1
    if strict_full_market:
        for key in ("v8", "v9"):
            base[key]["scan_all"] = True
        for key in ("v5", "v8", "v9", "combo"):
            base[key]["cap_min"] = 0.0
            base[key]["cap_max"] = 0.0
    return base


def apply_production_baseline_to_session(
    params: Dict[str, Dict[str, Any]],
    *,
    session_state: Any,
    now_text: Callable[[], str],
) -> None:
    v5 = params.get("v5", {})
    session_state["holding_days_v5"] = int(v5.get("holding_days", 8))
    session_state["score_threshold_v5"] = int(v5.get("score_threshold", 60))
    session_state["v5_top_percent"] = int(v5.get("top_percent", 1))
    session_state["cap_min_v5"] = float(v5.get("cap_min", 100.0))
    session_state["cap_max_v5"] = float(v5.get("cap_max", 15000.0))

    v8 = params.get("v8", {})
    session_state["holding_days_v8"] = int(v8.get("holding_days", 10))
    session_state["score_threshold_v8_tab1"] = int(v8.get("score_threshold", 65))
    session_state["v8_top_percent_tab1"] = int(v8.get("top_percent", 1))
    session_state["scan_all_v8_tab1"] = bool(v8.get("scan_all", False))
    session_state["cap_min_v8_tab1"] = float(v8.get("cap_min", 100.0))
    session_state["cap_max_v8_tab1"] = float(v8.get("cap_max", 15000.0))

    v9 = params.get("v9", {})
    session_state["score_threshold_v9"] = int(v9.get("score_threshold", 65))
    session_state["holding_days_v9"] = int(v9.get("holding_days", 20))
    session_state["lookback_days_v9"] = int(v9.get("lookback_days", 120))
    session_state["min_turnover_v9"] = float(v9.get("min_turnover", 5.0))
    session_state["top_percent_v9"] = int(v9.get("top_percent", 1))
    session_state["scan_all_v9"] = bool(v9.get("scan_all", False))
    session_state["cap_min_v9"] = float(v9.get("cap_min", 100.0))
    session_state["cap_max_v9"] = float(v9.get("cap_max", 15000.0))
    session_state["candidate_count_v9"] = int(v9.get("candidate_count", 800))

    combo = params.get("combo", {})
    session_state["combo_threshold"] = int(combo.get("score_threshold", 68))
    session_state["holding_days_combo"] = int(combo.get("holding_days", 10))
    session_state["combo_lookback_days"] = int(combo.get("lookback_days", 90))
    session_state["combo_min_turnover"] = float(combo.get("min_turnover", 5.0))
    session_state["combo_top_percent"] = int(combo.get("top_percent", 1))
    session_state["combo_cap_min"] = float(combo.get("cap_min", 100.0))
    session_state["combo_cap_max"] = float(combo.get("cap_max", 15000.0))
    session_state["combo_candidate_count"] = int(combo.get("candidate_count", 800))

    session_state["prod_unified_active"] = True
    session_state["prod_unified_applied_at"] = now_text()
    session_state["v9_center_defaults_initialized"] = True
    session_state["v9_params_locked_by_unified"] = True


def get_production_compare_params(
    *,
    session_state: Any,
    load_evolve_params: Callable[[str], Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    def _num(value: Any, default: float) -> float:
        try:
            if value is None:
                return float(default)
            return float(value)
        except Exception:
            return float(default)

    evo_v5 = load_evolve_params("v5_best.json").get("params", {})
    evo_v8 = load_evolve_params("v8_best.json").get("params", {})
    evo_v9 = load_evolve_params("v9_best.json").get("params", {})
    evo_combo = load_evolve_params("combo_best.json").get("params", {})

    thr_v8_raw = session_state.get("score_threshold_v8_tab1", evo_v8.get("score_threshold", [55, 70]))
    if isinstance(thr_v8_raw, (list, tuple)) and len(thr_v8_raw) >= 2:
        thr_v8 = int(round((float(thr_v8_raw[0]) + float(thr_v8_raw[1])) / 2.0))
    else:
        thr_v8 = int(round(_num(thr_v8_raw, 65)))

    return {
        "v5": {
            "holding_days": int(round(_num(session_state.get("holding_days_v5"), _num(evo_v5.get("holding_days"), 8)))),
            "score_threshold": int(round(_num(session_state.get("score_threshold_v5"), _num(evo_v5.get("score_threshold"), 60)))),
        },
        "v8": {
            "holding_days": int(round(_num(session_state.get("holding_days_v8"), _num(evo_v8.get("holding_days"), 10)))),
            "score_threshold": int(round(thr_v8)),
        },
        "v9": {
            "holding_days": int(round(_num(session_state.get("holding_days_v9"), _num(evo_v9.get("holding_days"), 20)))),
            "score_threshold": int(round(_num(session_state.get("score_threshold_v9"), _num(evo_v9.get("score_threshold"), 65)))),
        },
        "combo": {
            "holding_days": int(round(_num(session_state.get("holding_days_combo"), _num(evo_combo.get("holding_days"), 10)))),
            "score_threshold": int(round(_num(session_state.get("combo_threshold"), _num(evo_combo.get("combo_threshold"), 68)))),
        },
    }


def save_production_unified_profile(
    *,
    app_root: str,
    profile_name: str,
    strict_full_market: bool,
    params: Dict[str, Dict[str, Any]],
) -> Tuple[bool, str]:
    evo_dir = os.path.join(app_root, "evolution")
    path = os.path.join(evo_dir, "production_unified_profile.json")
    payload = {
        "run_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "profile": str(profile_name),
        "strict_full_market_mode": bool(strict_full_market),
        "strategies": params,
    }
    try:
        os.makedirs(evo_dir, exist_ok=True)
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
        return True, path
    except Exception as exc:
        return False, str(exc)


def build_unified_from_latest_evolve(
    *,
    profile: str,
    strict_full_market: bool,
    unified_cap_min: float,
    unified_cap_max: float,
    load_evolve_params: Callable[[str], Dict[str, Any]],
) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
    baseline = production_baseline_params(profile, strict_full_market=bool(strict_full_market))
    notes: List[str] = []

    def _to_int(value: Any, default: int) -> int:
        try:
            if isinstance(value, (list, tuple)) and value:
                vals = [float(x) for x in value[:2] if x is not None]
                if vals:
                    return int(round(sum(vals) / len(vals)))
            return int(round(float(value)))
        except Exception:
            return int(default)

    def _to_float(value: Any, default: float) -> float:
        try:
            return float(value)
        except Exception:
            return float(default)

    evo_map = {
        "v5": load_evolve_params("v5_best.json").get("params", {}),
        "v8": load_evolve_params("v8_best.json").get("params", {}),
        "v9": load_evolve_params("v9_best.json").get("params", {}),
        "combo": load_evolve_params("combo_best.json").get("params", {}),
    }

    for strategy_key in ("v5", "v8", "v9", "combo"):
        params = evo_map.get(strategy_key, {})
        if not isinstance(params, dict) or not params:
            notes.append(f"{strategy_key} 未找到自动进化参数，保留模板默认值")
            continue
        if strategy_key == "combo":
            baseline[strategy_key]["score_threshold"] = _to_int(params.get("combo_threshold"), baseline[strategy_key]["score_threshold"])
            baseline[strategy_key]["holding_days"] = _to_int(params.get("holding_days"), baseline[strategy_key]["holding_days"])
            baseline[strategy_key]["top_percent"] = _to_int(params.get("top_percent"), baseline[strategy_key]["top_percent"])
            baseline[strategy_key]["lookback_days"] = _to_int(params.get("lookback_days"), baseline[strategy_key]["lookback_days"])
            baseline[strategy_key]["min_turnover"] = _to_float(params.get("min_turnover"), baseline[strategy_key]["min_turnover"])
        else:
            baseline[strategy_key]["score_threshold"] = _to_int(params.get("score_threshold"), baseline[strategy_key]["score_threshold"])
            baseline[strategy_key]["holding_days"] = _to_int(params.get("holding_days"), baseline[strategy_key]["holding_days"])
            baseline[strategy_key]["top_percent"] = _to_int(params.get("top_percent"), baseline[strategy_key]["top_percent"])
            if strategy_key == "v9":
                baseline[strategy_key]["lookback_days"] = _to_int(params.get("lookback_days"), baseline[strategy_key]["lookback_days"])
                baseline[strategy_key]["min_turnover"] = _to_float(params.get("min_turnover"), baseline[strategy_key]["min_turnover"])

    for strategy_key in ("v5", "v8", "v9", "combo"):
        baseline[strategy_key]["cap_min"] = float(unified_cap_min)
        baseline[strategy_key]["cap_max"] = float(unified_cap_max)
    if strict_full_market:
        for strategy_key in ("v5", "v8", "v9", "combo"):
            baseline[strategy_key]["cap_min"] = 0.0
            baseline[strategy_key]["cap_max"] = 0.0
    return baseline, notes


def trigger_auto_evolve_optimize(
    *,
    app_root: str,
    detect_heavy_background_job: Callable[[], Tuple[bool, str]],
    now_text: Callable[[], str],
    force_now: bool = True,
) -> Tuple[bool, str]:
    busy, reason = detect_heavy_background_job()
    if busy:
        return False, reason

    script_path = os.path.join(app_root, "openclaw", "auto_evolve.py")
    if not os.path.exists(script_path):
        return False, "未找到 auto_evolve.py"

    py_candidates = [
        os.path.join(app_root, ".venv", "bin", "python"),
        "/opt/openclaw/venv311/bin/python",
        sys.executable,
        "python3",
    ]
    py_bin = ""
    for candidate in py_candidates:
        if candidate and os.path.isabs(candidate) and os.path.exists(candidate) and os.access(candidate, os.X_OK):
            py_bin = candidate
            break
        if candidate == "python3":
            py_bin = candidate
    if not py_bin:
        py_bin = "python3"

    log_path = os.path.join(app_root, "auto_evolve.log")
    env = os.environ.copy()
    env["AUTO_EVOLVE_PHASE"] = "optimize_only"
    if force_now:
        env["AUTO_EVOLVE_ENFORCE_WINDOW"] = "0"

    try:
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        log_fp = open(log_path, "a", encoding="utf-8")
        log_fp.write(f"\n[{now_text()}] [UI] manual optimize trigger start\n")
        log_fp.flush()
        proc = subprocess.Popen(
            [py_bin, script_path],
            cwd=app_root,
            env=env,
            stdout=log_fp,
            stderr=log_fp,
            start_new_session=True,
        )
        log_fp.close()
        return True, f"已启动后台更新任务（PID={proc.pid}）"
    except Exception as exc:
        return False, f"启动失败: {exc}"


def rollback_latest_promoted_params(*, app_root: str, strategy: str) -> Tuple[bool, str]:
    strategy = str(strategy or "").strip().upper()
    mapping = {
        "V4": "best_params.json",
        "V5": "v5_best.json",
        "V6": "v6_best.json",
        "V7": "v7_best.json",
        "V8": "v8_best.json",
        "V9": "v9_best.json",
        "COMBO": "combo_best.json",
        "STABLE_UPTREND": "stable_uptrend_best.json",
        "AI_V5": "ai_v5_best.json",
        "AI_V2": "ai_v2_best.json",
    }
    rel = mapping.get(strategy)
    if not rel:
        return False, f"不支持的策略: {strategy}"
    evo_dir = os.path.join(app_root, "evolution")
    active_path = os.path.join(evo_dir, rel)
    backups = sorted(glob.glob(f"{active_path}.bak_*"), reverse=True)
    if not backups:
        return False, "未找到可回滚备份"
    src = backups[0]
    try:
        with open(src, "r", encoding="utf-8") as handle:
            payload = handle.read()
        with open(active_path, "w", encoding="utf-8") as handle:
            handle.write(payload)
        if strategy == "V4":
            with open(os.path.join(evo_dir, "last_run.json"), "w", encoding="utf-8") as handle:
                handle.write(payload)
        return True, f"已回滚 {strategy} 到备份 {os.path.basename(src)}"
    except Exception as exc:
        return False, f"回滚失败: {exc}"
