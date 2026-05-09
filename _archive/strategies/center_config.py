from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional
import json

from openclaw.services.rejected_backtest_artifact_ledger_service import load_rejected_backtest_artifacts
from strategies.registry import get_profile

JsonDict = Dict[str, Any]


def default_center_config() -> JsonDict:
    return {
        "auto_apply_backtest_best": True,
        "backtest_best_dir": "logs/openclaw",
        "rejected_backtest_artifacts_file": "logs/openclaw/rejected_backtest_artifacts.jsonl",
        "runtime_defaults": {},
        "risk_overrides": {},
        "strategy_weights": {},
    }


def load_center_config(path: Path) -> JsonDict:
    if not path.exists():
        return default_center_config()
    try:
        if path.suffix.lower() == ".json":
            data = json.loads(path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else default_center_config()
        import yaml  # type: ignore

        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else default_center_config()
    except Exception:
        try:
            data = _parse_simple_yaml(path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else default_center_config()
        except Exception:
            return default_center_config()


def resolve_runtime_params(
    strategy: str,
    requested_score_threshold: Optional[int],
    requested_sample_size: Optional[int],
    requested_holding_days: Optional[int],
    center_config: JsonDict,
    project_root: Path,
) -> JsonDict:
    profile = get_profile(strategy)
    threshold_floor = int(profile.default_score_threshold)
    out = {
        "score_threshold": threshold_floor,
        "sample_size": int(profile.default_sample_size),
        "holding_days": int(profile.default_holding_days),
        "source": {
            "score_threshold": "profile_default",
            "sample_size": "profile_default",
            "holding_days": "profile_default",
        },
    }

    runtime_defaults = center_config.get("runtime_defaults") if isinstance(center_config, dict) else {}
    strategy_defaults = runtime_defaults.get(strategy, {}) if isinstance(runtime_defaults, dict) else {}
    for k in ("score_threshold", "sample_size", "holding_days"):
        if k in strategy_defaults:
            out[k] = int(strategy_defaults[k])
            out["source"][k] = "center_runtime_default"
            if k == "score_threshold":
                threshold_floor = max(threshold_floor, int(strategy_defaults[k]))

    if bool(center_config.get("auto_apply_backtest_best", True)):
        best_dir_raw = str(center_config.get("backtest_best_dir", "logs/openclaw"))
        best_dir = (project_root / best_dir_raw).resolve() if not Path(best_dir_raw).is_absolute() else Path(best_dir_raw)
        rejected_artifacts_path = str(
            center_config.get("rejected_backtest_artifacts_file")
            or os.getenv("AIRIVO_REJECTED_BACKTEST_ARTIFACTS_FILE", "")
        ).strip()
        best = find_latest_backtest_best(
            strategy=strategy,
            best_dir=best_dir,
            rejected_artifacts_path=rejected_artifacts_path,
        )
        if best:
            for k in ("score_threshold", "sample_size", "holding_days"):
                if k in best:
                    value = int(best[k])
                    if k == "score_threshold" and value < threshold_floor:
                        out[k] = threshold_floor
                        out["source"][k] = "threshold_floor"
                    else:
                        out[k] = value
                        out["source"][k] = "latest_backtest_best"

    cli_values = {
        "score_threshold": requested_score_threshold,
        "sample_size": requested_sample_size,
        "holding_days": requested_holding_days,
    }
    for k, v in cli_values.items():
        if v is not None:
            out[k] = int(v)
            out["source"][k] = "cli_override"

    return out


def find_latest_backtest_best(
    strategy: str,
    best_dir: Path,
    rejected_artifacts_path: str = "",
) -> Optional[JsonDict]:
    rejected_artifacts = _load_rejected_artifacts(rejected_artifacts_path)
    try:
        files = sorted(best_dir.rglob(f"backtest_sweep_{strategy}_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    except Exception:
        return None
    for p in files:
        if _artifact_is_rejected(p, rejected_artifacts):
            continue
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
            best = obj.get("best") or {}
            if not isinstance(best, dict):
                continue
            if str(best.get("status", "success")) != "success":
                continue
            if not _backtest_best_is_usable(obj, best):
                continue
            needed = {}
            for k in ("score_threshold", "sample_size", "holding_days"):
                if k not in best:
                    needed = {}
                    break
                needed[k] = int(best[k])
            if needed:
                needed["artifact"] = str(p)
                return needed
        except Exception:
            continue
    return None


def _load_rejected_artifacts(path: str) -> list[JsonDict]:
    if not str(path or "").strip():
        return []
    try:
        return load_rejected_backtest_artifacts(path)
    except Exception:
        return []


def _artifact_is_rejected(path: Path, rejected_artifacts: list[JsonDict]) -> bool:
    if not rejected_artifacts:
        return False
    normalized_path = str(path.resolve())
    for item in rejected_artifacts:
        rejected_path = str(item.get("artifact_path") or item.get("path") or "").strip()
        if rejected_path:
            try:
                if Path(rejected_path).resolve() == path.resolve():
                    return True
            except Exception:
                if rejected_path == normalized_path or rejected_path == str(path):
                    return True
        rejected_sha = str(item.get("artifact_sha256") or "").strip()
        if rejected_sha and rejected_sha == _safe_artifact_sha(path):
            return True
    return False


def _safe_artifact_sha(path: Path) -> str:
    try:
        import hashlib

        h = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return ""


def _backtest_best_is_usable(payload: JsonDict, best: JsonDict) -> bool:
    diagnostics = payload.get("strategy_backtest_diagnostics")
    if not isinstance(diagnostics, dict) or diagnostics.get("eligible_for_formal_ranking") is not True:
        return False
    credibility = payload.get("backtest_credibility")
    if not _backtest_credibility_is_usable(credibility):
        return False
    metric_keys = {"win_rate", "max_drawdown", "signal_density"}
    if metric_keys.issubset(best.keys()):
        win_rate = _float_metric(best.get("win_rate"), 0.0)
        max_drawdown = _float_metric(best.get("max_drawdown"), 1.0)
        signal_density = _float_metric(best.get("signal_density"), 0.0)
        return bool(win_rate >= 0.45 and max_drawdown <= 0.25 and signal_density > 0.0)
    return True


def _float_metric(value: Any, default: float) -> float:
    if value is None:
        return float(default)
    try:
        return float(value)
    except Exception:
        return float(default)


def _backtest_credibility_is_usable(credibility: Any) -> bool:
    if not isinstance(credibility, dict):
        return False
    if credibility.get("passed") is False:
        return False
    required_flags = (
        "point_in_time_data",
        "suspension_and_limit_handling",
        "volume_constraint",
        "cost_model",
        "slippage_model",
        "in_sample_out_of_sample_split",
        "parameter_sensitivity",
        "failed_backtests_recorded",
    )
    for key in required_flags:
        if credibility.get(key) is not True:
            return False
    metrics = credibility.get("metrics")
    if not isinstance(metrics, dict):
        return False
    if float(metrics.get("signal_density", 0.0) or 0.0) <= 0.0:
        return False
    if int(metrics.get("test_windows", 0) or 0) <= 0:
        return False
    return True


def apply_risk_overrides(strategy: str, thresholds: JsonDict, center_config: JsonDict) -> JsonDict:
    out = dict(thresholds or {})
    risk_overrides = center_config.get("risk_overrides") if isinstance(center_config, dict) else {}
    this = risk_overrides.get(strategy, {}) if isinstance(risk_overrides, dict) else {}
    if not isinstance(this, dict):
        return out
    for k in ("win_rate_min", "max_drawdown_max", "signal_density_min"):
        if k in this:
            out[k] = float(this[k])
    return out


def resolve_strategy_weight(strategy: str, center_config: JsonDict, default: float = 1.0) -> float:
    weights = center_config.get("strategy_weights") if isinstance(center_config, dict) else {}
    if not isinstance(weights, dict):
        return float(default)
    raw = weights.get(strategy)
    try:
        if raw is None:
            return float(default)
        return max(0.0, float(raw))
    except Exception:
        return float(default)


def resolve_run_policy(strategy: str, center_config: JsonDict, default_timeout_sec: int = 900) -> JsonDict:
    run_policy = center_config.get("run_policy") if isinstance(center_config, dict) else {}
    if not isinstance(run_policy, dict):
        return {"timeout_sec": int(default_timeout_sec)}

    out: JsonDict = {"timeout_sec": int(default_timeout_sec)}
    defaults = run_policy.get("default", {})
    if isinstance(defaults, dict):
        out.update(_pick_run_policy_fields(defaults))

    strategy_cfg = run_policy.get(strategy, {})
    if isinstance(strategy_cfg, dict):
        out.update(_pick_run_policy_fields(strategy_cfg))

    if "timeout_sec" not in out:
        out["timeout_sec"] = int(default_timeout_sec)
    return out


def _pick_run_policy_fields(src: JsonDict) -> JsonDict:
    out: JsonDict = {}
    for key in (
        "timeout_sec",
        "offline_stock_limit",
        "sample_size",
        "score_threshold",
        "holding_days",
        "retry_on_no_picks",
        "no_picks_retry_max",
    ):
        if key not in src:
            continue
        val = src.get(key)
        if val is None:
            continue
        try:
            out[key] = int(val)
        except Exception:
            continue
    return out


def _parse_simple_yaml(text: str) -> JsonDict:
    root: JsonDict = {}
    stack: list[Any] = [root]
    indents: list[int] = [0]

    for raw in text.splitlines():
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        line = raw.strip()

        while len(indents) > 1 and indent < indents[-1]:
            stack.pop()
            indents.pop()

        container = stack[-1]
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()

        if value == "":
            child: Any = {}
            if isinstance(container, dict):
                container[key] = child
            stack.append(child)
            indents.append(indent + 2)
            continue

        if isinstance(container, dict):
            container[key] = _yaml_scalar(value)

    return root


def _yaml_scalar(value: str) -> Any:
    low = value.lower()
    if low == "true":
        return True
    if low == "false":
        return False
    if low == "null":
        return None
    if value.startswith('"') and value.endswith('"'):
        return value[1:-1]
    if value.startswith("'") and value.endswith("'"):
        return value[1:-1]
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value
