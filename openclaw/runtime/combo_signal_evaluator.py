from __future__ import annotations

import math
from typing import Any, Callable, Dict, Optional

import pandas as pd

from openclaw.runtime.dataframe_utils import ensure_price_aliases


def resolve_combo_signal_config(
    *,
    combo_params: Dict[str, Any],
    combo_threshold: float,
    min_agree: int,
    market_env: str,
    production_only: bool,
    health_multipliers: Dict[str, float],
) -> Dict[str, Any]:
    env = str(market_env or "oscillation").strip().lower()
    if env not in {"bull", "bear", "oscillation"}:
        env = "oscillation"

    weights = {
        "v5": float(combo_params.get("w_v5", 0.33)),
        "v8": float(combo_params.get("w_v8", 0.34)),
        "v9": float(combo_params.get("w_v9", 0.33)),
    }
    if production_only:
        base_by_env = {
            "bull": {"v5": 0.55, "v8": 0.10, "v9": 0.35},
            "oscillation": {"v5": 0.45, "v8": 0.05, "v9": 0.50},
            "bear": {"v5": 0.20, "v8": 0.20, "v9": 0.60},
        }
        weights = dict(base_by_env.get(env, base_by_env["oscillation"]))

    weights = {k: float(v) * float(health_multipliers.get(k, 1.0)) for k, v in weights.items()}
    weight_sum = sum(weights.values())
    if weight_sum > 1e-9:
        weights = {k: float(v) / weight_sum for k, v in weights.items()}

    return {
        "thresholds": {
            "v5": float(combo_params.get("thr_v5", 60)),
            "v8": float(combo_params.get("thr_v8", 65)),
            "v9": float(combo_params.get("thr_v9", 60)),
        },
        "weights": weights,
        "combo_threshold": float(combo_params.get("combo_threshold", combo_threshold)),
        "min_agree": int(combo_params.get("min_agree", min_agree)),
        "market_env": env,
    }


def evaluate_combo_signal(
    *,
    ts_code: str,
    current_data: pd.DataFrame,
    v5_evaluator: Any,
    v8_evaluator: Any,
    v9_score_fn: Callable[[pd.DataFrame], Dict[str, Any]],
    index_data: Optional[pd.DataFrame],
    thresholds: Dict[str, float],
    weights: Dict[str, float],
    combo_threshold: float,
    min_agree: int,
    market_env: str,
) -> Optional[Dict[str, Any]]:
    current = ensure_price_aliases(current_data.copy())
    industry = str(current["industry"].iloc[-1]) if ("industry" in current.columns and len(current) > 0) else ""

    v5_score = _safe_v5_score(v5_evaluator, current)
    v8_score = _safe_v8_score(v8_evaluator, current, ts_code=ts_code, index_data=index_data, industry=industry)
    v9_score = _safe_v9_score(v9_score_fn, current)

    return evaluate_combo_score_components(
        scores={"v5": v5_score, "v8": v8_score, "v9": v9_score},
        thresholds=thresholds,
        weights=weights,
        combo_threshold=combo_threshold,
        min_agree=min_agree,
        market_env=market_env,
    )


def evaluate_combo_score_components(
    *,
    scores: Dict[str, Optional[float]],
    thresholds: Dict[str, float],
    weights: Dict[str, float],
    combo_threshold: float,
    min_agree: int,
    market_env: str,
) -> Optional[Dict[str, Any]]:
    active_keys = [key for key, value in scores.items() if value is not None and float(weights.get(key, 0.0)) > 0.0]
    if not active_keys:
        return None

    required_agree = max(1, min(int(min_agree), len(active_keys)))
    agree = sum(1 for key in active_keys if scores.get(key) is not None and float(scores[key]) >= float(thresholds.get(key, combo_threshold)))
    if agree < required_agree:
        return None

    weight_sum = sum(float(weights[key]) for key in active_keys)
    if weight_sum <= 1e-9:
        return None
    consensus = sum(float(scores[key]) * float(weights[key]) for key in active_keys) / weight_sum
    if consensus < float(combo_threshold):
        return None

    result: Dict[str, Any] = {
        "signal_strength": consensus,
        "agree_count": agree,
        "required_agree": int(required_agree),
        "market_env": str(market_env),
    }
    for key in scores:
        result[f"{key}_score"] = scores[key] if scores[key] is not None else math.nan
        result[f"w_{key}"] = float(weights.get(key, 0.0))
        result[f"{key}_contrib"] = (float(scores[key]) * float(weights.get(key, 0.0)) / weight_sum) if scores[key] is not None else 0.0
    return result


def finalize_combo_scan_score(
    *,
    consensus_result: Dict[str, Any],
    scores: Dict[str, Optional[float]],
    thresholds: Dict[str, float],
    weights: Dict[str, float],
    combo_threshold: float,
    disagree_std_weight: float,
    disagree_count_weight: float,
    market_adjust_strength: float,
    market_env: str,
    external_bonus: float,
) -> Dict[str, Any]:
    weighted_score = float(consensus_result.get("signal_strength", 0.0) or 0.0)
    active_keys = [key for key, value in scores.items() if value is not None and float(weights.get(key, 0.0)) > 0.0]
    score_list = [float(scores[key]) for key in active_keys if scores[key] is not None]
    score_std = _population_std(score_list) if len(score_list) > 1 else 0.0
    disagree_count = sum(
        1
        for key in active_keys
        if scores.get(key) is not None and float(scores[key]) < float(thresholds.get(key, combo_threshold))
    )
    penalty = (score_std * float(disagree_std_weight)) + (float(disagree_count) * float(disagree_count_weight))
    adj_factor = _market_adjustment_factor(market_env=market_env, market_adjust_strength=market_adjust_strength)
    final_score = (weighted_score * adj_factor) + float(external_bonus) - penalty
    contrib = {f"{key}贡献": float(consensus_result.get(f"{key}_contrib", 0.0) or 0.0) for key in scores}
    return {
        "scores": scores,
        "agree_count": int(consensus_result.get("agree_count", 0) or 0),
        "weighted_score": float(weighted_score),
        "penalty": float(penalty),
        "adj_factor": float(adj_factor),
        "extra": float(external_bonus),
        "final_score": float(final_score),
        "contrib": contrib,
        "required_agree": int(consensus_result.get("required_agree", 0) or 0),
    }


def _safe_v5_score(v5_evaluator: Any, current_data: pd.DataFrame) -> Optional[float]:
    try:
        result = v5_evaluator.evaluate_stock_v4(current_data)
        if result and result.get("success"):
            return float(result.get("final_score", 0))
    except Exception:
        return None
    return None


def _safe_v8_score(v8_evaluator: Any, current_data: pd.DataFrame, *, ts_code: str, index_data: Optional[pd.DataFrame], industry: str) -> Optional[float]:
    try:
        result = v8_evaluator.evaluate_stock_v8(
            current_data,
            ts_code=ts_code,
            index_data=index_data,
            industry=industry,
        )
        if result and result.get("success"):
            return float(result.get("final_score", 0))
    except Exception:
        return None
    return None


def _safe_v9_score(v9_score_fn: Callable[[pd.DataFrame], Dict[str, Any]], current_data: pd.DataFrame) -> Optional[float]:
    try:
        result = v9_score_fn(current_data)
        if result:
            return float(result.get("score", 0))
    except Exception:
        return None
    return None


def _market_adjustment_factor(*, market_env: str, market_adjust_strength: float) -> float:
    env = str(market_env or "oscillation").strip().lower()
    if env == "bull":
        env_multiplier = 1.02
    elif env == "bear":
        env_multiplier = 0.95
    else:
        env_multiplier = 0.98
    return 1.0 - float(market_adjust_strength) + (float(market_adjust_strength) * env_multiplier)


def _population_std(values: list[float]) -> float:
    if not values:
        return 0.0
    mean = sum(values) / len(values)
    return math.sqrt(sum((value - mean) ** 2 for value in values) / len(values))
