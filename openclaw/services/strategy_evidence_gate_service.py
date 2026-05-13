from __future__ import annotations

import os
from typing import Any, Dict, Iterable, List

from strategies.registry import get_profile


JsonDict = Dict[str, Any]


def unified_walk_forward_contract() -> JsonDict:
    return {
        "contract_version": "unified_walk_forward_evidence.v1",
        "mode": "walk_forward",
        "train_window_days": int(os.getenv("OPENCLAW_WF_TRAIN_DAYS", "90") or 90),
        "test_window_days": int(os.getenv("OPENCLAW_WF_TEST_DAYS", "30") or 30),
        "step_days": int(os.getenv("OPENCLAW_WF_STEP_DAYS", "30") or 30),
        "fixed_seed": int(os.getenv("OPENCLAW_WF_FIXED_SEED", "20260512") or 20260512),
        "stock_pool_policy": "deterministic_liquidity_ranked_pool",
        "fee_bps_per_side": float(os.getenv("OPENCLAW_BACKTEST_FEE_BPS", "3") or 3),
        "slippage_bps_per_side": float(os.getenv("OPENCLAW_BACKTEST_SLIPPAGE_BPS", "5") or 5),
        "required_outputs": [
            "win_rate",
            "return_mean",
            "return_median",
            "max_drawdown",
            "turnover",
            "coverage",
            "sample_count",
        ],
        "quality_floor": {
            "min_win_rate": float(os.getenv("OPENCLAW_WF_MIN_WIN_RATE", "0.45") or 0.45),
            "max_drawdown": float(os.getenv("OPENCLAW_WF_MAX_DRAWDOWN", "0.25") or 0.25),
            "min_signal_density": float(os.getenv("OPENCLAW_WF_MIN_SIGNAL_DENSITY", "0.0001") or 0.0001),
            "min_sample_count": int(os.getenv("OPENCLAW_WF_MIN_SAMPLE_COUNT", "30") or 30),
        },
        "display_policy": {
            "hide_accuracy_if_sample_count_below_minimum": True,
            "accuracy_requires_successful_walk_forward_gate": True,
        },
    }


def normalized_walk_forward_metrics(backtest_component: JsonDict) -> JsonDict:
    raw = backtest_component.get("walk_forward_metrics")
    if not isinstance(raw, dict):
        raw = {}
    return {
        "win_rate": _float(raw.get("win_rate")),
        "return_mean": _optional_float(raw.get("return_mean", raw.get("avg_return", raw.get("avg_return_pct")))),
        "return_median": _optional_float(raw.get("return_median", raw.get("median_return", raw.get("median_return_pct")))),
        "max_drawdown": _float(raw.get("max_drawdown"), 1.0),
        "turnover": _optional_float(raw.get("turnover", raw.get("turnover_rate"))),
        "coverage": _optional_float(raw.get("coverage", raw.get("signal_density"))),
        "sample_count": _int(raw.get("sample_count", raw.get("samples", raw.get("test_windows")))),
        "signal_density": _float(raw.get("signal_density")),
        "test_windows": _int(raw.get("test_windows")),
        "train_windows": _int(raw.get("train_windows")),
        "failed_windows": _int(raw.get("failed_windows")),
    }


def evaluate_strategy_top5_gate(strategy: str, review: JsonDict) -> JsonDict:
    strategy_name = str(strategy or "").strip().lower()
    profile = get_profile(strategy_name)
    backtest = review.get("backtest_component") if isinstance(review.get("backtest_component"), dict) else {}
    metrics = normalized_walk_forward_metrics(backtest)
    contract = unified_walk_forward_contract()
    floor = contract["quality_floor"]

    blocking: List[str] = []
    if profile.tier != "canary":
        blocking.append(f"strategy_not_canary:{strategy_name}:{profile.tier}")
    if backtest.get("passed") is not True:
        blocking.append("walk_forward_backtest_not_passed")
    if backtest.get("quality_floor_passed") is not True:
        blocking.append("walk_forward_quality_floor_not_passed")
    if float(metrics["win_rate"]) < float(floor["min_win_rate"]):
        blocking.append("win_rate_below_floor")
    if float(metrics["max_drawdown"]) > float(floor["max_drawdown"]):
        blocking.append("drawdown_above_floor")
    if float(metrics["signal_density"]) <= float(floor["min_signal_density"]):
        blocking.append("signal_density_below_floor")
    if int(metrics["sample_count"]) < int(floor["min_sample_count"]):
        blocking.append("sample_count_below_floor")

    missing_outputs = [
        key
        for key in ("return_mean", "return_median", "turnover", "coverage")
        if metrics.get(key) is None
    ]
    return {
        "gate_version": "strategy_top5_canary_gate.v1",
        "passed": not blocking,
        "blocking_reasons": blocking,
        "strategy": strategy_name,
        "strategy_tier": profile.tier,
        "strategy_stage": profile.stage,
        "evidence_status": profile.evidence_status,
        "contract": contract,
        "metrics": metrics,
        "missing_optional_outputs": missing_outputs,
        "display_accuracy": not blocking and int(metrics["sample_count"]) >= int(floor["min_sample_count"]),
    }


def evaluate_top5_artifact_canary_gate(payload: JsonDict) -> JsonDict:
    rows = [dict(item) for item in (payload.get("top5_portfolio_audit") or []) if isinstance(item, dict)]
    blocking: List[str] = []
    if not rows:
        blocking.append("top5_rows_missing")

    seen_refs = 0
    for row in rows:
        ts_code = str(row.get("ts_code") or "")
        source = row.get("source") if isinstance(row.get("source"), dict) else {}
        refs = [dict(item) for item in (source.get("signal_refs") or []) if isinstance(item, dict)]
        if not refs:
            blocking.append(f"missing_signal_refs:{ts_code}")
            continue
        for ref in refs:
            seen_refs += 1
            strategy = str(ref.get("strategy") or "").strip().lower()
            tier = str(ref.get("strategy_tier") or "").strip().lower()
            try:
                expected_tier = get_profile(strategy).tier
            except Exception:
                expected_tier = "unknown"
            if tier != "canary" or expected_tier != "canary":
                blocking.append(f"non_canary_signal_ref:{ts_code}:{strategy}:{tier or expected_tier}")

    recommendation = payload.get("recommendation_summary") if isinstance(payload.get("recommendation_summary"), dict) else {}
    for strategy in _iter_strategy_names(recommendation.get("top_strategies")):
        if get_profile(strategy).tier != "canary":
            blocking.append(f"top_strategy_not_canary:{strategy}")
    if seen_refs <= 0:
        blocking.append("no_traceable_canary_signal_refs")

    return {
        "gate_version": "top5_artifact_canary_gate.v1",
        "passed": not blocking,
        "blocking_reasons": sorted(set(blocking)),
        "checked_rows": len(rows),
        "checked_signal_refs": seen_refs,
        "policy": "Top5 trader brief may only be generated from canary strategies that passed the unified evidence gate.",
    }


def _iter_strategy_names(values: Any) -> Iterable[str]:
    if not isinstance(values, list):
        return []
    return [str(item or "").strip().lower() for item in values if str(item or "").strip()]


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)
