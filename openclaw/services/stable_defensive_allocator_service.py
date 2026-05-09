from __future__ import annotations

from typing import Any, Dict, Iterable, List

import pandas as pd


JsonDict = Dict[str, Any]


def build_stable_defensive_allocator_review(
    trades: Any,
    *,
    params: JsonDict | None = None,
) -> JsonDict:
    """Evaluate stable as a portfolio overlay, not as standalone alpha."""

    params = dict(params or {})
    df = _to_frame(trades)
    if df.empty or "future_return" not in df.columns:
        return {
            "available": False,
            "contract": _contract(params),
            "promotion_eligible": False,
            "blocking_reasons": ["missing_stable_trade_return_series"],
        }

    returns_pct = pd.to_numeric(df["future_return"], errors="coerce").dropna()
    if returns_pct.empty:
        return {
            "available": False,
            "contract": _contract(params),
            "promotion_eligible": False,
            "blocking_reasons": ["missing_valid_stable_trade_returns"],
        }

    scores = pd.to_numeric(df.get("signal_strength", pd.Series([60.0] * len(df))), errors="coerce").fillna(60.0)
    weights = _allocation_weights(
        scores=scores,
        min_weight=float(params.get("stable_allocator_min_weight", 0.10)),
        max_weight=float(params.get("stable_allocator_max_weight", 0.45)),
        threshold=float(params.get("score_threshold", 60.0)),
    )
    overlay_returns_pct = returns_pct.reset_index(drop=True) * weights.reset_index(drop=True)
    full_exposure_returns_pct = returns_pct.reset_index(drop=True)
    overlay_drawdown = _max_drawdown_from_pct_returns(overlay_returns_pct)
    full_drawdown = _max_drawdown_from_pct_returns(full_exposure_returns_pct)
    overlay_total = _compound_return_pct(overlay_returns_pct)
    full_total = _compound_return_pct(full_exposure_returns_pct)
    drawdown_reduction = max(0.0, full_drawdown - overlay_drawdown)
    stable_full_exposure_excess_return_pct = overlay_total - full_total
    benchmark_returns_pct = _benchmark_returns(df=df, params=params, length=len(overlay_returns_pct))
    benchmark_available = not benchmark_returns_pct.empty
    benchmark_total = _compound_return_pct(benchmark_returns_pct) if benchmark_available else None
    benchmark_drawdown = _max_drawdown_from_pct_returns(benchmark_returns_pct) if benchmark_available else None
    benchmark_excess_return_pct = (overlay_total - float(benchmark_total)) if benchmark_total is not None else None
    benchmark_drawdown_reduction = (
        max(0.0, float(benchmark_drawdown) - overlay_drawdown) if benchmark_drawdown is not None else None
    )
    blocking: List[str] = []
    if drawdown_reduction <= 0.0:
        blocking.append("portfolio_drawdown_reduction_not_proven")
    if not benchmark_available:
        blocking.append("missing_formal_pool_benchmark_return_series")
    else:
        if float(benchmark_drawdown_reduction or 0.0) <= 0.0:
            blocking.append("formal_pool_drawdown_reduction_not_proven")
        if float(benchmark_excess_return_pct or 0.0) < 0.0:
            blocking.append("non_negative_excess_return_vs_formal_pool_not_proven")

    return {
        "available": True,
        "contract": _contract(params),
        "promotion_eligible": False,
        "allocator_candidate_eligible": bool(
            benchmark_available
            and drawdown_reduction > 0.0
            and float(benchmark_drawdown_reduction or 0.0) > 0.0
            and float(benchmark_excess_return_pct or 0.0) >= 0.0
        ),
        "blocking_reasons": blocking,
        "sample_size": int(len(returns_pct)),
        "weight_profile": {
            "min": float(weights.min()) if not weights.empty else 0.0,
            "max": float(weights.max()) if not weights.empty else 0.0,
            "avg": float(weights.mean()) if not weights.empty else 0.0,
        },
        "overlay_total_return_pct": overlay_total,
        "full_exposure_total_return_pct": full_total,
        "formal_pool_benchmark_total_return_pct": benchmark_total,
        "overlay_max_drawdown": overlay_drawdown,
        "full_exposure_max_drawdown": full_drawdown,
        "formal_pool_benchmark_max_drawdown": benchmark_drawdown,
        "drawdown_reduction": drawdown_reduction,
        "benchmark_drawdown_reduction": benchmark_drawdown_reduction,
        "excess_return_pct": stable_full_exposure_excess_return_pct,
        "stable_full_exposure_excess_return_pct": stable_full_exposure_excess_return_pct,
        "benchmark_excess_return_pct": benchmark_excess_return_pct,
        "success_metric_passed": bool(
            benchmark_available
            and drawdown_reduction > 0.0
            and float(benchmark_drawdown_reduction or 0.0) > 0.0
            and float(benchmark_excess_return_pct or 0.0) >= 0.0
        ),
    }


def _contract(params: JsonDict) -> JsonDict:
    return {
        "role": "defensive_allocator_overlay",
        "not_standalone_alpha": True,
        "allocation_weight_bounds": {
            "min": float(params.get("stable_allocator_min_weight", 0.10)),
            "max": float(params.get("stable_allocator_max_weight", 0.45)),
        },
        "cash_fallback_rule": "unallocated_weight_stays_cash",
        "required_benchmarks": [
            "formal_strategy_pool_returns",
            "stable_full_exposure_returns",
            "cash_fallback_return",
        ],
        "success_metric": "portfolio_drawdown_reduction_with_non_negative_excess_return_and_no_hidden_turnover_cost",
    }


def _to_frame(trades: Any) -> pd.DataFrame:
    if isinstance(trades, pd.DataFrame):
        return trades.copy()
    if isinstance(trades, Iterable) and not isinstance(trades, (str, bytes, dict)):
        return pd.DataFrame(list(trades))
    return pd.DataFrame()


def _allocation_weights(*, scores: pd.Series, min_weight: float, max_weight: float, threshold: float) -> pd.Series:
    lo = max(0.0, min(float(min_weight), float(max_weight)))
    hi = min(1.0, max(float(min_weight), float(max_weight)))
    span = max(1.0, 100.0 - float(threshold))
    scaled = ((scores.astype(float) - float(threshold)) / span).clip(lower=0.0, upper=1.0)
    return lo + (hi - lo) * scaled


def _benchmark_returns(*, df: pd.DataFrame, params: JsonDict, length: int) -> pd.Series:
    if "formal_pool_return" in df.columns:
        return pd.to_numeric(df["formal_pool_return"], errors="coerce").dropna().reset_index(drop=True).head(length)
    raw = params.get("formal_pool_returns_pct") or params.get("benchmark_returns_pct")
    if raw is None:
        return pd.Series(dtype=float)
    if isinstance(raw, pd.Series):
        series = raw
    elif isinstance(raw, Iterable) and not isinstance(raw, (str, bytes, dict)):
        series = pd.Series(list(raw))
    else:
        return pd.Series(dtype=float)
    return pd.to_numeric(series, errors="coerce").dropna().reset_index(drop=True).head(length)


def _compound_return_pct(returns_pct: pd.Series) -> float:
    value = 1.0
    for item in returns_pct:
        value *= 1.0 + float(item) / 100.0
    return (value - 1.0) * 100.0


def _max_drawdown_from_pct_returns(returns_pct: pd.Series) -> float:
    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    for item in returns_pct:
        equity *= 1.0 + float(item) / 100.0
        peak = max(peak, equity)
        if peak > 0:
            max_dd = max(max_dd, (peak - equity) / peak)
    return float(max_dd)
