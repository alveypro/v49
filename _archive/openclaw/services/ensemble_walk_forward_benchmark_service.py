from __future__ import annotations

from typing import Any, Dict, Sequence


JsonDict = Dict[str, Any]


FORMAL_POOL_BENCHMARK_STRATEGIES = ("v4", "v5", "v8", "v9", "combo")
MIN_WALK_FORWARD_WINDOWS = 5


def build_ensemble_walk_forward_shadow_benchmark(
    windows: Sequence[JsonDict] | None,
    *,
    formal_pool_strategies: Sequence[str] = FORMAL_POOL_BENCHMARK_STRATEGIES,
    min_windows: int = MIN_WALK_FORWARD_WINDOWS,
) -> JsonDict:
    """Audit ensemble_core walk-forward benchmark readiness.

    P6 is a promotion gate, not an optimizer.  It requires multiple windows,
    after-cost shadow portfolio returns, and replayable formal pool benchmark
    returns before it emits comparison metrics.
    """

    rows = [item for item in (windows or []) if isinstance(item, dict)]
    blocking: list[str] = []
    if len(rows) < int(min_windows):
        blocking.append(f"insufficient_walk_forward_windows:{len(rows)}/{int(min_windows)}")

    valid = []
    for idx, row in enumerate(rows):
        window_blocking = _window_blocking(row)
        if window_blocking:
            blocking.extend([f"window_{idx}:{reason}" for reason in window_blocking])
            continue
        valid.append(row)

    if len(valid) < int(min_windows):
        blocking.append(f"insufficient_valid_after_cost_windows:{len(valid)}/{int(min_windows)}")

    if blocking:
        return _blocked(rows=rows, formal_pool_strategies=formal_pool_strategies, blocking=blocking)

    ensemble_returns = [float((row.get("execution_cost_replay") or {}).get("net_return", 0.0) or 0.0) for row in valid]
    benchmark_returns = [float((row.get("formal_pool_benchmark") or {}).get("avg_return_pct", 0.0) or 0.0) for row in valid]
    excess = [left - right for left, right in zip(ensemble_returns, benchmark_returns)]
    turnovers = [float((row.get("execution_cost_replay") or {}).get("turnover", 0.0) or 0.0) for row in valid]

    return {
        "benchmark_version": "ensemble_walk_forward_shadow_benchmark.v1",
        "research_only": True,
        "not_for_production": True,
        "passed": True,
        "formal_pool_strategies": list(formal_pool_strategies),
        "window_count": len(rows),
        "valid_window_count": len(valid),
        "after_cost_excess_return": round(sum(excess) / float(len(excess)), 6),
        "max_drawdown": _max_drawdown_pct(ensemble_returns),
        "hit_rate": round(sum(1 for value in excess if value > 0.0) / float(len(excess)), 6),
        "turnover": round(sum(turnovers) / float(len(turnovers)), 6),
        "capacity_utilization": _avg_capacity_utilization(valid),
        "industry_concentration": _max_industry_concentration(valid),
        "regime_split": _regime_split(valid, excess),
        "risk_contribution": _risk_contribution(valid),
        "blocking_reasons": [],
        "hard_boundaries": [
            "do_not_promote_to_observation_without_passing_walk_forward_shadow_benchmark",
            "do_not_compare_pre_cost_ensemble_against_after_cost_formal_pool",
        ],
    }


def _blocked(*, rows: Sequence[JsonDict], formal_pool_strategies: Sequence[str], blocking: Sequence[str]) -> JsonDict:
    return {
        "benchmark_version": "ensemble_walk_forward_shadow_benchmark.v1",
        "research_only": True,
        "not_for_production": True,
        "passed": False,
        "formal_pool_strategies": list(formal_pool_strategies),
        "window_count": len(rows),
        "valid_window_count": 0,
        "after_cost_excess_return": None,
        "max_drawdown": None,
        "hit_rate": None,
        "turnover": None,
        "capacity_utilization": None,
        "industry_concentration": None,
        "regime_split": {},
        "risk_contribution": {},
        "blocking_reasons": sorted(set(str(item) for item in blocking if str(item or ""))),
        "hard_boundaries": [
            "do_not_promote_to_observation_without_passing_walk_forward_shadow_benchmark",
            "do_not_compare_pre_cost_ensemble_against_after_cost_formal_pool",
        ],
    }


def _window_blocking(row: JsonDict) -> list[str]:
    blocking: list[str] = []
    execution = row.get("execution_cost_replay") if isinstance(row.get("execution_cost_replay"), dict) else {}
    benchmark = row.get("formal_pool_benchmark") if isinstance(row.get("formal_pool_benchmark"), dict) else {}
    if execution.get("research_only") is not True or execution.get("not_for_production") is not True:
        blocking.append("missing_research_only_after_cost_replay")
    if execution.get("blocking_reasons"):
        blocking.extend([f"execution_blocked:{reason}" for reason in execution.get("blocking_reasons") or []])
    if not execution.get("trade_replay"):
        blocking.append("missing_after_cost_trade_replay")
    if benchmark.get("available") is not True:
        blocking.append("missing_formal_pool_benchmark")
    return blocking


def _max_drawdown_pct(returns: Sequence[float]) -> float:
    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    for value in returns:
        equity *= 1.0 + float(value) / 100.0
        peak = max(peak, equity)
        if peak > 0.0:
            max_dd = min(max_dd, equity / peak - 1.0)
    return round(max_dd * 100.0, 6)


def _avg_capacity_utilization(rows: Sequence[JsonDict]) -> float:
    values = []
    for row in rows:
        execution = row.get("execution_cost_replay") if isinstance(row.get("execution_cost_replay"), dict) else {}
        for replay in execution.get("trade_replay") or []:
            if isinstance(replay, dict) and replay.get("traded") is True:
                values.append(float(replay.get("capacity_usage", 0.0) or 0.0))
    return round(sum(values) / float(len(values)), 6) if values else 0.0


def _max_industry_concentration(rows: Sequence[JsonDict]) -> float:
    values = []
    for row in rows:
        portfolio = row.get("shadow_portfolio") if isinstance(row.get("shadow_portfolio"), dict) else {}
        exposure = portfolio.get("industry_exposure") if isinstance(portfolio.get("industry_exposure"), dict) else {}
        if exposure:
            values.append(max(float(value or 0.0) for value in exposure.values()))
    return round(max(values), 6) if values else 0.0


def _regime_split(rows: Sequence[JsonDict], excess_returns: Sequence[float]) -> JsonDict:
    groups: dict[str, list[float]] = {}
    for idx, row in enumerate(rows):
        regime = row.get("market_regime") if isinstance(row.get("market_regime"), dict) else {}
        label = str(row.get("market_regime_label") or regime.get("label") or "unknown")
        groups.setdefault(label, []).append(float(excess_returns[idx] if idx < len(excess_returns) else 0.0))
    return {
        label: {
            "window_count": len(values),
            "avg_after_cost_excess_return": round(sum(values) / float(len(values)), 6) if values else 0.0,
            "hit_rate": round(sum(1 for value in values if value > 0.0) / float(len(values)), 6) if values else 0.0,
        }
        for label, values in sorted(groups.items())
    }


def _risk_contribution(rows: Sequence[JsonDict]) -> JsonDict:
    source_weights: dict[str, float] = {}
    industry_weights: dict[str, float] = {}
    for row in rows:
        portfolio = row.get("shadow_portfolio") if isinstance(row.get("shadow_portfolio"), dict) else {}
        for weight in portfolio.get("shadow_weights") or []:
            if not isinstance(weight, dict):
                continue
            value = float(weight.get("weight", 0.0) or 0.0)
            source = str(weight.get("source_strategy") or "unknown")
            industry = str(weight.get("industry") or "unknown")
            source_weights[source] = source_weights.get(source, 0.0) + value
            industry_weights[industry] = industry_weights.get(industry, 0.0) + value
    source_total = sum(source_weights.values())
    industry_total = sum(industry_weights.values())
    return {
        "source_strategy_weight_share": {
            key: round(value / source_total, 6) for key, value in sorted(source_weights.items()) if source_total > 0.0
        },
        "industry_weight_share": {
            key: round(value / industry_total, 6) for key, value in sorted(industry_weights.items()) if industry_total > 0.0
        },
    }
