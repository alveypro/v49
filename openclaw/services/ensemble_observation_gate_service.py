from __future__ import annotations

from typing import Any, Dict, Sequence


JsonDict = Dict[str, Any]


DEFAULT_REQUIRED_REGIMES = ("risk_on", "neutral")
DEFAULT_MIN_FRESH_WINDOWS = 8
DEFAULT_TARGET_FRESH_WINDOWS = 10
DEFAULT_MIN_REGIME_WINDOWS = {"risk_on": 3, "neutral": 2}
DEFAULT_MIN_OVERALL_HIT_RATE = 0.60
DEFAULT_MIN_REGIME_HIT_RATE = 0.50
DEFAULT_MAX_TURNOVER = 0.75
DEFAULT_MAX_INDUSTRY_CONCENTRATION = 0.30
DEFAULT_MAX_CAPACITY_UTILIZATION = 0.10


def build_ensemble_observation_gate(
    shadow_benchmark_payload: JsonDict | None,
    *,
    candidate: str = "hard_event_alpha_candidate",
    min_fresh_windows: int = DEFAULT_MIN_FRESH_WINDOWS,
    target_fresh_windows: int = DEFAULT_TARGET_FRESH_WINDOWS,
    required_regimes: Sequence[str] = DEFAULT_REQUIRED_REGIMES,
    min_regime_windows: JsonDict | None = None,
    min_overall_hit_rate: float = DEFAULT_MIN_OVERALL_HIT_RATE,
    min_regime_hit_rate: float = DEFAULT_MIN_REGIME_HIT_RATE,
    max_turnover: float = DEFAULT_MAX_TURNOVER,
    max_industry_concentration: float = DEFAULT_MAX_INDUSTRY_CONCENTRATION,
    max_capacity_utilization: float = DEFAULT_MAX_CAPACITY_UTILIZATION,
) -> JsonDict:
    """Harden the research-only path from shadow benchmark to observation review.

    This is not a pool mutation service.  Passing this gate only means the
    candidate has enough evidence for an explicit observation promotion review.
    """

    payload = shadow_benchmark_payload if isinstance(shadow_benchmark_payload, dict) else {}
    benchmark = payload.get("benchmark") if isinstance(payload.get("benchmark"), dict) else payload
    windows = payload.get("windows") if isinstance(payload.get("windows"), list) else []
    rule_freeze = payload.get("rule_freeze") if isinstance(payload.get("rule_freeze"), dict) else {}
    blocking: list[str] = []
    thresholds = {
        "min_fresh_windows": int(min_fresh_windows),
        "target_fresh_windows": int(target_fresh_windows),
        "required_regimes": list(required_regimes),
        "min_regime_windows": dict(DEFAULT_MIN_REGIME_WINDOWS | (min_regime_windows or {})),
        "min_overall_hit_rate": float(min_overall_hit_rate),
        "min_regime_hit_rate": float(min_regime_hit_rate),
        "min_after_cost_excess_return": 0.0,
        "max_turnover": float(max_turnover),
        "max_industry_concentration": float(max_industry_concentration),
        "max_capacity_utilization": float(max_capacity_utilization),
    }

    payload_candidate = str(payload.get("candidate") or benchmark.get("candidate") or "")
    if payload_candidate and payload_candidate != str(candidate):
        blocking.append(f"candidate_mismatch:{payload_candidate}!={candidate}")
    if payload.get("research_only") is not True and benchmark.get("research_only") is not True:
        blocking.append("missing_research_only_shadow_benchmark")
    if payload.get("not_for_production") is False or benchmark.get("not_for_production") is False:
        blocking.append("shadow_benchmark_marked_for_production")
    if benchmark.get("passed") is not True:
        blocking.append("shadow_benchmark_not_passed")
    for reason in benchmark.get("blocking_reasons") or []:
        blocking.append(f"shadow_benchmark_blocked:{reason}")
    if rule_freeze and rule_freeze.get("frozen") is not True:
        blocking.append("candidate_rule_not_frozen")
    if rule_freeze and rule_freeze.get("rule_hash") in (None, ""):
        blocking.append("missing_rule_hash")

    valid_window_count = int(float(benchmark.get("valid_window_count") or 0))
    unique_as_of_count = _unique_as_of_count(windows)
    evidence_window_count = max(valid_window_count, unique_as_of_count)
    if evidence_window_count < int(min_fresh_windows):
        blocking.append(f"insufficient_fresh_windows:{evidence_window_count}/{int(min_fresh_windows)}")
    if unique_as_of_count and unique_as_of_count < valid_window_count:
        blocking.append(f"duplicate_or_missing_as_of_windows:{unique_as_of_count}/{valid_window_count}")

    after_cost_excess = _optional_float(benchmark.get("after_cost_excess_return"))
    if after_cost_excess is None or after_cost_excess <= 0.0:
        blocking.append(f"non_positive_after_cost_excess_return:{after_cost_excess}")
    hit_rate = _optional_float(benchmark.get("hit_rate"))
    if hit_rate is None or hit_rate < float(min_overall_hit_rate):
        blocking.append(f"overall_hit_rate_below_floor:{hit_rate}/{float(min_overall_hit_rate)}")
    turnover = _optional_float(benchmark.get("turnover"))
    if turnover is None or turnover > float(max_turnover):
        blocking.append(f"turnover_above_cap:{turnover}/{float(max_turnover)}")
    capacity = _optional_float(benchmark.get("capacity_utilization"))
    if capacity is None or capacity > float(max_capacity_utilization):
        blocking.append(f"capacity_utilization_above_cap:{capacity}/{float(max_capacity_utilization)}")
    industry = _optional_float(benchmark.get("industry_concentration"))
    if industry is None or industry >= float(max_industry_concentration):
        blocking.append(f"industry_concentration_at_or_above_cap:{industry}/{float(max_industry_concentration)}")

    regime_split = benchmark.get("regime_split") if isinstance(benchmark.get("regime_split"), dict) else {}
    regime_reviews = _regime_reviews(
        regime_split,
        required_regimes=required_regimes,
        min_regime_windows=thresholds["min_regime_windows"],
        min_regime_hit_rate=float(min_regime_hit_rate),
    )
    blocking.extend(regime_reviews["blocking_reasons"])

    passed = not blocking
    return {
        "gate_version": "ensemble_observation_gate.v1",
        "candidate": str(candidate),
        "research_only": True,
        "not_for_production": True,
        "observation_gate_passed": passed,
        "observation_review_eligible": passed,
        "observation_pool_eligible": False,
        "formal_pool_eligible": False,
        "thresholds": thresholds,
        "evidence_summary": {
            "valid_window_count": valid_window_count,
            "unique_as_of_window_count": unique_as_of_count,
            "target_fresh_windows": int(target_fresh_windows),
            "after_cost_excess_return": after_cost_excess,
            "hit_rate": hit_rate,
            "turnover": turnover,
            "capacity_utilization": capacity,
            "industry_concentration": industry,
            "regime_split": regime_split,
            "regime_reviews": regime_reviews["reviews"],
        },
        "blocking_reasons": sorted(set(str(item) for item in blocking if str(item or ""))),
        "required_next_evidence": [] if passed else _required_next_evidence(blocking),
        "hard_boundaries": [
            "observation_gate_does_not_mutate_strategy_pool",
            "do_not_promote_to_observation_with_fewer_than_min_fresh_windows",
            "do_not_allow_single_window_regime_support",
            "do_not_ignore_turnover_or_industry_concentration_caps",
            "do_not_promote_to_formal_pool_from_observation_gate",
        ],
    }


def _regime_reviews(
    regime_split: JsonDict,
    *,
    required_regimes: Sequence[str],
    min_regime_windows: JsonDict,
    min_regime_hit_rate: float,
) -> JsonDict:
    reviews: JsonDict = {}
    blocking: list[str] = []
    for regime in required_regimes:
        raw = regime_split.get(str(regime)) if isinstance(regime_split.get(str(regime)), dict) else {}
        window_count = int(float(raw.get("window_count") or 0))
        avg_excess = _optional_float(raw.get("avg_after_cost_excess_return"))
        hit_rate = _optional_float(raw.get("hit_rate"))
        required_count = int(float(min_regime_windows.get(str(regime), 2) or 2))
        passed = True
        reasons: list[str] = []
        if window_count < required_count:
            passed = False
            reasons.append(f"insufficient_{regime}_windows:{window_count}/{required_count}")
        if avg_excess is None or avg_excess <= 0.0:
            passed = False
            reasons.append(f"regime_excess_not_positive:{regime}:{avg_excess}")
        if hit_rate is None or hit_rate < float(min_regime_hit_rate):
            passed = False
            reasons.append(f"regime_hit_rate_below_floor:{regime}:{hit_rate}/{float(min_regime_hit_rate)}")
        reviews[str(regime)] = {
            "window_count": window_count,
            "min_window_count": required_count,
            "avg_after_cost_excess_return": avg_excess,
            "hit_rate": hit_rate,
            "passed": passed,
            "blocking_reasons": reasons,
        }
        blocking.extend(reasons)
    return {"reviews": reviews, "blocking_reasons": blocking}


def _required_next_evidence(blocking: Sequence[str]) -> list[str]:
    required: list[str] = []
    text = " ".join(blocking)
    if "insufficient_fresh_windows" in text:
        required.append("add_3_to_5_more_fresh_as_of_windows_until_8_to_10_total")
    if "neutral" in text or "risk_on" in text:
        required.append("prove_regime_split_with_multiple_positive_neutral_and_risk_on_windows")
    if "turnover_above_cap" in text:
        required.append("reduce_shadow_portfolio_turnover_before_observation_review")
    if "industry_concentration" in text:
        required.append("tighten_industry_budget_or_cash_fallback_before_observation_review")
    if "after_cost" in text or "hit_rate" in text:
        required.append("rerun_after_cost_shadow_benchmark_after_alpha_or_allocator_fix")
    return sorted(set(required))


def _unique_as_of_count(windows: Sequence[Any]) -> int:
    values = set()
    for row in windows:
        if not isinstance(row, dict):
            continue
        as_of = str(row.get("as_of_date") or "").replace("-", "").strip()
        if as_of:
            values.add(as_of)
    return len(values)


def _optional_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return round(float(value), 6)
    except (TypeError, ValueError):
        return None
