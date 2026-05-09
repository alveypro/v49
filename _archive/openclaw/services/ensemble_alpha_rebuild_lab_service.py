from __future__ import annotations

import math
from typing import Any, Dict, Sequence


JsonDict = Dict[str, Any]


REBUILD_CANDIDATES = (
    "quality_adjusted_momentum",
    "flow_quality_confirmation",
    "quality_discounted_reversal",
    "industry_neutral_quality_momentum",
    "flow_trend_event_guard",
    "reversal_exhaustion_quality_guard",
    "hard_event_alpha_candidate",
)
DECAY_HORIZONS = (1, 3, 5, 10, 20)


def build_ensemble_alpha_rebuild_lab(
    fact_chain: JsonDict | None,
    *,
    min_samples: int = 30,
    min_research_windows: int = 5,
) -> JsonDict:
    """Evaluate rebuilt alpha sleeve candidates without promoting them.

    The lab intentionally treats a single as-of fact chain as insufficient
    research evidence.  Candidate IC can be inspected, but no candidate may feed
    portfolio construction until it survives multiple independent windows.
    """

    payload = fact_chain if isinstance(fact_chain, dict) else {}
    items = payload.get("sample_facts") if isinstance(payload.get("sample_facts"), list) else []
    as_of = str(payload.get("as_of_date") or "")
    research_window_count = len({as_of} if as_of else set())
    reviews = {
        candidate: _candidate_review(
            candidate=candidate,
            items=items,
            min_samples=int(min_samples),
            research_window_count=research_window_count,
            min_research_windows=int(min_research_windows),
        )
        for candidate in REBUILD_CANDIDATES
    }
    candidate_alpha = [
        name
        for name, review in reviews.items()
        if review.get("recommended_use") == "positive_alpha_candidate"
    ]
    return {
        "lab_version": "ensemble_alpha_rebuild_lab.v1",
        "research_only": True,
        "candidate_recipes": list(REBUILD_CANDIDATES),
        "research_window_count": research_window_count,
        "min_research_windows": int(min_research_windows),
        "candidate_reviews": reviews,
        "candidate_alpha_sleeves": candidate_alpha,
        "blocking_reasons": (
            []
            if candidate_alpha
            else ["no_rebuilt_alpha_candidate_passed_policy"]
        ),
        "hard_boundaries": [
            "do_not_promote_rebuilt_sleeves_from_single_window_ic",
            "do_not_replace_sleeve_policy_audit_with_lab_metrics",
            "do_not_feed_rebuilt_candidates_into_portfolio_until_walk_forward_validated",
        ],
    }


def build_ensemble_alpha_rebuild_multi_window_lab(
    fact_chains: Sequence[JsonDict] | None,
    *,
    min_samples: int = 30,
    min_research_windows: int = 5,
) -> JsonDict:
    """Evaluate rebuilt candidates across independent as-of windows."""

    chains = [item for item in (fact_chains or []) if isinstance(item, dict)]
    window_dates = sorted({str(item.get("as_of_date") or "") for item in chains if str(item.get("as_of_date") or "")})
    research_window_count = len(window_dates)
    all_items: list[JsonDict] = []
    window_reviews: dict[str, JsonDict] = {}
    for chain in chains:
        as_of = str(chain.get("as_of_date") or "")
        items = chain.get("sample_facts") if isinstance(chain.get("sample_facts"), list) else []
        all_items.extend(items)
        if as_of:
            window_reviews[as_of] = build_ensemble_alpha_rebuild_lab(
                chain,
                min_samples=min_samples,
                min_research_windows=1,
            )

    reviews = {
        candidate: _candidate_review(
            candidate=candidate,
            items=all_items,
            min_samples=int(min_samples) * max(1, int(min_research_windows)),
            research_window_count=research_window_count,
            min_research_windows=int(min_research_windows),
        )
        for candidate in REBUILD_CANDIDATES
    }
    for candidate, review in reviews.items():
        review["window_positive_count"] = _window_positive_count(candidate=candidate, window_reviews=window_reviews)
        if (
            review.get("recommended_use") == "positive_alpha_candidate"
            and int(review.get("window_positive_count", 0) or 0) < int(min_research_windows)
        ):
            review["recommended_use"] = "research_blocked_unstable_window_coverage"
            review["blocking_reasons"] = ["candidate_not_positive_alpha:research_blocked_unstable_window_coverage"]

    candidate_alpha = [
        name
        for name, review in reviews.items()
        if review.get("recommended_use") == "positive_alpha_candidate"
    ]
    blocking = []
    if research_window_count < int(min_research_windows):
        blocking.append(f"insufficient_research_windows:{research_window_count}/{int(min_research_windows)}")
    if not candidate_alpha:
        blocking.append("no_rebuilt_alpha_candidate_passed_policy")
    return {
        "lab_version": "ensemble_alpha_rebuild_multi_window_lab.v1",
        "research_only": True,
        "candidate_recipes": list(REBUILD_CANDIDATES),
        "research_window_count": research_window_count,
        "min_research_windows": int(min_research_windows),
        "window_dates": window_dates,
        "candidate_reviews": reviews,
        "window_reviews": window_reviews,
        "candidate_alpha_sleeves": candidate_alpha,
        "blocking_reasons": blocking,
        "hard_boundaries": [
            "do_not_promote_rebuilt_sleeves_without_minimum_independent_windows",
            "do_not_average_away_failed_windows",
            "do_not_feed_rebuilt_candidates_into_portfolio_until_policy_audit_accepts_them",
        ],
    }


def _candidate_review(
    *,
    candidate: str,
    items: Sequence[JsonDict],
    min_samples: int,
    research_window_count: int,
    min_research_windows: int,
) -> JsonDict:
    scores = _candidate_scores(candidate, items)
    active = [score for score in scores if score > 0.0]
    multi = _multi_horizon_attribution(candidate=candidate, items=items)
    h5 = multi.get("horizons", {}).get("5", {})
    recommended = _recommended_use(
        active_count=len(active),
        sample_count=int(h5.get("sample_count", 0) or 0),
        min_samples=min_samples,
        research_window_count=research_window_count,
        min_research_windows=min_research_windows,
        five_day_ic=h5.get("ic"),
        five_day_rank_ic=h5.get("rank_ic"),
        multi=multi,
    )
    blockers = []
    if recommended != "positive_alpha_candidate":
        blockers.append(f"candidate_not_positive_alpha:{recommended}")
    return {
        "candidate": candidate,
        "active_signal_count": len(active),
        "avg_score": sum(active) / float(len(active)) if active else 0.0,
        "ic": h5.get("ic"),
        "rank_ic": h5.get("rank_ic"),
        "multi_horizon_attribution": multi,
        "recommended_use": recommended,
        "blocking_reasons": blockers,
    }


def _window_positive_count(*, candidate: str, window_reviews: dict[str, JsonDict]) -> int:
    count = 0
    for review in window_reviews.values():
        candidate_review = (review.get("candidate_reviews") or {}).get(candidate) or {}
        if candidate_review.get("recommended_use") == "positive_alpha_candidate":
            count += 1
    return count


def _candidate_score(candidate: str, item: JsonDict) -> float:
    return _candidate_scores(candidate, [item])[0]


def _candidate_scores(candidate: str, items: Sequence[JsonDict]) -> list[float]:
    if candidate == "industry_neutral_quality_momentum":
        return _industry_neutral_quality_momentum_scores(items)
    if candidate == "flow_trend_event_guard":
        return [_flow_trend_event_guard_score(item) for item in items]
    if candidate == "reversal_exhaustion_quality_guard":
        return [_reversal_exhaustion_quality_guard_score(item) for item in items]
    if candidate == "hard_event_alpha_candidate":
        return [_hard_event_alpha_candidate_score(item) for item in items]
    return [_legacy_candidate_score(candidate, item) for item in items]


def _legacy_candidate_score(candidate: str, item: JsonDict) -> float:
    scores = item.get("sleeve_scores") if isinstance(item.get("sleeve_scores"), dict) else {}
    momentum = _score(scores, "momentum")
    reversal = _score(scores, "reversal")
    money_flow = _score(scores, "money_flow")
    quality = _score(scores, "quality_low_vol")
    event_risk = _score(scores, "event_risk")
    sector = _score(scores, "sector_rotation")
    if candidate == "quality_adjusted_momentum":
        return momentum * (quality / 100.0) * (0.8 + 0.2 * sector / 100.0)
    if candidate == "flow_quality_confirmation":
        return money_flow * (quality / 100.0)
    if candidate == "quality_discounted_reversal":
        event_discount = max(0.4, 1.0 - event_risk / 180.0)
        return reversal * (quality / 100.0) * event_discount
    return 0.0


def _industry_neutral_quality_momentum_scores(items: Sequence[JsonDict]) -> list[float]:
    momentum_values = [_item_score(item, "momentum") for item in items]
    global_pct = _percentiles(momentum_values)
    industry_indices: dict[str, list[int]] = {}
    for idx, item in enumerate(items):
        industry_indices.setdefault(_industry(item), []).append(idx)

    industry_pct = [global_pct[idx] for idx in range(len(items))]
    for indices in industry_indices.values():
        if len(indices) < 3:
            continue
        local_values = [momentum_values[idx] for idx in indices]
        local_pct = _percentiles(local_values)
        for pos, idx in enumerate(indices):
            industry_pct[idx] = local_pct[pos]

    out: list[float] = []
    for idx, item in enumerate(items):
        quality = _item_score(item, "quality_low_vol")
        money_flow = _item_score(item, "money_flow")
        event_risk = _item_score(item, "event_risk")
        sector = _item_score(item, "sector_rotation")
        event_penalty = max(0.35, 1.0 - event_risk / 140.0)
        flow_confirm = 0.7 + 0.3 * money_flow / 100.0
        sector_cap = 0.85 + 0.15 * sector / 100.0
        out.append(industry_pct[idx] * (quality / 100.0) * flow_confirm * event_penalty * sector_cap)
    return out


def _flow_trend_event_guard_score(item: JsonDict) -> float:
    momentum = _item_score(item, "momentum")
    money_flow = _item_score(item, "money_flow")
    quality = _item_score(item, "quality_low_vol")
    event_risk = _item_score(item, "event_risk")
    if momentum < 45.0 or money_flow < 50.0 or quality < 45.0:
        return 0.0
    return momentum * (money_flow / 100.0) * (quality / 100.0) * max(0.0, 1.0 - event_risk / 110.0)


def _reversal_exhaustion_quality_guard_score(item: JsonDict) -> float:
    reversal = _item_score(item, "reversal")
    money_flow = _item_score(item, "money_flow")
    quality = _item_score(item, "quality_low_vol")
    event_risk = _item_score(item, "event_risk")
    if reversal < 35.0 or quality < 55.0 or event_risk > 70.0:
        return 0.0
    return reversal * (quality / 100.0) * (0.6 + 0.4 * money_flow / 100.0) * max(0.25, 1.0 - event_risk / 130.0)


def _hard_event_alpha_candidate_score(item: JsonDict) -> float:
    hard_event = _item_score(item, "hard_event_alpha")
    capacity = _hard_component(item, "capacity_liquidity")
    flow = _hard_component(item, "money_flow_persistence")
    seat = _hard_component(item, "dragon_tiger_seat_quality")
    limit_structure = _hard_component(item, "limit_break_structure")
    crowding = _hard_component(item, "industry_crowding")
    margin = _hard_component(item, "margin_pressure")
    if hard_event <= 0.0 or capacity < 35.0:
        return 0.0

    # Capacity is a tradability floor, not standalone alpha.  High flow, high
    # seat quality, and hard limit-break structure can become exhaustion when
    # they cluster after the event trade has already been crowded.
    flow_confirm = min(flow, 62.0) / 100.0
    seat_confirm = min(seat, 58.0) / 100.0
    structure_confirm = min(limit_structure, 58.0) / 100.0
    crowding_confirm = min(crowding, 55.0) / 100.0
    confirmation = (
        (0.70 + 0.30 * flow_confirm)
        * (0.82 + 0.18 * seat_confirm)
        * (0.86 + 0.14 * structure_confirm)
        * (0.92 + 0.08 * crowding_confirm)
    )
    return hard_event * confirmation * _hard_event_exhaustion_penalty(
        capacity=capacity,
        flow=flow,
        seat=seat,
        limit_structure=limit_structure,
        margin=margin,
    )


def _hard_event_exhaustion_penalty(
    *,
    capacity: float,
    flow: float,
    seat: float,
    limit_structure: float,
    margin: float,
) -> float:
    exhaustion_points = 0
    if capacity >= 70.0 and flow >= 65.0:
        exhaustion_points += 1
    if flow >= 75.0 and seat >= 70.0:
        exhaustion_points += 1
    if seat >= 70.0 and limit_structure >= 60.0:
        exhaustion_points += 1
    if capacity >= 70.0 and seat >= 70.0 and limit_structure >= 60.0:
        exhaustion_points += 1
    if flow >= 75.0 and limit_structure <= 50.0:
        exhaustion_points += 1
    if margin >= 55.0 and flow >= 65.0:
        exhaustion_points += 1
    return max(0.22, 1.0 - 0.18 * float(exhaustion_points))


def hard_event_risk_off_exhaustion_veto(item: JsonDict, *, market_regime_label: str) -> JsonDict:
    """Predeclared risk-off veto for exhausted hard-event structures.

    The veto is intentionally regime-specific.  It does not improve alpha
    score; it only removes structures that historically behaved like
    high-attention exhaustion during market retreat.
    """

    if str(market_regime_label or "") != "risk_off":
        return {"vetoed": False, "reason": "", "multiplier": 1.0}
    hard_event = _item_score(item, "hard_event_alpha")
    capacity = _hard_component(item, "capacity_liquidity")
    flow = _hard_component(item, "money_flow_persistence")
    seat = _hard_component(item, "dragon_tiger_seat_quality")
    limit_structure = _hard_component(item, "limit_break_structure")
    source = str(item.get("strategy") or "")
    reasons: list[str] = []
    if hard_event >= 35.0 and capacity >= 70.0 and flow >= 60.0 and limit_structure >= 52.0:
        reasons.append("risk_off_high_event_flow_capacity_limit_structure_exhaustion")
    if hard_event >= 35.0 and capacity >= 70.0 and seat >= 75.0 and limit_structure >= 52.0:
        reasons.append("risk_off_high_event_seat_capacity_limit_structure_exhaustion")
    if (
        source in {"v6", "v8", "v9"}
        and hard_event >= 30.0
        and capacity >= 55.0
        and flow >= 50.0
        and limit_structure >= 45.0
    ):
        reasons.append(f"risk_off_negative_source_bias_exhaustion:{source}")
    if source != "combo" and hard_event >= 30.0 and capacity >= 35.0 and flow >= 50.0:
        reasons.append("risk_off_hard_event_without_cross_strategy_consensus")
    return {
        "vetoed": bool(reasons),
        "reason": ",".join(reasons),
        "multiplier": 0.0 if reasons else 1.0,
        "components": {
            "hard_event_alpha": round(hard_event, 6),
            "capacity_liquidity": round(capacity, 6),
            "money_flow_persistence": round(flow, 6),
            "dragon_tiger_seat_quality": round(seat, 6),
            "limit_break_structure": round(limit_structure, 6),
            "source_strategy": source,
        },
    }


def hard_event_neutral_noise_turnover_veto(
    item: JsonDict,
    *,
    market_regime_label: str,
    construction_score: float = 0.0,
    cross_strategy_source_count: int = 1,
    rule_version: str = "",
) -> JsonDict:
    """Predeclared neutral-regime guard for weak hard-event confirmation.

    Neutral tape should not buy every event headline.  This guard removes
    low-confirmation single-source signals before weights are built, so any
    turnover reduction comes from the alpha recipe rather than allocator cash.
    """

    if str(market_regime_label or "") != "neutral":
        return {"vetoed": False, "reason": "", "multiplier": 1.0}
    hard_event = _item_score(item, "hard_event_alpha")
    flow = _hard_component(item, "money_flow_persistence")
    seat = _hard_component(item, "dragon_tiger_seat_quality")
    limit_structure = _hard_component(item, "limit_break_structure")
    source = str(item.get("strategy") or "")
    reasons: list[str] = []
    if hard_event < 40.0:
        reasons.append("neutral_hard_event_below_conviction_floor")
    if source in {"v6", "v8", "v9"} and hard_event < 46.0 and flow < 58.0:
        reasons.append(f"neutral_high_churn_source_without_flow_confirmation:{source}")
    if source != "combo" and flow < 52.0 and seat < 50.0 and limit_structure < 50.0:
        reasons.append("neutral_single_source_without_flow_seat_or_structure_confirmation")
    if (
        "neutral_consensus_turnover_guard.v5" in str(rule_version or "")
        and source != "combo"
        and int(cross_strategy_source_count or 0) < 2
        and float(construction_score or 0.0) < 45.0
    ):
        reasons.append("neutral_single_source_low_conviction_turnover_churn")
    if "neutral_over_veto_rebalance_guard.v6" in str(rule_version or ""):
        reasons = []
        if hard_event < 38.0:
            reasons.append("neutral_v6_hard_event_below_conviction_floor")
        if source in {"v6", "v8", "v9"} and hard_event < 44.0 and flow < 55.0:
            reasons.append(f"neutral_v6_high_churn_source_without_flow_confirmation:{source}")
        if source != "combo" and flow < 48.0 and seat < 48.0 and limit_structure < 48.0:
            reasons.append("neutral_v6_single_source_without_flow_seat_or_structure_confirmation")
        if (
            source != "combo"
            and int(cross_strategy_source_count or 0) < 2
            and float(construction_score or 0.0) < 42.0
            and flow < 55.0
        ):
            reasons.append("neutral_v6_single_source_low_conviction_turnover_churn")
    return {
        "vetoed": bool(reasons),
        "reason": ",".join(reasons),
        "multiplier": 0.0 if reasons else 1.0,
        "components": {
            "hard_event_alpha": round(hard_event, 6),
            "money_flow_persistence": round(flow, 6),
            "dragon_tiger_seat_quality": round(seat, 6),
            "limit_break_structure": round(limit_structure, 6),
            "source_strategy": source,
            "construction_score": round(float(construction_score or 0.0), 6),
            "cross_strategy_source_count": int(cross_strategy_source_count or 0),
        },
    }


def _multi_horizon_attribution(*, candidate: str, items: Sequence[JsonDict]) -> JsonDict:
    horizons: JsonDict = {}
    score_values = _candidate_scores(candidate, items)
    scores_by_item = {id(item): score_values[idx] for idx, item in enumerate(items)}
    for horizon in DECAY_HORIZONS:
        scores: list[float] = []
        returns: list[float] = []
        for item in items:
            forward = (item.get("forward_returns") or {}).get(str(horizon), {})
            if forward.get("available") is not True:
                continue
            scores.append(scores_by_item.get(id(item), 0.0))
            returns.append(float(forward.get("return_pct", 0.0) or 0.0))
        ic = _pearson(scores, returns) if len(scores) >= 3 else None
        rank_ic = _spearman(scores, returns) if len(scores) >= 3 else None
        horizons[str(horizon)] = {
            "horizon_days": horizon,
            "sample_count": len(scores),
            "ic": ic,
            "rank_ic": rank_ic,
            "available": ic is not None or rank_ic is not None,
        }
    available = [item for item in horizons.values() if item.get("available") is True]
    return {
        "horizons": horizons,
        "decay_available": len(available) >= 2,
        "positive_horizon_count": len(
            [item for item in available if item.get("ic") is not None and float(item.get("ic")) > 0.0]
        ),
        "positive_rank_horizon_count": len(
            [item for item in available if item.get("rank_ic") is not None and float(item.get("rank_ic")) > 0.0]
        ),
    }


def _recommended_use(
    *,
    active_count: int,
    sample_count: int,
    min_samples: int,
    research_window_count: int,
    min_research_windows: int,
    five_day_ic: Any,
    five_day_rank_ic: Any,
    multi: JsonDict,
) -> str:
    if active_count <= 0:
        return "research_blocked_missing_active_signals"
    if sample_count < int(min_samples):
        return "research_blocked_insufficient_samples"
    if research_window_count < int(min_research_windows):
        return "research_blocked_insufficient_research_windows"
    if five_day_ic is None or float(five_day_ic) <= 0.0:
        return "research_blocked_negative_ic"
    if five_day_rank_ic is None or float(five_day_rank_ic) <= 0.0:
        return "research_blocked_unstable_rank_ic"
    if int(multi.get("positive_horizon_count", 0) or 0) < 2:
        return "research_blocked_unstable_decay"
    if int(multi.get("positive_rank_horizon_count", 0) or 0) < 2:
        return "research_blocked_unstable_rank_ic"
    return "positive_alpha_candidate"


def _score(scores: JsonDict, sleeve: str) -> float:
    raw = scores.get(sleeve) if isinstance(scores.get(sleeve), dict) else {}
    return max(0.0, min(100.0, float(raw.get("score", 0.0) or 0.0)))


def _item_score(item: JsonDict, sleeve: str) -> float:
    scores = item.get("sleeve_scores") if isinstance(item.get("sleeve_scores"), dict) else {}
    return _score(scores, sleeve)


def _hard_component(item: JsonDict, name: str) -> float:
    features = item.get("tushare_pro_alpha_features") if isinstance(item.get("tushare_pro_alpha_features"), dict) else {}
    evidence = features.get("evidence") if isinstance(features.get("evidence"), dict) else {}
    hard = evidence.get("hard_alpha") if isinstance(evidence.get("hard_alpha"), dict) else {}
    component = hard.get(name) if isinstance(hard.get(name), dict) else {}
    return max(0.0, min(100.0, float(component.get("score", 0.0) or 0.0)))


def _industry(item: JsonDict) -> str:
    features = item.get("tushare_pro_alpha_features") if isinstance(item.get("tushare_pro_alpha_features"), dict) else {}
    evidence = features.get("evidence") if isinstance(features.get("evidence"), dict) else {}
    return str(evidence.get("industry") or "UNKNOWN")


def _percentiles(values: Sequence[float]) -> list[float]:
    if not values:
        return []
    ranks = _ranks(values)
    if len(values) == 1:
        return [50.0]
    return [100.0 * (rank - 1.0) / float(len(values) - 1) for rank in ranks]


def _pearson(left: Sequence[float], right: Sequence[float]) -> float | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    mean_l = sum(left) / len(left)
    mean_r = sum(right) / len(right)
    cov = sum((a - mean_l) * (b - mean_r) for a, b in zip(left, right))
    var_l = sum((a - mean_l) ** 2 for a in left)
    var_r = sum((b - mean_r) ** 2 for b in right)
    if var_l <= 0.0 or var_r <= 0.0:
        return None
    return round(cov / math.sqrt(var_l * var_r), 6)


def _spearman(left: Sequence[float], right: Sequence[float]) -> float | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    return _pearson(_ranks(left), _ranks(right))


def _ranks(values: Sequence[float]) -> list[float]:
    indexed = sorted(enumerate(float(value) for value in values), key=lambda item: item[1])
    ranks = [0.0] * len(indexed)
    idx = 0
    while idx < len(indexed):
        end = idx
        while end + 1 < len(indexed) and indexed[end + 1][1] == indexed[idx][1]:
            end += 1
        rank = (idx + end + 2) / 2.0
        for pos in range(idx, end + 1):
            ranks[indexed[pos][0]] = rank
        idx = end + 1
    return ranks
