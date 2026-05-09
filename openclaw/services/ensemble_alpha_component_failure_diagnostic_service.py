from __future__ import annotations

from typing import Any, Dict, Sequence

from openclaw.services.ensemble_alpha_gate_contrast_service import _pearson, _spearman
from openclaw.services.ensemble_alpha_rebuild_lab_service import _candidate_score


JsonDict = Dict[str, Any]

HARD_COMPONENTS = (
    "money_flow_persistence",
    "dragon_tiger_seat_quality",
    "limit_break_structure",
    "industry_crowding",
    "capacity_liquidity",
    "margin_pressure",
)


def build_ensemble_alpha_component_failure_diagnostic(
    fact_chain: JsonDict | None,
    *,
    candidate: str = "hard_event_alpha_candidate",
    horizon: int = 5,
) -> JsonDict:
    """Component-level diagnostic for a failed rebuilt alpha window.

    This is a research-only failure analysis.  It is designed to explain why a
    candidate failed a fresh window, not to approve a new sleeve or gate.
    """

    chain = fact_chain if isinstance(fact_chain, dict) else {}
    rows = _rows(chain, candidate=candidate, horizon=int(horizon))
    scored = sorted(rows, key=lambda row: float(row.get("candidate_score", 0.0) or 0.0), reverse=True)
    bucket_size = max(1, len(scored) // 5) if scored else 0
    top_bucket = scored[:bucket_size] if bucket_size else []
    bottom_bucket = scored[-bucket_size:] if bucket_size else []
    component_reviews = {
        component: _component_review(component, rows=rows, top_bucket=top_bucket, bottom_bucket=bottom_bucket)
        for component in HARD_COMPONENTS
    }
    return {
        "diagnostic_version": "ensemble_alpha_component_failure_diagnostic.v1",
        "research_only": True,
        "as_of_date": str(chain.get("as_of_date") or ""),
        "candidate": candidate,
        "horizon_days": int(horizon),
        "sample_count": len(rows),
        "candidate_ic": _pearson([float(row.get("candidate_score", 0.0) or 0.0) for row in rows], [float(row.get("return_pct", 0.0) or 0.0) for row in rows]),
        "candidate_rank_ic": _spearman([float(row.get("candidate_score", 0.0) or 0.0) for row in rows], [float(row.get("return_pct", 0.0) or 0.0) for row in rows]),
        "top_score_bucket": _bucket_review(top_bucket),
        "bottom_score_bucket": _bucket_review(bottom_bucket),
        "component_reviews": component_reviews,
        "source_strategy_reviews": _source_strategy_reviews(rows),
        "failure_hypotheses": _failure_hypotheses(component_reviews, top_bucket=top_bucket, bottom_bucket=bottom_bucket),
        "hard_boundaries": [
            "do_not_use_component_failure_diagnostic_as_promotion_evidence",
            "do_not_create_post_hoc_component_gate_without_fresh_walk_forward",
            "do_not_treat_high_capacity_or_high_seat_quality_as_alpha_without_return_confirmation",
        ],
    }


def _rows(chain: JsonDict, *, candidate: str, horizon: int) -> list[JsonDict]:
    items = chain.get("sample_facts") if isinstance(chain.get("sample_facts"), list) else []
    out: list[JsonDict] = []
    for item in items:
        forward = (item.get("forward_returns") or {}).get(str(horizon), {})
        if forward.get("available") is not True:
            continue
        out.append(
            {
                "ts_code": str(item.get("ts_code") or ""),
                "strategy": str(item.get("strategy") or ""),
                "candidate_score": _candidate_score(candidate, item),
                "return_pct": float(forward.get("return_pct", 0.0) or 0.0),
                "components": _hard_components(item),
            }
        )
    return out


def _hard_components(item: JsonDict) -> JsonDict:
    features = item.get("tushare_pro_alpha_features") if isinstance(item.get("tushare_pro_alpha_features"), dict) else {}
    evidence = features.get("evidence") if isinstance(features.get("evidence"), dict) else {}
    hard = evidence.get("hard_alpha") if isinstance(evidence.get("hard_alpha"), dict) else {}
    out: JsonDict = {}
    for key in HARD_COMPONENTS:
        value = hard.get(key) if isinstance(hard.get(key), dict) else {}
        out[key] = float(value.get("score", 0.0) or 0.0)
    return out


def _component_review(component: str, *, rows: Sequence[JsonDict], top_bucket: Sequence[JsonDict], bottom_bucket: Sequence[JsonDict]) -> JsonDict:
    values = [float((row.get("components") or {}).get(component, 0.0) or 0.0) for row in rows]
    returns = [float(row.get("return_pct", 0.0) or 0.0) for row in rows]
    return {
        "component": component,
        "ic": _pearson(values, returns),
        "rank_ic": _spearman(values, returns),
        "avg": _avg(values),
        "top_score_bucket_avg": _avg([float((row.get("components") or {}).get(component, 0.0) or 0.0) for row in top_bucket]),
        "bottom_score_bucket_avg": _avg([float((row.get("components") or {}).get(component, 0.0) or 0.0) for row in bottom_bucket]),
    }


def _bucket_review(rows: Sequence[JsonDict]) -> JsonDict:
    return {
        "sample_count": len(rows),
        "avg_candidate_score": _avg([float(row.get("candidate_score", 0.0) or 0.0) for row in rows]),
        "avg_return_pct": _avg([float(row.get("return_pct", 0.0) or 0.0) for row in rows]),
        "negative_return_count": sum(1 for row in rows if float(row.get("return_pct", 0.0) or 0.0) < 0.0),
        "examples": [
            {
                "ts_code": row.get("ts_code"),
                "strategy": row.get("strategy"),
                "candidate_score": round(float(row.get("candidate_score", 0.0) or 0.0), 6),
                "return_pct": round(float(row.get("return_pct", 0.0) or 0.0), 6),
                "components": row.get("components") or {},
            }
            for row in rows[:8]
        ],
    }


def _source_strategy_reviews(rows: Sequence[JsonDict]) -> list[JsonDict]:
    by_strategy: dict[str, list[JsonDict]] = {}
    for row in rows:
        by_strategy.setdefault(str(row.get("strategy") or ""), []).append(row)
    return sorted(
        [
            {
                "strategy": strategy,
                "sample_count": len(group),
                "avg_candidate_score": _avg([float(row.get("candidate_score", 0.0) or 0.0) for row in group]),
                "avg_return_pct": _avg([float(row.get("return_pct", 0.0) or 0.0) for row in group]),
            }
            for strategy, group in by_strategy.items()
        ],
        key=lambda item: float(item.get("avg_return_pct", 0.0) or 0.0),
    )


def _failure_hypotheses(
    component_reviews: dict[str, JsonDict],
    *,
    top_bucket: Sequence[JsonDict],
    bottom_bucket: Sequence[JsonDict],
) -> list[str]:
    out: list[str] = []
    top_return = _avg([float(row.get("return_pct", 0.0) or 0.0) for row in top_bucket])
    bottom_return = _avg([float(row.get("return_pct", 0.0) or 0.0) for row in bottom_bucket])
    if top_return is not None and bottom_return is not None and top_return < bottom_return:
        out.append("candidate_top_bucket_underperformed_bottom_bucket")
    for component in ("capacity_liquidity", "dragon_tiger_seat_quality", "money_flow_persistence", "limit_break_structure"):
        review = component_reviews.get(component) or {}
        if review.get("ic") is not None and float(review.get("ic")) < 0.0:
            out.append(f"negative_component_ic:{component}")
        if review.get("rank_ic") is not None and float(review.get("rank_ic")) < 0.0:
            out.append(f"negative_component_rank_ic:{component}")
    cap = component_reviews.get("capacity_liquidity") or {}
    if float(cap.get("top_score_bucket_avg", 0.0) or 0.0) > float(cap.get("bottom_score_bucket_avg", 0.0) or 0.0) + 10.0 and top_return is not None and top_return < 0.0:
        out.append("high_capacity_top_bucket_still_lost_money")
    seat = component_reviews.get("dragon_tiger_seat_quality") or {}
    if float(seat.get("top_score_bucket_avg", 0.0) or 0.0) > float(seat.get("bottom_score_bucket_avg", 0.0) or 0.0) + 10.0 and top_return is not None and top_return < 0.0:
        out.append("high_seat_quality_top_bucket_still_lost_money")
    return out


def _avg(values: Sequence[float]) -> float | None:
    clean = [float(value) for value in values]
    if not clean:
        return None
    return round(sum(clean) / float(len(clean)), 6)
