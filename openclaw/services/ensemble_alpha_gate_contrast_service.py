from __future__ import annotations

import sqlite3
from typing import Any, Callable, Dict, Sequence

from openclaw.services.ensemble_alpha_failure_attribution_service import _market_regime
from openclaw.services.ensemble_alpha_rebuild_lab_service import _candidate_score


JsonDict = Dict[str, Any]


def build_ensemble_alpha_gate_contrast(
    conn: sqlite3.Connection,
    fact_chains: Sequence[JsonDict] | None,
    *,
    candidate: str = "hard_event_alpha_candidate",
    horizon: int = 5,
    blocked_sources: Sequence[str] = ("v6", "v8", "v9"),
) -> JsonDict:
    """Research-only contrast for risk-off and source contribution filters."""

    chains = [chain for chain in (fact_chains or []) if isinstance(chain, dict)]
    regimes = {str(chain.get("as_of_date") or ""): _market_regime(conn, str(chain.get("as_of_date") or "")) for chain in chains}
    source_set = {str(item) for item in blocked_sources if str(item or "")}
    scenarios = {
        "raw": lambda row: True,
        "risk_off_gate": lambda row: row.get("market_regime") != "risk_off",
        "source_filter": lambda row: str(row.get("strategy") or "") not in source_set,
        "risk_off_gate_plus_source_filter": lambda row: row.get("market_regime") != "risk_off"
        and str(row.get("strategy") or "") not in source_set,
    }
    rows = _scored_rows(chains, regimes=regimes, candidate=candidate, horizon=int(horizon))
    scenario_reviews = {
        name: _scenario_review(rows, keep=keep)
        for name, keep in scenarios.items()
    }
    best_name = _best_scenario_name(scenario_reviews)
    best = scenario_reviews.get(best_name, {})
    return {
        "contrast_version": "ensemble_alpha_gate_contrast.v1",
        "research_only": True,
        "candidate": candidate,
        "horizon_days": int(horizon),
        "blocked_sources": sorted(source_set),
        "window_regimes": regimes,
        "scenario_reviews": scenario_reviews,
        "best_research_scenario": best_name,
        "passed_research_gate": _scenario_passed(best),
        "blocking_reasons": [] if _scenario_passed(best) else ["no_gate_contrast_scenario_passed_research_policy"],
        "hard_boundaries": [
            "do_not_promote_gate_contrast_to_sleeve_policy_without_fresh_walk_forward",
            "do_not_hide_sample_loss_from_risk_off_or_source_filters",
            "do_not_use_source_filter_to_rewrite_formal_pool_strategy_status",
        ],
    }


def _scored_rows(
    chains: Sequence[JsonDict],
    *,
    regimes: dict[str, JsonDict],
    candidate: str,
    horizon: int,
) -> list[JsonDict]:
    rows: list[JsonDict] = []
    for chain in chains:
        as_of = str(chain.get("as_of_date") or "")
        regime = regimes.get(as_of) or {}
        items = chain.get("sample_facts") if isinstance(chain.get("sample_facts"), list) else []
        for item in items:
            forward = (item.get("forward_returns") or {}).get(str(horizon), {})
            if forward.get("available") is not True:
                continue
            rows.append(
                {
                    "as_of_date": as_of,
                    "market_regime": str(regime.get("label") or "unknown"),
                    "strategy": str(item.get("strategy") or ""),
                    "score": _candidate_score(candidate, item),
                    "return_pct": float(forward.get("return_pct", 0.0) or 0.0),
                }
            )
    return rows


def _scenario_review(rows: Sequence[JsonDict], *, keep: Callable[[JsonDict], bool]) -> JsonDict:
    kept = [row for row in rows if keep(row)]
    dropped = [row for row in rows if not keep(row)]
    windows = sorted({str(row.get("as_of_date") or "") for row in kept})
    window_reviews = [_window_review(window, [row for row in kept if row.get("as_of_date") == window]) for window in windows]
    ic = _pearson([float(row.get("score", 0.0) or 0.0) for row in kept], [float(row.get("return_pct", 0.0) or 0.0) for row in kept])
    rank_ic = _spearman([float(row.get("score", 0.0) or 0.0) for row in kept], [float(row.get("return_pct", 0.0) or 0.0) for row in kept])
    positive_windows = [
        row
        for row in window_reviews
        if row.get("ic") is not None and float(row.get("ic")) > 0.0 and row.get("rank_ic") is not None and float(row.get("rank_ic")) > 0.0
    ]
    sample_retention = len(kept) / float(len(rows)) if rows else 0.0
    return {
        "sample_count": len(kept),
        "dropped_sample_count": len(dropped),
        "sample_retention": round(sample_retention, 6),
        "window_count": len(windows),
        "positive_window_count": len(positive_windows),
        "ic": ic,
        "rank_ic": rank_ic,
        "window_reviews": window_reviews,
        "passed_research_gate": _scenario_passed(
            {
                "sample_count": len(kept),
                "window_count": len(windows),
                "positive_window_count": len(positive_windows),
                "sample_retention": sample_retention,
                "ic": ic,
                "rank_ic": rank_ic,
            }
        ),
    }


def _window_review(as_of: str, rows: Sequence[JsonDict]) -> JsonDict:
    return {
        "as_of_date": as_of,
        "sample_count": len(rows),
        "ic": _pearson([float(row.get("score", 0.0) or 0.0) for row in rows], [float(row.get("return_pct", 0.0) or 0.0) for row in rows]),
        "rank_ic": _spearman([float(row.get("score", 0.0) or 0.0) for row in rows], [float(row.get("return_pct", 0.0) or 0.0) for row in rows]),
        "avg_return_pct": _avg([float(row.get("return_pct", 0.0) or 0.0) for row in rows]),
    }


def _scenario_passed(review: JsonDict) -> bool:
    window_count = int(review.get("window_count", 0) or 0)
    positive_window_count = int(review.get("positive_window_count", 0) or 0)
    return (
        int(review.get("sample_count", 0) or 0) >= 120
        and window_count >= 4
        and positive_window_count == window_count
        and float(review.get("sample_retention", 0.0) or 0.0) >= 0.5
        and review.get("ic") is not None
        and float(review.get("ic")) > 0.0
        and review.get("rank_ic") is not None
        and float(review.get("rank_ic")) > 0.0
    )


def _best_scenario_name(reviews: dict[str, JsonDict]) -> str:
    ordered = sorted(
        reviews.items(),
        key=lambda item: (
            bool(item[1].get("passed_research_gate")),
            float(item[1].get("rank_ic") or -999.0),
            float(item[1].get("ic") or -999.0),
            float(item[1].get("sample_retention") or 0.0),
        ),
        reverse=True,
    )
    return ordered[0][0] if ordered else ""


def _avg(values: Sequence[float]) -> float:
    clean = [float(value) for value in values]
    return round(sum(clean) / float(len(clean)), 6) if clean else 0.0


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
    return round(cov / ((var_l * var_r) ** 0.5), 6)


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
