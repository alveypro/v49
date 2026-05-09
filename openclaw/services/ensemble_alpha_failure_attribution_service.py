from __future__ import annotations

import sqlite3
from typing import Any, Dict, Sequence

from openclaw.services.ensemble_alpha_rebuild_lab_service import (
    build_ensemble_alpha_rebuild_multi_window_lab,
    _candidate_score,
)


JsonDict = Dict[str, Any]


def build_ensemble_alpha_failure_attribution(
    conn: sqlite3.Connection,
    fact_chains: Sequence[JsonDict] | None,
    *,
    candidate: str = "hard_event_alpha_candidate",
    horizon: int = 5,
) -> JsonDict:
    """Explain why a rebuilt alpha candidate failed multi-window validation.

    This is a research-only diagnostic.  It must not be used as a promotion
    artifact because it explains failure modes after the lab has already
    blocked the candidate.
    """

    chains = [chain for chain in (fact_chains or []) if isinstance(chain, dict)]
    lab = build_ensemble_alpha_rebuild_multi_window_lab(chains)
    candidate_review = (lab.get("candidate_reviews") or {}).get(candidate) or {}
    window_reviews = lab.get("window_reviews") if isinstance(lab.get("window_reviews"), dict) else {}
    window_diagnostics = [
        _window_diagnostic(conn, chain=chain, candidate=candidate, horizon=int(horizon))
        for chain in chains
    ]
    failed = [
        item
        for item in window_diagnostics
        if ((window_reviews.get(str(item.get("as_of_date") or "")) or {}).get("candidate_reviews") or {})
        .get(candidate, {})
        .get("recommended_use")
        != "positive_alpha_candidate"
    ]
    successful = [
        item
        for item in window_diagnostics
        if ((window_reviews.get(str(item.get("as_of_date") or "")) or {}).get("candidate_reviews") or {})
        .get(candidate, {})
        .get("recommended_use")
        == "positive_alpha_candidate"
    ]
    focus = min(failed, key=lambda item: float(item.get("candidate_ic") or 0.0), default={})
    return {
        "attribution_version": "ensemble_alpha_failure_attribution.v1",
        "research_only": True,
        "candidate": candidate,
        "horizon_days": int(horizon),
        "candidate_review": candidate_review,
        "failed_windows": [item.get("as_of_date") for item in failed],
        "successful_windows": [item.get("as_of_date") for item in successful],
        "focus_failed_window": focus.get("as_of_date", ""),
        "window_diagnostics": window_diagnostics,
        "cross_window_component_shift": _component_shift(successful, failed),
        "inferred_failure_drivers": _failure_drivers(successful, failed, focus),
        "blocking_reasons": (
            []
            if failed
            else ["no_failed_candidate_window_found_for_attribution"]
        ),
        "hard_boundaries": [
            "do_not_use_failure_attribution_as_promotion_evidence",
            "do_not_average_away_failed_regime_windows",
            "do_not_promote_candidate_until_failure_window_is_repaired_and_retested",
        ],
    }


def _window_diagnostic(
    conn: sqlite3.Connection,
    *,
    chain: JsonDict,
    candidate: str,
    horizon: int,
) -> JsonDict:
    as_of = str(chain.get("as_of_date") or "")
    items = chain.get("sample_facts") if isinstance(chain.get("sample_facts"), list) else []
    regime = _market_regime(conn, as_of)
    rows: list[JsonDict] = []
    for item in items:
        forward = (item.get("forward_returns") or {}).get(str(horizon), {})
        if forward.get("available") is not True:
            continue
        score = _candidate_score(candidate, item)
        rows.append(
            {
                "ts_code": str(item.get("ts_code") or ""),
                "strategy": str(item.get("strategy") or ""),
                "score": score,
                "return_pct": float(forward.get("return_pct", 0.0) or 0.0),
                "components": _hard_components(item),
            }
        )
    return {
        "as_of_date": as_of,
        "sample_count": len(rows),
        "candidate_ic": _pearson([row["score"] for row in rows], [row["return_pct"] for row in rows]),
        "candidate_rank_ic": _spearman([row["score"] for row in rows], [row["return_pct"] for row in rows]),
        "top_score_bucket_return": _bucket_return(rows, top=True),
        "bottom_score_bucket_return": _bucket_return(rows, top=False),
        "market_regime": regime,
        "component_averages": _component_averages(rows),
        "source_strategy_bias": _source_strategy_bias(rows),
        "risk_off_exhaustion_profile": _risk_off_exhaustion_profile(rows, regime),
        "top_negative_examples": _top_negative_examples(rows),
    }


def _hard_components(item: JsonDict) -> JsonDict:
    features = item.get("tushare_pro_alpha_features") if isinstance(item.get("tushare_pro_alpha_features"), dict) else {}
    evidence = features.get("evidence") if isinstance(features.get("evidence"), dict) else {}
    hard = evidence.get("hard_alpha") if isinstance(evidence.get("hard_alpha"), dict) else {}
    out: JsonDict = {}
    out["hard_event_alpha"] = float((((item.get("sleeve_scores") or {}).get("hard_event_alpha") or {}).get("score", 0.0)) or 0.0)
    for key in (
        "money_flow_persistence",
        "dragon_tiger_seat_quality",
        "limit_break_structure",
        "industry_crowding",
        "capacity_liquidity",
        "margin_pressure",
    ):
        value = hard.get(key) if isinstance(hard.get(key), dict) else {}
        out[key] = float(value.get("score", 0.0) or 0.0)
    return out


def _market_regime(conn: sqlite3.Connection, as_of: str) -> JsonDict:
    if not _table_exists(conn, "daily_trading_data"):
        return {"available": False, "blocking_reasons": ["missing_daily_trading_data"]}
    try:
        rows = conn.execute(
            """
            SELECT pct_chg, amount
            FROM daily_trading_data
            WHERE REPLACE(trade_date, '-', '') = ?
              AND pct_chg IS NOT NULL
            """,
            (str(as_of).replace("-", ""),),
        ).fetchall()
    except sqlite3.Error:
        return {"available": False, "blocking_reasons": ["daily_trading_data_query_failed"]}
    pct_values = [float(row[0] or 0.0) for row in rows]
    if not pct_values:
        return {"available": False, "blocking_reasons": ["missing_market_regime_rows"]}
    advance_ratio = sum(1 for value in pct_values if value > 0.0) / float(len(pct_values))
    avg_pct = sum(pct_values) / float(len(pct_values))
    label = "neutral"
    if avg_pct <= -1.0 or advance_ratio <= 0.35:
        label = "risk_off"
    elif avg_pct >= 1.0 or advance_ratio >= 0.65:
        label = "risk_on"
    return {
        "available": True,
        "label": label,
        "stock_count": len(pct_values),
        "avg_pct_chg": round(avg_pct, 6),
        "advance_ratio": round(advance_ratio, 6),
        "limit_up_like_count": sum(1 for value in pct_values if value >= 9.5),
        "limit_down_like_count": sum(1 for value in pct_values if value <= -9.5),
        "total_amount": round(sum(float(row[1] or 0.0) for row in rows), 4),
    }


def _component_averages(rows: Sequence[JsonDict]) -> JsonDict:
    keys = (
        "money_flow_persistence",
        "dragon_tiger_seat_quality",
        "limit_break_structure",
        "industry_crowding",
        "capacity_liquidity",
        "margin_pressure",
    )
    return {
        key: _avg([float((row.get("components") or {}).get(key, 0.0) or 0.0) for row in rows])
        for key in keys
    }


def _source_strategy_bias(rows: Sequence[JsonDict]) -> list[JsonDict]:
    by_strategy: dict[str, list[JsonDict]] = {}
    for row in rows:
        by_strategy.setdefault(str(row.get("strategy") or ""), []).append(row)
    out = []
    for strategy, group in sorted(by_strategy.items()):
        out.append(
            {
                "strategy": strategy,
                "sample_count": len(group),
                "avg_candidate_score": _avg([float(row.get("score", 0.0) or 0.0) for row in group]),
                "avg_forward_return_pct": _avg([float(row.get("return_pct", 0.0) or 0.0) for row in group]),
            }
        )
    return sorted(out, key=lambda item: float(item.get("avg_forward_return_pct", 0.0) or 0.0))


def _top_negative_examples(rows: Sequence[JsonDict]) -> list[JsonDict]:
    negatives = [row for row in rows if float(row.get("return_pct", 0.0) or 0.0) < 0.0 and float(row.get("score", 0.0) or 0.0) > 0.0]
    negatives = sorted(negatives, key=lambda item: float(item.get("score", 0.0) or 0.0), reverse=True)
    return [
        {
            "ts_code": row.get("ts_code"),
            "strategy": row.get("strategy"),
            "candidate_score": round(float(row.get("score", 0.0) or 0.0), 6),
            "forward_return_pct": round(float(row.get("return_pct", 0.0) or 0.0), 6),
            "components": row.get("components") or {},
        }
        for row in negatives[:10]
    ]


def _risk_off_exhaustion_profile(rows: Sequence[JsonDict], regime: JsonDict) -> JsonDict:
    label = str((regime or {}).get("label") or "")
    exhausted = [row for row in rows if _is_risk_off_exhaustion_structure(row, label)]
    non_exhausted = [row for row in rows if row not in exhausted]
    exhausted_returns = [float(row.get("return_pct", 0.0) or 0.0) for row in exhausted]
    non_exhausted_returns = [float(row.get("return_pct", 0.0) or 0.0) for row in non_exhausted]
    negative_exhausted = [row for row in exhausted if float(row.get("return_pct", 0.0) or 0.0) < 0.0]
    return {
        "active": label == "risk_off",
        "market_regime_label": label or "unknown",
        "exhausted_signal_count": len(exhausted),
        "exhausted_negative_count": len(negative_exhausted),
        "exhausted_negative_ratio": round(len(negative_exhausted) / float(len(exhausted)), 6) if exhausted else None,
        "exhausted_avg_forward_return_pct": _avg(exhausted_returns) if exhausted_returns else None,
        "non_exhausted_avg_forward_return_pct": _avg(non_exhausted_returns) if non_exhausted_returns else None,
        "predeclared_structure": (
            "risk_off and hard_event_alpha>=35 with high capacity plus flow/seat/limit structure, "
            "or v6/v8/v9 source bias with elevated event-flow-capacity structure"
        ),
        "exhausted_top_negative_examples": [
            {
                "ts_code": row.get("ts_code"),
                "strategy": row.get("strategy"),
                "candidate_score": round(float(row.get("score", 0.0) or 0.0), 6),
                "forward_return_pct": round(float(row.get("return_pct", 0.0) or 0.0), 6),
                "components": row.get("components") or {},
            }
            for row in sorted(negative_exhausted, key=lambda item: float(item.get("score", 0.0) or 0.0), reverse=True)[:10]
        ],
    }


def _is_risk_off_exhaustion_structure(row: JsonDict, label: str) -> bool:
    if label != "risk_off":
        return False
    components = row.get("components") if isinstance(row.get("components"), dict) else {}
    hard_event = float(components.get("hard_event_alpha", 0.0) or 0.0)
    capacity = float(components.get("capacity_liquidity", 0.0) or 0.0)
    flow = float(components.get("money_flow_persistence", 0.0) or 0.0)
    seat = float(components.get("dragon_tiger_seat_quality", 0.0) or 0.0)
    limit_structure = float(components.get("limit_break_structure", 0.0) or 0.0)
    source = str(row.get("strategy") or "")
    if hard_event >= 35.0 and capacity >= 70.0 and flow >= 60.0 and limit_structure >= 52.0:
        return True
    if hard_event >= 35.0 and capacity >= 70.0 and seat >= 75.0 and limit_structure >= 52.0:
        return True
    return (
        source in {"v6", "v8", "v9"}
        and hard_event >= 30.0
        and capacity >= 55.0
        and flow >= 50.0
        and limit_structure >= 45.0
    )


def _bucket_return(rows: Sequence[JsonDict], *, top: bool) -> float | None:
    scored = sorted(rows, key=lambda item: float(item.get("score", 0.0) or 0.0), reverse=top)
    if not scored:
        return None
    size = max(1, len(scored) // 5)
    return _avg([float(row.get("return_pct", 0.0) or 0.0) for row in scored[:size]])


def _component_shift(successful: Sequence[JsonDict], failed: Sequence[JsonDict]) -> JsonDict:
    success_avg = _average_components(successful)
    failed_avg = _average_components(failed)
    return {
        "successful_window_avg": success_avg,
        "failed_window_avg": failed_avg,
        "failed_minus_successful": {
            key: round(float(failed_avg.get(key, 0.0) or 0.0) - float(success_avg.get(key, 0.0) or 0.0), 6)
            for key in sorted(set(success_avg) | set(failed_avg))
        },
    }


def _average_components(windows: Sequence[JsonDict]) -> JsonDict:
    keys: set[str] = set()
    for window in windows:
        keys.update((window.get("component_averages") or {}).keys())
    return {
        key: _avg([float((window.get("component_averages") or {}).get(key, 0.0) or 0.0) for window in windows])
        for key in sorted(keys)
    }


def _failure_drivers(successful: Sequence[JsonDict], failed: Sequence[JsonDict], focus: JsonDict) -> list[str]:
    drivers: list[str] = []
    focus_regime = focus.get("market_regime") if isinstance(focus.get("market_regime"), dict) else {}
    if focus_regime.get("label") == "risk_off":
        drivers.append("market_regime_risk_off")
    if float((focus.get("top_score_bucket_return") or 0.0)) < float((focus.get("bottom_score_bucket_return") or 0.0)):
        drivers.append("top_score_bucket_underperformed_bottom_bucket")
    shift = _component_shift(successful, failed).get("failed_minus_successful") or {}
    if float(shift.get("industry_crowding", 0.0) or 0.0) > 5.0:
        drivers.append("industry_crowding_higher_in_failed_window")
    if float(shift.get("limit_break_structure", 0.0) or 0.0) < -5.0:
        drivers.append("limit_break_structure_weaker_in_failed_window")
    if float(shift.get("capacity_liquidity", 0.0) or 0.0) < -5.0:
        drivers.append("capacity_liquidity_weaker_in_failed_window")
    exhaustion = focus.get("risk_off_exhaustion_profile") if isinstance(focus.get("risk_off_exhaustion_profile"), dict) else {}
    exhausted_avg = exhaustion.get("exhausted_avg_forward_return_pct")
    non_exhausted_avg = exhaustion.get("non_exhausted_avg_forward_return_pct")
    if exhaustion.get("active") is True and int(exhaustion.get("exhausted_negative_count") or 0) > 0:
        drivers.append("risk_off_high_event_flow_seat_capacity_exhaustion_negative")
    if exhausted_avg is not None and non_exhausted_avg is not None and float(exhausted_avg) < float(non_exhausted_avg):
        drivers.append("risk_off_exhaustion_underperformed_non_exhausted")
    worst_sources = [
        item.get("strategy")
        for item in (focus.get("source_strategy_bias") or [])[:3]
        if float(item.get("avg_forward_return_pct", 0.0) or 0.0) < 0.0
    ]
    if worst_sources:
        drivers.append("formal_pool_source_bias_negative:" + ",".join(str(item) for item in worst_sources if item))
    return drivers or ["failure_driver_not_isolated"]


def _avg(values: Sequence[float]) -> float:
    clean = [float(value) for value in values if value is not None]
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


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (str(table),),
        ).fetchone()
    except sqlite3.Error:
        return False
    return bool(row)
