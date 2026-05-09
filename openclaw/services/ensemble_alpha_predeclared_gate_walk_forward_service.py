from __future__ import annotations

import sqlite3
from typing import Any, Dict, Sequence

from openclaw.services.ensemble_alpha_failure_attribution_service import _market_regime
from openclaw.services.ensemble_alpha_gate_contrast_service import _pearson, _spearman
from openclaw.services.ensemble_alpha_rebuild_lab_service import _candidate_score


JsonDict = Dict[str, Any]


def build_ensemble_alpha_predeclared_gate_walk_forward(
    conn: sqlite3.Connection,
    fact_chains: Sequence[JsonDict] | None,
    *,
    candidate: str = "hard_event_alpha_candidate",
    horizon: int = 5,
    gate_name: str = "risk_off_gate",
    min_sample_count: int = 120,
    min_retained_windows: int = 4,
    min_sample_retention: float = 0.5,
    require_excluded_risk_off_window: bool = True,
    calibration_as_of_dates: Sequence[str] | None = None,
) -> JsonDict:
    """Validate a predeclared alpha gate on fresh walk-forward windows.

    This service is deliberately narrower than gate contrast.  It does not
    compare scenarios or choose winners after seeing returns.  The only
    supported gate is the predeclared risk-off exclusion that came from failure
    attribution, and the output remains research-only even when the validation
    metrics pass.
    """

    chains = [chain for chain in (fact_chains or []) if isinstance(chain, dict)]
    calibration_dates = {str(item).replace("-", "") for item in (calibration_as_of_dates or []) if str(item or "")}
    validation_dates = {str(chain.get("as_of_date") or "").replace("-", "") for chain in chains if str(chain.get("as_of_date") or "")}
    rows = _scored_rows(conn, chains=chains, candidate=candidate, horizon=int(horizon))
    retained = [row for row in rows if _keep_by_gate(row, gate_name=gate_name)]
    excluded = [row for row in rows if not _keep_by_gate(row, gate_name=gate_name)]
    retained_windows = sorted({str(row.get("as_of_date") or "") for row in retained})
    excluded_windows = sorted({str(row.get("as_of_date") or "") for row in excluded})
    retained_reviews = [_window_review(window, [row for row in retained if row.get("as_of_date") == window]) for window in retained_windows]
    excluded_reviews = [_excluded_window_review(window, [row for row in excluded if row.get("as_of_date") == window]) for window in excluded_windows]
    positive_windows = [
        review
        for review in retained_reviews
        if review.get("ic") is not None
        and float(review.get("ic")) > 0.0
        and review.get("rank_ic") is not None
        and float(review.get("rank_ic")) > 0.0
    ]
    sample_retention = len(retained) / float(len(rows)) if rows else 0.0
    validation = {
        "sample_count": len(retained),
        "raw_sample_count": len(rows),
        "excluded_sample_count": len(excluded),
        "sample_retention": round(sample_retention, 6),
        "retained_window_count": len(retained_windows),
        "excluded_window_count": len(excluded_windows),
        "positive_retained_window_count": len(positive_windows),
        "ic": _pearson([float(row.get("score", 0.0) or 0.0) for row in retained], [float(row.get("return_pct", 0.0) or 0.0) for row in retained]),
        "rank_ic": _spearman([float(row.get("score", 0.0) or 0.0) for row in retained], [float(row.get("return_pct", 0.0) or 0.0) for row in retained]),
        "retained_window_reviews": retained_reviews,
        "excluded_window_reviews": excluded_reviews,
    }
    blocking = _blocking_reasons(
        validation,
        min_sample_count=int(min_sample_count),
        min_retained_windows=int(min_retained_windows),
        min_sample_retention=float(min_sample_retention),
        require_excluded_risk_off_window=bool(require_excluded_risk_off_window),
        overlapping_calibration_dates=sorted(validation_dates & calibration_dates),
    )
    return {
        "walk_forward_version": "ensemble_alpha_predeclared_gate_walk_forward.v1",
        "research_only": True,
        "not_for_sleeve_policy": True,
        "candidate": candidate,
        "horizon_days": int(horizon),
        "predeclared_gate": {
            "gate_name": gate_name,
            "declared_before_validation": True,
            "rule": "exclude market_regime == risk_off",
            "regime_definition": "risk_off when avg_pct_chg <= -1.0 or advance_ratio <= 0.35",
            "source_strategy_filter_allowed": False,
            "scenario_selection_allowed": False,
            "calibration_as_of_dates": sorted(calibration_dates),
            "validation_as_of_dates": sorted(validation_dates),
        },
        "window_regimes": {str(row.get("as_of_date") or ""): row.get("market_regime") for row in _one_row_per_window(rows)},
        "validation_review": validation,
        "passed_predeclared_walk_forward_gate": not blocking,
        "blocking_reasons": blocking,
        "promotion_status": "research_only_blocked_from_observation",
        "hard_boundaries": [
            "do_not_use_predeclared_gate_walk_forward_as_sleeve_policy_approval",
            "do_not_add_source_strategy_filter_without_new_predeclared_walk_forward",
            "do_not_promote_ensemble_core_from_gate_validation_without_shadow_portfolio_and_after_cost_benchmark",
            "do_not_relabel_risk_off_exclusion_as_positive_alpha",
        ],
    }


def _scored_rows(
    conn: sqlite3.Connection,
    *,
    chains: Sequence[JsonDict],
    candidate: str,
    horizon: int,
) -> list[JsonDict]:
    rows: list[JsonDict] = []
    for chain in chains:
        as_of = str(chain.get("as_of_date") or "")
        regime = _market_regime(conn, as_of)
        items = chain.get("sample_facts") if isinstance(chain.get("sample_facts"), list) else []
        for item in items:
            forward = (item.get("forward_returns") or {}).get(str(horizon), {})
            if forward.get("available") is not True:
                continue
            rows.append(
                {
                    "as_of_date": as_of,
                    "market_regime": regime,
                    "market_regime_label": str(regime.get("label") or "unknown"),
                    "strategy": str(item.get("strategy") or ""),
                    "ts_code": str(item.get("ts_code") or ""),
                    "score": _candidate_score(candidate, item),
                    "return_pct": float(forward.get("return_pct", 0.0) or 0.0),
                }
            )
    return rows


def _keep_by_gate(row: JsonDict, *, gate_name: str) -> bool:
    if gate_name != "risk_off_gate":
        return False
    return str(row.get("market_regime_label") or "") != "risk_off"


def _window_review(as_of: str, rows: Sequence[JsonDict]) -> JsonDict:
    scores = [float(row.get("score", 0.0) or 0.0) for row in rows]
    returns = [float(row.get("return_pct", 0.0) or 0.0) for row in rows]
    return {
        "as_of_date": as_of,
        "sample_count": len(rows),
        "market_regime_label": str((rows[0].get("market_regime") or {}).get("label") or "unknown") if rows else "unknown",
        "ic": _pearson(scores, returns),
        "rank_ic": _spearman(scores, returns),
        "avg_return_pct": _avg(returns),
    }


def _excluded_window_review(as_of: str, rows: Sequence[JsonDict]) -> JsonDict:
    return {
        "as_of_date": as_of,
        "sample_count": len(rows),
        "market_regime_label": str((rows[0].get("market_regime") or {}).get("label") or "unknown") if rows else "unknown",
        "avg_return_pct": _avg([float(row.get("return_pct", 0.0) or 0.0) for row in rows]),
    }


def _blocking_reasons(
    review: JsonDict,
    *,
    min_sample_count: int,
    min_retained_windows: int,
    min_sample_retention: float,
    require_excluded_risk_off_window: bool,
    overlapping_calibration_dates: Sequence[str],
) -> list[str]:
    reasons: list[str] = []
    if overlapping_calibration_dates:
        reasons.append(f"validation_window_overlaps_calibration:{','.join(overlapping_calibration_dates)}")
    if int(review.get("raw_sample_count", 0) or 0) <= 0:
        reasons.append("missing_forward_return_samples")
    if int(review.get("sample_count", 0) or 0) < int(min_sample_count):
        reasons.append(f"insufficient_retained_samples:{int(review.get('sample_count', 0) or 0)}/{int(min_sample_count)}")
    retained_windows = int(review.get("retained_window_count", 0) or 0)
    if retained_windows < int(min_retained_windows):
        reasons.append(f"insufficient_retained_windows:{retained_windows}/{int(min_retained_windows)}")
    if int(review.get("positive_retained_window_count", 0) or 0) != retained_windows:
        reasons.append("not_all_retained_windows_positive_ic_and_rank_ic")
    if float(review.get("sample_retention", 0.0) or 0.0) < float(min_sample_retention):
        reasons.append(
            f"insufficient_sample_retention:{float(review.get('sample_retention', 0.0) or 0.0):.6f}/{float(min_sample_retention):.6f}"
        )
    if review.get("ic") is None or float(review.get("ic") or 0.0) <= 0.0:
        reasons.append("retained_population_ic_not_positive")
    if review.get("rank_ic") is None or float(review.get("rank_ic") or 0.0) <= 0.0:
        reasons.append("retained_population_rank_ic_not_positive")
    if require_excluded_risk_off_window and int(review.get("excluded_window_count", 0) or 0) <= 0:
        reasons.append("missing_excluded_risk_off_window_in_validation_set")
    return reasons


def _one_row_per_window(rows: Sequence[JsonDict]) -> list[JsonDict]:
    seen: set[str] = set()
    out: list[JsonDict] = []
    for row in rows:
        as_of = str(row.get("as_of_date") or "")
        if as_of in seen:
            continue
        seen.add(as_of)
        out.append(row)
    return out


def _avg(values: Sequence[float]) -> float | None:
    clean = [float(value) for value in values]
    if not clean:
        return None
    return round(sum(clean) / float(len(clean)), 6)
