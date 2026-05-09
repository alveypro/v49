from __future__ import annotations

import math
import time
from typing import Any, Callable, Dict, Optional

import pandas as pd

from openclaw.runtime.dataframe_utils import ensure_price_aliases


def prewarm_v8_base_context(v8_evaluator: Any, diagnostics: Optional[Dict[str, Any]] = None, *, top_n: int = 8) -> Dict[str, Any]:
    started = time.perf_counter()
    out: Dict[str, Any] = {"status": "unavailable", "elapsed_ms": 0.0}
    try:
        base = getattr(v8_evaluator, "v7_evaluator", None)
        if base is None:
            out["reason"] = "missing_v7_evaluator"
            return out
        if getattr(base, "current_regime", None) is not None:
            out["status"] = "already_warm"
            out["market_regime"] = getattr(base, "current_regime", None)
            out["hot_industries_count"] = len(getattr(base, "hot_industries", []) or [])
            return out
        market_analyzer = getattr(base, "market_analyzer", None)
        industry_analyzer = getattr(base, "industry_analyzer", None)
        if market_analyzer is None or industry_analyzer is None:
            out["reason"] = "missing_v7_context_analyzers"
            return out
        base.current_regime = market_analyzer.identify_market_regime()
        base.current_sentiment = market_analyzer.calculate_market_sentiment()
        base.hot_industries = industry_analyzer.get_hot_industries(top_n=top_n)
        out.update(
            {
                "status": "warmed",
                "market_regime": base.current_regime,
                "market_sentiment": float(base.current_sentiment or 0.0),
                "hot_industries_count": len(base.hot_industries or []),
            }
        )
        return out
    except Exception as exc:
        out["status"] = "failed"
        out["error"] = str(exc)
        return out
    finally:
        out["elapsed_ms"] = max(0.0, (time.perf_counter() - started) * 1000.0)
        if diagnostics is not None:
            diagnostics["v7_context_prewarm"] = dict(out)


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

    base_weights = dict(weights)
    normalized_health_multipliers = {k: float(health_multipliers.get(k, 1.0)) for k in weights}
    pre_normalized_weights = {k: float(v) * float(normalized_health_multipliers.get(k, 1.0)) for k, v in weights.items()}
    weights = dict(pre_normalized_weights)
    weight_sum = sum(weights.values())
    if weight_sum > 1e-9:
        weights = {k: float(v) / weight_sum for k, v in weights.items()}

    return {
        "thresholds": {
            "v5": float(combo_params.get("thr_v5", 60)),
            "v8": float(combo_params.get("thr_v8", 65)),
            "v9": float(combo_params.get("thr_v9", 60)),
        },
        "base_weights": base_weights,
        "health_multipliers": normalized_health_multipliers,
        "pre_normalized_weights": pre_normalized_weights,
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
    scores = evaluate_combo_component_scores(
        ts_code=ts_code,
        current_data=current_data,
        v5_evaluator=v5_evaluator,
        v8_evaluator=v8_evaluator,
        v9_score_fn=v9_score_fn,
        index_data=index_data,
    )
    return evaluate_combo_score_components(
        scores=scores,
        thresholds=thresholds,
        weights=weights,
        combo_threshold=combo_threshold,
        min_agree=min_agree,
        market_env=market_env,
    )


def evaluate_combo_component_scores(
    *,
    ts_code: str,
    current_data: pd.DataFrame,
    v5_evaluator: Any,
    v8_evaluator: Any,
    v9_score_fn: Callable[[pd.DataFrame], Dict[str, Any]],
    index_data: Optional[pd.DataFrame],
    timing_sink: Optional[Dict[str, Any]] = None,
    score_cache: Optional[Dict[str, Optional[float]]] = None,
    v5_candidate_aligned: bool = False,
    v5_threshold: float = 60.0,
) -> Dict[str, Optional[float]]:
    current = ensure_price_aliases(current_data.copy())
    industry = str(current["industry"].iloc[-1]) if ("industry" in current.columns and len(current) > 0) else ""
    scores: Dict[str, Optional[float]] = {}
    current_date = str(current["trade_date"].iloc[-1]) if ("trade_date" in current.columns and len(current) > 0) else ""
    cache_base = f"{ts_code}|{current_date}|{len(current)}"
    scores["v5"] = _get_or_compute_component_score(
        component="v5",
        cache_key=f"v5|aligned:{int(bool(v5_candidate_aligned))}|{cache_base}",
        timing_sink=timing_sink,
        score_cache=score_cache,
        compute=lambda: _safe_v5_score(
            v5_evaluator,
            current,
            timing_sink=timing_sink,
            candidate_aligned=v5_candidate_aligned,
            threshold=v5_threshold,
        ),
    )
    scores["v8"] = _get_or_compute_component_score(
        component="v8",
        cache_key=f"v8|{cache_base}",
        timing_sink=timing_sink,
        score_cache=score_cache,
        compute=lambda: _safe_v8_score(v8_evaluator, current, ts_code=ts_code, index_data=index_data, industry=industry, timing_sink=timing_sink),
    )
    scores["v9"] = _get_or_compute_component_score(
        component="v9",
        cache_key=f"v9|{cache_base}",
        timing_sink=timing_sink,
        score_cache=score_cache,
        compute=lambda: _safe_v9_score(v9_score_fn, current),
    )
    return scores


def _get_or_compute_component_score(
    *,
    component: str,
    cache_key: str,
    timing_sink: Optional[Dict[str, Any]],
    score_cache: Optional[Dict[str, Optional[float]]],
    compute: Callable[[], Optional[float]],
) -> Optional[float]:
    if score_cache is not None and cache_key in score_cache:
        _record_component_cache(timing_sink, component, hit=True)
        return score_cache[cache_key]
    _record_component_cache(timing_sink, component, hit=False)
    started = time.perf_counter()
    score = compute()
    _record_component_timing(timing_sink, component, time.perf_counter() - started)
    if score_cache is not None:
        score_cache[cache_key] = score
    return score


def _record_component_cache(timing_sink: Optional[Dict[str, Any]], component: str, *, hit: bool) -> None:
    if timing_sink is None:
        return
    cache_stats = timing_sink.setdefault("component_score_cache", {})
    item = cache_stats.setdefault(str(component), {"hit": 0, "miss": 0})
    key = "hit" if hit else "miss"
    item[key] = int(item.get(key, 0) or 0) + 1


def _record_component_timing(timing_sink: Optional[Dict[str, Any]], component: str, elapsed_sec: float) -> None:
    if timing_sink is None:
        return
    elapsed_ms = max(0.0, float(elapsed_sec) * 1000.0)
    timings = timing_sink.setdefault("component_timing_ms", {})
    stats = timings.setdefault(str(component), {"count": 0, "sum": 0.0, "max": elapsed_ms})
    stats["count"] = int(stats.get("count", 0) or 0) + 1
    stats["sum"] = float(stats.get("sum", 0.0) or 0.0) + elapsed_ms
    stats["max"] = max(float(stats.get("max", 0.0) or 0.0), elapsed_ms)
    totals = timing_sink.setdefault("component_timing_totals_ms", {"count": 0, "sum": 0.0, "max": elapsed_ms})
    totals["count"] = int(totals.get("count", 0) or 0) + 1
    totals["sum"] = float(totals.get("sum", 0.0) or 0.0) + elapsed_ms
    totals["max"] = max(float(totals.get("max", 0.0) or 0.0), elapsed_ms)


def slice_index_data_for_combo_history(index_data: Optional[pd.DataFrame], current_data: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Keep combo component evaluators point-in-time during rolling replay."""

    if index_data is None or index_data.empty:
        return index_data
    if current_data is None or current_data.empty or "trade_date" not in current_data.columns or "trade_date" not in index_data.columns:
        return index_data
    current_date = str(current_data["trade_date"].iloc[-1])
    idx = index_data.copy()
    idx["_trade_date_key"] = idx["trade_date"].astype(str)
    out = idx[idx["_trade_date_key"] <= current_date].drop(columns=["_trade_date_key"])
    return out if not out.empty else index_data.iloc[:0].copy()


def record_combo_component_diagnostics(
    diagnostics: Dict[str, Any],
    *,
    scores: Dict[str, Optional[float]],
    thresholds: Dict[str, float],
    combo_threshold: float,
) -> None:
    """Freeze score distribution for each combo component without changing gates."""

    score_stats = diagnostics.setdefault("component_score_stats", {})
    near_threshold = diagnostics.setdefault("component_near_threshold", {})
    for key in ("v5", "v8", "v9"):
        value = scores.get(key)
        if value is None:
            continue
        score = float(value)
        stats = score_stats.setdefault(
            key,
            {"count": 0, "sum": 0.0, "min": score, "max": score, "p50_samples": []},
        )
        stats["count"] = int(stats.get("count", 0) or 0) + 1
        stats["sum"] = float(stats.get("sum", 0.0) or 0.0) + score
        stats["min"] = min(float(stats.get("min", score) or score), score)
        stats["max"] = max(float(stats.get("max", score) or score), score)
        samples = stats.setdefault("p50_samples", [])
        if isinstance(samples, list) and len(samples) < 1000:
            samples.append(score)
        threshold = float(thresholds.get(key, combo_threshold))
        if score >= threshold:
            bucket = "pass"
        elif score >= threshold - 5.0:
            bucket = "within_5"
        elif score >= threshold - 10.0:
            bucket = "within_10"
        else:
            bucket = "far_below"
        comp = near_threshold.setdefault(key, {"pass": 0, "within_5": 0, "within_10": 0, "far_below": 0})
        comp[bucket] = int(comp.get(bucket, 0) or 0) + 1


def record_combo_gate_diagnostics(
    diagnostics: Dict[str, Any],
    *,
    scores: Dict[str, Optional[float]],
    thresholds: Dict[str, float],
    weights: Dict[str, float],
    combo_threshold: float,
    min_agree: int,
) -> Dict[str, Any]:
    """Record pair agreement and weighted consensus gaps without changing gates."""

    active_keys = [key for key, value in scores.items() if value is not None and float(weights.get(key, 0.0)) > 0.0]
    required_agree = max(1, min(int(min_agree), len(active_keys))) if active_keys else max(1, int(min_agree))
    pass_keys = [
        key
        for key in active_keys
        if scores.get(key) is not None and float(scores[key]) >= float(thresholds.get(key, combo_threshold))
    ]
    pair_counts = diagnostics.setdefault("pair_agreement", {})
    for left, right in (("v5", "v8"), ("v5", "v9"), ("v8", "v9")):
        if left in pass_keys and right in pass_keys:
            pair = f"{left}+{right}"
            pair_counts[pair] = int(pair_counts.get(pair, 0) or 0) + 1

    weight_sum = sum(float(weights[key]) for key in active_keys)
    weighted_consensus = (
        sum(float(scores[key]) * float(weights[key]) for key in active_keys if scores[key] is not None) / weight_sum
        if weight_sum > 1e-9
        else None
    )
    if len(pass_keys) >= required_agree and weighted_consensus is not None:
        gap = float(combo_threshold) - float(weighted_consensus)
        stats = diagnostics.setdefault(
            "weighted_consensus_candidates",
            {
                "count": 0,
                "sum": 0.0,
                "min": float(weighted_consensus),
                "max": float(weighted_consensus),
                "below_combo_threshold": 0,
                "gap_sum": 0.0,
                "max_gap": max(0.0, gap),
            },
        )
        stats["count"] = int(stats.get("count", 0) or 0) + 1
        stats["sum"] = float(stats.get("sum", 0.0) or 0.0) + float(weighted_consensus)
        stats["min"] = min(float(stats.get("min", weighted_consensus) or weighted_consensus), float(weighted_consensus))
        stats["max"] = max(float(stats.get("max", weighted_consensus) or weighted_consensus), float(weighted_consensus))
        if weighted_consensus < float(combo_threshold):
            stats["below_combo_threshold"] = int(stats.get("below_combo_threshold", 0) or 0) + 1
            stats["gap_sum"] = float(stats.get("gap_sum", 0.0) or 0.0) + max(0.0, gap)
            stats["max_gap"] = max(float(stats.get("max_gap", 0.0) or 0.0), max(0.0, gap))

    return {
        "active_count": len(active_keys),
        "agree_count": len(pass_keys),
        "required_agree": int(required_agree),
        "weighted_consensus": weighted_consensus,
        "breakpoint": classify_combo_consensus_breakpoint(
            scores=scores,
            thresholds=thresholds,
            weights=weights,
            combo_threshold=combo_threshold,
            min_agree=min_agree,
        ),
    }


def classify_combo_consensus_breakpoint(
    *,
    scores: Dict[str, Optional[float]],
    thresholds: Dict[str, float],
    weights: Dict[str, float],
    combo_threshold: float,
    min_agree: int,
) -> Dict[str, Any]:
    active_keys = [key for key, value in scores.items() if value is not None and float(weights.get(key, 0.0)) > 0.0]
    if not active_keys:
        return {
            "type": "no_active_components",
            "active_components": [],
            "passing_components": [],
            "failed_components": [],
            "required_agree": max(1, int(min_agree)),
            "weighted_consensus": None,
            "combo_gap": None,
        }
    required_agree = max(1, min(int(min_agree), len(active_keys)))
    passing = [
        key
        for key in active_keys
        if scores.get(key) is not None and float(scores[key]) >= float(thresholds.get(key, combo_threshold))
    ]
    failed = [key for key in active_keys if key not in passing]
    weight_sum = sum(float(weights[key]) for key in active_keys)
    weighted_consensus = (
        sum(float(scores[key]) * float(weights[key]) for key in active_keys if scores[key] is not None) / weight_sum
        if weight_sum > 1e-9
        else None
    )
    component_gaps = {
        key: float(thresholds.get(key, combo_threshold)) - float(scores[key])
        for key in active_keys
        if scores.get(key) is not None
    }
    combo_gap = (float(combo_threshold) - float(weighted_consensus)) if weighted_consensus is not None else None
    if len(passing) < required_agree:
        breakpoint_type = "component_agreement_shortfall"
    elif weighted_consensus is None:
        breakpoint_type = "weighted_consensus_unavailable"
    elif weighted_consensus < float(combo_threshold):
        breakpoint_type = "weighted_consensus_gap"
    else:
        breakpoint_type = "passed"
    return {
        "type": breakpoint_type,
        "active_components": active_keys,
        "passing_components": passing,
        "failed_components": failed,
        "required_agree": int(required_agree),
        "agree_count": int(len(passing)),
        "weighted_consensus": weighted_consensus,
        "combo_gap": combo_gap,
        "component_gaps": component_gaps,
        "largest_component_gap": max(component_gaps.items(), key=lambda item: item[1])[0] if component_gaps else "",
    }


def freeze_combo_component_diagnostics(diagnostics: Dict[str, Any]) -> Dict[str, Any]:
    """Finalize combo component score stats for audit payloads."""

    out = dict(diagnostics)
    frozen: Dict[str, Any] = {}
    for key, payload in (diagnostics.get("component_score_stats") or {}).items():
        if not isinstance(payload, dict):
            continue
        count = int(payload.get("count", 0) or 0)
        samples = payload.get("p50_samples") if isinstance(payload.get("p50_samples"), list) else []
        samples_sorted = sorted(float(x) for x in samples)
        p50 = samples_sorted[len(samples_sorted) // 2] if samples_sorted else 0.0
        frozen[str(key)] = {
            "count": count,
            "avg": (float(payload.get("sum", 0.0) or 0.0) / float(count)) if count > 0 else 0.0,
            "min": float(payload.get("min", 0.0) or 0.0),
            "max": float(payload.get("max", 0.0) or 0.0),
            "p50": float(p50),
        }
    out["component_score_stats"] = frozen
    timing_frozen: Dict[str, Any] = {}
    for key, payload in (diagnostics.get("component_timing_ms") or {}).items():
        if not isinstance(payload, dict):
            continue
        count = int(payload.get("count", 0) or 0)
        timing_frozen[str(key)] = {
            "count": count,
            "avg": (float(payload.get("sum", 0.0) or 0.0) / float(count)) if count > 0 else 0.0,
            "max": float(payload.get("max", 0.0) or 0.0),
        }
    if timing_frozen:
        out["component_timing_ms"] = timing_frozen
    cache_frozen: Dict[str, Any] = {}
    for key, payload in (diagnostics.get("component_score_cache") or {}).items():
        if not isinstance(payload, dict):
            continue
        hit = int(payload.get("hit", 0) or 0)
        miss = int(payload.get("miss", 0) or 0)
        total = hit + miss
        cache_frozen[str(key)] = {
            "hit": hit,
            "miss": miss,
            "hit_rate": (float(hit) / float(total)) if total > 0 else 0.0,
        }
    if cache_frozen:
        out["component_score_cache"] = cache_frozen
    v8_stage_timing_frozen: Dict[str, Any] = {}
    for key, payload in (diagnostics.get("v8_stage_timing_ms") or {}).items():
        if not isinstance(payload, dict):
            continue
        count = int(payload.get("count", 0) or 0)
        v8_stage_timing_frozen[str(key)] = {
            "count": count,
            "avg": (float(payload.get("sum", 0.0) or 0.0) / float(count)) if count > 0 else 0.0,
            "max": float(payload.get("max", 0.0) or 0.0),
        }
    if v8_stage_timing_frozen:
        out["v8_stage_timing_ms"] = v8_stage_timing_frozen
    v7_stage_timing_frozen: Dict[str, Any] = {}
    for key, payload in (diagnostics.get("v7_stage_timing_ms") or {}).items():
        if not isinstance(payload, dict):
            continue
        count = int(payload.get("count", 0) or 0)
        v7_stage_timing_frozen[str(key)] = {
            "count": count,
            "avg": (float(payload.get("sum", 0.0) or 0.0) / float(count)) if count > 0 else 0.0,
            "max": float(payload.get("max", 0.0) or 0.0),
        }
    if v7_stage_timing_frozen:
        out["v7_stage_timing_ms"] = v7_stage_timing_frozen
    v5_breakdown_frozen: Dict[str, Any] = {}
    for key, payload in (diagnostics.get("v5_score_breakdown") or {}).items():
        if not isinstance(payload, dict):
            continue
        count = int(payload.get("count", 0) or 0)
        v5_breakdown_frozen[str(key)] = {
            "count": count,
            "avg": (float(payload.get("sum", 0.0) or 0.0) / float(count)) if count > 0 else 0.0,
            "min": float(payload.get("min", 0.0) or 0.0),
            "max": float(payload.get("max", 0.0) or 0.0),
        }
    if v5_breakdown_frozen:
        out["v5_score_breakdown"] = v5_breakdown_frozen
    v5_combo_counts = diagnostics.get("v5_synergy_combo_counts")
    if isinstance(v5_combo_counts, dict):
        out["v5_synergy_combo_counts"] = {str(k): int(v or 0) for k, v in v5_combo_counts.items()}
    v5_risk_counts = diagnostics.get("v5_risk_reason_counts")
    if isinstance(v5_risk_counts, dict):
        out["v5_risk_reason_counts"] = {str(k): int(v or 0) for k, v in v5_risk_counts.items()}
    v5_candidate_filter = diagnostics.get("v5_candidate_filter")
    if isinstance(v5_candidate_filter, dict):
        total = int(v5_candidate_filter.get("total", 0) or 0)
        applicable = int(v5_candidate_filter.get("applicable", 0) or 0)
        filtered = int(v5_candidate_filter.get("filtered_out", 0) or 0)
        out["v5_candidate_filter"] = {
            "total": total,
            "applicable": applicable,
            "filtered_out": filtered,
            "applicable_rate": (float(applicable) / float(total)) if total > 0 else 0.0,
            "reason_counts": dict(v5_candidate_filter.get("reason_counts") or {}),
        }
    timing_totals = diagnostics.get("component_timing_totals_ms")
    if isinstance(timing_totals, dict):
        count = int(timing_totals.get("count", 0) or 0)
        out["component_timing_totals_ms"] = {
            "count": count,
            "avg": (float(timing_totals.get("sum", 0.0) or 0.0) / float(count)) if count > 0 else 0.0,
            "max": float(timing_totals.get("max", 0.0) or 0.0),
        }
    candidates = diagnostics.get("weighted_consensus_candidates")
    if isinstance(candidates, dict):
        count = int(candidates.get("count", 0) or 0)
        below = int(candidates.get("below_combo_threshold", 0) or 0)
        out["weighted_consensus_candidates"] = {
            "count": count,
            "avg": (float(candidates.get("sum", 0.0) or 0.0) / float(count)) if count > 0 else 0.0,
            "min": float(candidates.get("min", 0.0) or 0.0),
            "max": float(candidates.get("max", 0.0) or 0.0),
            "below_combo_threshold": below,
            "avg_gap": (float(candidates.get("gap_sum", 0.0) or 0.0) / float(below)) if below > 0 else 0.0,
            "max_gap": float(candidates.get("max_gap", 0.0) or 0.0),
        }
    out["consensus_breakpoint_summary"] = summarize_combo_consensus_breakpoints(out)
    return out


def summarize_combo_consensus_breakpoints(diagnostics: Dict[str, Any]) -> Dict[str, Any]:
    near_threshold = diagnostics.get("component_near_threshold") if isinstance(diagnostics.get("component_near_threshold"), dict) else {}
    pair_agreement = diagnostics.get("pair_agreement") if isinstance(diagnostics.get("pair_agreement"), dict) else {}
    weighted = diagnostics.get("weighted_consensus_candidates") if isinstance(diagnostics.get("weighted_consensus_candidates"), dict) else {}
    component_pressure: Dict[str, Any] = {}
    for key, buckets in near_threshold.items():
        if not isinstance(buckets, dict):
            continue
        total = sum(int(buckets.get(name, 0) or 0) for name in ("pass", "within_5", "within_10", "far_below"))
        near = int(buckets.get("within_5", 0) or 0) + int(buckets.get("within_10", 0) or 0)
        component_pressure[str(key)] = {
            "total": int(total),
            "pass": int(buckets.get("pass", 0) or 0),
            "near_miss": int(near),
            "far_below": int(buckets.get("far_below", 0) or 0),
            "near_miss_rate": (float(near) / float(total)) if total > 0 else 0.0,
        }
    weakest_component = ""
    if component_pressure:
        weakest_component = max(
            component_pressure.items(),
            key=lambda item: (item[1]["far_below"], item[1]["near_miss"], item[0]),
        )[0]
    return {
        "component_pressure": component_pressure,
        "weakest_component": weakest_component,
        "pair_agreement": {str(k): int(v) for k, v in pair_agreement.items()},
        "weighted_consensus_gap": {
            "candidate_count": int(weighted.get("count", 0) or 0),
            "below_combo_threshold": int(weighted.get("below_combo_threshold", 0) or 0),
            "avg_gap": float(weighted.get("avg_gap", 0.0) or 0.0),
            "max_gap": float(weighted.get("max_gap", 0.0) or 0.0),
        },
    }


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


def _safe_v5_score(
    v5_evaluator: Any,
    current_data: pd.DataFrame,
    *,
    timing_sink: Optional[Dict[str, Any]] = None,
    candidate_aligned: bool = False,
    threshold: float = 60.0,
) -> Optional[float]:
    try:
        result = v5_evaluator.evaluate_stock_v4(current_data)
        _record_v5_score_breakdown(timing_sink, result)
        if result and result.get("success"):
            if candidate_aligned and not _record_v5_candidate_filter(timing_sink, result, threshold=float(threshold)):
                return None
            return float(result.get("final_score", 0))
    except Exception:
        return None
    return None


def _record_v5_candidate_filter(timing_sink: Optional[Dict[str, Any]], result: Dict[str, Any], *, threshold: float) -> bool:
    dim_scores = result.get("dim_scores")
    if not isinstance(dim_scores, dict):
        dim_scores = result.get("dimension_scores")
    if not isinstance(dim_scores, dict):
        dim_scores = {}
    final_score = _safe_float(result.get("final_score"), 0.0)
    launch = _safe_float(dim_scores.get("启动确认"), 0.0)
    volume_price = _safe_float(dim_scores.get("量价配合"), 0.0)
    main_force = _safe_float(dim_scores.get("主力行为"), 0.0)
    synergy = _safe_float(result.get("synergy_bonus"), 0.0)

    applicable = bool(
        final_score >= float(threshold)
        or (launch >= 8.0 and (volume_price >= 8.0 or main_force >= 6.0))
        or (synergy > 0.0 and launch >= 6.0)
    )
    reasons = []
    if launch < 8.0:
        reasons.append("launch_confirmation_below_candidate_floor")
    if volume_price < 8.0:
        reasons.append("volume_price_below_candidate_floor")
    if main_force < 6.0:
        reasons.append("main_force_below_candidate_floor")
    if synergy <= 0.0:
        reasons.append("no_v5_synergy")

    if timing_sink is not None:
        stats = timing_sink.setdefault("v5_candidate_filter", {"total": 0, "applicable": 0, "filtered_out": 0, "reason_counts": {}})
        stats["total"] = int(stats.get("total", 0) or 0) + 1
        if applicable:
            stats["applicable"] = int(stats.get("applicable", 0) or 0) + 1
        else:
            stats["filtered_out"] = int(stats.get("filtered_out", 0) or 0) + 1
            reason_counts = stats.setdefault("reason_counts", {})
            for reason in reasons:
                reason_counts[reason] = int(reason_counts.get(reason, 0) or 0) + 1
    return applicable


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _record_v5_score_breakdown(timing_sink: Optional[Dict[str, Any]], result: Any) -> None:
    if timing_sink is None or not isinstance(result, dict):
        return
    for key in ("final_score", "base_score", "synergy_bonus", "risk_penalty"):
        _record_v5_numeric_breakdown(timing_sink, key, result.get(key))

    dim_scores = result.get("dim_scores")
    if not isinstance(dim_scores, dict):
        dim_scores = result.get("dimension_scores")
    if isinstance(dim_scores, dict):
        for key, value in dim_scores.items():
            _record_v5_numeric_breakdown(timing_sink, f"dim:{key}", value)

    combo = str(result.get("synergy_combo") or "").strip()
    if combo:
        counts = timing_sink.setdefault("v5_synergy_combo_counts", {})
        counts[combo] = int(counts.get(combo, 0) or 0) + 1

    risk_reasons = result.get("risk_reasons")
    if isinstance(risk_reasons, (list, tuple, set)):
        counts = timing_sink.setdefault("v5_risk_reason_counts", {})
        for reason in risk_reasons:
            key = str(reason or "").strip()
            if key:
                counts[key] = int(counts.get(key, 0) or 0) + 1


def _record_v5_numeric_breakdown(timing_sink: Dict[str, Any], key: str, raw_value: Any) -> None:
    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        return
    breakdown = timing_sink.setdefault("v5_score_breakdown", {})
    item = breakdown.setdefault(str(key), {"count": 0, "sum": 0.0, "min": value, "max": value})
    item["count"] = int(item.get("count", 0) or 0) + 1
    item["sum"] = float(item.get("sum", 0.0) or 0.0) + value
    item["min"] = min(float(item.get("min", value)), value)
    item["max"] = max(float(item.get("max", value)), value)


def _safe_v8_score(
    v8_evaluator: Any,
    current_data: pd.DataFrame,
    *,
    ts_code: str,
    index_data: Optional[pd.DataFrame],
    industry: str,
    timing_sink: Optional[Dict[str, Any]] = None,
) -> Optional[float]:
    try:
        result = v8_evaluator.evaluate_stock_v8(
            current_data,
            ts_code=ts_code,
            index_data=index_data,
            industry=industry,
        )
        _record_v8_stage_timing(timing_sink, result)
        if result and result.get("success"):
            return float(result.get("final_score", 0))
    except Exception:
        return None
    return None


def _record_v8_stage_timing(timing_sink: Optional[Dict[str, Any]], result: Any) -> None:
    if timing_sink is None or not isinstance(result, dict):
        return
    runtime_diagnostics = result.get("runtime_diagnostics")
    if not isinstance(runtime_diagnostics, dict):
        return
    stage_timing = runtime_diagnostics.get("stage_timing_ms")
    if not isinstance(stage_timing, dict):
        stage_timing = {}
    _record_stage_timing_payload(timing_sink, "v8_stage_timing_ms", stage_timing)
    v7_stage_timing = runtime_diagnostics.get("v7_stage_timing_ms")
    if isinstance(v7_stage_timing, dict):
        _record_stage_timing_payload(timing_sink, "v7_stage_timing_ms", v7_stage_timing)


def _record_stage_timing_payload(timing_sink: Dict[str, Any], key_name: str, stage_timing: Dict[str, Any]) -> None:
    out = timing_sink.setdefault(key_name, {})
    for key, raw_value in stage_timing.items():
        try:
            elapsed_ms = max(0.0, float(raw_value))
        except (TypeError, ValueError):
            continue
        item = out.setdefault(str(key), {"count": 0, "sum": 0.0, "max": elapsed_ms})
        item["count"] = int(item.get("count", 0) or 0) + 1
        item["sum"] = float(item.get("sum", 0.0) or 0.0) + elapsed_ms
        item["max"] = max(float(item.get("max", 0.0) or 0.0), elapsed_ms)


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
