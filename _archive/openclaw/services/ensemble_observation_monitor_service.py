from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from typing import Any, Dict, Iterable, Sequence

from openclaw.services.ensemble_observation_promotion_apply_service import valid_observation_promotion_records


JsonDict = Dict[str, Any]


DEFAULT_THRESHOLDS = {
    "min_after_cost_excess_return": 0.0,
    "min_hit_rate": 0.60,
    "max_turnover": 0.75,
    "max_industry_concentration": 0.30,
    "max_capacity_utilization": 0.10,
    "max_drawdown_floor": -15.0,
    "min_neutral_hit_rate": 0.50,
    "min_risk_on_hit_rate": 0.50,
    "min_risk_off_hit_rate": 0.50,
    "max_single_source_weight_share": 0.50,
    "max_single_industry_weight_share": 0.30,
}


def build_ensemble_observation_monitor(
    *,
    observation_records: Iterable[JsonDict],
    shadow_benchmark_artifact_path: str,
    stage_audit_artifact_path: str = "",
    output_dir: str = "",
    operator_name: str = "",
) -> JsonDict:
    records = valid_observation_promotion_records([dict(item) for item in observation_records if isinstance(item, dict)])
    benchmark_payload = _load_json(shadow_benchmark_artifact_path)
    stage_audit = _load_json(stage_audit_artifact_path)
    benchmark = benchmark_payload.get("benchmark") if isinstance(benchmark_payload.get("benchmark"), dict) else {}
    windows = [dict(item) for item in benchmark_payload.get("windows") or [] if isinstance(item, dict)]
    thresholds = dict(DEFAULT_THRESHOLDS)
    blocking: list[str] = []
    warnings: list[str] = []

    if not records:
        blocking.append("missing_applied_observation_record")
    if benchmark.get("research_only") is not True and benchmark_payload.get("research_only") is not True:
        blocking.append("benchmark_not_research_only")
    if benchmark.get("passed") is not True:
        blocking.append("shadow_benchmark_not_passed")
    for reason in benchmark.get("blocking_reasons") or []:
        blocking.append(f"shadow_benchmark_blocked:{reason}")

    metric_reviews = _metric_reviews(benchmark, thresholds)
    blocking.extend(metric_reviews["blocking_reasons"])
    warnings.extend(metric_reviews["warning_reasons"])
    regime_reviews = _regime_reviews(benchmark, thresholds)
    blocking.extend(regime_reviews["blocking_reasons"])
    warnings.extend(regime_reviews["warning_reasons"])
    throttle_review = _allocator_throttle_review(windows)
    warnings.extend(throttle_review["warning_reasons"])
    shadow_review = _formal_shadow_review(windows)
    blocking.extend(shadow_review["blocking_reasons"])
    risk_review = _risk_contribution_review(benchmark, thresholds)
    warnings.extend(risk_review["warning_reasons"])

    if stage_audit:
        if stage_audit.get("passed") is not True:
            blocking.append("stage_audit_not_passed")
        top = [str(item or "") for item in stage_audit.get("top_strategies") or []]
        if "ensemble_core" in top:
            blocking.append("ensemble_core_unexpectedly_in_formal_top")
        observation = {str((item or {}).get("strategy") or "") for item in stage_audit.get("observation_pool") or []}
        if "ensemble_core" not in observation:
            blocking.append("ensemble_core_missing_from_stage_observation_pool")

    status = "observation_monitor_blocked" if blocking else ("observation_monitor_watch" if warnings else "observation_monitor_healthy")
    payload: JsonDict = {
        "artifact_version": "ensemble_observation_monitor.v1",
        "created_at": _now_text(),
        "operator_name": str(operator_name or ""),
        "strategy": "ensemble_core",
        "candidate": str(benchmark_payload.get("candidate") or "hard_event_alpha_candidate"),
        "current_pool": "observation",
        "status": status,
        "observation_monitor_passed": not blocking,
        "formal_candidate_allowed": False,
        "formal_ranking_allowed": False,
        "production_candidate_allowed": False,
        "formal_top_allowed": False,
        "thresholds": thresholds,
        "source_artifacts": {
            "shadow_benchmark": str(shadow_benchmark_artifact_path or ""),
            "stage_audit": str(stage_audit_artifact_path or ""),
        },
        "observation_records": records,
        "metric_reviews": metric_reviews["reviews"],
        "regime_reviews": regime_reviews["reviews"],
        "allocator_throttle_review": throttle_review["review"],
        "formal_pool_shadow_review": shadow_review["review"],
        "risk_contribution_review": risk_review["review"],
        "blocking_reasons": sorted(set(blocking)),
        "warning_reasons": sorted(set(warnings)),
        "required_next_action": "continue_observation_monitoring" if not blocking else "repair_observation_monitor_blockers",
        "hard_boundaries": [
            "observation_monitor_does_not_promote_to_formal",
            "observation_monitor_does_not_publish_formal_top",
            "allocator_throttle_is_risk_control_not_alpha_evidence",
            "formal_candidate_requires_separate_review_after_independent_observation_period",
        ],
    }
    if output_dir:
        artifacts = _write_artifacts(Path(output_dir), payload)
        payload["artifacts"] = artifacts
        _write_json(Path(artifacts["json"]), payload)
    return payload


def _metric_reviews(benchmark: JsonDict, thresholds: JsonDict) -> JsonDict:
    reviews: JsonDict = {}
    blocking: list[str] = []
    warnings: list[str] = []
    specs = {
        "after_cost_excess_return": (benchmark.get("after_cost_excess_return"), ">=", thresholds["min_after_cost_excess_return"]),
        "hit_rate": (benchmark.get("hit_rate"), ">=", thresholds["min_hit_rate"]),
        "turnover": (benchmark.get("turnover"), "<=", thresholds["max_turnover"]),
        "industry_concentration": (benchmark.get("industry_concentration"), "<", thresholds["max_industry_concentration"]),
        "capacity_utilization": (benchmark.get("capacity_utilization"), "<=", thresholds["max_capacity_utilization"]),
        "max_drawdown": (benchmark.get("max_drawdown"), ">=", thresholds["max_drawdown_floor"]),
    }
    for name, (raw, op, limit) in specs.items():
        value = _optional_float(raw)
        passed = _compare(value, op, float(limit))
        reviews[name] = {"value": value, "operator": op, "threshold": float(limit), "passed": passed}
        if not passed:
            blocking.append(f"{name}_threshold_failed:{value}:{op}:{float(limit)}")
    turnover = _optional_float(benchmark.get("turnover"))
    industry = _optional_float(benchmark.get("industry_concentration"))
    if turnover is not None and turnover >= 0.90 * float(thresholds["max_turnover"]):
        warnings.append(f"turnover_near_cap:{turnover}/{thresholds['max_turnover']}")
    if industry is not None and industry >= 0.90 * float(thresholds["max_industry_concentration"]):
        warnings.append(f"industry_concentration_near_cap:{industry}/{thresholds['max_industry_concentration']}")
    return {"reviews": reviews, "blocking_reasons": blocking, "warning_reasons": warnings}


def _regime_reviews(benchmark: JsonDict, thresholds: JsonDict) -> JsonDict:
    split = benchmark.get("regime_split") if isinstance(benchmark.get("regime_split"), dict) else {}
    reviews: JsonDict = {}
    blocking: list[str] = []
    warnings: list[str] = []
    regime_specs = [
        ("neutral", thresholds["min_neutral_hit_rate"]),
        ("risk_on", thresholds["min_risk_on_hit_rate"]),
    ]
    if "risk_off" in split:
        regime_specs.append(("risk_off", thresholds["min_risk_off_hit_rate"]))
    for regime, min_hit in regime_specs:
        raw = split.get(regime) if isinstance(split.get(regime), dict) else {}
        avg_excess = _optional_float(raw.get("avg_after_cost_excess_return"))
        hit_rate = _optional_float(raw.get("hit_rate"))
        window_count = int(float(raw.get("window_count") or 0))
        passed = bool(window_count > 0 and avg_excess is not None and avg_excess > 0.0 and hit_rate is not None and hit_rate >= float(min_hit))
        reviews[regime] = {
            "window_count": window_count,
            "avg_after_cost_excess_return": avg_excess,
            "hit_rate": hit_rate,
            "min_hit_rate": float(min_hit),
            "passed": passed,
        }
        if not passed:
            blocking.append(f"regime_monitor_failed:{regime}")
    if "risk_off" not in split:
        warnings.append("missing_risk_off_observation_window")
        reviews["risk_off"] = {"window_count": 0, "passed": None, "monitoring_gap": True}
    return {"reviews": reviews, "blocking_reasons": blocking, "warning_reasons": warnings}


def _allocator_throttle_review(windows: Sequence[JsonDict]) -> JsonDict:
    controls = []
    warnings: list[str] = []
    for row in windows:
        portfolio = row.get("shadow_portfolio") if isinstance(row.get("shadow_portfolio"), dict) else {}
        ctrl = portfolio.get("allocator_controls") if isinstance(portfolio.get("allocator_controls"), dict) else {}
        if ctrl:
            controls.append(ctrl)
    throttle_present = any(float(item.get("target_gross_exposure") or 0.0) < float(item.get("pre_throttle_invested_weight") or 0.0) for item in controls)
    neutral_controls = [item for item in controls if str(item.get("market_regime_label") or "") == "neutral"]
    avg_neutral_gross = _avg([float(item.get("target_gross_exposure") or 0.0) for item in neutral_controls])
    if throttle_present:
        warnings.append("allocator_throttle_present_do_not_treat_as_alpha_improvement")
    return {
        "review": {
            "throttle_present": throttle_present,
            "window_count_with_controls": len(controls),
            "neutral_window_count_with_controls": len(neutral_controls),
            "avg_neutral_target_gross_exposure": avg_neutral_gross,
            "interpretation": "risk_control_only_not_alpha_evidence",
        },
        "warning_reasons": warnings,
    }


def _formal_shadow_review(windows: Sequence[JsonDict]) -> JsonDict:
    blocking: list[str] = []
    valid = 0
    for idx, row in enumerate(windows):
        benchmark = row.get("formal_pool_benchmark") if isinstance(row.get("formal_pool_benchmark"), dict) else {}
        execution = row.get("execution_cost_replay") if isinstance(row.get("execution_cost_replay"), dict) else {}
        if benchmark.get("available") is not True:
            blocking.append(f"window_{idx}:missing_formal_pool_shadow_benchmark")
        elif execution.get("research_only") is True and execution.get("not_for_production") is True:
            valid += 1
        else:
            blocking.append(f"window_{idx}:missing_research_only_after_cost_replay")
    return {"review": {"window_count": len(windows), "valid_shadow_compare_windows": valid, "passed": not blocking}, "blocking_reasons": blocking}


def _risk_contribution_review(benchmark: JsonDict, thresholds: JsonDict) -> JsonDict:
    risk = benchmark.get("risk_contribution") if isinstance(benchmark.get("risk_contribution"), dict) else {}
    sources = risk.get("source_strategy_weight_share") if isinstance(risk.get("source_strategy_weight_share"), dict) else {}
    industries = risk.get("industry_weight_share") if isinstance(risk.get("industry_weight_share"), dict) else {}
    max_source = max((float(v or 0.0) for v in sources.values()), default=0.0)
    max_industry = max((float(v or 0.0) for v in industries.values()), default=0.0)
    warnings: list[str] = []
    if max_source > float(thresholds["max_single_source_weight_share"]):
        warnings.append(f"source_strategy_weight_share_concentrated:{max_source}")
    if max_industry >= float(thresholds["max_single_industry_weight_share"]):
        warnings.append(f"industry_weight_share_concentrated:{max_industry}")
    return {
        "review": {
            "source_strategy_weight_share": sources,
            "industry_weight_share": industries,
            "max_source_strategy_weight_share": round(max_source, 6),
            "max_industry_weight_share": round(max_industry, 6),
        },
        "warning_reasons": warnings,
    }


def _compare(value: float | None, op: str, limit: float) -> bool:
    if value is None:
        return False
    if op == ">=":
        return value >= limit
    if op == "<=":
        return value <= limit
    if op == "<":
        return value < limit
    return False


def _optional_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return round(float(value), 6)
    except (TypeError, ValueError):
        return None


def _avg(values: Sequence[float]) -> float | None:
    return round(sum(values) / float(len(values)), 6) if values else None


def _load_json(path: str) -> JsonDict:
    if not str(path or "").strip():
        return {}
    p = Path(path)
    if not p.exists():
        return {}
    payload = json.loads(p.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _write_artifacts(output_dir: Path, payload: JsonDict) -> JsonDict:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    candidate = str(payload.get("candidate") or "ensemble_core")
    json_path = output_dir / f"ensemble_observation_monitor_{candidate}_{ts}.json"
    md_path = output_dir / f"ensemble_observation_monitor_{candidate}_{ts}.md"
    _write_json(json_path, payload)
    _write_markdown(md_path, payload)
    return {"json": str(json_path), "markdown": str(md_path)}


def _write_markdown(path: Path, payload: JsonDict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Ensemble Observation Monitor",
        "",
        f"- strategy: `{payload.get('strategy')}`",
        f"- candidate: `{payload.get('candidate')}`",
        f"- current_pool: `{payload.get('current_pool')}`",
        f"- status: `{payload.get('status')}`",
        f"- formal_candidate_allowed: `{payload.get('formal_candidate_allowed')}`",
        f"- formal_ranking_allowed: `{payload.get('formal_ranking_allowed')}`",
        "",
        "## Blocking Reasons",
        "",
    ]
    blocking = payload.get("blocking_reasons") or []
    lines.extend([f"- `{item}`" for item in blocking] if blocking else ["- none"])
    lines.extend(["", "## Warning Reasons", ""])
    warnings = payload.get("warning_reasons") or []
    lines.extend([f"- `{item}`" for item in warnings] if warnings else ["- none"])
    lines.extend(["", "## Hard Boundaries", ""])
    lines.extend(f"- `{item}`" for item in payload.get("hard_boundaries") or [])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_json(path: Path, payload: JsonDict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
