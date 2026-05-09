"""Backtest diagnostics for unified strategy competition.

This module does not decide strategy eligibility.  It only explains why a
strategy did or did not produce credible backtest evidence from existing sweep
facts, so failed strategies can improve without lowering gates or inventing a
second truth source.
"""

from __future__ import annotations

from typing import Any, Dict, Sequence

from openclaw.services.backtest_credibility_service import evaluate_backtest_credibility


JsonDict = Dict[str, Any]


def _float_with_default(value: Any, default: float) -> float:
    if value is None:
        return float(default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def build_strategy_backtest_diagnostics(
    *,
    strategy: str,
    rows: Sequence[JsonDict],
    errors: Sequence[JsonDict],
    backtest_credibility: JsonDict | None = None,
) -> JsonDict:
    strategy_key = str(strategy or "").lower()
    rows = list(rows or [])
    errors = list(errors or [])
    credibility = _normalize_credibility_review(backtest_credibility or {})
    blocking = _normalize_blocking_reasons(credibility.get("blocking_reasons", []) or [])
    successful_rows = [r for r in rows if str(r.get("status")) == "success"]
    best = _best_successful_row(rows)

    window_diagnostics = _collect_window_diagnostics(rows=rows, errors=errors)
    failure_classes = _classify_failures(
        strategy=strategy_key,
        rows=rows,
        errors=errors,
        blocking=blocking,
        best=best,
        window_diagnostics=window_diagnostics,
    )
    next_actions = _next_actions(strategy=strategy_key, failure_classes=failure_classes, best=best)
    repair_plan = _repair_experiment_plan(
        strategy=strategy_key,
        failure_classes=failure_classes,
        window_diagnostics=window_diagnostics,
        best=best,
    )

    return {
        "strategy": strategy_key,
        "diagnostic_version": "strategy_backtest_diagnostics.v1",
        "eligible_for_formal_ranking": bool(credibility.get("passed") is True and _quality_floor_passed(best)),
        "credible_evidence_present": bool(credibility.get("passed") is True),
        "quality_floor_passed": _quality_floor_passed(best),
        "successful_param_runs": len(successful_rows),
        "failed_param_runs": len([r for r in rows if str(r.get("status")) != "success"]),
        "best_evidence": _compact_best(best),
        "window_diagnostics": window_diagnostics,
        "failure_classes": failure_classes,
        "next_actions": next_actions,
        "repair_experiment_plan": repair_plan,
        "hard_boundaries": [
            "do_not_lower_thresholds",
            "do_not_patch_labels_without_evidence",
            "do_not_promote_legacy_return_only_samples",
            "do_not_publish_formal_top_list_from_observation_pool",
        ],
    }


def _normalize_credibility_review(credibility: JsonDict) -> JsonDict:
    if "passed" in credibility and "blocking_reasons" in credibility:
        return dict(credibility)
    review = evaluate_backtest_credibility(credibility)
    out = dict(credibility)
    out["passed"] = bool(review.get("passed") is True)
    out["blocking_reasons"] = list(review.get("blocking_reasons") or [])
    return out


def _normalize_blocking_reasons(reasons: Sequence[Any]) -> list[str]:
    out: list[str] = []
    aliases = {
        "missing_or_failed:suspension_and_limit_handling": "missing_execution_constraint_evidence",
        "missing_or_failed:volume_constraint": "missing_execution_constraint_evidence",
        "missing_or_failed:cost_model": "missing_cost_or_slippage_evidence",
        "missing_or_failed:slippage_model": "missing_cost_or_slippage_evidence",
    }
    for reason in reasons:
        text = str(reason)
        out.append(text)
        alias = aliases.get(text)
        if alias:
            out.append(alias)
    return sorted(set(out))


def _best_successful_row(rows: Sequence[JsonDict]) -> JsonDict | None:
    successful = [r for r in rows if str(r.get("status")) == "success"]
    if not successful:
        return None
    return sorted(successful, key=lambda r: float(r.get("objective", -9999.0) or -9999.0), reverse=True)[0]


def _quality_floor_passed(best: JsonDict | None) -> bool:
    if not best:
        return False
    win_rate = float(best.get("win_rate", 0.0) or 0.0)
    signal_density = float(best.get("signal_density", 0.0) or 0.0)
    max_drawdown = _float_with_default(best.get("max_drawdown"), 1.0)
    return bool(win_rate >= 0.45 and signal_density > 0.0 and max_drawdown <= 0.25)


def _compact_best(best: JsonDict | None) -> JsonDict:
    if not best:
        return {}
    return {
        "status": best.get("status"),
        "score_threshold": best.get("score_threshold"),
        "sample_size": best.get("sample_size"),
        "holding_days": best.get("holding_days"),
        "win_rate": best.get("win_rate"),
        "max_drawdown": best.get("max_drawdown"),
        "signal_density": best.get("signal_density"),
        "objective": best.get("objective"),
        "rolling_test_windows": best.get("rolling_test_windows"),
    }


def _classify_failures(
    *,
    strategy: str,
    rows: Sequence[JsonDict],
    errors: Sequence[JsonDict],
    blocking: Sequence[str],
    best: JsonDict | None,
    window_diagnostics: JsonDict,
) -> list[str]:
    classes: list[str] = []
    error_text = " ".join(str(e.get("error", "")) for e in errors).lower()

    if not rows:
        classes.append("no_backtest_runs")
    if "timeout" in error_text:
        classes.append("runtime_timeout")
    if not best:
        classes.append("no_successful_parameter_run")
    if "missing_successful_test_windows" in blocking or "0 successful test windows" in error_text:
        classes.append("no_successful_rolling_test_window")
    if "missing_positive_signal_density" in blocking:
        classes.append("zero_signal_density")
    if (
        "missing_tradeability_filter" in blocking
        or "missing_volume_constraint" in blocking
        or "missing_execution_constraint_evidence" in blocking
    ):
        classes.append("missing_execution_constraint_evidence")
    if (
        "missing_slippage_model" in blocking
        or "missing_cost_model" in blocking
        or "missing_cost_or_slippage_evidence" in blocking
    ):
        classes.append("missing_cost_or_slippage_evidence")

    if best:
        win_rate = float(best.get("win_rate", 0.0) or 0.0)
        max_drawdown = _float_with_default(best.get("max_drawdown"), 1.0)
        if win_rate < 0.45:
            classes.append("weak_out_of_sample_win_rate")
        if max_drawdown > 0.25:
            classes.append("drawdown_above_quality_floor")

    if strategy in {"v4", "v5"} and not _has_runtime_execution_constraints(rows):
        classes.append("legacy_return_sample_not_credible_without_constraints")
    if strategy == "combo":
        classes.append("combo_component_consensus_diagnostic_required")
    if strategy == "v6" and _v6_score_model_calibration_required(window_diagnostics):
        classes.append("v6_score_model_calibration_required")
        if _int_from_path(window_diagnostics, "near_threshold", "within_10") <= 0:
            classes.append("v6_no_near_threshold_samples")
        if _bool_from_path(window_diagnostics, "v6_runtime_review", "short_cycle_noise_review", "coarse_step"):
            classes.append("v6_dense_replay_review_required")
    if strategy == "v6" and _v6_entry_gate_reconfirmation_gap(window_diagnostics):
        classes.append("v6_entry_gate_reconfirmation_samples_below_candidate_band")
    if strategy == "v6" and _v6_pullback_secondary_confirmation_gap(window_diagnostics):
        classes.append("v6_pullback_secondary_confirmation_missing_technical_breakout")
    if strategy == "v6" and _v6_pullback_technical_confirmation_not_scored(window_diagnostics):
        classes.append("v6_pullback_technical_confirmation_not_reflected_in_breakthrough_score")
    if strategy == "v6" and _v6_signal_conversion_gap(window_diagnostics, classes):
        classes.append("v6_threshold_passes_do_not_convert_to_successful_windows")
    if strategy == "v6" and _int_from_path(window_diagnostics, "reason_counts", "mandatory_filter:pit_data_unavailable") > 0:
        classes.append("v6_pit_data_availability_blocks_mandatory_filter_evidence")
    if strategy == "v7" and _v7_signal_generation_gap(window_diagnostics, classes):
        classes.append("v7_near_threshold_signal_generation_gap")
    if strategy in {"v6", "v8"} and (
        "no_successful_rolling_test_window" in classes or "zero_signal_density" in classes
    ) and "v6_score_model_calibration_required" not in classes and "v6_entry_gate_reconfirmation_samples_below_candidate_band" not in classes:
        classes.append("factor_score_distribution_diagnostic_required")
    if strategy == "ai":
        classes.append("backtest_handler_missing_or_not_credible")
    if strategy == "ensemble_core":
        classes.append("ensemble_core_portfolio_contract_missing")

    return sorted(set(classes))


def _v6_score_model_calibration_required(window_diagnostics: JsonDict) -> bool:
    if not bool(window_diagnostics.get("available") is True):
        return False
    if int(window_diagnostics.get("score_distribution_count", 0) or 0) <= 0:
        return False
    evaluated = int(window_diagnostics.get("evaluated", 0) or 0)
    if evaluated <= 0:
        return False
    passed = int(window_diagnostics.get("passed_threshold", 0) or 0)
    if passed > 0:
        return False
    threshold = _float_or_default(window_diagnostics.get("min_threshold"), 0.0)
    max_score = _float_or_default(window_diagnostics.get("max_score"), 0.0)
    within_10 = _int_from_path(window_diagnostics, "near_threshold", "within_10")
    return bool(threshold > 0.0 and max_score <= threshold - 10.0 and within_10 <= 0)


def _v6_entry_gate_reconfirmation_gap(window_diagnostics: JsonDict) -> bool:
    if not bool(window_diagnostics.get("available") is True):
        return False
    if int(window_diagnostics.get("passed_threshold", 0) or 0) > 0:
        return False
    review = window_diagnostics.get("entry_gate_review")
    if not isinstance(review, dict):
        return False
    mode_counts = review.get("mode_counts") if isinstance(review.get("mode_counts"), dict) else {}
    pullback_count = int(mode_counts.get("pullback_reconfirmed", 0) or 0)
    overheated_count = int(mode_counts.get("wait_for_pullback_reconfirmation", 0) or 0)
    if pullback_count <= 0 or overheated_count <= 0:
        return False
    passed_score_max = _float_or_default(review.get("passed_score_max"), 0.0)
    blocked_score_max = _float_or_default(review.get("blocked_score_max"), 0.0)
    threshold = _float_or_default(window_diagnostics.get("min_threshold"), 0.0)
    return bool(threshold > 0.0 and passed_score_max < threshold and blocked_score_max > passed_score_max)


def _v6_pullback_secondary_confirmation_gap(window_diagnostics: JsonDict) -> bool:
    quality = window_diagnostics.get("entry_gate_quality_by_mode")
    if not isinstance(quality, dict):
        return False
    pullback = quality.get("pullback_reconfirmed")
    if not isinstance(pullback, dict):
        return False
    count = int(pullback.get("count", 0) or 0)
    if count <= 0:
        return False
    confirmations = pullback.get("secondary_confirmation_counts")
    if not isinstance(confirmations, dict):
        return False
    technical = int(confirmations.get("technical_breakout_reconfirmed", 0) or 0)
    quality_ready = int(confirmations.get("quality_ready", 0) or 0)
    return bool(technical <= 0 or quality_ready <= 0)


def _v6_pullback_technical_confirmation_not_scored(window_diagnostics: JsonDict) -> bool:
    quality = window_diagnostics.get("entry_gate_quality_by_mode")
    if not isinstance(quality, dict):
        return False
    pullback = quality.get("pullback_reconfirmed")
    if not isinstance(pullback, dict):
        return False
    count = int(pullback.get("count", 0) or 0)
    if count <= 0:
        return False
    confirmations = pullback.get("secondary_confirmation_counts")
    if not isinstance(confirmations, dict):
        return False
    technical = int(confirmations.get("technical_breakout_reconfirmed", 0) or 0)
    if technical <= 0:
        return False
    dimensions = pullback.get("dimension_scores") if isinstance(pullback.get("dimension_scores"), dict) else {}
    breakthrough = dimensions.get("技术突破") if isinstance(dimensions.get("技术突破"), dict) else {}
    avg_breakthrough = _float_or_default(breakthrough.get("avg"), 0.0) if isinstance(breakthrough, dict) else 0.0
    return bool(avg_breakthrough < 2.0)


def _v6_signal_conversion_gap(window_diagnostics: JsonDict, classes: Sequence[str]) -> bool:
    if "no_successful_rolling_test_window" not in classes and "zero_signal_density" not in classes:
        return False
    if not bool(window_diagnostics.get("available") is True):
        return False
    return int(window_diagnostics.get("passed_threshold", 0) or 0) > 0


def _v7_signal_generation_gap(window_diagnostics: JsonDict, classes: Sequence[str]) -> bool:
    if "no_successful_rolling_test_window" not in classes and "zero_signal_density" not in classes:
        return False
    if not bool(window_diagnostics.get("available") is True):
        return False
    if int(window_diagnostics.get("score_distribution_count", 0) or 0) <= 0:
        return False
    if int(window_diagnostics.get("passed_threshold", 0) or 0) > 0:
        return False
    threshold = _float_or_default(window_diagnostics.get("min_threshold"), 0.0)
    max_score = _float_or_default(window_diagnostics.get("max_score"), 0.0)
    near = _int_from_path(window_diagnostics, "near_threshold", "within_10")
    return bool(threshold > 0.0 and near > 0 and max_score >= threshold - 10.0)


def _int_from_path(payload: JsonDict, *path: str) -> int:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return 0
        current = current.get(key)
    try:
        return int(current or 0)
    except (TypeError, ValueError):
        return 0


def _bool_from_path(payload: JsonDict, *path: str) -> bool:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return False
        current = current.get(key)
    return bool(current is True)


def _has_runtime_execution_constraints(rows: Sequence[JsonDict]) -> bool:
    for row in rows:
        if (
            str(row.get("status")) == "success"
            and bool(row.get("tradeability_filter_enabled") is True)
            and bool(row.get("volume_constraint_enabled") is True)
        ):
            return True
    return False


def _collect_window_diagnostics(*, rows: Sequence[JsonDict], errors: Sequence[JsonDict]) -> JsonDict:
    diagnostics: list[JsonDict] = []
    for source in list(rows or []) + list(errors or []):
        for item in source.get("run_diagnostics", []) or []:
            if isinstance(item, dict):
                diagnostics.append(item)
        for item in source.get("failure_diagnostics", []) or []:
            if isinstance(item, dict):
                diagnostics.append(item)
    diagnostics = _dedupe_window_diagnostics(diagnostics)
    if not diagnostics:
        return {"available": False, "diagnostic_count": 0}

    score_diags = [x for x in diagnostics if str(x.get("type")) == "score_distribution"]
    combo_diags = [x for x in diagnostics if str(x.get("type")) == "combo_consensus"]
    if combo_diags:
        return _aggregate_combo_diagnostics(combo_diags, total_count=len(diagnostics))
    if not score_diags:
        return {"available": True, "diagnostic_count": len(diagnostics)}

    evaluated = sum(int(x.get("evaluated", 0) or 0) for x in score_diags)
    passed = sum(int(x.get("passed_threshold", 0) or 0) for x in score_diags)
    missing = sum(int(x.get("missing_score", 0) or 0) for x in score_diags)
    max_score = max((float(x.get("max_score", 0.0) or 0.0) for x in score_diags), default=0.0)
    avg_scores = [float(x.get("avg_score", 0.0) or 0.0) for x in score_diags if int(x.get("score_count", 0) or 0) > 0]
    thresholds = [float(x.get("threshold", 0.0) or 0.0) for x in score_diags if x.get("threshold") is not None]
    near_threshold = {"within_2": 0, "within_5": 0, "within_10": 0}
    reason_counts: Dict[str, int] = {}
    score_breakdown = _aggregate_score_breakdown(score_diags)
    stage_timing_ms = _aggregate_stage_timing(score_diags)
    v6_runtime_review = _aggregate_v6_runtime_review(score_diags)
    top_near_threshold_samples = _aggregate_top_near_threshold_samples(score_diags)
    entry_gate_review = _aggregate_entry_gate_review(score_diags)
    entry_gate_passed_samples = _aggregate_entry_gate_passed_samples(score_diags)
    entry_gate_quality_by_mode = _aggregate_entry_gate_quality_by_mode(score_diags)
    for diag in score_diags:
        for key, value in (diag.get("reason_counts") or {}).items():
            reason_counts[str(key)] = int(reason_counts.get(str(key), 0) or 0) + int(value or 0)
        near = diag.get("near_threshold") if isinstance(diag.get("near_threshold"), dict) else {}
        runtime = diag.get("v6_runtime_diagnostics") if isinstance(diag.get("v6_runtime_diagnostics"), dict) else {}
        runtime_near = runtime.get("threshold_near_samples") if isinstance(runtime.get("threshold_near_samples"), dict) else {}
        for key in near_threshold:
            near_threshold[key] += int(near.get(key, runtime_near.get(key, 0)) or 0)
    return {
        "available": True,
        "diagnostic_count": len(diagnostics),
        "score_distribution_count": len(score_diags),
        "evaluated": evaluated,
        "passed_threshold": passed,
        "pass_rate": (float(passed) / float(evaluated)) if evaluated > 0 else 0.0,
        "missing_score": missing,
        "min_threshold": min(thresholds) if thresholds else 0.0,
        "max_threshold": max(thresholds) if thresholds else 0.0,
        "near_threshold": near_threshold,
        "max_score": max_score,
        "avg_score_mean": (sum(avg_scores) / len(avg_scores)) if avg_scores else 0.0,
        "reason_counts": reason_counts,
        "score_breakdown": score_breakdown,
        "stage_timing_ms": stage_timing_ms,
        "v6_runtime_review": v6_runtime_review,
        "top_near_threshold_samples": top_near_threshold_samples,
        "entry_gate_review": entry_gate_review,
        "entry_gate_passed_samples": entry_gate_passed_samples,
        "entry_gate_quality_by_mode": entry_gate_quality_by_mode,
    }


def _aggregate_top_near_threshold_samples(score_diags: Sequence[JsonDict], limit: int = 10) -> list[JsonDict]:
    samples: list[JsonDict] = []
    for diag in score_diags:
        raw_samples = diag.get("top_near_threshold_samples")
        if not isinstance(raw_samples, list):
            continue
        for item in raw_samples:
            if isinstance(item, dict):
                samples.append(dict(item))
    samples.sort(key=lambda row: _float_or_default(row.get("final_score"), 0.0), reverse=True)
    return samples[: int(limit)]


def _aggregate_entry_gate_passed_samples(score_diags: Sequence[JsonDict], limit: int = 10) -> list[JsonDict]:
    samples: list[JsonDict] = []
    for diag in score_diags:
        raw_samples = diag.get("entry_gate_passed_samples")
        if not isinstance(raw_samples, list):
            continue
        for item in raw_samples:
            if isinstance(item, dict):
                samples.append(dict(item))
    samples.sort(key=lambda row: _float_or_default(row.get("final_score"), 0.0), reverse=True)
    return samples[: int(limit)]


def _aggregate_entry_gate_review(score_diags: Sequence[JsonDict]) -> JsonDict:
    out: JsonDict = {
        "observed": 0,
        "passed": 0,
        "blocked": 0,
        "mode_counts": {},
        "reason_counts": {},
        "overheat_flag_counts": {},
        "passed_score_max": 0.0,
        "blocked_score_max": 0.0,
    }
    found = False
    for diag in score_diags:
        review = diag.get("entry_gate_review")
        if not isinstance(review, dict):
            continue
        found = True
        out["observed"] = int(out.get("observed", 0) or 0) + int(review.get("observed", 0) or 0)
        out["passed"] = int(out.get("passed", 0) or 0) + int(review.get("passed", 0) or 0)
        out["blocked"] = int(out.get("blocked", 0) or 0) + int(review.get("blocked", 0) or 0)
        out["passed_score_max"] = max(
            _float_or_default(out.get("passed_score_max"), 0.0),
            _float_or_default(review.get("passed_score_max"), 0.0),
        )
        out["blocked_score_max"] = max(
            _float_or_default(out.get("blocked_score_max"), 0.0),
            _float_or_default(review.get("blocked_score_max"), 0.0),
        )
        for key in ("mode_counts", "reason_counts", "overheat_flag_counts"):
            _merge_count_map(out.setdefault(key, {}), review.get(key))
    return out if found else {}


def _aggregate_entry_gate_quality_by_mode(score_diags: Sequence[JsonDict]) -> JsonDict:
    accum: Dict[str, JsonDict] = {}
    for diag in score_diags:
        quality = diag.get("entry_gate_quality_by_mode")
        if not isinstance(quality, dict):
            continue
        for mode, payload in quality.items():
            if not isinstance(payload, dict):
                continue
            count = int(payload.get("count", 0) or 0)
            if count <= 0:
                continue
            item = accum.setdefault(
                str(mode),
                {
                    "count": 0,
                    "score_sum": 0.0,
                    "score_max": _float_or_default(payload.get("max_score"), 0.0),
                    "gap_sum": 0.0,
                    "gap_min": _float_or_default(payload.get("min_gap_to_threshold"), 0.0),
                    "base_score": {},
                    "synergy_bonus": {},
                    "risk_penalty": {},
                    "dimension_scores": {},
                    "dimension_shortfall_to_reference": {},
                    "secondary_confirmation_counts": {},
                    "secondary_confirmation_metrics": {},
                },
            )
            item["count"] = int(item.get("count", 0) or 0) + count
            item["score_sum"] = float(item.get("score_sum", 0.0) or 0.0) + _float_or_default(payload.get("avg_score"), 0.0) * count
            item["score_max"] = max(_float_or_default(item.get("score_max"), 0.0), _float_or_default(payload.get("max_score"), 0.0))
            item["gap_sum"] = float(item.get("gap_sum", 0.0) or 0.0) + _float_or_default(payload.get("avg_gap_to_threshold"), 0.0) * count
            item["gap_min"] = min(
                _float_or_default(item.get("gap_min"), _float_or_default(payload.get("min_gap_to_threshold"), 0.0)),
                _float_or_default(payload.get("min_gap_to_threshold"), 0.0),
            )
            for key in ("base_score", "synergy_bonus", "risk_penalty"):
                _merge_weighted_metric(item.setdefault(key, {}), payload.get(key))
            for key in ("dimension_scores", "dimension_shortfall_to_reference"):
                _merge_weighted_metric_map(item.setdefault(key, {}), payload.get(key))
            _merge_count_map(item.setdefault("secondary_confirmation_counts", {}), payload.get("secondary_confirmation_counts"))
            _merge_weighted_metric_map(item.setdefault("secondary_confirmation_metrics", {}), payload.get("secondary_confirmation_metrics"))
    out: JsonDict = {}
    for mode, payload in accum.items():
        count = int(payload.get("count", 0) or 0)
        if count <= 0:
            continue
        out[mode] = {
            "count": count,
            "avg_score": float(payload.get("score_sum", 0.0) or 0.0) / float(count),
            "max_score": _float_or_default(payload.get("score_max"), 0.0),
            "avg_gap_to_threshold": float(payload.get("gap_sum", 0.0) or 0.0) / float(count),
            "min_gap_to_threshold": _float_or_default(payload.get("gap_min"), 0.0),
            "base_score": _freeze_weighted_metric(payload.get("base_score")),
            "synergy_bonus": _freeze_weighted_metric(payload.get("synergy_bonus")),
            "risk_penalty": _freeze_weighted_metric(payload.get("risk_penalty")),
            "dimension_scores": _freeze_weighted_metric_map(payload.get("dimension_scores")),
            "dimension_shortfall_to_reference": _freeze_weighted_metric_map(payload.get("dimension_shortfall_to_reference")),
            "secondary_confirmation_counts": dict(payload.get("secondary_confirmation_counts") or {}),
            "secondary_confirmation_metrics": _freeze_weighted_metric_map(payload.get("secondary_confirmation_metrics")),
        }
    return out


def _merge_weighted_metric(target: Any, source: Any) -> None:
    if not isinstance(target, dict) or not isinstance(source, dict):
        return
    count = int(source.get("count", 0) or 0)
    if count <= 0:
        return
    target["count"] = int(target.get("count", 0) or 0) + count
    target["weighted_sum"] = float(target.get("weighted_sum", 0.0) or 0.0) + _float_or_default(source.get("avg"), 0.0) * count
    source_min = _float_or_default(source.get("min"), _float_or_default(source.get("avg"), 0.0))
    source_max = _float_or_default(source.get("max"), _float_or_default(source.get("avg"), 0.0))
    if int(target.get("count", 0) or 0) == count:
        target["min"] = source_min
        target["max"] = source_max
    else:
        target["min"] = min(_float_or_default(target.get("min"), source_min), source_min)
        target["max"] = max(_float_or_default(target.get("max"), source_max), source_max)


def _merge_weighted_metric_map(target: Any, source: Any) -> None:
    if not isinstance(target, dict) or not isinstance(source, dict):
        return
    for key, payload in source.items():
        _merge_weighted_metric(target.setdefault(str(key), {}), payload)


def _freeze_weighted_metric(payload: Any) -> JsonDict:
    if not isinstance(payload, dict):
        return {}
    count = int(payload.get("count", 0) or 0)
    if count <= 0:
        return {}
    return {
        "count": count,
        "avg": float(payload.get("weighted_sum", 0.0) or 0.0) / float(count),
        "min": _float_or_default(payload.get("min"), 0.0),
        "max": _float_or_default(payload.get("max"), 0.0),
    }


def _freeze_weighted_metric_map(payload: Any) -> JsonDict:
    if not isinstance(payload, dict):
        return {}
    out: JsonDict = {}
    for key, value in payload.items():
        frozen = _freeze_weighted_metric(value)
        if frozen:
            out[str(key)] = frozen
    return out


def _merge_count_map(target: Any, source: Any) -> None:
    if not isinstance(target, dict) or not isinstance(source, dict):
        return
    for key, value in source.items():
        target[str(key)] = int(target.get(str(key), 0) or 0) + int(value or 0)


def _aggregate_stage_timing(score_diags: Sequence[JsonDict]) -> JsonDict:
    accum: Dict[str, Dict[str, float]] = {}
    for diag in score_diags:
        timing = diag.get("stage_timing_ms")
        if not isinstance(timing, dict):
            continue
        for key, payload in timing.items():
            if not isinstance(payload, dict):
                continue
            count = int(payload.get("count", 0) or 0)
            if count <= 0:
                continue
            avg = _float_or_default(payload.get("avg"), 0.0)
            max_value = _float_or_default(payload.get("max"), avg)
            item = accum.setdefault(str(key), {"count": 0.0, "weighted_sum": 0.0, "max": max_value})
            item["count"] += float(count)
            item["weighted_sum"] += avg * float(count)
            item["max"] = max(float(item.get("max", max_value)), max_value)
    out: JsonDict = {}
    for key, payload in accum.items():
        count = int(payload.get("count", 0.0) or 0.0)
        if count <= 0:
            continue
        out[key] = {
            "count": count,
            "avg": float(payload.get("weighted_sum", 0.0) or 0.0) / float(count),
            "max": float(payload.get("max", 0.0) or 0.0),
        }
    return out


def _aggregate_v6_runtime_review(score_diags: Sequence[JsonDict]) -> JsonDict:
    runtime_items = [
        diag.get("v6_runtime_diagnostics")
        for diag in score_diags
        if isinstance(diag.get("v6_runtime_diagnostics"), dict)
    ]
    if not runtime_items:
        return {}
    relaxed = 0
    replay_steps: list[int] = []
    coarse = False
    candidate_modes: Dict[str, int] = {}
    point_in_time = True
    production_candidate_allowed = False
    for item in runtime_items:
        relaxed += int(item.get("candidate_filter_relaxed_count", 0) or 0)
        step = int(item.get("replay_step", 0) or 0)
        if step > 0:
            replay_steps.append(step)
        mode = str(item.get("candidate_filter_mode", "") or "")
        if mode:
            candidate_modes[mode] = int(candidate_modes.get(mode, 0) or 0) + 1
        point_in_time = bool(point_in_time and item.get("point_in_time_context") is True)
        production_candidate_allowed = bool(production_candidate_allowed or item.get("production_candidate_allowed") is True)
        noise = item.get("short_cycle_noise_review") if isinstance(item.get("short_cycle_noise_review"), dict) else {}
        coarse = bool(coarse or noise.get("coarse_step") is True)
    return {
        "point_in_time_context": point_in_time,
        "production_candidate_allowed": production_candidate_allowed,
        "candidate_filter_relaxed_count": relaxed,
        "candidate_filter_modes": candidate_modes,
        "replay_step_min": min(replay_steps) if replay_steps else 0,
        "replay_step_max": max(replay_steps) if replay_steps else 0,
        "short_cycle_noise_review": {
            "coarse_step": coarse,
            "reason": "short_cycle_strategy_requires_dense_replay_review" if coarse else "",
        },
    }


def _dedupe_window_diagnostics(diagnostics: Sequence[JsonDict]) -> list[JsonDict]:
    seen = set()
    out: list[JsonDict] = []
    for diag in diagnostics:
        key = (
            str(diag.get("type")),
            str(diag.get("strategy")),
            float(diag.get("threshold", diag.get("combo_threshold", 0.0)) or 0.0),
            int(diag.get("evaluated", 0) or 0),
            int(diag.get("passed_threshold", 0) or 0),
            float(diag.get("max_score", 0.0) or 0.0),
            str(sorted((diag.get("reason_counts") or diag.get("drop_reasons") or {}).items())),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(diag)
    return out


def _aggregate_score_breakdown(score_diags: Sequence[JsonDict]) -> JsonDict:
    accum: Dict[str, Dict[str, float]] = {}
    for diag in score_diags:
        breakdown = diag.get("score_breakdown")
        if not isinstance(breakdown, dict):
            continue
        for key, payload in breakdown.items():
            if not isinstance(payload, dict):
                continue
            count = int(payload.get("count", 0) or 0)
            if count <= 0:
                continue
            avg = _float_or_default(payload.get("avg"), 0.0)
            min_value = _float_or_default(payload.get("min"), avg)
            max_value = _float_or_default(payload.get("max"), avg)
            item = accum.setdefault(
                str(key),
                {"count": 0.0, "weighted_sum": 0.0, "min": min_value, "max": max_value},
            )
            item["count"] += float(count)
            item["weighted_sum"] += avg * float(count)
            item["min"] = min(float(item.get("min", min_value)), min_value)
            item["max"] = max(float(item.get("max", max_value)), max_value)
    out: JsonDict = {}
    for key, payload in accum.items():
        count = int(payload.get("count", 0.0) or 0.0)
        if count <= 0:
            continue
        out[key] = {
            "count": count,
            "avg": float(payload.get("weighted_sum", 0.0) or 0.0) / float(count),
            "min": float(payload.get("min", 0.0) or 0.0),
            "max": float(payload.get("max", 0.0) or 0.0),
        }
    return out


def _float_or_default(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _aggregate_combo_diagnostics(combo_diags: Sequence[JsonDict], *, total_count: int) -> JsonDict:
    evaluated = sum(int(x.get("evaluated", 0) or 0) for x in combo_diags)
    component_available: Dict[str, int] = {}
    component_pass: Dict[str, int] = {}
    component_score_stats: Dict[str, Dict[str, float]] = {}
    component_timing_stats: Dict[str, Dict[str, float]] = {}
    component_cache_stats: Dict[str, Dict[str, int]] = {}
    v8_stage_timing_stats: Dict[str, Dict[str, float]] = {}
    v7_stage_timing_stats: Dict[str, Dict[str, float]] = {}
    v5_score_breakdown_stats: Dict[str, Dict[str, float]] = {}
    v5_synergy_combo_counts: Dict[str, int] = {}
    v5_risk_reason_counts: Dict[str, int] = {}
    v5_candidate_filter = {"total": 0, "applicable": 0, "filtered_out": 0, "reason_counts": {}}
    component_near_threshold: Dict[str, Dict[str, int]] = {}
    pair_agreement: Dict[str, int] = {}
    weight_context: Dict[str, Any] = {}
    weighted_candidates = {
        "count": 0.0,
        "weighted_sum": 0.0,
        "min": 0.0,
        "max": 0.0,
        "below_combo_threshold": 0.0,
        "gap_sum": 0.0,
        "max_gap": 0.0,
    }
    agree_hist: Dict[str, int] = {}
    drop_reasons: Dict[str, int] = {}
    for diag in combo_diags:
        if not weight_context:
            weight_context = {
                "base_weights": dict(diag.get("base_weights") or {}),
                "health_multipliers": dict(diag.get("health_multipliers") or {}),
                "health_evidence": dict(diag.get("health_evidence") or {}),
                "pre_normalized_weights": dict(diag.get("pre_normalized_weights") or {}),
                "weights": dict(diag.get("weights") or {}),
            }
        for key, value in (diag.get("component_available") or {}).items():
            component_available[str(key)] = int(component_available.get(str(key), 0) or 0) + int(value or 0)
        for key, value in (diag.get("component_pass") or {}).items():
            component_pass[str(key)] = int(component_pass.get(str(key), 0) or 0) + int(value or 0)
        for key, payload in (diag.get("component_score_stats") or {}).items():
            if not isinstance(payload, dict):
                continue
            count = int(payload.get("count", 0) or 0)
            if count <= 0:
                continue
            avg = _float_or_default(payload.get("avg"), 0.0)
            min_score = _float_or_default(payload.get("min"), avg)
            max_score = _float_or_default(payload.get("max"), avg)
            p50 = _float_or_default(payload.get("p50"), avg)
            item = component_score_stats.setdefault(
                str(key),
                {"count": 0.0, "weighted_sum": 0.0, "min": min_score, "max": max_score, "p50_weighted_sum": 0.0},
            )
            item["count"] += float(count)
            item["weighted_sum"] += avg * float(count)
            item["p50_weighted_sum"] += p50 * float(count)
            item["min"] = min(float(item.get("min", min_score)), min_score)
            item["max"] = max(float(item.get("max", max_score)), max_score)
        for key, payload in (diag.get("component_timing_ms") or {}).items():
            if not isinstance(payload, dict):
                continue
            count = int(payload.get("count", 0) or 0)
            if count <= 0:
                continue
            avg_ms = _float_or_default(payload.get("avg"), 0.0)
            max_ms = _float_or_default(payload.get("max"), avg_ms)
            item = component_timing_stats.setdefault(str(key), {"count": 0.0, "weighted_sum": 0.0, "max": max_ms})
            item["count"] += float(count)
            item["weighted_sum"] += avg_ms * float(count)
            item["max"] = max(float(item.get("max", max_ms)), max_ms)
        for key, payload in (diag.get("component_score_cache") or {}).items():
            if not isinstance(payload, dict):
                continue
            item = component_cache_stats.setdefault(str(key), {"hit": 0, "miss": 0})
            item["hit"] = int(item.get("hit", 0) or 0) + int(payload.get("hit", 0) or 0)
            item["miss"] = int(item.get("miss", 0) or 0) + int(payload.get("miss", 0) or 0)
        for key, payload in (diag.get("v8_stage_timing_ms") or {}).items():
            if not isinstance(payload, dict):
                continue
            count = int(payload.get("count", 0) or 0)
            if count <= 0:
                continue
            avg_ms = _float_or_default(payload.get("avg"), 0.0)
            max_ms = _float_or_default(payload.get("max"), avg_ms)
            item = v8_stage_timing_stats.setdefault(str(key), {"count": 0.0, "weighted_sum": 0.0, "max": max_ms})
            item["count"] += float(count)
            item["weighted_sum"] += avg_ms * float(count)
            item["max"] = max(float(item.get("max", max_ms)), max_ms)
        for key, payload in (diag.get("v7_stage_timing_ms") or {}).items():
            if not isinstance(payload, dict):
                continue
            count = int(payload.get("count", 0) or 0)
            if count <= 0:
                continue
            avg_ms = _float_or_default(payload.get("avg"), 0.0)
            max_ms = _float_or_default(payload.get("max"), avg_ms)
            item = v7_stage_timing_stats.setdefault(str(key), {"count": 0.0, "weighted_sum": 0.0, "max": max_ms})
            item["count"] += float(count)
            item["weighted_sum"] += avg_ms * float(count)
            item["max"] = max(float(item.get("max", max_ms)), max_ms)
        for key, payload in (diag.get("v5_score_breakdown") or {}).items():
            if not isinstance(payload, dict):
                continue
            count = int(payload.get("count", 0) or 0)
            if count <= 0:
                continue
            avg = _float_or_default(payload.get("avg"), 0.0)
            min_value = _float_or_default(payload.get("min"), avg)
            max_value = _float_or_default(payload.get("max"), avg)
            item = v5_score_breakdown_stats.setdefault(
                str(key),
                {"count": 0.0, "weighted_sum": 0.0, "min": min_value, "max": max_value},
            )
            item["count"] += float(count)
            item["weighted_sum"] += avg * float(count)
            item["min"] = min(float(item.get("min", min_value)), min_value)
            item["max"] = max(float(item.get("max", max_value)), max_value)
        for key, value in (diag.get("v5_synergy_combo_counts") or {}).items():
            v5_synergy_combo_counts[str(key)] = int(v5_synergy_combo_counts.get(str(key), 0) or 0) + int(value or 0)
        for key, value in (diag.get("v5_risk_reason_counts") or {}).items():
            v5_risk_reason_counts[str(key)] = int(v5_risk_reason_counts.get(str(key), 0) or 0) + int(value or 0)
        candidate_filter = diag.get("v5_candidate_filter")
        if isinstance(candidate_filter, dict):
            v5_candidate_filter["total"] = int(v5_candidate_filter["total"]) + int(candidate_filter.get("total", 0) or 0)
            v5_candidate_filter["applicable"] = int(v5_candidate_filter["applicable"]) + int(candidate_filter.get("applicable", 0) or 0)
            v5_candidate_filter["filtered_out"] = int(v5_candidate_filter["filtered_out"]) + int(candidate_filter.get("filtered_out", 0) or 0)
            reason_counts = v5_candidate_filter["reason_counts"]
            if isinstance(reason_counts, dict):
                for key, value in (candidate_filter.get("reason_counts") or {}).items():
                    reason_counts[str(key)] = int(reason_counts.get(str(key), 0) or 0) + int(value or 0)
        for key, payload in (diag.get("component_near_threshold") or {}).items():
            if not isinstance(payload, dict):
                continue
            item = component_near_threshold.setdefault(str(key), {})
            for bucket, value in payload.items():
                item[str(bucket)] = int(item.get(str(bucket), 0) or 0) + int(value or 0)
        for key, value in (diag.get("pair_agreement") or {}).items():
            pair_agreement[str(key)] = int(pair_agreement.get(str(key), 0) or 0) + int(value or 0)
        candidates = diag.get("weighted_consensus_candidates")
        if isinstance(candidates, dict):
            count = int(candidates.get("count", 0) or 0)
            if count > 0:
                avg = _float_or_default(candidates.get("avg"), 0.0)
                min_value = _float_or_default(candidates.get("min"), avg)
                max_value = _float_or_default(candidates.get("max"), avg)
                if int(weighted_candidates["count"]) <= 0:
                    weighted_candidates["min"] = min_value
                    weighted_candidates["max"] = max_value
                weighted_candidates["count"] += float(count)
                weighted_candidates["weighted_sum"] += avg * float(count)
                weighted_candidates["min"] = min(float(weighted_candidates["min"]), min_value)
                weighted_candidates["max"] = max(float(weighted_candidates["max"]), max_value)
                below = int(candidates.get("below_combo_threshold", 0) or 0)
                weighted_candidates["below_combo_threshold"] += float(below)
                weighted_candidates["gap_sum"] += _float_or_default(candidates.get("avg_gap"), 0.0) * float(below)
                weighted_candidates["max_gap"] = max(float(weighted_candidates["max_gap"]), _float_or_default(candidates.get("max_gap"), 0.0))
        for key, value in (diag.get("agree_count_histogram") or {}).items():
            agree_hist[str(key)] = int(agree_hist.get(str(key), 0) or 0) + int(value or 0)
        for key, value in (diag.get("drop_reasons") or {}).items():
            drop_reasons[str(key)] = int(drop_reasons.get(str(key), 0) or 0) + int(value or 0)
    component_pass_rate = {
        key: (float(component_pass.get(key, 0)) / float(value)) if int(value or 0) > 0 else 0.0
        for key, value in component_available.items()
    }
    frozen_score_stats: Dict[str, JsonDict] = {}
    for key, payload in component_score_stats.items():
        count = int(payload.get("count", 0.0) or 0.0)
        if count <= 0:
            continue
        frozen_score_stats[key] = {
            "count": count,
            "avg": float(payload.get("weighted_sum", 0.0) or 0.0) / float(count),
            "min": float(payload.get("min", 0.0) or 0.0),
            "max": float(payload.get("max", 0.0) or 0.0),
            "p50": float(payload.get("p50_weighted_sum", 0.0) or 0.0) / float(count),
        }
    frozen_timing_stats: Dict[str, JsonDict] = {}
    for key, payload in component_timing_stats.items():
        count = int(payload.get("count", 0.0) or 0.0)
        if count <= 0:
            continue
        frozen_timing_stats[key] = {
            "count": count,
            "avg": float(payload.get("weighted_sum", 0.0) or 0.0) / float(count),
            "max": float(payload.get("max", 0.0) or 0.0),
        }
    frozen_cache_stats: Dict[str, JsonDict] = {}
    for key, payload in component_cache_stats.items():
        hit = int(payload.get("hit", 0) or 0)
        miss = int(payload.get("miss", 0) or 0)
        total = hit + miss
        frozen_cache_stats[key] = {
            "hit": hit,
            "miss": miss,
            "hit_rate": (float(hit) / float(total)) if total > 0 else 0.0,
        }
    frozen_v8_stage_timing: Dict[str, JsonDict] = {}
    for key, payload in v8_stage_timing_stats.items():
        count = int(payload.get("count", 0.0) or 0.0)
        if count <= 0:
            continue
        frozen_v8_stage_timing[key] = {
            "count": count,
            "avg": float(payload.get("weighted_sum", 0.0) or 0.0) / float(count),
            "max": float(payload.get("max", 0.0) or 0.0),
        }
    frozen_v7_stage_timing: Dict[str, JsonDict] = {}
    for key, payload in v7_stage_timing_stats.items():
        count = int(payload.get("count", 0.0) or 0.0)
        if count <= 0:
            continue
        frozen_v7_stage_timing[key] = {
            "count": count,
            "avg": float(payload.get("weighted_sum", 0.0) or 0.0) / float(count),
            "max": float(payload.get("max", 0.0) or 0.0),
        }
    frozen_v5_score_breakdown: Dict[str, JsonDict] = {}
    for key, payload in v5_score_breakdown_stats.items():
        count = int(payload.get("count", 0.0) or 0.0)
        if count <= 0:
            continue
        frozen_v5_score_breakdown[key] = {
            "count": count,
            "avg": float(payload.get("weighted_sum", 0.0) or 0.0) / float(count),
            "min": float(payload.get("min", 0.0) or 0.0),
            "max": float(payload.get("max", 0.0) or 0.0),
        }
    return {
        "available": True,
        "diagnostic_count": int(total_count),
        "combo_consensus_count": len(combo_diags),
        "evaluated": evaluated,
        "component_available": component_available,
        "component_pass": component_pass,
        "component_pass_rate": component_pass_rate,
        "component_score_stats": frozen_score_stats,
        "component_timing_ms": frozen_timing_stats,
        "component_score_cache": frozen_cache_stats,
        "v8_stage_timing_ms": frozen_v8_stage_timing,
        "v7_stage_timing_ms": frozen_v7_stage_timing,
        "v5_score_breakdown": frozen_v5_score_breakdown,
        "v5_synergy_combo_counts": v5_synergy_combo_counts,
        "v5_risk_reason_counts": v5_risk_reason_counts,
        "v5_candidate_filter": {
            "total": int(v5_candidate_filter["total"]),
            "applicable": int(v5_candidate_filter["applicable"]),
            "filtered_out": int(v5_candidate_filter["filtered_out"]),
            "applicable_rate": (float(v5_candidate_filter["applicable"]) / float(v5_candidate_filter["total"])) if int(v5_candidate_filter["total"]) > 0 else 0.0,
            "reason_counts": dict(v5_candidate_filter["reason_counts"]) if isinstance(v5_candidate_filter["reason_counts"], dict) else {},
        },
        "component_near_threshold": component_near_threshold,
        "weight_context": weight_context,
        "pair_agreement": pair_agreement,
        "weighted_consensus_candidates": {
            "count": int(weighted_candidates["count"]),
            "avg": (float(weighted_candidates["weighted_sum"]) / float(weighted_candidates["count"])) if weighted_candidates["count"] > 0 else 0.0,
            "min": float(weighted_candidates["min"]),
            "max": float(weighted_candidates["max"]),
            "below_combo_threshold": int(weighted_candidates["below_combo_threshold"]),
            "avg_gap": (float(weighted_candidates["gap_sum"]) / float(weighted_candidates["below_combo_threshold"])) if weighted_candidates["below_combo_threshold"] > 0 else 0.0,
            "max_gap": float(weighted_candidates["max_gap"]),
        },
        "agree_count_histogram": agree_hist,
        "drop_reasons": drop_reasons,
    }


def _next_actions(*, strategy: str, failure_classes: Sequence[str], best: JsonDict | None) -> list[str]:
    classes = set(failure_classes)
    actions: list[str] = []
    if "runtime_timeout" in classes:
        actions.append("profile_evaluator_hot_path_and_cache_reused_factor_inputs")
    if "factor_score_distribution_diagnostic_required" in classes:
        actions.append("freeze_window_level_score_distribution_before_changing_any_threshold")
    if "v6_score_model_calibration_required" in classes:
        actions.append("calibrate_v6_score_model_against_point_in_time_runtime_distribution_without_lowering_gate")
    if "v6_no_near_threshold_samples" in classes:
        actions.append("review_v6_dimension_caps_synergy_and_risk_penalty_before_any_candidate_ranking")
    if "v6_dense_replay_review_required" in classes:
        actions.append("rerun_v6_short_cycle_probe_with_dense_replay_before_changing_runtime_defaults")
    if "v6_entry_gate_reconfirmation_samples_below_candidate_band" in classes:
        actions.append("review_v6_pullback_reconfirmed_samples_and_calibrate_quality_dimensions_without_lowering_gate")
    if "v6_pullback_secondary_confirmation_missing_technical_breakout" in classes:
        actions.append("require_pullback_reconfirmed_samples_to_show_technical_breakout_before_any_score_lift")
    if "v6_pullback_technical_confirmation_not_reflected_in_breakthrough_score" in classes:
        actions.append("map_pullback_platform_or_ma_reclaim_confirmation_into_breakthrough_diagnostics_before_score_lift")
    if "v6_threshold_passes_do_not_convert_to_successful_windows" in classes:
        actions.append("trace_v6_passed_threshold_samples_through_tradeability_exit_and_rolling_window_pipeline")
    if "v6_pit_data_availability_blocks_mandatory_filter_evidence" in classes:
        actions.append("separate_v6_missing_pit_money_sector_data_from_true_mandatory_filter_failures")
    if "v7_near_threshold_signal_generation_gap" in classes:
        actions.append("repair_v7_signal_generation_or_rebase_on_v8_filters_before_any_quality_floor_review")
    if "combo_component_consensus_diagnostic_required" in classes:
        actions.append("compare_v5_v8_v9_component_pass_rates_and_consensus_dropoff_by_window")
    if "missing_execution_constraint_evidence" in classes:
        actions.append("route_strategy_backtest_through_runtime_engine_with_tradeability_and_volume_constraints")
    if "legacy_return_sample_not_credible_without_constraints" in classes:
        actions.append("keep_legacy_returns_as_reference_only_until_runtime_replay_constraints_exist")
    if "weak_out_of_sample_win_rate" in classes:
        actions.append("move_strategy_to_observation_pool_until_oos_quality_improves")
    if "backtest_handler_missing_or_not_credible" in classes:
        actions.append("implement_real_runtime_backtest_handler_before_strategy_competition")
    if strategy == "stable" and best:
        actions.append("rebuild_stable_as_defensive_allocator_overlay_and_measure_portfolio_drawdown_reduction")
    if not actions:
        actions.append("eligible_for_candidate_ranking_but_still_requires_release_and_execution_chain_context")
    return actions


def _repair_experiment_plan(
    *,
    strategy: str,
    failure_classes: Sequence[str],
    window_diagnostics: JsonDict,
    best: JsonDict | None,
) -> JsonDict:
    classes = set(failure_classes)
    base: JsonDict = {
        "plan_version": "strategy_repair_experiment_plan.v1",
        "strategy": strategy,
        "eligible_for_quality_floor_tuning": False,
        "execution_contract": {
            "entrypoint": "tools/all_strategy_evidence_run.py",
            "required_artifacts": [
                "backtest_sweep_json",
                "strategy_backtest_diagnostics",
                "rejected_backtest_ledger_entry_when_failed",
                "stage_audit_json",
            ],
            "must_preserve_pool_status_until_passed": True,
        },
        "forbidden_shortcuts": [
            "lower_threshold_to_create_signal_density",
            "optimize_win_rate_before_successful_rolling_windows_exist",
            "ignore_tradeability_cost_or_volume_constraints",
        ],
    }
    if strategy == "v6":
        experiments = []
        if "v6_threshold_passes_do_not_convert_to_successful_windows" in classes:
            experiments.append(
                {
                    "type": "v6_signal_conversion_trace",
                    "action": "trace_passed_threshold_samples_through_tradeability_exit_and_rolling_window_pipeline",
                    "input_evidence": {
                        "passed_threshold": int(window_diagnostics.get("passed_threshold", 0) or 0),
                        "near_threshold": dict(window_diagnostics.get("near_threshold") or {}),
                        "max_score": _float_or_default(window_diagnostics.get("max_score"), 0.0),
                    },
                    "runtime_params": {
                        "score_threshold": int((best or {}).get("score_threshold", 75) or 75),
                        "replay_step": 10,
                        "max_evaluations": 240,
                        "strategy_arg": "v6",
                        "sweep_max_runs": 0,
                    },
                    "success_metric": "passed_threshold_samples_create_successful_rolling_test_windows_and_positive_signal_density",
                }
            )
        if "v6_entry_gate_reconfirmation_samples_below_candidate_band" in classes:
            experiments.append(
                {
                    "type": "v6_entry_gate_quality_calibration",
                    "action": "calibrate_pullback_reconfirmed_quality_dimensions_without_lowering_gate",
                    "success_metric": "pullback_reconfirmed_samples_enter_candidate_band_with_point_in_time_trace",
                }
            )
        if "v6_pit_data_availability_blocks_mandatory_filter_evidence" in classes:
            experiments.append(
                {
                    "type": "v6_pit_money_sector_data_contract_probe",
                    "action": "verify_money_flow_and_sector_performance_data_available_before_treating_mandatory_filter_failures_as_alpha_rejections",
                    "success_metric": "mandatory_filter_failures_split_into_pit_data_unavailable_vs_true_factor_failure",
                }
            )
        if not experiments:
            experiments.append(
                {
                    "type": "v6_score_distribution_probe",
                    "action": "freeze_score_distribution_and_mandatory_filter_dropoff_before_any_alpha_tuning",
                    "success_metric": "successful_rolling_windows>0 and signal_density>0",
                }
            )
        base.update(
            {
                "priority": "signal_generation_before_quality_floor",
                "experiments": experiments,
                "promotion_blocked_until": [
                    "successful_rolling_windows>0",
                    "signal_density>0",
                    "cost_slippage_volume_constraints_present",
                ],
            }
        )
        return base
    if strategy == "v7":
        decision = _v7_rebase_or_retire_decision(window_diagnostics)
        base.update(
            {
                "priority": "near_threshold_signal_generation_or_rebase_decision",
                "rebase_or_retire_decision": decision,
                "experiments": [
                    {
                        "type": "v7_near_threshold_conversion_probe",
                        "action": "inspect_within_10_gap_samples_and_rebase_signal_gate_on_v8_filters_or_retire",
                        "input_evidence": {
                            "near_threshold": dict(window_diagnostics.get("near_threshold") or {}),
                            "max_score": _float_or_default(window_diagnostics.get("max_score"), 0.0),
                            "threshold": _float_or_default(window_diagnostics.get("min_threshold"), 60.0),
                            "decision": decision,
                        },
                        "runtime_params": {
                            "score_threshold": int((best or {}).get("score_threshold", 60) or 60),
                            "replay_step": 20,
                            "max_evaluations": 240,
                            "strategy_arg": "v7",
                            "sweep_max_runs": 0,
                        },
                        "success_metric": "signals_above_60_generate_successful_rolling_windows_with_tradeability_constraints",
                    }
                ],
                "promotion_blocked_until": [
                    "successful_rolling_windows>0",
                    "signal_density>0_under_threshold_60",
                    "rebase_or_retire_decision_recorded",
                ],
            }
        )
        return base
    return {
        **base,
        "eligible_for_quality_floor_tuning": bool(
            best and "weak_out_of_sample_win_rate" in classes and "no_successful_rolling_test_window" not in classes
        ),
        "experiments": [],
    }


def _v7_rebase_or_retire_decision(window_diagnostics: JsonDict) -> JsonDict:
    near = window_diagnostics.get("near_threshold") if isinstance(window_diagnostics.get("near_threshold"), dict) else {}
    samples = window_diagnostics.get("top_near_threshold_samples")
    if not isinstance(samples, list):
        samples = []
    blocking_counts: dict[str, int] = {}
    confirmation_counts: list[int] = []
    for sample in samples:
        if not isinstance(sample, dict):
            continue
        conversion = sample.get("near_threshold_conversion") if isinstance(sample.get("near_threshold_conversion"), dict) else {}
        for reason in conversion.get("blocking_reasons") or []:
            key = str(reason or "")
            if key:
                blocking_counts[key] = int(blocking_counts.get(key, 0) or 0) + 1
        if "confirmation_count" in conversion:
            try:
                confirmation_counts.append(int(conversion.get("confirmation_count") or 0))
            except (TypeError, ValueError):
                pass
    max_confirmation = max(confirmation_counts) if confirmation_counts else 0
    within_2 = int(near.get("within_2", 0) or 0)
    within_10 = int(near.get("within_10", 0) or 0)
    if within_10 <= 0:
        action = "retire_standalone_v7_gate"
        reason = "no_near_threshold_population"
    elif blocking_counts.get("industry_heat_negative", 0) > 0:
        action = "rebase_on_v8_market_and_sector_filters_before_any_v7_gate_repair"
        reason = "near_threshold_samples_blocked_by_negative_industry_heat"
    elif max_confirmation < 4:
        action = "retire_or_rebase_standalone_v7_gate"
        reason = "near_threshold_samples_lack_broad_factor_confirmation"
    elif within_2 > 0:
        action = "run_v8_rebase_shadow_comparison_only"
        reason = "near_threshold_population_exists_but_requires_v8_filter_confirmation"
    else:
        action = "retire_standalone_v7_gate"
        reason = "near_threshold_population_too_sparse_for_standalone_repair"
    return {
        "decision_version": "v7_rebase_or_retire_decision.v1",
        "recommended_action": action,
        "reason": reason,
        "near_threshold": dict(near),
        "sample_count": len(samples),
        "blocking_reason_counts": blocking_counts,
        "max_confirmation_count": int(max_confirmation),
        "hard_boundary": "do_not_lower_v7_threshold_or_promote_without_v8_filter_shadow_evidence",
    }
