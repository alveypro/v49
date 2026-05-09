from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.candidate_quality_baseline_registry import CandidateQualityBaselineRegistry
from src.utils.project_paths import resolve_artifacts_path, resolve_experiments_path


CANDIDATE_QUALITY_EVALUATION_VERSION = "candidate_quality_evaluation.v1"
BENCHMARK_SUITE_VERSION = "candidate_quality_benchmark_suite.v1"
REQUIRED_BUCKETS = ("top1", "top3", "top5", "top10")
REQUIRED_WINDOWS = ("20d", "60d", "120d")
REQUIRED_REGIMES = ("bull", "bear", "range", "high_vol")
BENCHMARK_NAMES = (
    "universe_equal_weight",
    "benchmark_index_proxy",
    "top1_only",
    "top3_equal_weight",
    "top5_equal_weight",
    "top10_equal_weight",
    "previous_formal_baseline",
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _safe_float(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _safe_int(value: object) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _load_optional_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return _read_json(path)


def _build_bucket_metrics(validation_payload: dict[str, Any]) -> dict[str, dict[str, float | None]]:
    summary = validation_payload.get("summary") or {}
    variants = validation_payload.get("variants") or {}
    if not isinstance(summary, dict):
        summary = {}
    if not isinstance(variants, dict):
        variants = {}

    def variant(name: str) -> dict[str, Any]:
        payload = variants.get(name) or {}
        return payload if isinstance(payload, dict) else {}

    return {
        "top1": {
            "avg_return": _safe_float(summary.get("avg_top1_return_5d")),
            "avg_excess_return": _safe_float(variant("top1").get("avg_excess_return_5d")),
            "win_rate": _safe_float(variant("top1").get("win_rate_5d")),
        },
        "top3": {
            "avg_return": _safe_float(variant("top3").get("avg_return_5d")),
            "avg_excess_return": _safe_float(variant("top3").get("avg_excess_return_5d")),
            "win_rate": _safe_float(variant("top3").get("win_rate_5d")),
        },
        "top5": {
            "avg_return": _safe_float(summary.get("avg_basket_return_5d")),
            "avg_excess_return": _safe_float(summary.get("avg_excess_return_5d")),
            "win_rate": _safe_float(summary.get("basket_win_rate_5d")),
        },
        "top10": {
            "avg_return": _safe_float(variant("top10").get("avg_return_5d")),
            "avg_excess_return": _safe_float(variant("top10").get("avg_excess_return_5d")),
            "win_rate": _safe_float(variant("top10").get("win_rate_5d")),
        },
    }


def _build_regime_breakdown(regime_payload: dict[str, Any] | None) -> dict[str, dict[str, float | None]]:
    if not isinstance(regime_payload, dict):
        return {name: {"environment_score": None} for name in REQUIRED_REGIMES}
    return {
        name: {
            "environment_score": _safe_float(((regime_payload.get(name) or {}) if isinstance(regime_payload.get(name), dict) else {}).get("avg_environment_score"))
        }
        for name in REQUIRED_REGIMES
    }


def _load_active_pointer(artifacts_dir: Path) -> dict[str, Any]:
    current_path = artifacts_dir / "current_result_pointer" / "current.json"
    if not current_path.exists():
        return {}
    return _read_json(current_path)


def _load_failure_summary(failure_payload: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(failure_payload, dict):
        return {
            "status": "missing",
            "primary_failure_category": None,
            "false_positive_cases": None,
            "missed_winner_cases": None,
            "rank_too_low_cases": None,
            "risk_gate_blocked_but_later_strong_cases": None,
            "evidence_insufficient_cases": None,
            "regime_mismatch_cases": None,
            "contributing_categories": [],
        }
    return {
        "status": str(failure_payload.get("status") or "unknown"),
        "primary_failure_category": failure_payload.get("primary_failure_category"),
        "false_positive_cases": _safe_int(failure_payload.get("false_positive_cases")),
        "missed_winner_cases": _safe_int(failure_payload.get("missed_winner_cases")),
        "rank_too_low_cases": _safe_int(failure_payload.get("rank_too_low_cases")),
        "risk_gate_blocked_but_later_strong_cases": _safe_int(failure_payload.get("risk_gate_blocked_but_later_strong_cases")),
        "evidence_insufficient_cases": _safe_int(failure_payload.get("evidence_insufficient_cases")),
        "regime_mismatch_cases": _safe_int(failure_payload.get("regime_mismatch_cases")),
        "contributing_categories": list(failure_payload.get("contributing_categories") or []),
    }


def _load_previous_formal_baseline(
    *,
    previous_summary_path: str | Path | None,
    artifacts_dir: Path,
) -> dict[str, Any] | None:
    if previous_summary_path is not None:
        return _load_optional_json(resolve_experiments_path(previous_summary_path))
    registry = CandidateQualityBaselineRegistry(
        baselines_dir=artifacts_dir / "candidate_quality_baselines"
    )
    snapshot = registry.get_current_snapshot()
    if not isinstance(snapshot, dict):
        return None
    summary_path = str(snapshot.get("source_summary_path") or "").strip()
    if not summary_path:
        return None
    return _load_optional_json(Path(summary_path))


def _load_multiwindow_payload(exp_dir: Path) -> dict[str, Any] | None:
    return _load_optional_json(exp_dir / "candidate_quality_multiwindow_latest.json")


def _build_bucket_window_metrics(
    validation_payload: dict[str, Any],
    multiwindow_payload: dict[str, Any] | None,
) -> dict[str, dict[str, dict[str, float | None]]]:
    bucket_metrics_20d = _build_bucket_metrics(validation_payload)
    windows_payload = {}
    if isinstance(multiwindow_payload, dict):
        candidate = multiwindow_payload.get("windows") or {}
        if isinstance(candidate, dict):
            windows_payload = candidate

    def _window_bucket_payload(window: str, bucket: str) -> dict[str, Any]:
        raw_window = windows_payload.get(window) or {}
        if not isinstance(raw_window, dict):
            return {}
        raw_bucket = raw_window.get(bucket) or {}
        return raw_bucket if isinstance(raw_bucket, dict) else {}

    bucket_window_metrics: dict[str, dict[str, dict[str, float | None]]] = {
        "20d": bucket_metrics_20d,
    }
    for window in ("60d", "120d"):
        bucket_window_metrics[window] = {}
        for bucket in REQUIRED_BUCKETS:
            payload = _window_bucket_payload(window, bucket)
            bucket_window_metrics[window][bucket] = {
                "avg_return": _safe_float(payload.get("avg_return")),
                "avg_excess_return": _safe_float(payload.get("avg_excess_return")),
                "win_rate": _safe_float(payload.get("win_rate")),
            }
    return bucket_window_metrics


def _blocking_reasons(
    *,
    bucket_window_metrics: dict[str, dict[str, dict[str, float | None]]],
    validation_summary: dict[str, Any],
    regime_breakdown: dict[str, dict[str, float | None]],
    benchmark_report: dict[str, Any] | None,
    previous_summary: dict[str, Any] | None,
    failure_summary: dict[str, Any],
) -> list[str]:
    reasons: list[str] = []
    rebalance_dates = _safe_int(validation_summary.get("rebalance_dates"))
    if not rebalance_dates:
        reasons.append("sample_count_missing_or_zero")
    for window in REQUIRED_WINDOWS:
        window_payload = bucket_window_metrics.get(window) or {}
        for bucket in REQUIRED_BUCKETS:
            metrics = window_payload.get(bucket) or {}
            if metrics.get("avg_return") is None:
                reasons.append(f"missing_{window}_{bucket}_avg_return")
            if metrics.get("avg_excess_return") is None:
                reasons.append(f"missing_{window}_{bucket}_avg_excess_return")
    if benchmark_report is None:
        reasons.append("benchmark_report_missing")
    if previous_summary is None:
        reasons.append("previous_formal_baseline_missing")
    density_payload = validation_summary.get("multiwindow_sample_density") or {}
    if isinstance(density_payload, dict):
        for window in ("60d", "120d"):
            density = density_payload.get(window) or {}
            if not isinstance(density, dict):
                density = {}
            if str(density.get("status") or "blocked") != "passed":
                reasons.append(f"{window}_sample_density_insufficient")
    for regime in REQUIRED_REGIMES:
        if (regime_breakdown.get(regime) or {}).get("environment_score") is None:
            reasons.append(f"regime_breakdown_missing:{regime}")
    if failure_summary.get("status") == "missing":
        reasons.append("failure_attribution_missing")
    return reasons


def build_candidate_quality_evaluation(
    *,
    exp_dir: str | Path = "data/experiments",
    artifacts_dir: str | Path = "artifacts",
    benchmark_report_path: str | Path | None = None,
    failure_attribution_path: str | Path | None = None,
    previous_summary_path: str | Path | None = None,
    benchmark_suite_version: str = BENCHMARK_SUITE_VERSION,
) -> dict[str, Any]:
    resolved_exp_dir = resolve_experiments_path(exp_dir)
    resolved_artifacts_dir = resolve_artifacts_path(artifacts_dir)

    validation_payload = _read_json(resolved_exp_dir / "candidates_basket_validation_latest.json")
    validation_summary = validation_payload.get("summary") or {}
    if not isinstance(validation_summary, dict):
        raise ValueError("candidate basket validation summary missing")

    regime_payload = _load_optional_json(resolved_exp_dir / "grid_backtest_regime_profiles_latest.json")
    benchmark_report = (
        _load_optional_json(resolve_experiments_path(benchmark_report_path))
        if benchmark_report_path is not None
        else _load_optional_json(resolved_exp_dir / "stock_primary_result_benchmark_report.json")
    )
    failure_payload = (
        _load_optional_json(resolve_experiments_path(failure_attribution_path))
        if failure_attribution_path is not None
        else _load_optional_json(resolved_exp_dir / "primary_result_failure_attribution_latest.json")
        or _load_optional_json(resolved_artifacts_dir / "primary_result_failure_attribution" / "summary.json")
    )
    previous_summary = _load_previous_formal_baseline(
        previous_summary_path=previous_summary_path,
        artifacts_dir=resolved_artifacts_dir,
    )
    multiwindow_payload = _load_multiwindow_payload(resolved_exp_dir)

    pointer = _load_active_pointer(resolved_artifacts_dir)
    bucket_metrics = _build_bucket_metrics(validation_payload)
    bucket_window_metrics = _build_bucket_window_metrics(validation_payload, multiwindow_payload)
    regime_breakdown = _build_regime_breakdown(regime_payload)
    failure_summary = _load_failure_summary(failure_payload)
    generated_at = _utc_now_iso()
    evaluation_id = f"candidate-quality-{generated_at[:19].replace(':', '').replace('-', '').replace('T', '-')}"
    multiwindow_sample_density = {}
    if isinstance(multiwindow_payload, dict):
        density_payload = multiwindow_payload.get("sample_density") or {}
        if isinstance(density_payload, dict):
            multiwindow_sample_density = density_payload
    validation_summary_for_blocking = dict(validation_summary)
    validation_summary_for_blocking["multiwindow_sample_density"] = multiwindow_sample_density

    blocking_reasons = _blocking_reasons(
        bucket_window_metrics=bucket_window_metrics,
        validation_summary=validation_summary_for_blocking,
        regime_breakdown=regime_breakdown,
        benchmark_report=benchmark_report,
        previous_summary=previous_summary,
        failure_summary=failure_summary,
    )
    pass_or_fail = "passed" if not blocking_reasons else "blocked"
    sample_count = _safe_int(validation_summary.get("rebalance_dates")) or 0
    source_scope = str(pointer.get("source_scope") or "stock").strip() or "stock"

    benchmark_rows: list[dict[str, object]] = []
    for window, buckets in bucket_window_metrics.items():
        for bucket, metrics in buckets.items():
            benchmark_name = {
                "top1": "top1_only",
                "top3": "top3_equal_weight",
                "top5": "top5_equal_weight",
                "top10": "top10_equal_weight",
            }[bucket]
            benchmark_rows.append(
                {
                    "bucket": bucket,
                    "window": window,
                    "benchmark_name": benchmark_name,
                    "avg_return": metrics.get("avg_return"),
                    "avg_excess_return": metrics.get("avg_excess_return"),
                    "win_rate": metrics.get("win_rate"),
                    "max_drawdown": None,
                    "signal_coverage": None,
                    "ranking_consistency_score": _safe_float((validation_payload.get("ranking_consistency") or {}).get("ranking_consistency_score")),
                }
            )
    for window in REQUIRED_WINDOWS:
        for benchmark_name in ("universe_equal_weight", "benchmark_index_proxy", "previous_formal_baseline"):
            benchmark_rows.append(
                {
                    "bucket": "reference",
                    "window": window,
                    "benchmark_name": benchmark_name,
                    "avg_return": None,
                    "avg_excess_return": None,
                    "win_rate": None,
                    "max_drawdown": None,
                    "signal_coverage": None,
                    "ranking_consistency_score": None,
                }
            )

    return {
        "evaluation_version": CANDIDATE_QUALITY_EVALUATION_VERSION,
        "evaluation_id": evaluation_id,
        "generated_at": generated_at,
        "status": "passed",
        "pass_or_fail": pass_or_fail,
        "blocking_reasons": blocking_reasons,
        "run_id": pointer.get("run_id"),
        "result_id": pointer.get("result_id"),
        "source_scope": source_scope,
        "evaluation_window": list(REQUIRED_WINDOWS),
        "benchmark_suite_version": benchmark_suite_version,
        "benchmark_names": list(BENCHMARK_NAMES),
        "sample_count": sample_count,
        "sample_windows": list(REQUIRED_WINDOWS),
        "multiwindow_sample_density": multiwindow_sample_density,
        "bucket_metrics": bucket_metrics,
        "bucket_window_metrics": bucket_window_metrics,
        "regime_breakdown": regime_breakdown,
        "failure_attribution_summary": failure_summary,
        "benchmark_report_summary": benchmark_report,
        "previous_formal_baseline_summary": previous_summary,
        "benchmark_table_rows": benchmark_rows,
    }


def write_candidate_quality_evaluation_artifacts(
    evaluation_payload: dict[str, Any],
    *,
    output_dir: str | Path = "data/experiments",
) -> dict[str, str]:
    resolved_output_dir = resolve_experiments_path(output_dir)
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    summary_path = resolved_output_dir / "candidate_quality_summary.json"
    benchmark_table_path = resolved_output_dir / "candidate_quality_benchmark_table.csv"
    failure_path = resolved_output_dir / "candidate_quality_failure_attribution.json"

    summary_payload = {
        key: value
        for key, value in evaluation_payload.items()
        if key not in {"benchmark_table_rows"}
    }
    summary_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    with benchmark_table_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=(
                "bucket",
                "window",
                "benchmark_name",
                "avg_return",
                "avg_excess_return",
                "win_rate",
                "max_drawdown",
                "signal_coverage",
                "ranking_consistency_score",
            ),
        )
        writer.writeheader()
        writer.writerows(evaluation_payload.get("benchmark_table_rows") or [])

    failure_path.write_text(
        json.dumps(evaluation_payload.get("failure_attribution_summary") or {}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return {
        "candidate_quality_summary": str(summary_path),
        "candidate_quality_benchmark_table": str(benchmark_table_path),
        "candidate_quality_failure_attribution": str(failure_path),
    }
