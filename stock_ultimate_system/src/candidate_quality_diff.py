from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.candidate_quality_baseline_registry import CandidateQualityBaselineRegistry
from src.candidate_quality_evaluation import REQUIRED_BUCKETS, REQUIRED_WINDOWS
from src.utils.project_paths import resolve_artifacts_path, resolve_experiments_path


CANDIDATE_QUALITY_DIFF_VERSION = "candidate_quality_diff.v1"
LONG_WINDOWS = ("60d", "120d")
FAILURE_SUMMARY_FIELDS = (
    "false_positive_cases",
    "missed_winner_cases",
    "rank_too_low_cases",
    "risk_gate_blocked_but_later_strong_cases",
    "evidence_insufficient_cases",
    "regime_mismatch_cases",
    "risk_control_failure_cases",
    "benchmark_underperformance_cases",
    "negative_absolute_return_cases",
    "source_risk_mismatch_cases",
    "weak_source_signal_cases",
    "weak_success_cases",
    "unclassified_failure_cases",
)
FAILURE_DETERIORATION_SEVERITY = {
    "risk_control_failure_cases": 100,
    "negative_absolute_return_cases": 95,
    "benchmark_underperformance_cases": 90,
    "false_positive_cases": 80,
    "source_risk_mismatch_cases": 75,
    "weak_source_signal_cases": 70,
    "rank_too_low_cases": 65,
    "missed_winner_cases": 65,
    "risk_gate_blocked_but_later_strong_cases": 60,
    "regime_mismatch_cases": 55,
    "evidence_insufficient_cases": 50,
    "weak_success_cases": 40,
    "unclassified_failure_cases": 30,
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _resolve_experiments_file(path: str | Path, *, exp_dir: Path) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    if len(candidate.parts) >= 2 and candidate.parts[0] == "data" and candidate.parts[1] == "experiments":
        return exp_dir.joinpath(*candidate.parts[2:])
    return resolve_experiments_path(candidate)


def _load_optional_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return _read_json(path)


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


def _load_failure_summary_from_summary(
    summary_payload: dict[str, Any] | None,
    *,
    summary_path: Path | None = None,
) -> dict[str, Any] | None:
    if isinstance(summary_payload, dict):
        embedded = summary_payload.get("failure_attribution_summary")
        if isinstance(embedded, dict):
            return embedded
    if summary_path is None:
        return None
    sibling = summary_path.with_name("candidate_quality_failure_attribution.json")
    return _load_optional_json(sibling)


def _normalize_failure_summary(payload: dict[str, Any] | None) -> dict[str, int | None] | None:
    if not isinstance(payload, dict):
        return None
    return {field: _safe_int(payload.get(field)) for field in FAILURE_SUMMARY_FIELDS}


def _load_previous_formal_baseline(
    *,
    previous_summary_path: str | Path | None,
    artifacts_dir: Path,
    exp_dir: Path,
) -> tuple[dict[str, Any] | None, Path | None]:
    if previous_summary_path is not None:
        resolved = _resolve_experiments_file(previous_summary_path, exp_dir=exp_dir)
        return _load_optional_json(resolved), resolved
    registry = CandidateQualityBaselineRegistry(
        baselines_dir=artifacts_dir / "candidate_quality_baselines"
    )
    snapshot = registry.get_current_snapshot()
    if not isinstance(snapshot, dict):
        return None, None
    summary_path = str(snapshot.get("source_summary_path") or "").strip()
    if not summary_path:
        return None, None
    resolved = Path(summary_path)
    return _load_optional_json(resolved), resolved


def _load_current_failure_summary(
    *,
    current_summary: dict[str, Any],
    current_summary_path: Path,
    exp_dir: Path,
    artifacts_dir: Path,
    failure_summary_path: str | Path | None,
) -> tuple[dict[str, int | None] | None, str | None]:
    if failure_summary_path is not None:
        resolved = _resolve_experiments_file(failure_summary_path, exp_dir=exp_dir)
        payload = _load_optional_json(resolved)
        return _normalize_failure_summary(payload), str(resolved) if payload is not None else None

    embedded = _load_failure_summary_from_summary(current_summary, summary_path=current_summary_path)
    if embedded is not None:
        sibling = current_summary_path.with_name("candidate_quality_failure_attribution.json")
        source_path = sibling if sibling.exists() else None
        return _normalize_failure_summary(embedded), str(source_path) if source_path is not None else None

    latest = _load_optional_json(exp_dir / "primary_result_failure_attribution_latest.json")
    if latest is not None:
        return _normalize_failure_summary(latest), str(exp_dir / "primary_result_failure_attribution_latest.json")

    summary = _load_optional_json(artifacts_dir / "primary_result_failure_attribution" / "summary.json")
    if summary is not None:
        return _normalize_failure_summary(summary), str(artifacts_dir / "primary_result_failure_attribution" / "summary.json")

    return None, None


def _window_bucket_metrics(summary: dict[str, Any]) -> dict[str, dict[str, dict[str, float | None]]]:
    payload = summary.get("bucket_window_metrics") or {}
    if not isinstance(payload, dict):
        payload = {}
    metrics: dict[str, dict[str, dict[str, float | None]]] = {}
    for window in REQUIRED_WINDOWS:
        raw_window = payload.get(window) or {}
        raw_window = raw_window if isinstance(raw_window, dict) else {}
        metrics[window] = {}
        for bucket in REQUIRED_BUCKETS:
            raw_bucket = raw_window.get(bucket) or {}
            raw_bucket = raw_bucket if isinstance(raw_bucket, dict) else {}
            metrics[window][bucket] = {
                "avg_return": _safe_float(raw_bucket.get("avg_return")),
                "avg_excess_return": _safe_float(raw_bucket.get("avg_excess_return")),
                "win_rate": _safe_float(raw_bucket.get("win_rate")),
            }
    return metrics


def _sample_density(summary: dict[str, Any]) -> dict[str, dict[str, int | str]]:
    payload = summary.get("multiwindow_sample_density") or {}
    if not isinstance(payload, dict):
        payload = {}
    density: dict[str, dict[str, int | str]] = {}
    for window in LONG_WINDOWS:
        raw = payload.get(window) or {}
        raw = raw if isinstance(raw, dict) else {}
        density[window] = {
            "sample_total": _safe_int(raw.get("sample_total")) or 0,
            "minimum_required": _safe_int(raw.get("minimum_required")) or 0,
            "status": str(raw.get("status") or "blocked"),
        }
    return density


def _compare_bucket_windows(
    current_summary: dict[str, Any],
    previous_summary: dict[str, Any],
) -> tuple[dict[str, dict[str, dict[str, float | None]]], list[str], dict[str, list[str]]]:
    current_metrics = _window_bucket_metrics(current_summary)
    previous_metrics = _window_bucket_metrics(previous_summary)
    deltas: dict[str, dict[str, dict[str, float | None]]] = {}
    blocking_reasons: list[str] = []
    bucket_advantage_windows: dict[str, list[str]] = {bucket: [] for bucket in REQUIRED_BUCKETS}
    for window in REQUIRED_WINDOWS:
        deltas[window] = {}
        for bucket in REQUIRED_BUCKETS:
            deltas[window][bucket] = {}
            current_bucket = current_metrics[window][bucket]
            previous_bucket = previous_metrics[window][bucket]
            for metric_name in ("avg_return", "avg_excess_return", "win_rate"):
                current_value = current_bucket.get(metric_name)
                previous_value = previous_bucket.get(metric_name)
                if current_value is None:
                    blocking_reasons.append(f"current_missing_{window}_{bucket}_{metric_name}")
                    delta = None
                elif previous_value is None:
                    blocking_reasons.append(f"previous_missing_{window}_{bucket}_{metric_name}")
                    delta = None
                else:
                    delta = current_value - previous_value
                deltas[window][bucket][metric_name] = delta
            avg_excess_delta = deltas[window][bucket]["avg_excess_return"]
            if isinstance(avg_excess_delta, float) and avg_excess_delta > 0:
                bucket_advantage_windows[bucket].append(window)
    return deltas, sorted(set(blocking_reasons)), bucket_advantage_windows


def _compare_failure_summaries(
    current_summary: dict[str, int | None] | None,
    previous_summary: dict[str, int | None] | None,
) -> tuple[dict[str, int | None], list[str], bool | None]:
    deltas: dict[str, int | None] = {}
    blocking_reasons: list[str] = []
    degraded = False
    comparable = False
    for field in FAILURE_SUMMARY_FIELDS:
        current_value = None if current_summary is None else current_summary.get(field)
        previous_value = None if previous_summary is None else previous_summary.get(field)
        if current_value is None:
            blocking_reasons.append(f"current_failure_summary_missing:{field}")
            deltas[field] = None
            continue
        if previous_value is None:
            blocking_reasons.append(f"previous_failure_summary_missing:{field}")
            deltas[field] = None
            continue
        comparable = True
        delta = current_value - previous_value
        deltas[field] = delta
        if delta > 0:
            degraded = True
    if not comparable:
        return deltas, sorted(set(blocking_reasons)), None
    return deltas, sorted(set(blocking_reasons)), not degraded


def _failure_deterioration_explanation(
    deltas: dict[str, int | None],
) -> dict[str, Any]:
    deteriorated: list[dict[str, Any]] = []
    improved: list[dict[str, Any]] = []
    unchanged_fields: list[str] = []
    for field in FAILURE_SUMMARY_FIELDS:
        delta = deltas.get(field)
        if not isinstance(delta, int):
            continue
        severity_rank = FAILURE_DETERIORATION_SEVERITY.get(field, 0)
        entry = {
            "field": field,
            "delta": delta,
            "severity_rank": severity_rank,
        }
        if delta > 0:
            deteriorated.append(entry)
        elif delta < 0:
            improved.append(entry)
        else:
            unchanged_fields.append(field)
    deteriorated.sort(key=lambda item: (-int(item["severity_rank"]), -int(item["delta"]), str(item["field"])))
    improved.sort(key=lambda item: (-int(item["severity_rank"]), int(item["delta"]), str(item["field"])))
    return {
        "has_deterioration": bool(deteriorated),
        "top_deteriorated_categories": deteriorated[:5],
        "top_improved_categories": improved[:5],
        "unchanged_field_total": len(unchanged_fields),
    }


def _remediation_actions(failure_deterioration: dict[str, Any]) -> list[str]:
    actions: list[str] = []
    for item in list(failure_deterioration.get("top_deteriorated_categories", []) or []):
        if not isinstance(item, dict):
            continue
        field = str(item.get("field") or "").strip()
        if field == "risk_control_failure_cases":
            actions.append("prioritize risk-control review items before ranking or timing adjustments")
        elif field == "negative_absolute_return_cases":
            actions.append("prioritize entry-timing review and stop-policy recalibration for deteriorated negative-return cases")
        elif field == "benchmark_underperformance_cases":
            actions.append("prioritize ranking-factor benchmark review before baseline promotion")
        elif field == "false_positive_cases":
            actions.append("tighten top-ranked candidate precision review for false-positive deterioration")
        elif field == "source_risk_mismatch_cases":
            actions.append("review audit-gate thresholds for high-risk source candidates before expanding coverage")
        elif field == "weak_source_signal_cases":
            actions.append("deprioritize weak-signal candidates until stronger evidence or regime confirmation is available")
        elif field == "rank_too_low_cases":
            actions.append("inspect ranking-order misses and raise review priority for missed high-conviction winners")
        elif field == "missed_winner_cases":
            actions.append("review missed-winner samples alongside top-bucket ranking changes")
        elif field == "risk_gate_blocked_but_later_strong_cases":
            actions.append("audit risk-gate blocks that later outperformed before changing gate strictness")
        elif field == "regime_mismatch_cases":
            actions.append("revisit regime segmentation before promoting candidate-selection changes")
        elif field == "evidence_insufficient_cases":
            actions.append("increase evidence completeness requirements before accepting candidate-quality improvements")
    deduped: list[str] = []
    seen: set[str] = set()
    for action in actions:
        if action in seen:
            continue
        seen.add(action)
        deduped.append(action)
    return deduped


def _priority_band_for_failure_field(field: str) -> str:
    severity_rank = FAILURE_DETERIORATION_SEVERITY.get(field, 0)
    if severity_rank >= 90:
        return "critical"
    if severity_rank >= 70:
        return "high"
    if severity_rank >= 50:
        return "medium"
    return "low"


def _iteration_schedule(failure_deterioration: dict[str, Any]) -> list[dict[str, Any]]:
    schedule: list[dict[str, Any]] = []
    for index, item in enumerate(list(failure_deterioration.get("top_deteriorated_categories", []) or []), start=1):
        if not isinstance(item, dict):
            continue
        field = str(item.get("field") or "").strip()
        if not field:
            continue
        action = _remediation_actions({"top_deteriorated_categories": [item]})
        schedule.append(
            {
                "sequence": index,
                "failure_field": field,
                "priority_band": _priority_band_for_failure_field(field),
                "delta": item.get("delta"),
                "recommended_action": action[0] if action else None,
            }
        )
    return schedule


def _compare_sample_density(
    current_summary: dict[str, Any],
    previous_summary: dict[str, Any] | None,
) -> tuple[dict[str, dict[str, int | str | None]], list[str]]:
    current_density = _sample_density(current_summary)
    previous_density = _sample_density(previous_summary or {})
    result: dict[str, dict[str, int | str | None]] = {}
    blocking_reasons: list[str] = []
    for window in LONG_WINDOWS:
        current_window = current_density[window]
        previous_window = previous_density[window]
        sample_total_delta = current_window["sample_total"] - previous_window["sample_total"]
        result[window] = {
            "current_sample_total": current_window["sample_total"],
            "previous_sample_total": previous_window["sample_total"],
            "sample_total_delta": sample_total_delta,
            "current_minimum_required": current_window["minimum_required"],
            "previous_minimum_required": previous_window["minimum_required"],
            "current_status": current_window["status"],
            "previous_status": previous_window["status"],
        }
        if str(current_window["status"]) != "passed":
            blocking_reasons.append(f"{window}_sample_density_insufficient")
    return result, sorted(set(blocking_reasons))


def build_candidate_quality_diff(
    *,
    current_summary_path: str | Path = "data/experiments/candidate_quality_summary.json",
    previous_summary_path: str | Path | None = None,
    failure_summary_path: str | Path | None = None,
    exp_dir: str | Path = "data/experiments",
    artifacts_dir: str | Path = "artifacts",
) -> dict[str, Any]:
    resolved_exp_dir = resolve_experiments_path(exp_dir)
    resolved_artifacts_dir = resolve_artifacts_path(artifacts_dir)
    resolved_current_summary_path = _resolve_experiments_file(current_summary_path, exp_dir=resolved_exp_dir)

    current_summary = _read_json(resolved_current_summary_path)
    previous_summary, resolved_previous_summary_path = _load_previous_formal_baseline(
        previous_summary_path=previous_summary_path,
        artifacts_dir=resolved_artifacts_dir,
        exp_dir=resolved_exp_dir,
    )
    current_failure_summary, current_failure_summary_path = _load_current_failure_summary(
        current_summary=current_summary,
        current_summary_path=resolved_current_summary_path,
        exp_dir=resolved_exp_dir,
        artifacts_dir=resolved_artifacts_dir,
        failure_summary_path=failure_summary_path,
    )
    previous_failure_payload = _load_failure_summary_from_summary(
        previous_summary,
        summary_path=resolved_previous_summary_path,
    )
    previous_failure_summary = _normalize_failure_summary(previous_failure_payload)

    generated_at = _utc_now_iso()
    diff_id = f"candidate-quality-diff-{generated_at[:19].replace(':', '').replace('-', '').replace('T', '-')}"
    blocking_reasons: list[str] = []
    if previous_summary is None:
        blocking_reasons.append("previous_formal_baseline_missing")

    bucket_window_deltas: dict[str, dict[str, dict[str, float | None]]] = {}
    bucket_advantage_windows = {bucket: [] for bucket in REQUIRED_BUCKETS}
    if previous_summary is not None:
        bucket_window_deltas, bucket_blockers, bucket_advantage_windows = _compare_bucket_windows(
            current_summary,
            previous_summary,
        )
        blocking_reasons.extend(bucket_blockers)

    failure_summary_deltas, failure_blockers, no_failure_degradation = _compare_failure_summaries(
        current_failure_summary,
        previous_failure_summary,
    )
    blocking_reasons.extend(failure_blockers)
    failure_deterioration = _failure_deterioration_explanation(failure_summary_deltas)
    sample_density_delta, density_blockers = _compare_sample_density(current_summary, previous_summary)
    blocking_reasons.extend(density_blockers)

    top_bucket_advantage = (
        bool(bucket_advantage_windows["top1"]) or bool(bucket_advantage_windows["top3"])
    )
    stable_advantage_windows = sorted(
        {
            window
            for window in REQUIRED_WINDOWS
            if any(window in bucket_advantage_windows[bucket] for bucket in ("top1", "top3", "top5", "top10"))
        }
    )
    improvement_gate = {
        "has_two_window_stable_advantage": len(stable_advantage_windows) >= 2,
        "has_top1_or_top3_excess_advantage": top_bucket_advantage,
        "no_new_failure_degradation": no_failure_degradation,
    }
    diff_status = "passed" if not blocking_reasons else "blocked"
    improvement_gate["pass_or_fail"] = (
        "passed"
        if diff_status == "passed"
        and improvement_gate["has_two_window_stable_advantage"]
        and improvement_gate["has_top1_or_top3_excess_advantage"]
        and improvement_gate["no_new_failure_degradation"] is True
        else ("blocked" if diff_status == "blocked" else "failed")
    )

    return {
        "diff_version": CANDIDATE_QUALITY_DIFF_VERSION,
        "diff_id": diff_id,
        "generated_at": generated_at,
        "status": "passed",
        "pass_or_fail": diff_status,
        "blocking_reasons": sorted(set(blocking_reasons)),
        "source_scope": str(current_summary.get("source_scope") or "stock").strip() or "stock",
        "current_evaluation_id": current_summary.get("evaluation_id"),
        "previous_evaluation_id": None if previous_summary is None else previous_summary.get("evaluation_id"),
        "current_summary_path": str(resolved_current_summary_path),
        "previous_summary_path": None if resolved_previous_summary_path is None else str(resolved_previous_summary_path),
        "current_failure_summary_path": current_failure_summary_path,
        "previous_failure_summary_path": (
            None
            if resolved_previous_summary_path is None
            else str(resolved_previous_summary_path.with_name("candidate_quality_failure_attribution.json"))
        ),
        "bucket_window_deltas": bucket_window_deltas,
        "bucket_advantage_windows": bucket_advantage_windows,
        "stable_advantage_windows": stable_advantage_windows,
        "failure_summary_deltas": failure_summary_deltas,
        "failure_deterioration": failure_deterioration,
        "recommended_remediation_actions": _remediation_actions(failure_deterioration),
        "iteration_schedule": _iteration_schedule(failure_deterioration),
        "sample_density_delta": sample_density_delta,
        "improvement_gate": improvement_gate,
    }


def write_candidate_quality_diff_artifact(
    payload: dict[str, Any],
    *,
    output_dir: str | Path = "data/experiments",
) -> str:
    resolved_output_dir = resolve_experiments_path(output_dir)
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    output_path = resolved_output_dir / "candidate_quality_diff.json"
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return str(output_path)
