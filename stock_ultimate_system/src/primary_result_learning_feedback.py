from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from src.artifact_registry import sha256_file
from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_LEARNING_FEEDBACK_VERSION = "primary_result_learning_feedback.v1"
SUPPORTED_ATTRIBUTION_VERSION = "primary_result_failure_attribution.v1"
REVIEW_PRIORITIES = ("critical", "high", "medium", "low", "none")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _categories(attribution_payload: dict[str, object]) -> set[str]:
    categories = attribution_payload.get("contributing_categories")
    if not isinstance(categories, list):
        return set()
    return {
        _normalize_text(item.get("category"))
        for item in categories
        if isinstance(item, dict) and _normalize_text(item.get("category"))
    }


def _change(
    *,
    change_id: str,
    affected_module: str,
    recommendation: str,
    severity: str,
    requires_baseline_revalidation: bool,
    evidence_category: str,
) -> dict[str, object]:
    return {
        "change_id": change_id,
        "affected_module": affected_module,
        "recommendation": recommendation,
        "severity": severity,
        "requires_baseline_revalidation": requires_baseline_revalidation,
        "evidence_category": evidence_category,
        "do_not_auto_apply": True,
    }


def _recommended_changes(attribution_payload: dict[str, object]) -> list[dict[str, object]]:
    categories = _categories(attribution_payload)
    changes: list[dict[str, object]] = []
    if "data_quality_failure" in categories:
        changes.append(
            _change(
                change_id="repair_observation_data_quality",
                affected_module="observation_metrics",
                recommendation="repair source price history validation before using this sample for learning",
                severity="critical",
                requires_baseline_revalidation=False,
                evidence_category="data_quality_failure",
            )
        )
    if "risk_control_failure" in categories:
        changes.append(
            _change(
                change_id="tighten_drawdown_controls",
                affected_module="risk_control",
                recommendation="review drawdown floor, stop policy, and position sizing for similar primary results",
                severity="high",
                requires_baseline_revalidation=True,
                evidence_category="risk_control_failure",
            )
        )
    if "benchmark_underperformance" in categories:
        changes.append(
            _change(
                change_id="review_selection_factors",
                affected_module="candidate_selection",
                recommendation="review ranking factors because the result underperformed its benchmark",
                severity="high",
                requires_baseline_revalidation=True,
                evidence_category="benchmark_underperformance",
            )
        )
    if "negative_absolute_return" in categories:
        changes.append(
            _change(
                change_id="review_entry_timing",
                affected_module="execution_timing",
                recommendation="review entry timing and stop policy for negative absolute return cases",
                severity="high",
                requires_baseline_revalidation=True,
                evidence_category="negative_absolute_return",
            )
        )
    if "market_drag" in categories:
        changes.append(
            _change(
                change_id="add_market_regime_guard",
                affected_module="market_regime_filter",
                recommendation="separate market-regime drag from stock-specific selection failures before changing ranking logic",
                severity="medium",
                requires_baseline_revalidation=True,
                evidence_category="market_drag",
            )
        )
    if "source_risk_mismatch" in categories:
        changes.append(
            _change(
                change_id="raise_high_risk_review_bar",
                affected_module="audit_gate",
                recommendation="require manual review or lower allocation for high-risk source candidates",
                severity="medium",
                requires_baseline_revalidation=True,
                evidence_category="source_risk_mismatch",
            )
        )
    if "weak_source_signal" in categories:
        changes.append(
            _change(
                change_id="deprioritize_weak_signal",
                affected_module="candidate_selection",
                recommendation="deprioritize low or none signal candidates until supported by stronger evidence",
                severity="medium",
                requires_baseline_revalidation=True,
                evidence_category="weak_source_signal",
            )
        )
    if "weak_success" in categories:
        changes.append(
            _change(
                change_id="exclude_weak_success_from_champion_examples",
                affected_module="learning_dataset",
                recommendation="exclude weak success cases from high-confidence learning examples until reviewed",
                severity="medium",
                requires_baseline_revalidation=False,
                evidence_category="weak_success",
            )
        )
    if "unclassified_failure" in categories:
        changes.append(
            _change(
                change_id="manual_failure_review",
                affected_module="review_workflow",
                recommendation="route unclassified failure to manual review before changing strategy logic",
                severity="medium",
                requires_baseline_revalidation=False,
                evidence_category="unclassified_failure",
            )
        )
    return changes


def _review_priority(attribution_payload: dict[str, object], changes: list[dict[str, object]]) -> tuple[str, list[str]]:
    categories = _categories(attribution_payload)
    reasons: list[str] = []
    if "data_quality_failure" in categories:
        reasons.append("data_quality_failure_requires_dataset_repair")
    if "risk_control_failure" in categories:
        reasons.append("risk_control_failure_requires_risk_model_review")
    if "benchmark_underperformance" in categories:
        reasons.append("benchmark_underperformance_requires_ranking_review")
    if "negative_absolute_return" in categories:
        reasons.append("negative_absolute_return_requires_timing_review")
    if "market_drag" in categories:
        reasons.append("market_drag_requires_regime_review")
    if "source_risk_mismatch" in categories:
        reasons.append("source_risk_mismatch_requires_gate_review")
    if "weak_source_signal" in categories:
        reasons.append("weak_source_signal_requires_selection_review")
    if "weak_success" in categories:
        reasons.append("weak_success_requires_dataset_curation_review")
    if "unclassified_failure" in categories:
        reasons.append("unclassified_failure_requires_manual_review")

    if any(change.get("severity") == "critical" for change in changes):
        return "critical", reasons or ["critical_change_present"]
    if {"risk_control_failure", "benchmark_underperformance", "negative_absolute_return"} & categories:
        return "high", reasons or ["high_severity_failure_category_present"]
    if any(change.get("severity") == "high" for change in changes):
        return "high", reasons or ["high_change_present"]
    if any(change.get("severity") == "medium" for change in changes):
        return "medium", reasons or ["medium_change_present"]
    if changes:
        return "low", reasons or ["governed_changes_present"]
    return "none", reasons


def build_primary_result_learning_feedback(
    attribution_payload: dict[str, object],
    *,
    source_attribution_path: str | Path | None = None,
    generated_at: str | None = None,
) -> dict[str, object]:
    if attribution_payload.get("attribution_version") != SUPPORTED_ATTRIBUTION_VERSION:
        raise ValueError("primary result failure attribution version is invalid")
    if attribution_payload.get("status") != "passed":
        raise ValueError("primary result failure attribution must be passed")

    attribution_required = bool(attribution_payload.get("attribution_required"))
    changes = _recommended_changes(attribution_payload) if attribution_required else []
    requires_baseline_revalidation = any(bool(change.get("requires_baseline_revalidation")) for change in changes)
    max_severity = "none"
    for severity in ("critical", "high", "medium", "low"):
        if any(change.get("severity") == severity for change in changes):
            max_severity = severity
            break
    review_priority, priority_reasons = _review_priority(attribution_payload, changes)

    source_path = resolve_project_path(source_attribution_path) if source_attribution_path is not None else None
    return {
        "feedback_version": PRIMARY_RESULT_LEARNING_FEEDBACK_VERSION,
        "generated_at": generated_at or _utc_now_iso(),
        "status": "passed",
        "result_id": attribution_payload.get("result_id"),
        "ts_code": attribution_payload.get("ts_code"),
        "stock_name": attribution_payload.get("stock_name"),
        "outcome": attribution_payload.get("outcome"),
        "attribution_required": attribution_required,
        "primary_failure_category": attribution_payload.get("primary_failure_category"),
        "recommended_changes": changes,
        "change_total": len(changes),
        "max_severity": max_severity,
        "review_priority": review_priority,
        "priority_reasons": priority_reasons,
        "requires_baseline_revalidation": requires_baseline_revalidation,
        "do_not_auto_apply": True,
        "governance_note": (
            "learning feedback is an input for review and benchmark revalidation; it must not mutate strategy rules automatically"
        ),
        "source_attribution_path": str(source_path) if source_path is not None else None,
        "source_attribution_hash": sha256_file(source_path) if source_path is not None else None,
    }


def build_primary_result_learning_feedback_from_path(
    *,
    attribution_path: str | Path,
) -> dict[str, object]:
    resolved_attribution_path = resolve_project_path(attribution_path)
    if not resolved_attribution_path.exists():
        raise FileNotFoundError(f"primary result failure attribution artifact missing: {resolved_attribution_path}")
    return build_primary_result_learning_feedback(
        _read_json(resolved_attribution_path),
        source_attribution_path=resolved_attribution_path,
    )
