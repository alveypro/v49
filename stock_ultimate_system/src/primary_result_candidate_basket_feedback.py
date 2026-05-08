from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.artifact_registry import sha256_file
from src.artifact_source_guard import assert_path_is_not_temp_source, should_enforce_production_source_guard
from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_CANDIDATE_BASKET_FEEDBACK_VERSION = "primary_result_candidate_basket_feedback.v1"
TERMINAL_CANDIDATE_BASKET_OBSERVATION_STATUSES = {"completed", "failed"}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _parse_iso_date(value: object) -> datetime | None:
    text = _normalize_text(value)
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    if len(normalized) == 10:
        normalized = normalized + "T00:00:00+00:00"
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _window_horizon_label(window_started_at: object, window_ended_at: object) -> str:
    started = _parse_iso_date(window_started_at)
    ended = _parse_iso_date(window_ended_at)
    if started is None or ended is None:
        return "unknown"
    days = max((ended.date() - started.date()).days, 0)
    if days <= 7:
        return "5D"
    if days <= 14:
        return "10D"
    return f"{days}D"


def _change(
    *,
    change_id: str,
    affected_module: str,
    recommendation: str,
    severity: str,
) -> dict[str, Any]:
    return {
        "change_id": change_id,
        "affected_module": affected_module,
        "recommendation": recommendation,
        "severity": severity,
        "do_not_auto_apply": True,
    }


def build_primary_result_candidate_basket_feedback(
    *,
    observation_path: str | Path,
    performance_summary_path: str | Path | None = None,
    output_path: str | Path | None = None,
    min_success_return: float = 0.0,
    min_excess_return: float = 0.0,
    max_drawdown_floor: float = -0.08,
    enforce_production_source_guard: bool | None = None,
) -> tuple[int, dict[str, Any]]:
    enforce_guard = (
        should_enforce_production_source_guard(output_path)
        if enforce_production_source_guard is None
        else bool(enforce_production_source_guard)
    )
    resolved_observation_path = resolve_project_path(observation_path)
    if enforce_guard:
        assert_path_is_not_temp_source(resolved_observation_path, field_name="source_observation_path")
    observation = _read_json(resolved_observation_path)
    if observation.get("observation_version") != "primary_result_candidate_basket_observation.v1":
        raise ValueError("candidate basket observation version is invalid")

    observation_status = _normalize_text(observation.get("status")).lower()
    if observation_status not in TERMINAL_CANDIDATE_BASKET_OBSERVATION_STATUSES:
        raise ValueError("candidate basket feedback requires completed or failed observation")

    performance_summary: dict[str, Any] = {}
    resolved_performance_summary_path: Path | None = None
    if performance_summary_path:
        resolved_performance_summary_path = resolve_project_path(performance_summary_path)
        if enforce_guard:
            assert_path_is_not_temp_source(
                resolved_performance_summary_path,
                field_name="source_performance_summary_path",
            )
        if resolved_performance_summary_path.exists():
            performance_summary = _read_json(resolved_performance_summary_path)

    metrics = dict(observation.get("metrics", {}) or {})
    basket_return = float(metrics.get("basket_return", 0.0) or 0.0)
    benchmark_return = float(metrics.get("benchmark_return", 0.0) or 0.0)
    excess_return = float(metrics.get("excess_return", 0.0) or 0.0)
    max_drawdown = float(metrics.get("max_drawdown", 0.0) or 0.0)
    completion_criteria = dict(observation.get("completion_criteria", {}) or {})
    criteria_passed = bool(completion_criteria.get("passed"))

    change_total_before = 0
    recommended_changes: list[dict[str, Any]] = []
    if excess_return < min_excess_return:
        recommended_changes.append(
            _change(
                change_id="review_selection_factors",
                affected_module="candidate_selection",
                recommendation="review ranking factors and candidate score weighting against recent underperformance",
                severity="high",
            )
        )
    if max_drawdown < max_drawdown_floor:
        recommended_changes.append(
            _change(
                change_id="tighten_risk_overlay",
                affected_module="basket_construction",
                recommendation="tighten basket risk overlay and single-name concentration controls after drawdown breach",
                severity="high",
            )
        )
    summary_avg_excess = float(performance_summary.get("average_excess_return", 0.0) or 0.0)
    summary_entry_total = int(performance_summary.get("entry_total", 0) or 0)
    if summary_entry_total >= 3 and summary_avg_excess < 0:
        recommended_changes.append(
            _change(
                change_id="reduce_position_concentration",
                affected_module="portfolio_risk",
                recommendation="reduce concentrated basket weights until rolling excess return recovers above zero",
                severity="medium",
            )
        )
    if basket_return < min_success_return and not recommended_changes:
        recommended_changes.append(
            _change(
                change_id="review_entry_timing",
                affected_module="execution_timing",
                recommendation="review candidate entry timing because basket closed below the minimum success return",
                severity="medium",
            )
        )

    change_total_before = len(recommended_changes)
    attribution_required = change_total_before > 0
    strong_success = (
        observation_status == "completed"
        and criteria_passed
        and excess_return >= max(min_excess_return, 0.02)
        and max_drawdown >= max_drawdown_floor + 0.03
    )
    if strong_success:
        feedback_level = "reinforce"
        summary_note = "recent basket observation is strong enough to reinforce the current selection profile"
    elif attribution_required and (excess_return < min_excess_return or max_drawdown < max_drawdown_floor):
        feedback_level = "tighten"
        summary_note = "recent basket observation requires tighter selection and basket risk controls"
    elif attribution_required:
        feedback_level = "review"
        summary_note = "recent basket observation requires review before trusting the current candidate profile"
    else:
        feedback_level = "hold"
        summary_note = "recent basket observation does not yet justify a governed strategy change"

    window = dict(observation.get("observation_window", {}) or {})
    payload = {
        "feedback_version": PRIMARY_RESULT_CANDIDATE_BASKET_FEEDBACK_VERSION,
        "generated_at": _utc_now_iso(),
        "status": "completed",
        "basket_id": _normalize_text(observation.get("basket_id")),
        "observation_status": observation_status,
        "outcome": "success" if observation_status == "completed" and criteria_passed else "failed",
        "window_started_at": _normalize_text(window.get("started_at")),
        "window_ended_at": _normalize_text(window.get("ended_at")),
        "window_label": _window_horizon_label(window.get("started_at"), window.get("ended_at")),
        "basket_return": basket_return,
        "benchmark_return": benchmark_return,
        "excess_return": excess_return,
        "max_drawdown": max_drawdown,
        "feedback_level": feedback_level,
        "summary_note": summary_note,
        "attribution_required": attribution_required,
        "change_total": len(recommended_changes),
        "requires_manual_review": attribution_required,
        "do_not_auto_apply": True,
        "recommended_changes": recommended_changes,
        "performance_context": {
            "entry_total": summary_entry_total,
            "success_rate": performance_summary.get("success_rate"),
            "average_excess_return": performance_summary.get("average_excess_return"),
            "worst_max_drawdown": performance_summary.get("worst_max_drawdown"),
        },
        "source_observation_path": str(resolved_observation_path),
        "source_observation_hash": sha256_file(resolved_observation_path),
        "source_performance_summary_path": str(resolved_performance_summary_path) if resolved_performance_summary_path else None,
        "source_performance_summary_hash": (
            sha256_file(resolved_performance_summary_path)
            if resolved_performance_summary_path is not None and resolved_performance_summary_path.exists()
            else None
        ),
        "production_boundary": (
            "candidate basket feedback only turns closed basket observations into governed review signals; "
            "it never mutates ranking rules, portfolio caps, or strategy profiles automatically"
        ),
    }
    if output_path:
        _write_json(resolve_project_path(output_path), payload)
    return 0, payload
