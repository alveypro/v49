from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from typing import Any, Dict


JsonDict = Dict[str, Any]


def build_ensemble_risk_off_alpha_repair_review(
    *,
    monitor_artifact_path: str,
    throttle_attribution_artifact_path: str,
    failure_attribution_artifact_path: str = "",
    output_dir: str = "",
    operator_name: str = "",
) -> JsonDict:
    monitor = _load_json(monitor_artifact_path)
    attribution = _load_json(throttle_attribution_artifact_path)
    failure = _load_json(failure_attribution_artifact_path)
    summary = attribution.get("summary") if isinstance(attribution.get("summary"), dict) else {}
    regimes = attribution.get("regime_attribution") if isinstance(attribution.get("regime_attribution"), dict) else {}
    blocking: list[str] = []
    warnings: list[str] = []
    if monitor.get("observation_monitor_passed") is not True:
        blocking.append("throttled_observation_monitor_not_passed")
    if monitor.get("formal_candidate_allowed") is True or monitor.get("formal_ranking_allowed") is True:
        blocking.append("monitor_attempted_formal_eligibility")
    if attribution.get("formal_candidate_allowed") is True or attribution.get("formal_ranking_allowed") is True:
        blocking.append("attribution_attempted_formal_eligibility")

    alpha_excess = _f(summary.get("unthrottled_after_cost_excess_return"))
    alpha_hit = _f(summary.get("unthrottled_hit_rate"))
    alpha_drawdown = _f(summary.get("unthrottled_max_drawdown"))
    alpha_turnover = _f(summary.get("unthrottled_turnover"))
    if alpha_excess is None or alpha_excess <= 0.0:
        blocking.append(f"unthrottled_alpha_excess_not_positive:{alpha_excess}")
    if alpha_hit is None or alpha_hit < 0.60:
        blocking.append(f"unthrottled_alpha_hit_rate_below_floor:{alpha_hit}/0.6")
    if alpha_drawdown is None or alpha_drawdown < -15.0:
        blocking.append(f"unthrottled_alpha_drawdown_below_floor:{alpha_drawdown}/-15.0")
    if alpha_turnover is None or alpha_turnover > 0.75:
        blocking.append(f"unthrottled_alpha_turnover_above_cap:{alpha_turnover}/0.75")
    for regime in ("risk_off", "neutral", "risk_on"):
        item = regimes.get(regime) if isinstance(regimes.get(regime), dict) else {}
        value = _f(item.get("unthrottled_avg_after_cost_excess_return"))
        hit = _f(item.get("unthrottled_hit_rate"))
        if value is None:
            blocking.append(f"missing_unthrottled_regime_attribution:{regime}")
        elif value <= 0.0:
            blocking.append(f"unthrottled_regime_excess_not_positive:{regime}:{value}")
        if hit is None or hit < 0.50:
            blocking.append(f"unthrottled_regime_hit_rate_below_floor:{regime}:{hit}/0.5")
    throttle_delta = _f(summary.get("allocator_throttle_excess_delta"))
    if throttle_delta is not None and throttle_delta > 0.0:
        warnings.append(f"positive_result_remains_allocator_throttle_supported:{throttle_delta}")

    payload: JsonDict = {
        "artifact_version": "ensemble_risk_off_alpha_repair_review.v1",
        "created_at": _now_text(),
        "operator_name": str(operator_name or ""),
        "strategy": "ensemble_core",
        "candidate": str(monitor.get("candidate") or attribution.get("candidate") or "hard_event_alpha_candidate"),
        "current_pool": "observation",
        "status": "risk_off_alpha_repair_passed" if not blocking else "risk_off_alpha_repair_blocked",
        "risk_off_alpha_repair_passed": not blocking,
        "observation_monitor_watch_allowed": not blocking,
        "formal_candidate_allowed": False,
        "formal_ranking_allowed": False,
        "production_candidate_allowed": False,
        "source_artifacts": {
            "monitor": str(monitor_artifact_path or ""),
            "throttle_attribution": str(throttle_attribution_artifact_path or ""),
            "failure_attribution": str(failure_attribution_artifact_path or ""),
        },
        "failure_attribution_summary": _failure_summary(failure),
        "unthrottled_alpha_summary": {
            "after_cost_excess_return": alpha_excess,
            "hit_rate": alpha_hit,
            "max_drawdown": alpha_drawdown,
            "turnover": alpha_turnover,
            "regime_attribution": regimes,
        },
        "blocking_reasons": sorted(set(blocking)),
        "warning_reasons": sorted(set(warnings)),
        "required_next_action": "redesign_alpha_or_risk_off_veto_before_observation_watch" if blocking else "continue_observation_watch_without_formal_review",
        "hard_boundaries": [
            "risk_off_repair_review_does_not_promote_to_formal",
            "do_not_allow_observation_watch_when_unthrottled_alpha_proxy_fails",
            "do_not_package_allocator_throttle_as_standalone_alpha",
            "formal_candidate_requires_separate_future_review",
        ],
    }
    if output_dir:
        artifacts = _write_artifacts(Path(output_dir), payload)
        payload["artifacts"] = artifacts
        _write_json(Path(artifacts["json"]), payload)
    return payload


def _failure_summary(payload: JsonDict) -> JsonDict:
    attr = payload.get("attribution") if isinstance(payload.get("attribution"), dict) else payload
    return {
        "failed_windows": list(attr.get("failed_windows") or []),
        "successful_windows": list(attr.get("successful_windows") or []),
        "focus_failed_window": str(attr.get("focus_failed_window") or ""),
        "inferred_failure_drivers": list(attr.get("inferred_failure_drivers") or []),
    }


def _f(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return round(float(value), 6)
    except (TypeError, ValueError):
        return None


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
    json_path = output_dir / f"ensemble_risk_off_alpha_repair_review_{candidate}_{ts}.json"
    md_path = output_dir / f"ensemble_risk_off_alpha_repair_review_{candidate}_{ts}.md"
    _write_json(json_path, payload)
    _write_markdown(md_path, payload)
    return {"json": str(json_path), "markdown": str(md_path)}


def _write_markdown(path: Path, payload: JsonDict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Ensemble Risk-Off Alpha Repair Review",
        "",
        f"- strategy: `{payload.get('strategy')}`",
        f"- candidate: `{payload.get('candidate')}`",
        f"- status: `{payload.get('status')}`",
        f"- observation_monitor_watch_allowed: `{payload.get('observation_monitor_watch_allowed')}`",
        f"- formal_candidate_allowed: `{payload.get('formal_candidate_allowed')}`",
        "",
        "## Blocking Reasons",
        "",
    ]
    blocking = payload.get("blocking_reasons") or []
    lines.extend([f"- `{item}`" for item in blocking] if blocking else ["- none"])
    lines.extend(["", "## Hard Boundaries", ""])
    lines.extend(f"- `{item}`" for item in payload.get("hard_boundaries") or [])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_json(path: Path, payload: JsonDict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
