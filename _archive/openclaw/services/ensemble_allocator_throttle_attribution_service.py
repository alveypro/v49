from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from typing import Any, Dict, Sequence


JsonDict = Dict[str, Any]


def build_ensemble_allocator_throttle_attribution(
    *,
    throttled_benchmark_artifact_path: str,
    unthrottled_benchmark_artifact_path: str,
    output_dir: str = "",
    operator_name: str = "",
) -> JsonDict:
    throttled_payload = _load_json(throttled_benchmark_artifact_path)
    unthrottled_payload = _load_json(unthrottled_benchmark_artifact_path)
    throttled = throttled_payload.get("benchmark") if isinstance(throttled_payload.get("benchmark"), dict) else {}
    unthrottled = unthrottled_payload.get("benchmark") if isinstance(unthrottled_payload.get("benchmark"), dict) else {}
    windows = _window_attribution(throttled_payload.get("windows") or [], unthrottled_payload.get("windows") or [])
    blocking: list[str] = []
    warnings: list[str] = []
    if throttled.get("passed") is not True:
        blocking.append("throttled_shadow_benchmark_not_passed")
    if unthrottled.get("passed") is not True:
        blocking.append("unthrottled_shadow_benchmark_not_passed")
    if not windows:
        blocking.append("missing_overlapping_attribution_windows")
    alpha_excess = _optional_float(unthrottled.get("after_cost_excess_return"))
    throttled_excess = _optional_float(throttled.get("after_cost_excess_return"))
    risk_control_delta = _delta(throttled_excess, alpha_excess)
    alpha_hit_rate = _optional_float(unthrottled.get("hit_rate"))
    throttled_hit_rate = _optional_float(throttled.get("hit_rate"))
    alpha_drawdown = _optional_float(unthrottled.get("max_drawdown"))
    throttled_drawdown = _optional_float(throttled.get("max_drawdown"))
    alpha_turnover = _optional_float(unthrottled.get("turnover"))
    throttled_turnover = _optional_float(throttled.get("turnover"))
    if alpha_excess is None or alpha_excess <= 0.0:
        warnings.append(f"unthrottled_alpha_after_cost_excess_not_positive:{alpha_excess}")
    if alpha_hit_rate is None or alpha_hit_rate < 0.60:
        warnings.append(f"unthrottled_alpha_hit_rate_below_observation_floor:{alpha_hit_rate}")
    if risk_control_delta is not None and risk_control_delta > 0.0:
        warnings.append(f"positive_result_helped_by_allocator_throttle:{risk_control_delta}")

    payload: JsonDict = {
        "artifact_version": "ensemble_allocator_throttle_attribution.v1",
        "created_at": _now_text(),
        "operator_name": str(operator_name or ""),
        "strategy": "ensemble_core",
        "candidate": str(throttled_payload.get("candidate") or unthrottled_payload.get("candidate") or "hard_event_alpha_candidate"),
        "current_pool": "observation",
        "research_only": True,
        "not_for_production": True,
        "formal_candidate_allowed": False,
        "formal_ranking_allowed": False,
        "production_candidate_allowed": False,
        "source_artifacts": {
            "throttled_benchmark": str(throttled_benchmark_artifact_path or ""),
            "unthrottled_benchmark": str(unthrottled_benchmark_artifact_path or ""),
        },
        "summary": {
            "alpha_component_proxy": "unthrottled_after_cost_shadow_benchmark",
            "risk_control_component_proxy": "throttled_minus_unthrottled",
            "unthrottled_after_cost_excess_return": alpha_excess,
            "throttled_after_cost_excess_return": throttled_excess,
            "allocator_throttle_excess_delta": risk_control_delta,
            "unthrottled_hit_rate": alpha_hit_rate,
            "throttled_hit_rate": throttled_hit_rate,
            "hit_rate_delta": _delta(throttled_hit_rate, alpha_hit_rate),
            "unthrottled_max_drawdown": alpha_drawdown,
            "throttled_max_drawdown": throttled_drawdown,
            "drawdown_delta": _delta(throttled_drawdown, alpha_drawdown),
            "unthrottled_turnover": alpha_turnover,
            "throttled_turnover": throttled_turnover,
            "turnover_delta": _delta(throttled_turnover, alpha_turnover),
        },
        "regime_attribution": _regime_attribution(throttled, unthrottled),
        "window_attribution": windows,
        "blocking_reasons": sorted(set(blocking)),
        "warning_reasons": sorted(set(warnings)),
        "conclusion": _conclusion(alpha_excess=alpha_excess, risk_control_delta=risk_control_delta),
        "hard_boundaries": [
            "allocator_throttle_attribution_does_not_promote_to_formal",
            "do_not_count_cash_or_degrossing_as_alpha",
            "unthrottled_alpha_must_stand_on_its_own_before_formal_candidate_review",
            "risk_control_improvement_can_support_observation_monitoring_only",
        ],
    }
    if output_dir:
        artifacts = _write_artifacts(Path(output_dir), payload)
        payload["artifacts"] = artifacts
        _write_json(Path(artifacts["json"]), payload)
    return payload


def _window_attribution(throttled_windows: Sequence[Any], unthrottled_windows: Sequence[Any]) -> list[JsonDict]:
    throttled_by_date = {str((row or {}).get("as_of_date") or ""): row for row in throttled_windows if isinstance(row, dict)}
    rows: list[JsonDict] = []
    for raw in unthrottled_windows:
        if not isinstance(raw, dict):
            continue
        as_of = str(raw.get("as_of_date") or "")
        other = throttled_by_date.get(as_of)
        if not isinstance(other, dict):
            continue
        t_exec = other.get("execution_cost_replay") if isinstance(other.get("execution_cost_replay"), dict) else {}
        u_exec = raw.get("execution_cost_replay") if isinstance(raw.get("execution_cost_replay"), dict) else {}
        formal = raw.get("formal_pool_benchmark") if isinstance(raw.get("formal_pool_benchmark"), dict) else {}
        t_port = other.get("shadow_portfolio") if isinstance(other.get("shadow_portfolio"), dict) else {}
        u_port = raw.get("shadow_portfolio") if isinstance(raw.get("shadow_portfolio"), dict) else {}
        t_net = _optional_float(t_exec.get("net_return"))
        u_net = _optional_float(u_exec.get("net_return"))
        formal_return = _optional_float(formal.get("avg_return_pct"))
        rows.append(
            {
                "as_of_date": as_of,
                "market_regime_label": str(raw.get("market_regime_label") or other.get("market_regime_label") or "unknown"),
                "unthrottled_net_return": u_net,
                "throttled_net_return": t_net,
                "formal_pool_avg_return": formal_return,
                "unthrottled_excess": _delta(u_net, formal_return),
                "throttled_excess": _delta(t_net, formal_return),
                "throttle_return_delta": _delta(t_net, u_net),
                "unthrottled_turnover": _optional_float(u_port.get("turnover_estimate")),
                "throttled_turnover": _optional_float(t_port.get("turnover_estimate")),
                "unthrottled_cash_weight": _optional_float(u_port.get("cash_weight")),
                "throttled_cash_weight": _optional_float(t_port.get("cash_weight")),
            }
        )
    return rows


def _regime_attribution(throttled: JsonDict, unthrottled: JsonDict) -> JsonDict:
    out: JsonDict = {}
    t_split = throttled.get("regime_split") if isinstance(throttled.get("regime_split"), dict) else {}
    u_split = unthrottled.get("regime_split") if isinstance(unthrottled.get("regime_split"), dict) else {}
    for regime in sorted(set(t_split) | set(u_split)):
        t = t_split.get(regime) if isinstance(t_split.get(regime), dict) else {}
        u = u_split.get(regime) if isinstance(u_split.get(regime), dict) else {}
        t_excess = _optional_float(t.get("avg_after_cost_excess_return"))
        u_excess = _optional_float(u.get("avg_after_cost_excess_return"))
        out[regime] = {
            "unthrottled_avg_after_cost_excess_return": u_excess,
            "throttled_avg_after_cost_excess_return": t_excess,
            "allocator_throttle_excess_delta": _delta(t_excess, u_excess),
            "unthrottled_hit_rate": _optional_float(u.get("hit_rate")),
            "throttled_hit_rate": _optional_float(t.get("hit_rate")),
            "window_count": int(float(t.get("window_count") or u.get("window_count") or 0)),
        }
    return out


def _conclusion(*, alpha_excess: float | None, risk_control_delta: float | None) -> str:
    if alpha_excess is None:
        return "insufficient_evidence_to_separate_alpha_from_allocator_throttle"
    if alpha_excess <= 0.0 and risk_control_delta is not None and risk_control_delta > 0.0:
        return "positive_observation_result_is_allocator_risk_control_dependent_not_standalone_alpha"
    if alpha_excess > 0.0:
        return "unthrottled_alpha_positive_but_still_observation_only"
    return "unthrottled_alpha_not_positive"


def _delta(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return round(float(left) - float(right), 6)


def _optional_float(value: Any) -> float | None:
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
    json_path = output_dir / f"ensemble_allocator_throttle_attribution_{candidate}_{ts}.json"
    md_path = output_dir / f"ensemble_allocator_throttle_attribution_{candidate}_{ts}.md"
    _write_json(json_path, payload)
    _write_markdown(md_path, payload)
    return {"json": str(json_path), "markdown": str(md_path)}


def _write_markdown(path: Path, payload: JsonDict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    lines = [
        "# Ensemble Allocator Throttle Attribution",
        "",
        f"- strategy: `{payload.get('strategy')}`",
        f"- candidate: `{payload.get('candidate')}`",
        f"- current_pool: `{payload.get('current_pool')}`",
        f"- conclusion: `{payload.get('conclusion')}`",
        f"- unthrottled_after_cost_excess_return: `{summary.get('unthrottled_after_cost_excess_return')}`",
        f"- throttled_after_cost_excess_return: `{summary.get('throttled_after_cost_excess_return')}`",
        f"- allocator_throttle_excess_delta: `{summary.get('allocator_throttle_excess_delta')}`",
        f"- formal_candidate_allowed: `{payload.get('formal_candidate_allowed')}`",
        "",
        "## Warning Reasons",
        "",
    ]
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
