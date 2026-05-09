from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CANDIDATE_RISK_STATE_VERSION = "candidate_risk_state.v1"
CANDIDATE_RISK_STATE_TRANSITION_VERSION = "candidate_risk_state_transition.v1"
CANDIDATE_STATE_AUDIT_VERSION = "candidate_state_audit.v1"
ALLOWED_RISK_STATES = {"watch", "review", "degrade", "invalid", "closed"}


@dataclass(frozen=True)
class CandidateRiskStateConfig:
    invalid_drawdown_threshold: float = -0.10
    invalid_return_threshold: float = -0.08
    degrade_drawdown_threshold: float = -0.06
    degrade_return_threshold: float = -0.04


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: str | Path | None) -> dict[str, Any]:
    if not path or not Path(path).exists():
        return {}
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _load_jsonl(path: str | Path | None) -> list[dict[str, Any]]:
    if not path or not Path(path).exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValueError(f"expected object jsonl entry: {path}")
        rows.append(payload)
    return rows


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _by_observation_id(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(item.get("observation_id") or ""): item for item in payload.get("items", []) or payload.get("candidates", []) or []}


def _capacity_by_code(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(item.get("ts_code") or ""): item for item in payload.get("items", []) or payload.get("candidates", []) or []}


def _previous_state_by_observation_id(transitions: list[dict[str, Any]]) -> dict[str, str]:
    previous: dict[str, str] = {}
    for item in transitions:
        observation_id = str(item.get("observation_id") or "")
        to_state = str(item.get("to_state") or "")
        if observation_id and to_state in ALLOWED_RISK_STATES:
            previous[observation_id] = to_state
    return previous


def _return_values(result_item: dict[str, Any]) -> list[float]:
    values = []
    for value in (result_item.get("returns", {}) or {}).values():
        if value is not None:
            values.append(_to_float(value))
    return values


def _derive_candidate_state(
    *,
    snapshot_item: dict[str, Any],
    result_item: dict[str, Any],
    attribution_item: dict[str, Any],
    capacity_item: dict[str, Any],
    portfolio_quality: dict[str, Any],
    config: CandidateRiskStateConfig,
) -> tuple[str, list[str], list[dict[str, Any]]]:
    reasons: list[str] = []
    evidence: list[dict[str, Any]] = []
    result_status = str(result_item.get("status") or "pending")
    capacity_state = str(capacity_item.get("state") or "")
    portfolio_quality_status = str(portfolio_quality.get("status") or "")
    max_drawdown = result_item.get("max_drawdown")
    returns = _return_values(result_item)
    failure_category = str(attribution_item.get("primary_failure_category") or "")

    data_quality_level = str(snapshot_item.get("data_quality_level") or "").lower()
    if data_quality_level in {"block", "blocked", "fail", "failed"}:
        reasons.append("data_quality_failed")
        evidence.append({"source": "snapshot", "data_quality_level": data_quality_level})
        return "invalid", reasons, evidence
    if result_status == "blocked" and failure_category == "data_or_calendar_insufficient":
        reasons.append("observation_data_or_calendar_insufficient")
        evidence.append({"source": "observation_result", "blocking_reasons": result_item.get("blocking_reasons", [])})
        return "invalid", reasons, evidence
    if max_drawdown is not None and _to_float(max_drawdown) <= config.invalid_drawdown_threshold:
        reasons.append("max_drawdown_exceeds_invalid_threshold")
        evidence.append({"source": "observation_result", "max_drawdown": max_drawdown})
        return "invalid", reasons, evidence
    if returns and min(returns) <= config.invalid_return_threshold:
        reasons.append("return_exceeds_invalid_threshold")
        evidence.append({"source": "observation_result", "returns": result_item.get("returns", {})})
        return "invalid", reasons, evidence

    if result_status == "completed":
        pending_hits = [
            key
            for key, value in (result_item.get("hit_status", {}) or {}).items()
            if str(value or "") == "pending"
        ]
        if not pending_hits:
            reasons.append("observation_horizons_completed")
            evidence.append({"source": "observation_result", "hit_status": result_item.get("hit_status", {})})
            return "closed", reasons, evidence

    if portfolio_quality_status == "blocked":
        reasons.append("portfolio_quality_blocked")
        evidence.append(
            {
                "source": "candidate_portfolio_quality",
                "quality_score": portfolio_quality.get("quality_score"),
                "blocking_reasons": portfolio_quality.get("blocking_reasons", []),
            }
        )
    if capacity_state == "blocked":
        reasons.append("capacity_blocked")
        evidence.append({"source": "portfolio_capacity", "capacity": capacity_item})
    if capacity_state == "review":
        reasons.append("capacity_review")
        evidence.append({"source": "portfolio_capacity", "capacity": capacity_item})
    if max_drawdown is not None and _to_float(max_drawdown) <= config.degrade_drawdown_threshold:
        reasons.append("max_drawdown_exceeds_degrade_threshold")
        evidence.append({"source": "observation_result", "max_drawdown": max_drawdown})
    if returns and min(returns) <= config.degrade_return_threshold:
        reasons.append("return_exceeds_degrade_threshold")
        evidence.append({"source": "observation_result", "returns": result_item.get("returns", {})})
    if reasons:
        return "degrade", reasons, evidence

    if result_status == "pending":
        reasons.append("observation_window_pending")
        evidence.append({"source": "observation_result", "blocking_reasons": result_item.get("blocking_reasons", [])})
        return "review", reasons, evidence
    if portfolio_quality_status == "review":
        reasons.append("portfolio_quality_review")
        evidence.append({"source": "candidate_portfolio_quality", "review_reasons": portfolio_quality.get("review_reasons", [])})
        return "review", reasons, evidence

    reasons.append("normal_watch")
    evidence.append({"source": "snapshot", "observation_status": snapshot_item.get("observation_status")})
    return "watch", reasons, evidence


def build_candidate_risk_state(
    *,
    snapshot_path: str | Path,
    observation_result_path: str | Path | None = None,
    failure_attribution_path: str | Path | None = None,
    portfolio_quality_path: str | Path | None = None,
    portfolio_capacity_path: str | Path | None = None,
    previous_transition_path: str | Path | None = None,
    config: CandidateRiskStateConfig | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    cfg = config or CandidateRiskStateConfig()
    snapshot = _read_json(snapshot_path)
    observation_result = _read_json(observation_result_path)
    failure_attribution = _read_json(failure_attribution_path)
    portfolio_quality = _read_json(portfolio_quality_path)
    portfolio_capacity = _read_json(portfolio_capacity_path)
    previous_states = _previous_state_by_observation_id(_load_jsonl(previous_transition_path))
    result_by_id = _by_observation_id(observation_result)
    attribution_by_id = _by_observation_id(failure_attribution)
    capacity_by_code = _capacity_by_code(portfolio_capacity)

    generated_at = _utc_now()
    items: list[dict[str, Any]] = []
    transitions: list[dict[str, Any]] = []
    audit_checks: list[dict[str, Any]] = []
    state_counts: dict[str, int] = {}
    blocking_reasons: list[str] = []

    for snapshot_item in snapshot.get("items", []) or []:
        observation_id = str(snapshot_item.get("observation_id") or "")
        code = str(snapshot_item.get("ts_code") or "")
        result_item = result_by_id.get(observation_id, {})
        attribution_item = attribution_by_id.get(observation_id, {})
        capacity_item = capacity_by_code.get(code, {})
        state, reasons, evidence = _derive_candidate_state(
            snapshot_item=snapshot_item,
            result_item=result_item,
            attribution_item=attribution_item,
            capacity_item=capacity_item,
            portfolio_quality=portfolio_quality,
            config=cfg,
        )
        if state not in ALLOWED_RISK_STATES:
            blocking_reasons.append("unknown_state")
        if not reasons:
            blocking_reasons.append("state_transition_without_reason")
        previous_state = previous_states.get(observation_id, "watch")
        state_counts[state] = state_counts.get(state, 0) + 1
        items.append(
            {
                "observation_id": observation_id,
                "ts_code": code,
                "stock_name": snapshot_item.get("stock_name"),
                "industry": snapshot_item.get("industry"),
                "risk_state": state,
                "previous_state": previous_state,
                "state_changed": previous_state != state,
                "reasons": reasons,
                "evidence": evidence,
                "normal_observation_allowed": state in {"watch", "review"},
                "selected_at": snapshot_item.get("selected_at"),
                "lineage_hash": snapshot_item.get("lineage_hash"),
            }
        )
        transitions.append(
            {
                "schema_version": CANDIDATE_RISK_STATE_TRANSITION_VERSION,
                "generated_at": generated_at,
                "observation_id": observation_id,
                "ts_code": code,
                "from_state": previous_state,
                "to_state": state,
                "state_changed": previous_state != state,
                "reasons": reasons,
                "evidence": evidence,
            }
        )
        audit_checks.append(
            {
                "observation_id": observation_id,
                "ts_code": code,
                "state_known": state in ALLOWED_RISK_STATES,
                "has_reason": bool(reasons),
                "has_evidence": bool(evidence),
                "transition_recorded": True,
            }
        )

    invalid_normal = [item["ts_code"] for item in items if item["risk_state"] == "invalid" and item["normal_observation_allowed"]]
    if invalid_normal:
        blocking_reasons.append("invalid_candidate_marked_normal")
    status = "blocked" if blocking_reasons else "passed"
    state_payload = {
        "schema_version": CANDIDATE_RISK_STATE_VERSION,
        "status": status,
        "generated_at": generated_at,
        "allowed_states": sorted(ALLOWED_RISK_STATES),
        "snapshot_date": snapshot.get("snapshot_date"),
        "candidate_count": len(items),
        "state_counts": state_counts,
        "blocking_reasons": sorted(set(blocking_reasons)),
        "items": items,
    }
    audit_payload = {
        "schema_version": CANDIDATE_STATE_AUDIT_VERSION,
        "status": status,
        "generated_at": generated_at,
        "snapshot_date": snapshot.get("snapshot_date"),
        "candidate_count": len(items),
        "blocking_reasons": sorted(set(blocking_reasons)),
        "checks": audit_checks,
        "summary": {
            "unknown_state_count": sum(1 for check in audit_checks if not check["state_known"]),
            "missing_reason_count": sum(1 for check in audit_checks if not check["has_reason"]),
            "missing_evidence_count": sum(1 for check in audit_checks if not check["has_evidence"]),
        },
    }
    return state_payload, transitions, audit_payload


def write_risk_state_payload(payload: dict[str, Any], *, output_path: str | Path) -> str:
    resolved = Path(output_path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(resolved)


def write_risk_state_transitions(transitions: list[dict[str, Any]], *, output_path: str | Path) -> str:
    resolved = Path(output_path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    with resolved.open("w", encoding="utf-8") as handle:
        for item in transitions:
            handle.write(json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n")
    return str(resolved)
