from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from typing import Any, Dict, List

from openclaw.services.lineage_service import new_decision_id


JsonDict = Dict[str, Any]


def build_ensemble_observation_promotion_apply(
    *,
    promotion_decision_artifact_path: str,
    output_dir: str,
    observation_ledger_path: str,
    operator_name: str = "",
    decision_id: str = "",
) -> JsonDict:
    decision = _load_json(promotion_decision_artifact_path)
    blocking = _blocking_reasons(decision)
    applied = not blocking
    source_decision_id = str(decision.get("decision_id") or "")
    candidate = str(decision.get("candidate") or "hard_event_alpha_candidate")
    apply_id = str(decision_id or new_decision_id())
    ledger_record: JsonDict = {
        "record_version": "ensemble_observation_pool_record.v1",
        "record_id": apply_id,
        "created_at": _now_text(),
        "strategy": "ensemble_core",
        "candidate": candidate,
        "from_pool": "research_only",
        "to_pool": "observation",
        "status": "applied" if applied else "blocked",
        "source_promotion_decision_id": source_decision_id,
        "source_promotion_decision_artifact": str(promotion_decision_artifact_path or ""),
        "operator_name": str(operator_name or ""),
        "formal_pool_eligible": False,
        "formal_ranking_allowed": False,
        "production_candidate_allowed": False,
        "competes_for_formal_top": False,
        "reason": "manual_research_only_to_observation_transition_after_observation_gate",
        "blocking_reasons": blocking,
    }
    payload: JsonDict = {
        "artifact_version": "ensemble_observation_promotion_apply.v1",
        "created_at": _now_text(),
        "decision_id": apply_id,
        "decision_type": "manual_apply_research_only_to_observation",
        "strategy": "ensemble_core",
        "candidate": candidate,
        "source_promotion_decision_artifact": str(promotion_decision_artifact_path or ""),
        "source_promotion_decision_id": source_decision_id,
        "status": "observation_pool_record_applied" if applied else "observation_pool_record_blocked",
        "observation_pool_record_applied": applied,
        "strategy_pool_mutation": "observation_ledger_only",
        "formal_candidate_allowed": False,
        "formal_ranking_allowed": False,
        "production_candidate_allowed": False,
        "blocking_reasons": blocking,
        "ledger_record": ledger_record,
        "hard_boundaries": [
            "apply_artifact_only_records_observation_pool_transition",
            "apply_artifact_does_not_grant_formal_pool_eligibility",
            "apply_artifact_does_not_change_formal_top",
            "formal_candidate_requires_separate_observation_stability_period",
            "allocator_throttle_remains_portfolio_control_not_alpha_evidence",
        ],
    }
    artifacts = _write_artifacts(Path(output_dir), payload)
    payload["artifacts"] = artifacts
    _write_json(Path(artifacts["json"]), payload)
    if applied:
        _append_ledger(Path(observation_ledger_path), ledger_record)
    return payload


def load_observation_promotion_records(path: str) -> List[JsonDict]:
    p = Path(path)
    if not p.exists():
        return []
    records: List[JsonDict] = []
    for line in p.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            records.append(item)
    return records


def valid_observation_promotion_records(records: List[JsonDict]) -> List[JsonDict]:
    out: List[JsonDict] = []
    seen: set[str] = set()
    for item in records:
        if item.get("record_version") != "ensemble_observation_pool_record.v1":
            continue
        if item.get("status") != "applied":
            continue
        if item.get("strategy") != "ensemble_core":
            continue
        if item.get("from_pool") != "research_only" or item.get("to_pool") != "observation":
            continue
        if item.get("formal_pool_eligible") is True or item.get("formal_ranking_allowed") is True:
            continue
        decision_id = str(item.get("source_promotion_decision_id") or item.get("record_id") or "")
        if decision_id in seen:
            continue
        seen.add(decision_id)
        out.append(dict(item))
    return out


def _blocking_reasons(decision: JsonDict) -> List[str]:
    blocking: List[str] = []
    if decision.get("artifact_version") != "ensemble_observation_promotion_decision.v1":
        blocking.append("invalid_source_promotion_decision_artifact")
    if decision.get("status") != "observation_promotion_review_ready":
        blocking.append("source_promotion_decision_not_ready")
    if decision.get("observation_promotion_allowed") is not True:
        blocking.append("source_observation_promotion_not_allowed")
    if decision.get("strategy_pool_mutation_allowed") is not False:
        blocking.append("source_decision_must_be_read_only_before_manual_apply")
    if decision.get("formal_candidate_allowed") is True or decision.get("formal_ranking_allowed") is True:
        blocking.append("source_decision_attempts_formal_eligibility")
    if decision.get("production_candidate_allowed") is True:
        blocking.append("source_decision_attempts_production_candidate")
    if decision.get("blocking_reasons"):
        blocking.append("source_promotion_decision_has_blocking_reasons")
    return sorted(set(blocking))


def _append_ledger(path: Path, record: JsonDict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = load_observation_promotion_records(str(path))
    source_decision_id = str(record.get("source_promotion_decision_id") or "")
    if any(str(item.get("source_promotion_decision_id") or "") == source_decision_id for item in existing):
        return
    path.write_text(
        (path.read_text(encoding="utf-8") if path.exists() else "")
        + json.dumps(record, ensure_ascii=False, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )


def _write_artifacts(output_dir: Path, payload: JsonDict) -> JsonDict:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    candidate = str(payload.get("candidate") or "ensemble_core")
    json_path = output_dir / f"ensemble_observation_promotion_apply_{candidate}_{ts}.json"
    md_path = output_dir / f"ensemble_observation_promotion_apply_{candidate}_{ts}.md"
    _write_json(json_path, payload)
    _write_markdown(md_path, payload)
    return {"json": str(json_path), "markdown": str(md_path)}


def _write_markdown(path: Path, payload: JsonDict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Ensemble Observation Promotion Apply",
        "",
        f"- decision_id: `{payload.get('decision_id')}`",
        f"- strategy: `{payload.get('strategy')}`",
        f"- candidate: `{payload.get('candidate')}`",
        f"- status: `{payload.get('status')}`",
        f"- observation_pool_record_applied: `{payload.get('observation_pool_record_applied')}`",
        f"- strategy_pool_mutation: `{payload.get('strategy_pool_mutation')}`",
        f"- formal_candidate_allowed: `{payload.get('formal_candidate_allowed')}`",
        f"- formal_ranking_allowed: `{payload.get('formal_ranking_allowed')}`",
        "",
        "## Blocking Reasons",
        "",
    ]
    blocking = payload.get("blocking_reasons") or []
    lines.extend([f"- `{item}`" for item in blocking] if blocking else ["- none"])
    lines.extend(["", "## Hard Boundaries", ""])
    lines.extend(f"- `{item}`" for item in payload.get("hard_boundaries") or [])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _load_json(path: str) -> JsonDict:
    if not str(path or "").strip():
        return {}
    p = Path(path)
    if not p.exists():
        return {}
    payload = json.loads(p.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: JsonDict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
