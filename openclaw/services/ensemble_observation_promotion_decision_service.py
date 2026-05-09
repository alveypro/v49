from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from typing import Any, Dict

from openclaw.services.lineage_service import new_decision_id


JsonDict = Dict[str, Any]


def build_ensemble_observation_promotion_decision(
    *,
    observation_gate_artifact_path: str,
    stage_audit_artifact_path: str,
    output_dir: str,
    operator_name: str = "",
    decision_id: str = "",
) -> JsonDict:
    gate_payload = _load_json(observation_gate_artifact_path)
    stage_audit = _load_json(stage_audit_artifact_path)
    gate = gate_payload.get("gate") if isinstance(gate_payload.get("gate"), dict) else gate_payload
    source_shadow_path = str(gate_payload.get("source_shadow_benchmark_json") or "")
    shadow_payload = _load_json(source_shadow_path)
    benchmark = shadow_payload.get("benchmark") if isinstance(shadow_payload.get("benchmark"), dict) else {}
    rule_freeze = shadow_payload.get("rule_freeze") if isinstance(shadow_payload.get("rule_freeze"), dict) else {}
    candidate = str(gate.get("candidate") or shadow_payload.get("candidate") or "ensemble_core")
    blocking = _blocking_reasons(gate=gate, benchmark=benchmark, stage_audit=stage_audit, rule_freeze=rule_freeze)
    observation_ready = not blocking
    payload: JsonDict = {
        "artifact_version": "ensemble_observation_promotion_decision.v1",
        "created_at": _now_text(),
        "decision_id": str(decision_id or new_decision_id()),
        "decision_type": "research_to_observation_manual_review",
        "strategy": "ensemble_core",
        "candidate": candidate,
        "operator_name": str(operator_name or ""),
        "current_pool": "research_only",
        "target_pool": "observation",
        "status": "observation_promotion_review_ready" if observation_ready else "observation_promotion_blocked",
        "observation_promotion_allowed": observation_ready,
        "strategy_pool_mutation_allowed": False,
        "formal_candidate_allowed": False,
        "formal_ranking_allowed": False,
        "production_candidate_allowed": False,
        "evidence": {
            "observation_gate_artifact": str(observation_gate_artifact_path or ""),
            "stage_audit_artifact": str(stage_audit_artifact_path or ""),
            "source_shadow_benchmark_artifact": source_shadow_path,
            "observation_gate": {
                "passed": bool(gate.get("observation_gate_passed") is True),
                "observation_review_eligible": bool(gate.get("observation_review_eligible") is True),
                "observation_pool_eligible": bool(gate.get("observation_pool_eligible") is True),
                "formal_pool_eligible": bool(gate.get("formal_pool_eligible") is True),
                "blocking_reasons": list(gate.get("blocking_reasons") or []),
                "evidence_summary": gate.get("evidence_summary") if isinstance(gate.get("evidence_summary"), dict) else {},
            },
            "shadow_benchmark": {
                "passed": bool(benchmark.get("passed") is True),
                "after_cost_excess_return": benchmark.get("after_cost_excess_return"),
                "hit_rate": benchmark.get("hit_rate"),
                "turnover": benchmark.get("turnover"),
                "industry_concentration": benchmark.get("industry_concentration"),
                "capacity_utilization": benchmark.get("capacity_utilization"),
                "regime_split": benchmark.get("regime_split") if isinstance(benchmark.get("regime_split"), dict) else {},
                "blocking_reasons": list(benchmark.get("blocking_reasons") or []),
            },
            "rule_freeze": {
                "frozen": bool(rule_freeze.get("frozen") is True),
                "rule_version": str(rule_freeze.get("rule_version") or ""),
                "rule_hash": str(rule_freeze.get("rule_hash") or ""),
                "sleeve_policy_approved": bool(rule_freeze.get("sleeve_policy_approved") is True),
            },
            "stage_audit": {
                "passed": bool(stage_audit.get("passed") is True),
                "blocking_reasons": list(stage_audit.get("blocking_reasons") or []),
                "top_strategies": list(stage_audit.get("top_strategies") or []),
                "observation_pool": list(stage_audit.get("observation_pool") or []),
            },
        },
        "blocking_reasons": blocking,
        "required_next_action": (
            "manual_apply_research_only_to_observation_pool_transition"
            if observation_ready
            else "repair_blocking_reasons_before_observation_decision"
        ),
        "hard_boundaries": [
            "decision_artifact_does_not_mutate_strategy_pool",
            "observation_promotion_is_not_formal_pool_eligibility",
            "do_not_publish_formal_top_from_ensemble_observation_decision",
            "do_not_use_allocator_throttle_as_alpha_improvement",
            "formal_candidate_requires_separate_observation_stability_period",
        ],
    }
    artifacts = _write_artifacts(Path(output_dir), payload)
    payload["artifacts"] = artifacts
    _write_json(Path(artifacts["json"]), payload)
    return payload


def _blocking_reasons(*, gate: JsonDict, benchmark: JsonDict, stage_audit: JsonDict, rule_freeze: JsonDict) -> list[str]:
    blocking: list[str] = []
    if gate.get("research_only") is not True:
        blocking.append("observation_gate_not_research_only")
    if gate.get("observation_gate_passed") is not True:
        blocking.append("observation_gate_not_passed")
    if gate.get("observation_review_eligible") is not True:
        blocking.append("observation_review_not_eligible")
    if gate.get("observation_pool_eligible") is True:
        blocking.append("gate_attempted_direct_observation_pool_eligibility")
    if gate.get("formal_pool_eligible") is True:
        blocking.append("gate_attempted_formal_pool_eligibility")
    for reason in gate.get("blocking_reasons") or []:
        blocking.append(f"observation_gate_blocked:{reason}")
    if benchmark.get("passed") is not True:
        blocking.append("shadow_benchmark_not_passed")
    for reason in benchmark.get("blocking_reasons") or []:
        blocking.append(f"shadow_benchmark_blocked:{reason}")
    if rule_freeze and rule_freeze.get("frozen") is not True:
        blocking.append("candidate_rule_not_frozen")
    if rule_freeze and not str(rule_freeze.get("rule_hash") or ""):
        blocking.append("missing_rule_hash")
    if rule_freeze.get("sleeve_policy_approved") is True:
        blocking.append("rule_freeze_unexpectedly_approved_sleeve")
    if stage_audit.get("passed") is not True:
        blocking.append("stage_audit_not_passed")
    return sorted(set(blocking))


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
    json_path = output_dir / f"ensemble_observation_promotion_decision_{candidate}_{ts}.json"
    md_path = output_dir / f"ensemble_observation_promotion_decision_{candidate}_{ts}.md"
    _write_json(json_path, payload)
    _write_markdown(md_path, payload)
    return {"json": str(json_path), "markdown": str(md_path)}


def _write_markdown(path: Path, payload: JsonDict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    evidence = payload.get("evidence") if isinstance(payload.get("evidence"), dict) else {}
    gate = evidence.get("observation_gate") if isinstance(evidence.get("observation_gate"), dict) else {}
    benchmark = evidence.get("shadow_benchmark") if isinstance(evidence.get("shadow_benchmark"), dict) else {}
    lines = [
        "# Ensemble Observation Promotion Decision",
        "",
        f"- decision_id: `{payload.get('decision_id')}`",
        f"- strategy: `{payload.get('strategy')}`",
        f"- candidate: `{payload.get('candidate')}`",
        f"- decision_type: `{payload.get('decision_type')}`",
        f"- status: `{payload.get('status')}`",
        f"- current_pool: `{payload.get('current_pool')}`",
        f"- target_pool: `{payload.get('target_pool')}`",
        f"- observation_promotion_allowed: `{payload.get('observation_promotion_allowed')}`",
        f"- strategy_pool_mutation_allowed: `{payload.get('strategy_pool_mutation_allowed')}`",
        f"- formal_candidate_allowed: `{payload.get('formal_candidate_allowed')}`",
        f"- formal_ranking_allowed: `{payload.get('formal_ranking_allowed')}`",
        "",
        "## Evidence",
        "",
        f"- observation_gate_passed: `{gate.get('passed')}`",
        f"- after_cost_excess_return: `{benchmark.get('after_cost_excess_return')}`",
        f"- hit_rate: `{benchmark.get('hit_rate')}`",
        f"- turnover: `{benchmark.get('turnover')}`",
        f"- industry_concentration: `{benchmark.get('industry_concentration')}`",
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
