from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List

from openclaw.services.lineage_service import new_decision_id
from openclaw.services.rejected_backtest_artifact_ledger_service import load_rejected_backtest_artifacts


JsonDict = Dict[str, Any]
REQUIRED_EXECUTION_EVIDENCE_FIELDS = (
    "decision_id",
    "order_fill_attribution",
    "slippage",
    "miss_reason",
    "manual_override",
    "linked_run_id",
)


def build_promotion_decision_artifact(
    *,
    strategy: str,
    sweep_artifact_path: str,
    stage_audit_artifact_path: str,
    rejected_ledger_path: str,
    output_dir: str,
    operator_name: str = "",
    decision_id: str = "",
    execution_evidence: JsonDict | None = None,
) -> JsonDict:
    sweep = _load_json(sweep_artifact_path)
    stage_audit = _load_json(stage_audit_artifact_path)
    rejected = load_rejected_backtest_artifacts(rejected_ledger_path)
    strategy_name = str(strategy or sweep.get("strategy") or "").strip().lower()
    linked_run_id = str(sweep.get("run_id") or "")
    rejected_matches = _matching_rejected_artifacts(rejected, artifact_path=sweep_artifact_path, strategy=strategy_name)
    diagnostics = sweep.get("strategy_backtest_diagnostics") if isinstance(sweep.get("strategy_backtest_diagnostics"), dict) else {}
    credibility = sweep.get("backtest_credibility") if isinstance(sweep.get("backtest_credibility"), dict) else {}
    execution_review = _normalize_execution_evidence(execution_evidence)
    blocking = _blocking_reasons(
        diagnostics=diagnostics,
        credibility=credibility,
        rejected_matches=rejected_matches,
        stage_audit=stage_audit,
        execution_evidence=execution_review,
    )
    status = "candidate_execution_evidence_ready" if not blocking and execution_review.get("passed") is True else "blocked_for_production"
    payload: JsonDict = {
        "artifact_version": "promotion_decision_artifact.v1",
        "created_at": _now_text(),
        "decision_id": str(decision_id or new_decision_id()),
        "decision_type": "candidate_discussion",
        "strategy": strategy_name,
        "operator_name": str(operator_name or ""),
        "status": status,
        "production_candidate_allowed": False,
        "formal_ranking_allowed": False,
        "linked_run_id": linked_run_id,
        "evidence": {
            "sweep_artifact": str(sweep_artifact_path or ""),
            "stage_audit_artifact": str(stage_audit_artifact_path or ""),
            "rejected_ledger": str(rejected_ledger_path or ""),
            "backtest_credibility": credibility,
            "strategy_backtest_diagnostics": diagnostics,
            "rejected_ledger_check": {
                "matched_rejected_entries": rejected_matches,
                "matched_rejected_count": len(rejected_matches),
                "reused_as_runtime_default": any(item.get("reused_as_runtime_default") is True for item in rejected_matches),
            },
            "stage_audit": {
                "passed": bool(stage_audit.get("passed") is True),
                "artifact_path": str(stage_audit_artifact_path or ""),
                "blocking_reasons": list(stage_audit.get("blocking_reasons") or []),
            },
            "execution_evidence": execution_review,
        },
        "blocking_reasons": blocking,
        "hard_boundaries": [
            "do_not_publish_formal_top_list_from_candidate_discussion",
            "do_not_promote_without_execution_fact_chain",
            "do_not_reuse_rejected_artifacts_as_runtime_default",
        ],
    }
    artifacts = _write_artifacts(Path(output_dir), payload)
    payload["artifacts"] = artifacts
    _write_json(Path(artifacts["json"]), payload)
    return payload


def _blocking_reasons(
    *,
    diagnostics: JsonDict,
    credibility: JsonDict,
    rejected_matches: List[JsonDict],
    stage_audit: JsonDict,
    execution_evidence: JsonDict,
) -> List[str]:
    blocking: List[str] = []
    if diagnostics.get("eligible_for_formal_ranking") is not True:
        blocking.append("backtest_not_eligible_for_formal_ranking")
    if diagnostics.get("credible_evidence_present") is not True:
        blocking.append("backtest_credibility_not_passed")
    if diagnostics.get("quality_floor_passed") is not True:
        blocking.append("backtest_quality_floor_not_passed")
    if not credibility:
        blocking.append("missing_backtest_credibility")
    if rejected_matches:
        blocking.append("sweep_artifact_present_in_rejected_ledger")
    if any(item.get("reused_as_runtime_default") is True for item in rejected_matches):
        blocking.append("rejected_artifact_reused_as_runtime_default")
    if stage_audit.get("passed") is not True:
        blocking.append("stage_audit_not_passed")
    if execution_evidence.get("passed") is not True:
        blocking.append("execution_fact_chain_missing")
    return blocking


def _normalize_execution_evidence(execution_evidence: JsonDict | None) -> JsonDict:
    if not isinstance(execution_evidence, dict) or not execution_evidence:
        return {
            "present": False,
            "passed": False,
            "missing_fields": list(REQUIRED_EXECUTION_EVIDENCE_FIELDS),
            "blocking_reasons": ["execution_evidence_missing"],
        }
    blocking = list(execution_evidence.get("blocking_reasons") or [])
    missing = list(blocking)
    return {
        **execution_evidence,
        "present": True,
        "passed": bool(execution_evidence.get("passed") is True and not blocking),
        "missing_fields": missing,
    }


def _matching_rejected_artifacts(rejected: Iterable[JsonDict], *, artifact_path: str, strategy: str) -> List[JsonDict]:
    target_path = str(artifact_path or "")
    target_strategy = str(strategy or "").lower()
    return [
        dict(item)
        for item in rejected or []
        if str(item.get("artifact_path") or "") == target_path and str(item.get("strategy") or "").lower() == target_strategy
    ]


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
    strategy = str(payload.get("strategy") or "unknown")
    json_path = output_dir / f"promotion_decision_{strategy}_{ts}.json"
    md_path = output_dir / f"promotion_decision_{strategy}_{ts}.md"
    _write_json(json_path, payload)
    _write_markdown(md_path, payload)
    return {"json": str(json_path), "markdown": str(md_path)}


def _write_markdown(path: Path, payload: JsonDict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    execution_gap = ((payload.get("evidence") or {}).get("execution_evidence") or {})
    lines = [
        "# Promotion Decision Artifact",
        "",
        f"- decision_id: `{payload.get('decision_id')}`",
        f"- strategy: `{payload.get('strategy')}`",
        f"- decision_type: `{payload.get('decision_type')}`",
        f"- status: `{payload.get('status')}`",
        f"- production_candidate_allowed: `{payload.get('production_candidate_allowed')}`",
        f"- linked_run_id: `{payload.get('linked_run_id')}`",
        "",
        "## Blocking Reasons",
        "",
    ]
    blocking = payload.get("blocking_reasons") or []
    lines.extend([f"- `{item}`" for item in blocking] if blocking else ["- none"])
    lines.extend(["", "## Missing Execution Evidence", ""])
    missing = execution_gap.get("missing_fields") or []
    lines.extend([f"- `{item}`" for item in missing] if missing else ["- none"])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_json(path: Path, payload: JsonDict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
