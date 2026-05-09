from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from typing import Any, Dict, List

from openclaw.services.lineage_service import apply_professional_migrations, canonical_json


JsonDict = Dict[str, Any]

RERUN_ORDER = (
    "shadow_execution_evidence",
    "independent_validation",
    "operational_controls",
    "competition_audit_rerun",
    "production_readiness",
    "release_chain_adjudication",
    "formal_validation_result_review",
    "evidence_chain_manifest",
    "release_chain_recheck",
)

COMMAND_BY_STEP = {
    "shadow_execution_evidence": "python3 tools/record_strategy_competition_shadow_feedback.py",
    "independent_validation": "python3 tools/build_strategy_competition_independent_validation.py",
    "operational_controls": "python3 tools/build_strategy_competition_operational_controls.py",
    "competition_audit_rerun": "python3 tools/build_current_strategy_competition_audit.py",
    "production_readiness": "python3 tools/build_strategy_competition_production_readiness.py",
    "release_chain_adjudication": "python3 tools/adjudicate_strategy_competition_release_chain.py",
    "formal_validation_result_review": "python3 tools/review_strategy_competition_formal_validation_results.py",
    "evidence_chain_manifest": "python3 tools/build_strategy_competition_evidence_chain_manifest.py",
    "release_chain_recheck": "python3 tools/adjudicate_strategy_competition_release_chain.py",
}


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _load_json(path: str | Path) -> JsonDict:
    if not _clean_text(path):
        return {}
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"json payload must be object: {path}")
    return payload


def _hash_file(path: str | Path) -> str:
    text = _clean_text(path)
    if not text:
        return ""
    file_path = Path(text)
    if not file_path.exists() or not file_path.is_file():
        return ""
    return sha256(file_path.read_bytes()).hexdigest()


def _rerun_steps(closure_review: JsonDict) -> List[JsonDict]:
    closure_hash = _clean_text(closure_review.get("closure_review_hash"))
    source_manifest_hash = _clean_text(closure_review.get("source_manifest_hash"))
    return [
        {
            "step": step,
            "command": COMMAND_BY_STEP[step],
            "requires_closure_review_hash": closure_hash,
            "requires_source_manifest_hash": source_manifest_hash,
            "output_must_be_collected": True,
            "passed_output_required_before_next_step": True,
        }
        for step in RERUN_ORDER
    ]


def build_strategy_competition_formal_rerun_plan(
    conn: sqlite3.Connection,
    *,
    closure_review_artifact_path: str | Path,
    output_dir: str | Path,
    operator_name: str = "strategy_competition_formal_rerun_plan",
) -> JsonDict:
    """Create a deterministic formal rerun plan after remediation closure review."""

    apply_professional_migrations(conn)
    closure_review = _load_json(closure_review_artifact_path)
    closure_review_hash = _hash_file(closure_review_artifact_path)
    ready = (
        closure_review.get("closure_review_status") == "remediation_closure_accepted_for_rerun"
        and closure_review.get("passed") is True
    )
    blocking_reasons = [] if ready else ["remediation_closure_review_not_accepted"]
    payload: JsonDict = {
        "artifact_version": "strategy_competition_formal_rerun_plan.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": _clean_text(closure_review.get("competition_run_id")),
        "rerun_plan_status": "formal_rerun_plan_ready" if ready else "formal_rerun_plan_blocked",
        "passed": ready,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "source_closure_review_artifact": str(closure_review_artifact_path),
        "source_closure_review_hash": closure_review_hash,
        "source_manifest_hash": _clean_text(closure_review.get("source_manifest_hash")),
        "closure_review_status": _clean_text(closure_review.get("closure_review_status")),
        "blocking_reasons": blocking_reasons,
        "rerun_order": list(RERUN_ORDER),
        "rerun_steps": _rerun_steps(closure_review) if ready else [],
        "allowed_next_actions": (
            ["execute_rerun_steps_in_order_and_collect_outputs"]
            if ready
            else ["complete_remediation_closure_review_before_formal_rerun"]
        ),
        "rerun_plan_contract": {
            "requires_accepted_remediation_closure_review": True,
            "requires_closure_review_hash_match": True,
            "requires_fixed_rerun_order": True,
            "each_step_output_must_pass_before_next_step": True,
            "does_not_create_production_eligibility": True,
            "does_not_create_live_order_authority": True,
        },
        "hard_boundaries": [
            "formal_rerun_plan_is_not_validator_pass",
            "ready_rerun_plan_only_allows_sequential_validator_execution",
            "formal_rerun_outputs_must_rebuild_manifest_and_court_of_record",
            "formal_rerun_plan_cannot_create_production_or_live_order_authority",
        ],
    }
    payload["rerun_plan_hash"] = sha256(
        canonical_json(
            {
                "competition_run_id": payload["competition_run_id"],
                "source_closure_review_hash": payload["source_closure_review_hash"],
                "rerun_plan_status": payload["rerun_plan_status"],
                "rerun_steps": payload["rerun_steps"],
            }
        ).encode("utf-8")
    ).hexdigest()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_formal_rerun_plan_{payload['competition_run_id'] or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload
