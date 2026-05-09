from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from typing import Any, Dict, List

from openclaw.services.lineage_service import apply_professional_migrations, canonical_json


JsonDict = Dict[str, Any]


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _load_json(path: str | Path) -> JsonDict:
    if not _clean_text(path):
        return {}
    file_path = Path(path)
    if not file_path.exists() or not file_path.is_file():
        return {}
    payload = json.loads(file_path.read_text(encoding="utf-8"))
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


def _submitted_outputs(submission: JsonDict) -> Dict[str, JsonDict]:
    outputs: Dict[str, JsonDict] = {}
    for item in submission.get("rerun_outputs") or []:
        if isinstance(item, dict):
            step = _clean_text(item.get("step"))
            if step:
                outputs[step] = item
    return outputs


def _step_reviews(plan: JsonDict, submission: JsonDict) -> List[JsonDict]:
    outputs = _submitted_outputs(submission)
    reviews: List[JsonDict] = []
    previous_passed = True
    for step in plan.get("rerun_steps") or []:
        if not isinstance(step, dict):
            continue
        name = _clean_text(step.get("step"))
        output = outputs.get(name) or {}
        artifact = _clean_text(output.get("artifact"))
        artifact_hash = _hash_file(artifact)
        artifact_payload = _load_json(artifact)
        reasons: List[str] = []
        if not previous_passed:
            reasons.append("previous_step_not_passed")
        if not output:
            reasons.append("rerun_output_missing")
        if output and not artifact:
            reasons.append("rerun_output_artifact_missing")
        if output and artifact and not artifact_hash:
            reasons.append("rerun_output_artifact_file_missing")
        if output and artifact and artifact_hash and _clean_text(output.get("artifact_hash")) not in ("", artifact_hash):
            reasons.append("rerun_output_artifact_hash_mismatch")
        if output and output.get("passed") is not True:
            reasons.append("rerun_output_not_passed")
        if output and artifact and artifact_hash and artifact_payload.get("passed") is not True:
            reasons.append("rerun_output_payload_not_passed")
        passed = not reasons
        previous_passed = previous_passed and passed
        reviews.append(
            {
                "step": name,
                "command": _clean_text(step.get("command")),
                "artifact": artifact,
                "artifact_hash": artifact_hash,
                "step_status": "passed" if passed else "blocked",
                "passed": passed,
                "blocking_reasons": reasons,
            }
        )
    return reviews


def build_strategy_competition_formal_rerun_result_review(
    conn: sqlite3.Connection,
    *,
    rerun_plan_artifact_path: str | Path,
    rerun_output_submission_path: str | Path = "",
    output_dir: str | Path,
    operator_name: str = "strategy_competition_formal_rerun_result_review",
) -> JsonDict:
    """Review submitted outputs from a formal rerun plan without granting release authority."""

    apply_professional_migrations(conn)
    plan = _load_json(rerun_plan_artifact_path)
    submission = _load_json(rerun_output_submission_path)
    plan_hash = _hash_file(rerun_plan_artifact_path)
    submission_hash = _hash_file(rerun_output_submission_path)
    step_reviews = _step_reviews(plan, submission) if plan.get("rerun_plan_status") == "formal_rerun_plan_ready" else []
    blocking_reasons: List[str] = []
    if plan.get("rerun_plan_status") != "formal_rerun_plan_ready" or plan.get("passed") is not True:
        blocking_reasons.append("formal_rerun_plan_not_ready")
    if not submission:
        blocking_reasons.append("rerun_output_submission_missing")
    if submission and _clean_text(submission.get("rerun_plan_hash")) != _clean_text(plan.get("rerun_plan_hash")):
        blocking_reasons.append("rerun_plan_hash_mismatch")
    for review in step_reviews:
        for reason in review.get("blocking_reasons") or []:
            blocking_reasons.append(f"{review['step']}:{reason}")
    accepted = not blocking_reasons and bool(step_reviews)
    payload: JsonDict = {
        "artifact_version": "strategy_competition_formal_rerun_result_review.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": _clean_text(plan.get("competition_run_id")),
        "rerun_result_review_status": "formal_rerun_results_accepted" if accepted else "formal_rerun_results_blocked",
        "passed": accepted,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "source_rerun_plan_artifact": str(rerun_plan_artifact_path),
        "source_rerun_plan_hash": plan_hash,
        "source_manifest_hash": _clean_text(plan.get("source_manifest_hash")),
        "rerun_output_submission_artifact": str(rerun_output_submission_path or ""),
        "rerun_output_submission_hash": submission_hash,
        "step_reviews": step_reviews,
        "passed_step_count": sum(1 for item in step_reviews if item.get("passed") is True),
        "blocked_step_count": sum(1 for item in step_reviews if item.get("passed") is not True),
        "blocking_reasons": blocking_reasons,
        "allowed_next_actions": (
            ["rebuild_evidence_chain_manifest_and_release_chain_court_of_record"]
            if accepted
            else ["complete_formal_rerun_outputs_in_fixed_order"]
        ),
        "rerun_result_review_contract": {
            "requires_ready_formal_rerun_plan": True,
            "requires_rerun_plan_hash_match": True,
            "requires_all_step_outputs_in_fixed_order": True,
            "requires_each_step_payload_passed": True,
            "does_not_create_production_eligibility": True,
            "does_not_create_live_order_authority": True,
        },
        "hard_boundaries": [
            "formal_rerun_result_review_is_not_release_approval",
            "accepted_rerun_results_require_manifest_and_court_of_record_rebuild",
            "partial_rerun_outputs_cannot_advance_release_chain",
            "rerun_result_review_cannot_create_production_or_live_order_authority",
        ],
    }
    payload["rerun_result_review_hash"] = sha256(
        canonical_json(
            {
                "competition_run_id": payload["competition_run_id"],
                "source_rerun_plan_hash": payload["source_rerun_plan_hash"],
                "rerun_output_submission_hash": payload["rerun_output_submission_hash"],
                "step_reviews": payload["step_reviews"],
                "rerun_result_review_status": payload["rerun_result_review_status"],
            }
        ).encode("utf-8")
    ).hexdigest()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_formal_rerun_result_review_{payload['competition_run_id'] or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload
