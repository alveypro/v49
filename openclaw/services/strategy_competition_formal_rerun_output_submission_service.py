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
    text = _clean_text(path)
    if not text:
        return {}
    file_path = Path(text)
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


def _expected_steps(plan: JsonDict) -> List[JsonDict]:
    steps: List[JsonDict] = []
    for item in plan.get("rerun_steps") or []:
        if isinstance(item, dict):
            step = _clean_text(item.get("step"))
            if step:
                steps.append(item)
    return steps


def _submitted_outputs(submission: JsonDict) -> Dict[str, JsonDict]:
    outputs: Dict[str, JsonDict] = {}
    for item in submission.get("rerun_outputs") or []:
        if isinstance(item, dict):
            step = _clean_text(item.get("step"))
            if step:
                outputs[step] = item
    return outputs


def _output_reviews(plan: JsonDict, submission: JsonDict) -> List[JsonDict]:
    plan_steps = _expected_steps(plan)
    outputs = _submitted_outputs(submission)
    reviews: List[JsonDict] = []
    previous_passed = True
    for plan_step in plan_steps:
        step = _clean_text(plan_step.get("step"))
        output = outputs.get(step) or {}
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
                "step": step,
                "command": _clean_text(plan_step.get("command")),
                "artifact": artifact,
                "artifact_hash": artifact_hash,
                "output_status": "passed" if passed else "blocked",
                "passed": passed,
                "blocking_reasons": reasons,
            }
        )
    return reviews


def build_strategy_competition_formal_rerun_output_submission(
    conn: sqlite3.Connection,
    *,
    rerun_plan_artifact_path: str | Path,
    step_output_artifact_paths: Dict[str, str],
    output_dir: str | Path,
    operator_name: str = "strategy_competition_formal_rerun_output_submission",
) -> JsonDict:
    """Package rerun step outputs in the fixed order required by formal rerun review."""

    apply_professional_migrations(conn)
    plan = _load_json(rerun_plan_artifact_path)
    plan_hash = _hash_file(rerun_plan_artifact_path)
    submitted_outputs: List[JsonDict] = []
    for step, path in step_output_artifact_paths.items():
        artifact = _clean_text(path)
        artifact_hash = _hash_file(artifact)
        artifact_payload = _load_json(artifact)
        submitted_outputs.append(
            {
                "step": step,
                "artifact": artifact,
                "artifact_hash": artifact_hash,
                "passed": artifact_payload.get("passed") is True,
            }
        )
    reviews = _output_reviews(plan, {"rerun_outputs": submitted_outputs})
    blocking_reasons: List[str] = []
    if plan.get("rerun_plan_status") != "formal_rerun_plan_ready" or plan.get("passed") is not True:
        blocking_reasons.append("formal_rerun_plan_not_ready")
    if not plan_hash:
        blocking_reasons.append("rerun_plan_artifact_missing")
    if not step_output_artifact_paths:
        blocking_reasons.append("rerun_output_submission_missing")
    expected_steps = [step.get("step") for step in _expected_steps(plan)]
    submitted_steps = list(step_output_artifact_paths.keys())
    if expected_steps and submitted_steps != expected_steps:
        blocking_reasons.append("rerun_output_submission_step_order_invalid")
    for review in reviews:
        for reason in review.get("blocking_reasons") or []:
            blocking_reasons.append(f"{review['step']}:{reason}")
    ready = not blocking_reasons and bool(reviews)
    payload: JsonDict = {
        "artifact_version": "strategy_competition_formal_rerun_output_submission.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": _clean_text(plan.get("competition_run_id")),
        "source_rerun_plan_artifact": str(rerun_plan_artifact_path),
        "source_rerun_plan_hash": plan_hash,
        "source_manifest_hash": _clean_text(plan.get("source_manifest_hash")),
        "rerun_output_submission_status": "formal_rerun_output_submission_ready" if ready else "formal_rerun_output_submission_blocked",
        "passed": ready,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "rerun_outputs": reviews,
        "blocking_reasons": blocking_reasons,
        "allowed_next_actions": (
            ["submit_to_formal_rerun_result_review"]
            if ready
            else ["complete_formal_rerun_outputs_in_fixed_order"]
        ),
        "rerun_output_submission_contract": {
            "requires_ready_formal_rerun_plan": True,
            "requires_fixed_step_order": True,
            "requires_each_output_payload_passed": True,
            "does_not_create_production_eligibility": True,
            "does_not_create_live_order_authority": True,
        },
        "hard_boundaries": [
            "formal_rerun_output_submission_is_not_result_review",
            "partial_rerun_outputs_cannot_be_packaged_as_review",
            "formal_rerun_output_submission_does_not_create_production_or_live_order_authority",
        ],
    }
    payload["rerun_output_submission_hash"] = sha256(
        canonical_json(
            {
                "competition_run_id": payload["competition_run_id"],
                "source_rerun_plan_hash": payload["source_rerun_plan_hash"],
                "source_manifest_hash": payload["source_manifest_hash"],
                "rerun_output_submission_status": payload["rerun_output_submission_status"],
                "rerun_outputs": payload["rerun_outputs"],
            }
        ).encode("utf-8")
    ).hexdigest()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_formal_rerun_output_submission_{payload['competition_run_id'] or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload
