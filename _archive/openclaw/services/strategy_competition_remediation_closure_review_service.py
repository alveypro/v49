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


def _submission_closures(submission: JsonDict) -> Dict[str, JsonDict]:
    closures: Dict[str, JsonDict] = {}
    for item in submission.get("item_closures") or []:
        if isinstance(item, dict):
            artifact = _clean_text(item.get("artifact"))
            if artifact:
                closures[artifact] = item
    return closures


def _validator_artifact_passed(path: str) -> bool:
    payload = _load_json(path)
    return payload.get("passed") is True if payload else False


def _closure_reviews(work_order: JsonDict, submission: JsonDict) -> List[JsonDict]:
    closures = _submission_closures(submission)
    reviews: List[JsonDict] = []
    for item in work_order.get("work_items") or []:
        if not isinstance(item, dict):
            continue
        artifact = _clean_text(item.get("artifact"))
        closure = closures.get(artifact) or {}
        validator_artifact = _clean_text(closure.get("validator_artifact"))
        validator_hash = _hash_file(validator_artifact)
        reasons: List[str] = []
        if not closure:
            reasons.append("closure_missing")
        if closure and _clean_text(closure.get("validator_tool")) != _clean_text(item.get("validator_tool")):
            reasons.append("validator_tool_mismatch")
        if closure and not validator_artifact:
            reasons.append("validator_artifact_missing")
        if closure and validator_artifact and not validator_hash:
            reasons.append("validator_artifact_file_missing")
        if closure and validator_artifact and validator_hash and _clean_text(closure.get("validator_artifact_hash")) not in ("", validator_hash):
            reasons.append("validator_artifact_hash_mismatch")
        if closure and closure.get("validator_passed") is not True:
            reasons.append("validator_not_passed")
        if closure and validator_artifact and validator_hash and not _validator_artifact_passed(validator_artifact):
            reasons.append("validator_artifact_payload_not_passed")
        closed = not reasons
        reviews.append(
            {
                "artifact": artifact,
                "owner_role": _clean_text(item.get("owner_role")),
                "validator_tool": _clean_text(item.get("validator_tool")),
                "validator_artifact": validator_artifact,
                "validator_artifact_hash": validator_hash,
                "closure_status": "closed" if closed else "blocked",
                "closed": closed,
                "blocking_reasons": reasons,
            }
        )
    return reviews


def build_strategy_competition_remediation_closure_review(
    conn: sqlite3.Connection,
    *,
    work_order_artifact_path: str | Path,
    closure_submission_path: str | Path = "",
    output_dir: str | Path,
    operator_name: str = "strategy_competition_remediation_closure_review",
) -> JsonDict:
    """Review whether remediation work-order items were closed by their designated validators."""

    apply_professional_migrations(conn)
    work_order = _load_json(work_order_artifact_path)
    submission = _load_json(closure_submission_path)
    work_order_hash = _hash_file(work_order_artifact_path)
    submission_hash = _hash_file(closure_submission_path)
    reviews = _closure_reviews(work_order, submission)
    blocking_reasons: List[str] = []
    if not submission:
        blocking_reasons.append("closure_submission_missing")
    if submission and _clean_text(submission.get("work_order_hash")) != _clean_text(work_order.get("work_order_hash")):
        blocking_reasons.append("work_order_hash_mismatch")
    if submission and _clean_text(submission.get("source_manifest_hash")) != _clean_text(work_order.get("source_manifest_hash")):
        blocking_reasons.append("source_manifest_hash_mismatch")
    for review in reviews:
        for reason in review.get("blocking_reasons") or []:
            blocking_reasons.append(f"{review['artifact']}:{reason}")
    accepted = not blocking_reasons and bool(reviews or work_order.get("work_order_status") == "no_remediation_required")
    payload: JsonDict = {
        "artifact_version": "strategy_competition_remediation_closure_review.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": _clean_text(work_order.get("competition_run_id")),
        "closure_review_status": "remediation_closure_accepted_for_rerun" if accepted else "remediation_closure_blocked",
        "passed": accepted,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "source_work_order_artifact": str(work_order_artifact_path),
        "source_work_order_hash": work_order_hash,
        "source_manifest_hash": _clean_text(work_order.get("source_manifest_hash")),
        "closure_submission_artifact": str(closure_submission_path or ""),
        "closure_submission_hash": submission_hash,
        "closure_reviews": reviews,
        "closed_work_item_count": sum(1 for item in reviews if item.get("closed") is True),
        "blocked_work_item_count": sum(1 for item in reviews if item.get("closed") is not True),
        "blocking_reasons": blocking_reasons,
        "allowed_next_actions": (
            ["rerun_formal_validators_then_rebuild_manifest_and_court_of_record"]
            if accepted
            else ["complete_remediation_closure_submission_for_all_work_items"]
        ),
        "closure_review_contract": {
            "requires_work_order_hash_match": True,
            "requires_source_manifest_hash_match": True,
            "requires_designated_validator_artifacts": True,
            "accepted_closure_only_allows_formal_rerun": True,
            "does_not_create_production_eligibility": True,
            "does_not_create_live_order_authority": True,
        },
        "hard_boundaries": [
            "closure_review_is_not_formal_validation_pass",
            "accepted_closure_review_requires_rerun_of_formal_validators",
            "closure_review_cannot_create_production_or_live_order_authority",
            "partial_closure_submission_cannot_advance_release_chain",
        ],
    }
    payload["closure_review_hash"] = sha256(
        canonical_json(
            {
                "competition_run_id": payload["competition_run_id"],
                "source_work_order_hash": payload["source_work_order_hash"],
                "closure_submission_hash": payload["closure_submission_hash"],
                "closure_reviews": payload["closure_reviews"],
                "closure_review_status": payload["closure_review_status"],
            }
        ).encode("utf-8")
    ).hexdigest()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_remediation_closure_review_{payload['competition_run_id'] or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload
