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


def _lineage_matches(payload: JsonDict, expected_hash: str) -> bool:
    if not expected_hash:
        return False
    hashes = payload.get("source_artifact_hashes") if isinstance(payload.get("source_artifact_hashes"), dict) else {}
    candidates = (
        payload.get("source_post_rerun_live_authority_review_hash"),
        payload.get("live_authority_review_hash"),
        hashes.get("post_rerun_live_authority_review"),
    )
    return any(_clean_text(item) == expected_hash for item in candidates)


def build_strategy_competition_post_rerun_broker_guard_review(
    conn: sqlite3.Connection,
    *,
    post_rerun_live_authority_review_artifact_path: str | Path,
    broker_guard_submission_artifact_path: str | Path = "",
    broker_submission_guard_artifact_path: str | Path = "",
    output_dir: str | Path,
    operator_name: str = "strategy_competition_post_rerun_broker_guard_review",
) -> JsonDict:
    """Review broker submission guard after post-rerun live authority without submitting orders."""

    apply_professional_migrations(conn)
    authority_review = _load_json(post_rerun_live_authority_review_artifact_path)
    submission = _load_json(broker_guard_submission_artifact_path)
    authority_review_file_hash = _hash_file(post_rerun_live_authority_review_artifact_path)
    submission_hash = _hash_file(broker_guard_submission_artifact_path)
    expected_authority_review_hash = _clean_text(authority_review.get("live_authority_review_hash"))
    if submission:
        expected_authority_review_hash = _clean_text(submission.get("source_post_rerun_live_authority_review_hash")) or expected_authority_review_hash
        broker_submission_guard_artifact_path = _clean_text(submission.get("source_broker_submission_guard_artifact") or broker_submission_guard_artifact_path)
    guard = _load_json(broker_submission_guard_artifact_path)
    guard_hash = _hash_file(broker_submission_guard_artifact_path)
    blocking_reasons: List[str] = []

    if authority_review.get("live_authority_review_status") != "post_rerun_live_authority_ready_for_broker_guard" or authority_review.get("passed") is not True:
        blocking_reasons.append("post_rerun_live_authority_review_not_ready")
    if broker_guard_submission_artifact_path and not submission:
        blocking_reasons.append("post_rerun_broker_guard_submission_missing")
    if submission and str(submission.get("broker_guard_submission_status") or "") != "post_rerun_broker_guard_submission_ready":
        blocking_reasons.append("post_rerun_broker_guard_submission_not_ready")
    if submission and _clean_text(submission.get("source_post_rerun_live_authority_review_hash")) and _clean_text(submission.get("source_post_rerun_live_authority_review_hash")) != _clean_text(authority_review.get("live_authority_review_hash")):
        blocking_reasons.append("post_rerun_broker_guard_submission_authority_hash_mismatch")
    if not guard:
        blocking_reasons.append("broker_submission_guard_artifact_missing")
    elif guard.get("guard_status") != "broker_submission_guard_passed" or guard.get("passed") is not True:
        blocking_reasons.append("broker_submission_guard_not_passed")
    if guard and not _lineage_matches(guard, expected_authority_review_hash):
        blocking_reasons.append("broker_guard_live_authority_review_hash_mismatch")
    for field in ("broker_adapter", "idempotency_key", "submission_mode"):
        if guard and not _clean_text(guard.get(field)):
            blocking_reasons.append(f"broker_guard_{field}_missing")
    if guard.get("broker_submission_confirmed") is True or guard.get("execution_fills_confirmed") is True:
        blocking_reasons.append("broker_guard_attempted_submission_or_fill_confirmation")

    ready = not blocking_reasons
    payload: JsonDict = {
        "artifact_version": "strategy_competition_post_rerun_broker_guard_review.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": _clean_text(authority_review.get("competition_run_id")),
        "broker_guard_review_status": "post_rerun_broker_guard_ready_for_adapter" if ready else "post_rerun_broker_guard_blocked",
        "passed": ready,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "broker_submission_allowed": False,
        "broker_submission_confirmed": False,
        "execution_fills_confirmed": False,
        "source_post_rerun_live_authority_review_artifact": str(post_rerun_live_authority_review_artifact_path),
        "source_post_rerun_live_authority_review_hash": authority_review_file_hash,
        "source_broker_guard_submission_artifact": str(broker_guard_submission_artifact_path or ""),
        "source_broker_guard_submission_hash": submission_hash,
        "expected_live_authority_review_hash": expected_authority_review_hash,
        "broker_submission_guard_artifact": str(broker_submission_guard_artifact_path or ""),
        "broker_submission_guard_hash": guard_hash,
        "blocking_reasons": blocking_reasons,
        "allowed_next_actions": (
            ["call_broker_adapter_then_record_broker_submission_response"]
            if ready
            else ["submit_broker_guard_pack_with_matching_live_authority_review_hash"]
        ),
        "broker_guard_review_contract": {
            "requires_post_rerun_live_authority_review_ready": True,
            "requires_broker_guard_submission_ready": True,
            "requires_broker_submission_guard_passed": True,
            "requires_live_authority_review_hash_lineage": True,
            "does_not_call_broker_adapter": True,
            "does_not_confirm_submission_or_fills": True,
        },
        "hard_boundaries": [
            "post_rerun_broker_guard_review_is_not_submission_pack",
            "post_rerun_broker_guard_review_is_not_broker_response",
            "broker_guard_review_does_not_confirm_submission",
            "broker_response_execution_feedback_and_post_trade_reconciliation_remain_required",
            "submitted_or_guard_passed_does_not_equal_filled",
        ],
    }
    payload["broker_guard_review_hash"] = sha256(
        canonical_json(
            {
                "competition_run_id": payload["competition_run_id"],
                "source_post_rerun_live_authority_review_hash": payload["source_post_rerun_live_authority_review_hash"],
                "source_broker_guard_submission_hash": payload["source_broker_guard_submission_hash"],
                "broker_submission_guard_hash": payload["broker_submission_guard_hash"],
                "broker_guard_review_status": payload["broker_guard_review_status"],
                "blocking_reasons": payload["blocking_reasons"],
            }
        ).encode("utf-8")
    ).hexdigest()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_post_rerun_broker_guard_review_{payload['competition_run_id'] or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload
