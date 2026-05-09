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


def build_strategy_competition_post_rerun_broker_response_review(
    conn: sqlite3.Connection,
    *,
    post_rerun_broker_guard_review_artifact_path: str | Path,
    broker_response_submission_artifact_path: str | Path = "",
    broker_submission_response_evidence_artifact_path: str | Path = "",
    output_dir: str | Path,
    operator_name: str = "strategy_competition_post_rerun_broker_response_review",
) -> JsonDict:
    """Review post-rerun broker response evidence without treating it as execution fills."""

    apply_professional_migrations(conn)
    guard_review = _load_json(post_rerun_broker_guard_review_artifact_path)
    submission = _load_json(broker_response_submission_artifact_path)
    response = _load_json(broker_submission_response_evidence_artifact_path)
    guard_review_hash = _hash_file(post_rerun_broker_guard_review_artifact_path)
    submission_hash = _hash_file(broker_response_submission_artifact_path)
    response_hash = _hash_file(broker_submission_response_evidence_artifact_path)
    expected_guard_hash = _clean_text(guard_review.get("broker_submission_guard_hash"))
    if submission:
        expected_guard_hash = (
            _clean_text(submission.get("expected_broker_submission_guard_hash"))
            or _clean_text(submission.get("source_post_rerun_broker_guard_review_hash"))
            or expected_guard_hash
        )
        broker_submission_response_evidence_artifact_path = _clean_text(
            submission.get("source_broker_submission_response_evidence_artifact")
            or broker_submission_response_evidence_artifact_path
        )
    response = _load_json(broker_submission_response_evidence_artifact_path)
    response_hash = _hash_file(broker_submission_response_evidence_artifact_path)
    response_hashes = response.get("source_artifact_hashes") if isinstance(response.get("source_artifact_hashes"), dict) else {}
    response_guard_hash = _clean_text(response_hashes.get("broker_submission_guard"))
    blocking_reasons: List[str] = []

    if guard_review.get("broker_guard_review_status") != "post_rerun_broker_guard_ready_for_adapter" or guard_review.get("passed") is not True:
        blocking_reasons.append("post_rerun_broker_guard_review_not_ready")
    if broker_response_submission_artifact_path and not submission:
        blocking_reasons.append("broker_response_submission_missing")
    if submission and str(submission.get("broker_response_submission_status") or "") != "post_rerun_broker_response_submission_ready":
        blocking_reasons.append("broker_response_submission_not_ready")
    if (
        submission
        and _clean_text(submission.get("source_post_rerun_broker_guard_review_hash"))
        and _clean_text(submission.get("source_post_rerun_broker_guard_review_hash")) != guard_review_hash
    ):
        blocking_reasons.append("broker_response_submission_guard_review_hash_mismatch")
    if not response:
        blocking_reasons.append("broker_submission_response_evidence_missing")
    elif response.get("response_status") != "broker_submission_response_accepted" or response.get("passed") is not True:
        blocking_reasons.append("broker_submission_response_evidence_not_accepted")
    if response and expected_guard_hash and response_guard_hash != expected_guard_hash:
        blocking_reasons.append("broker_response_guard_hash_mismatch")
    if response.get("execution_fills_confirmed") is True:
        blocking_reasons.append("broker_response_attempted_fill_confirmation")
    for order in response.get("order_responses") or []:
        if isinstance(order, dict) and order.get("fills"):
            blocking_reasons.append("broker_response_contains_fills")
            break

    ready = not blocking_reasons
    payload: JsonDict = {
        "artifact_version": "strategy_competition_post_rerun_broker_response_review.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": _clean_text(guard_review.get("competition_run_id")) or _clean_text(response.get("competition_run_id")),
        "broker_response_review_status": "post_rerun_broker_response_ready_for_execution_feedback" if ready else "post_rerun_broker_response_blocked",
        "passed": ready,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "broker_submission_confirmed": bool(ready and response.get("broker_submission_confirmed") is True),
        "execution_fills_confirmed": False,
        "post_trade_reconciliation_passed": False,
        "source_post_rerun_broker_guard_review_artifact": str(post_rerun_broker_guard_review_artifact_path),
        "source_post_rerun_broker_guard_review_hash": guard_review_hash,
        "source_broker_response_submission_artifact": str(broker_response_submission_artifact_path or ""),
        "source_broker_response_submission_hash": submission_hash,
        "expected_broker_submission_guard_hash": expected_guard_hash,
        "broker_submission_response_evidence_artifact": str(broker_submission_response_evidence_artifact_path or ""),
        "broker_submission_response_evidence_hash": response_hash,
        "blocking_reasons": blocking_reasons,
        "allowed_next_actions": (
            ["record_broker_execution_feedback_with_matching_response_hash"]
            if ready
            else ["complete_post_rerun_broker_guard_review_and_broker_response_evidence"]
        ),
        "broker_response_review_contract": {
            "requires_post_rerun_broker_guard_review_ready": True,
            "requires_broker_response_submission_ready": True,
            "requires_broker_submission_response_accepted": True,
            "requires_broker_guard_hash_lineage": True,
            "does_not_confirm_fills": True,
            "does_not_create_post_trade_reconciliation": True,
        },
        "hard_boundaries": [
            "post_rerun_broker_response_review_is_not_execution_feedback",
            "broker_submission_confirmed_does_not_mean_filled",
            "fills_require_separate_execution_feedback",
            "post_trade_reconciliation_remains_required_after_execution_feedback",
        ],
    }
    payload["broker_response_review_hash"] = sha256(
        canonical_json(
            {
                "competition_run_id": payload["competition_run_id"],
                "source_post_rerun_broker_guard_review_hash": payload["source_post_rerun_broker_guard_review_hash"],
                "source_broker_response_submission_hash": payload["source_broker_response_submission_hash"],
                "broker_submission_response_evidence_hash": payload["broker_submission_response_evidence_hash"],
                "broker_response_review_status": payload["broker_response_review_status"],
                "blocking_reasons": payload["blocking_reasons"],
            }
        ).encode("utf-8")
    ).hexdigest()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_post_rerun_broker_response_review_{payload['competition_run_id'] or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload
