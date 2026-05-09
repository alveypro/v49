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
    candidates = (
        payload.get("source_post_rerun_release_readiness_hash"),
        payload.get("release_readiness_hash"),
        payload.get("source_artifact_hashes", {}).get("post_rerun_release_readiness")
        if isinstance(payload.get("source_artifact_hashes"), dict)
        else "",
    )
    return any(_clean_text(item) == expected_hash for item in candidates)


def build_strategy_competition_post_rerun_live_authority_review(
    conn: sqlite3.Connection,
    *,
    post_rerun_release_readiness_artifact_path: str | Path,
    live_authority_submission_artifact_path: str | Path = "",
    live_order_authority_artifact_path: str | Path = "",
    output_dir: str | Path,
    operator_name: str = "strategy_competition_post_rerun_live_authority_review",
) -> JsonDict:
    """Review live-order authority after post-rerun release readiness without calling broker."""

    apply_professional_migrations(conn)
    readiness = _load_json(post_rerun_release_readiness_artifact_path)
    submission = _load_json(live_authority_submission_artifact_path)
    readiness_file_hash = _hash_file(post_rerun_release_readiness_artifact_path)
    submission_hash = _hash_file(live_authority_submission_artifact_path)
    expected_readiness_hash = _clean_text(readiness.get("release_readiness_hash"))
    if submission:
        expected_readiness_hash = _clean_text(submission.get("source_post_rerun_release_readiness_hash")) or expected_readiness_hash
        live_order_authority_artifact_path = _clean_text(submission.get("source_live_order_authority_artifact") or live_order_authority_artifact_path)
    authority = _load_json(live_order_authority_artifact_path)
    authority_hash = _hash_file(live_order_authority_artifact_path)
    blocking_reasons: List[str] = []

    if readiness.get("release_readiness_status") != "post_rerun_release_ready_for_live_authority_check" or readiness.get("passed") is not True:
        blocking_reasons.append("post_rerun_release_readiness_not_ready")
    if live_authority_submission_artifact_path and not submission:
        blocking_reasons.append("post_rerun_live_authority_submission_missing")
    if submission and str(submission.get("live_authority_submission_status") or "") != "post_rerun_live_authority_submission_ready":
        blocking_reasons.append("post_rerun_live_authority_submission_not_ready")
    if submission and _clean_text(submission.get("source_post_rerun_release_readiness_hash")) and _clean_text(submission.get("source_post_rerun_release_readiness_hash")) != _clean_text(readiness.get("release_readiness_hash")):
        blocking_reasons.append("post_rerun_live_authority_submission_readiness_hash_mismatch")
    if not authority:
        blocking_reasons.append("live_order_authority_artifact_missing")
    elif authority.get("authority_status") != "live_order_submission_allowed" or authority.get("passed") is not True:
        blocking_reasons.append("live_order_authority_not_allowed")
    if authority and not _lineage_matches(authority, expected_readiness_hash):
        blocking_reasons.append("live_order_authority_readiness_hash_mismatch")
    orders = authority.get("orders") if isinstance(authority.get("orders"), list) else []
    if authority and (not orders or len(orders) > 5):
        blocking_reasons.append("live_order_authority_order_set_invalid")
    if authority.get("broker_submission_allowed") is True or authority.get("broker_submission_confirmed") is True:
        blocking_reasons.append("live_order_authority_attempted_broker_submission")

    allowed = not blocking_reasons
    payload: JsonDict = {
        "artifact_version": "strategy_competition_post_rerun_live_authority_review.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": _clean_text(readiness.get("competition_run_id")),
        "live_authority_review_status": "post_rerun_live_authority_ready_for_broker_guard" if allowed else "post_rerun_live_authority_blocked",
        "passed": allowed,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "broker_submission_allowed": False,
        "source_post_rerun_release_readiness_artifact": str(post_rerun_release_readiness_artifact_path),
        "source_post_rerun_release_readiness_hash": readiness_file_hash,
        "source_live_authority_submission_artifact": str(live_authority_submission_artifact_path or ""),
        "source_live_authority_submission_hash": submission_hash,
        "expected_release_readiness_hash": expected_readiness_hash,
        "live_order_authority_artifact": str(live_order_authority_artifact_path or ""),
        "live_order_authority_hash": authority_hash,
        "order_count": len(orders),
        "blocking_reasons": blocking_reasons,
        "allowed_next_actions": (
            ["run_broker_submission_guard_with_matching_live_authority_hash"]
            if allowed
            else ["submit_live_authority_pack_with_matching_readiness_hash"]
        ),
        "live_authority_review_contract": {
            "requires_post_rerun_release_readiness_ready": True,
            "requires_live_authority_submission_ready": True,
            "requires_live_order_authority_allowed": True,
            "requires_readiness_hash_lineage": True,
            "does_not_create_new_live_order_authority": True,
            "does_not_call_broker_adapter": True,
        },
        "hard_boundaries": [
            "post_rerun_live_authority_review_is_not_submission_pack",
            "post_rerun_live_authority_review_is_not_broker_submission",
            "live_authority_review_does_not_execute_orders",
            "broker_submission_requires_separate_guard_and_response",
            "execution_and_post_trade_feedback_remain_required",
        ],
    }
    payload["live_authority_review_hash"] = sha256(
        canonical_json(
            {
                "competition_run_id": payload["competition_run_id"],
                "source_post_rerun_release_readiness_hash": payload["source_post_rerun_release_readiness_hash"],
                "source_live_authority_submission_hash": payload["source_live_authority_submission_hash"],
                "live_order_authority_hash": payload["live_order_authority_hash"],
                "live_authority_review_status": payload["live_authority_review_status"],
                "blocking_reasons": payload["blocking_reasons"],
            }
        ).encode("utf-8")
    ).hexdigest()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_post_rerun_live_authority_review_{payload['competition_run_id'] or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload
