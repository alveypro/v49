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


def _lineage_matches(payload: JsonDict, expected_hash: str) -> bool:
    if not expected_hash:
        return False
    candidates = (
        payload.get("source_post_rerun_live_authority_review_hash"),
        payload.get("live_authority_review_hash"),
        payload.get("source_artifact_hashes", {}).get("post_rerun_live_authority_review")
        if isinstance(payload.get("source_artifact_hashes"), dict)
        else "",
    )
    return any(_clean_text(item) == expected_hash for item in candidates)


def _guard_item(name: str, path: str | Path, expected_hash: str) -> JsonDict:
    payload = _load_json(path)
    artifact_hash = _hash_file(path)
    reasons: List[str] = []
    if not payload:
        reasons.append(f"{name}_missing")
    if payload and not artifact_hash:
        reasons.append(f"{name}_file_missing")
    if payload and expected_hash and not _lineage_matches(payload, expected_hash):
        reasons.append(f"{name}_authority_review_hash_mismatch")
    if payload and payload.get("broker_submission_confirmed") is True:
        reasons.append(f"{name}_attempted_submission_confirmation")
    if payload and payload.get("execution_fills_confirmed") is True:
        reasons.append(f"{name}_attempted_fill_confirmation")
    if payload and payload.get("broker_adapter") and payload.get("broker_submission_allowed") is True:
        # allowed to say submission is allowed in guard, but not confirmed
        pass
    return {
        "name": name,
        "artifact": str(path or ""),
        "artifact_hash": artifact_hash,
        "artifact_version": _clean_text(payload.get("artifact_version")),
        "status": _clean_text(payload.get("guard_status")) or "missing",
        "passed": not reasons,
        "blocking_reasons": reasons,
    }


def build_strategy_competition_post_rerun_broker_guard_submission(
    conn: sqlite3.Connection,
    *,
    post_rerun_live_authority_review_artifact_path: str | Path,
    broker_submission_guard_artifact_path: str | Path,
    output_dir: str | Path,
    operator_name: str = "strategy_competition_post_rerun_broker_guard_submission",
) -> JsonDict:
    """Package broker guard evidence before review."""

    apply_professional_migrations(conn)
    authority_review = _load_json(post_rerun_live_authority_review_artifact_path)
    guard = _load_json(broker_submission_guard_artifact_path)
    authority_review_hash = _hash_file(post_rerun_live_authority_review_artifact_path)
    guard_hash = _hash_file(broker_submission_guard_artifact_path)
    expected_authority_review_hash = _clean_text(authority_review.get("live_authority_review_hash"))
    items = [
        _guard_item("post_rerun_live_authority_review", post_rerun_live_authority_review_artifact_path, expected_authority_review_hash),
        _guard_item("broker_submission_guard", broker_submission_guard_artifact_path, expected_authority_review_hash),
    ]
    blocking_reasons: List[str] = []
    if authority_review.get("live_authority_review_status") != "post_rerun_live_authority_ready_for_broker_guard" or authority_review.get("passed") is not True:
        blocking_reasons.append("post_rerun_live_authority_review_not_ready")
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
        "artifact_version": "strategy_competition_post_rerun_broker_guard_submission.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": _clean_text(authority_review.get("competition_run_id")),
        "source_post_rerun_live_authority_review_artifact": str(post_rerun_live_authority_review_artifact_path),
        "source_post_rerun_live_authority_review_hash": expected_authority_review_hash,
        "source_post_rerun_live_authority_review_artifact_hash": authority_review_hash,
        "source_broker_submission_guard_artifact": str(broker_submission_guard_artifact_path),
        "source_broker_submission_guard_hash": guard_hash,
        "broker_guard_submission_status": "post_rerun_broker_guard_submission_ready" if ready else "post_rerun_broker_guard_submission_blocked",
        "passed": ready,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "broker_submission_allowed": False,
        "broker_submission_confirmed": False,
        "execution_fills_confirmed": False,
        "broker_guard_inputs": items,
        "blocking_reasons": blocking_reasons,
        "allowed_next_actions": (
            ["submit_to_post_rerun_broker_guard_review"]
            if ready
            else ["complete_post_rerun_live_authority_review_and_broker_submission_guard"]
        ),
        "broker_guard_submission_contract": {
            "requires_post_rerun_live_authority_review_ready": True,
            "requires_broker_submission_guard_passed": True,
            "requires_live_authority_review_hash_lineage": True,
            "does_not_call_broker_adapter": True,
            "does_not_confirm_submission_or_fills": True,
        },
        "hard_boundaries": [
            "post_rerun_broker_guard_submission_is_not_broker_guard_review",
            "broker_guard_submission_does_not_confirm_submission",
            "broker_response_execution_feedback_and_post_trade_reconciliation_remain_required",
            "submitted_or_guard_passed_does_not_equal_filled",
        ],
    }
    payload["broker_guard_submission_hash"] = sha256(
        canonical_json(
            {
                "competition_run_id": payload["competition_run_id"],
                "source_post_rerun_live_authority_review_hash": payload["source_post_rerun_live_authority_review_hash"],
                "source_post_rerun_live_authority_review_artifact_hash": payload["source_post_rerun_live_authority_review_artifact_hash"],
                "source_broker_submission_guard_hash": payload["source_broker_submission_guard_hash"],
                "broker_guard_submission_status": payload["broker_guard_submission_status"],
                "broker_guard_inputs": payload["broker_guard_inputs"],
            }
        ).encode("utf-8")
    ).hexdigest()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_post_rerun_broker_guard_submission_{payload['competition_run_id'] or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload
