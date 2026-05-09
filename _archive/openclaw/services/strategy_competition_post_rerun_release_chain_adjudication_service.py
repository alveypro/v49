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


def build_strategy_competition_post_rerun_release_chain_adjudication(
    conn: sqlite3.Connection,
    *,
    post_rerun_release_readiness_artifact_path: str | Path,
    post_rerun_live_authority_review_artifact_path: str | Path = "",
    output_dir: str | Path,
    operator_name: str = "strategy_competition_post_rerun_release_chain_adjudication",
) -> JsonDict:
    """Adjudicate the post-rerun release path without granting broker authority."""

    apply_professional_migrations(conn)
    readiness = _load_json(post_rerun_release_readiness_artifact_path)
    authority = _load_json(post_rerun_live_authority_review_artifact_path)
    readiness_hash = _hash_file(post_rerun_release_readiness_artifact_path)
    authority_hash = _hash_file(post_rerun_live_authority_review_artifact_path)
    expected_readiness_hash = _clean_text(readiness.get("release_readiness_hash"))
    blocking_reasons: List[str] = []

    if readiness.get("release_readiness_status") != "post_rerun_release_ready_for_live_authority_check" or readiness.get("passed") is not True:
        blocking_reasons.append("post_rerun_release_readiness_not_ready")
    if not authority:
        blocking_reasons.append("live_authority_review_artifact_missing")
    elif authority.get("live_authority_review_status") != "post_rerun_live_authority_ready_for_broker_guard" or authority.get("passed") is not True:
        blocking_reasons.append("post_rerun_live_authority_review_not_ready")
    if authority and not _lineage_matches(authority, expected_readiness_hash):
        blocking_reasons.append("post_rerun_live_authority_readiness_hash_mismatch")
    if authority.get("live_order_authority_granted") is True or authority.get("broker_submission_allowed") is True:
        blocking_reasons.append("post_rerun_release_chain_attempted_permission_grant")

    ready = not blocking_reasons
    payload: JsonDict = {
        "artifact_version": "strategy_competition_post_rerun_release_chain_adjudication.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": _clean_text(readiness.get("competition_run_id")) or _clean_text(authority.get("competition_run_id")),
        "release_chain_status": "post_rerun_release_chain_ready_for_broker_guard" if ready else "post_rerun_release_chain_blocked",
        "passed": ready,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "broker_submission_allowed": False,
        "source_post_rerun_release_readiness_artifact": str(post_rerun_release_readiness_artifact_path),
        "source_post_rerun_release_readiness_hash": readiness_hash,
        "expected_release_readiness_hash": expected_readiness_hash,
        "post_rerun_live_authority_review_artifact": str(post_rerun_live_authority_review_artifact_path or ""),
        "post_rerun_live_authority_review_hash": authority_hash,
        "blocking_reasons": blocking_reasons,
        "allowed_next_actions": (
            ["run_broker_submission_guard_with_matching_live_authority_hash"]
            if ready
            else ["complete_post_rerun_release_readiness_and_live_authority_review"]
        ),
        "release_chain_contract": {
            "requires_post_rerun_release_readiness_ready": True,
            "requires_post_rerun_live_authority_review_ready": True,
            "requires_readiness_hash_lineage": True,
            "does_not_create_new_live_order_authority": True,
            "does_not_call_broker_adapter": True,
        },
        "hard_boundaries": [
            "post_rerun_release_chain_adjudication_is_not_broker_submission",
            "release_chain_adjudication_does_not_execute_orders",
            "broker_submission_requires_separate_guard_and_response",
            "execution_and_post_trade_feedback_remain_required",
        ],
    }
    payload["release_chain_hash"] = sha256(
        canonical_json(
            {
                "competition_run_id": payload["competition_run_id"],
                "source_post_rerun_release_readiness_hash": payload["source_post_rerun_release_readiness_hash"],
                "post_rerun_live_authority_review_hash": payload["post_rerun_live_authority_review_hash"],
                "release_chain_status": payload["release_chain_status"],
                "blocking_reasons": payload["blocking_reasons"],
            }
        ).encode("utf-8")
    ).hexdigest()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_post_rerun_release_chain_adjudication_{payload['competition_run_id'] or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload
