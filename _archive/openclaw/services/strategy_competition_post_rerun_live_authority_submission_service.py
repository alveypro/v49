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
        payload.get("source_post_rerun_release_readiness_hash"),
        payload.get("release_readiness_hash"),
        payload.get("source_artifact_hashes", {}).get("post_rerun_release_readiness")
        if isinstance(payload.get("source_artifact_hashes"), dict)
        else "",
    )
    return any(_clean_text(item) == expected_hash for item in candidates)


def _authority_item(name: str, path: str | Path, expected_hash: str) -> JsonDict:
    payload = _load_json(path)
    artifact_hash = _hash_file(path)
    reasons: List[str] = []
    if not payload:
        reasons.append(f"{name}_missing")
    if payload and not artifact_hash:
        reasons.append(f"{name}_file_missing")
    if payload and expected_hash and not _lineage_matches(payload, expected_hash):
        reasons.append(f"{name}_readiness_hash_mismatch")
    if payload and payload.get("broker_submission_allowed") is True:
        reasons.append(f"{name}_attempted_broker_submission")
    if payload and payload.get("broker_submission_confirmed") is True:
        reasons.append(f"{name}_attempted_broker_confirmation")
    if payload and payload.get("live_order_authority_granted") is True:
        reasons.append(f"{name}_attempted_live_authority_claim")
    if payload and _clean_text(payload.get("authority_status")) != "live_order_submission_allowed":
        reasons.append(f"{name}_status_invalid")
    return {
        "name": name,
        "artifact": str(path or ""),
        "artifact_hash": artifact_hash,
        "artifact_version": _clean_text(payload.get("artifact_version")),
        "status": _clean_text(payload.get("authority_status")) or "missing",
        "passed": not reasons,
        "blocking_reasons": reasons,
    }


def build_strategy_competition_post_rerun_live_authority_submission(
    conn: sqlite3.Connection,
    *,
    post_rerun_release_readiness_artifact_path: str | Path,
    live_order_authority_artifact_path: str | Path,
    output_dir: str | Path,
    operator_name: str = "strategy_competition_post_rerun_live_authority_submission",
) -> JsonDict:
    """Package the live authority request before the live-authority review."""

    apply_professional_migrations(conn)
    readiness = _load_json(post_rerun_release_readiness_artifact_path)
    authority = _load_json(live_order_authority_artifact_path)
    expected_readiness_hash = _clean_text(readiness.get("release_readiness_hash"))
    readiness_artifact_hash = _hash_file(post_rerun_release_readiness_artifact_path)
    authority_hash = _hash_file(live_order_authority_artifact_path)
    inputs = [
        _authority_item("post_rerun_release_readiness", post_rerun_release_readiness_artifact_path, expected_readiness_hash),
        _authority_item("live_order_authority", live_order_authority_artifact_path, expected_readiness_hash),
    ]
    blocking_reasons: List[str] = []
    if readiness.get("release_readiness_status") != "post_rerun_release_ready_for_live_authority_check" or readiness.get("passed") is not True:
        blocking_reasons.append("post_rerun_release_readiness_not_ready")
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

    ready = not blocking_reasons
    payload: JsonDict = {
        "artifact_version": "strategy_competition_post_rerun_live_authority_submission.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": _clean_text(readiness.get("competition_run_id")),
        "source_post_rerun_release_readiness_artifact": str(post_rerun_release_readiness_artifact_path),
        "source_post_rerun_release_readiness_hash": expected_readiness_hash,
        "source_post_rerun_release_readiness_artifact_hash": readiness_artifact_hash,
        "source_live_order_authority_artifact": str(live_order_authority_artifact_path),
        "source_live_order_authority_hash": authority_hash,
        "live_authority_submission_status": "post_rerun_live_authority_submission_ready" if ready else "post_rerun_live_authority_submission_blocked",
        "passed": ready,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "broker_submission_allowed": False,
        "live_authority_inputs": inputs,
        "blocking_reasons": blocking_reasons,
        "allowed_next_actions": (
            ["submit_to_post_rerun_live_authority_review"]
            if ready
            else ["complete_post_rerun_release_readiness_and_live_order_authority_check"]
        ),
        "live_authority_submission_contract": {
            "requires_post_rerun_release_readiness_ready": True,
            "requires_live_order_authority_allowed": True,
            "requires_readiness_hash_lineage": True,
            "does_not_create_new_live_order_authority": True,
            "does_not_call_broker_adapter": True,
        },
        "hard_boundaries": [
            "post_rerun_live_authority_submission_is_not_live_authority_review",
            "live_authority_submission_does_not_execute_orders",
            "live_authority_submission_cannot_create_broker_permission",
            "live_trading_requires_separate_broker_guard_and_execution_chain",
        ],
    }
    payload["live_authority_submission_hash"] = sha256(
        canonical_json(
            {
                "competition_run_id": payload["competition_run_id"],
                "source_post_rerun_release_readiness_hash": payload["source_post_rerun_release_readiness_hash"],
                "source_post_rerun_release_readiness_artifact_hash": payload["source_post_rerun_release_readiness_artifact_hash"],
                "source_live_order_authority_hash": payload["source_live_order_authority_hash"],
                "live_authority_submission_status": payload["live_authority_submission_status"],
                "live_authority_inputs": payload["live_authority_inputs"],
            }
        ).encode("utf-8")
    ).hexdigest()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_post_rerun_live_authority_submission_{payload['competition_run_id'] or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload
