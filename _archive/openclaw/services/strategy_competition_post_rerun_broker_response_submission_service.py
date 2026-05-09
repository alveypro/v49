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
        payload.get("source_post_rerun_broker_guard_review_hash"),
        payload.get("broker_guard_review_hash"),
        payload.get("broker_submission_guard_hash"),
        payload.get("source_broker_submission_guard_hash"),
        hashes.get("post_rerun_broker_guard_review"),
        hashes.get("broker_submission_guard"),
    )
    return any(_clean_text(item) == expected_hash for item in candidates)


def _response_item(name: str, path: str | Path, expected_hash: str) -> JsonDict:
    payload = _load_json(path)
    artifact_hash = _hash_file(path)
    reasons: List[str] = []
    if not payload:
        reasons.append(f"{name}_missing")
    if payload and not artifact_hash:
        reasons.append(f"{name}_file_missing")
    if payload and expected_hash and not _lineage_matches(payload, expected_hash):
        reasons.append(f"{name}_broker_guard_review_hash_mismatch")
    if payload and payload.get("execution_fills_confirmed") is True:
        reasons.append(f"{name}_attempted_fill_confirmation")
    if payload and payload.get("response_status") != "broker_submission_response_accepted":
        reasons.append(f"{name}_status_invalid")
    return {
        "name": name,
        "artifact": str(path or ""),
        "artifact_hash": artifact_hash,
        "artifact_version": _clean_text(payload.get("artifact_version")),
        "status": _clean_text(payload.get("response_status")) or "missing",
        "passed": not reasons,
        "blocking_reasons": reasons,
    }


def build_strategy_competition_post_rerun_broker_response_submission(
    conn: sqlite3.Connection,
    *,
    post_rerun_broker_guard_review_artifact_path: str | Path,
    broker_submission_response_evidence_artifact_path: str | Path,
    output_dir: str | Path,
    operator_name: str = "strategy_competition_post_rerun_broker_response_submission",
) -> JsonDict:
    """Package broker response evidence before review."""

    apply_professional_migrations(conn)
    guard_review = _load_json(post_rerun_broker_guard_review_artifact_path)
    response = _load_json(broker_submission_response_evidence_artifact_path)
    guard_review_hash = _hash_file(post_rerun_broker_guard_review_artifact_path)
    response_hash = _hash_file(broker_submission_response_evidence_artifact_path)
    expected_guard_hash = _clean_text(guard_review.get("broker_submission_guard_hash"))
    items = [
        _response_item("post_rerun_broker_guard_review", post_rerun_broker_guard_review_artifact_path, expected_guard_hash),
        _response_item("broker_submission_response_evidence", broker_submission_response_evidence_artifact_path, expected_guard_hash),
    ]
    blocking_reasons: List[str] = []
    if guard_review.get("broker_guard_review_status") != "post_rerun_broker_guard_ready_for_adapter" or guard_review.get("passed") is not True:
        blocking_reasons.append("post_rerun_broker_guard_review_not_ready")
    if not response:
        blocking_reasons.append("broker_submission_response_evidence_missing")
    elif response.get("response_status") != "broker_submission_response_accepted" or response.get("passed") is not True:
        blocking_reasons.append("broker_submission_response_evidence_not_accepted")
    if response and not _lineage_matches(response, expected_guard_hash):
        blocking_reasons.append("broker_response_guard_hash_mismatch")
    if response.get("execution_fills_confirmed") is True:
        blocking_reasons.append("broker_response_attempted_fill_confirmation")
    for order in response.get("order_responses") or []:
        if isinstance(order, dict) and order.get("fills"):
            blocking_reasons.append("broker_response_contains_fills")
            break

    ready = not blocking_reasons
    payload: JsonDict = {
        "artifact_version": "strategy_competition_post_rerun_broker_response_submission.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": _clean_text(guard_review.get("competition_run_id")) or _clean_text(response.get("competition_run_id")),
        "source_post_rerun_broker_guard_review_artifact": str(post_rerun_broker_guard_review_artifact_path),
        "source_post_rerun_broker_guard_review_hash": guard_review_hash,
        "expected_broker_submission_guard_hash": expected_guard_hash,
        "source_broker_submission_response_evidence_artifact": str(broker_submission_response_evidence_artifact_path),
        "source_broker_submission_response_evidence_hash": response_hash,
        "broker_response_submission_status": "post_rerun_broker_response_submission_ready" if ready else "post_rerun_broker_response_submission_blocked",
        "passed": ready,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "broker_submission_confirmed": False,
        "execution_fills_confirmed": False,
        "broker_response_inputs": items,
        "blocking_reasons": blocking_reasons,
        "allowed_next_actions": (
            ["submit_to_post_rerun_broker_response_review"]
            if ready
            else ["complete_post_rerun_broker_guard_review_and_broker_response_evidence"]
        ),
        "broker_response_submission_contract": {
            "requires_post_rerun_broker_guard_review_ready": True,
            "requires_broker_submission_response_accepted": True,
            "requires_broker_guard_review_hash_lineage": True,
            "does_not_confirm_fills": True,
            "does_not_create_post_trade_reconciliation": True,
        },
        "hard_boundaries": [
            "post_rerun_broker_response_submission_is_not_broker_response_review",
            "broker_response_submission_does_not_confirm_fills",
            "fills_require_separate_execution_feedback",
            "post_trade_reconciliation_remains_required_after_execution_feedback",
        ],
    }
    payload["broker_response_submission_hash"] = sha256(
        canonical_json(
            {
                "competition_run_id": payload["competition_run_id"],
                "source_post_rerun_broker_guard_review_hash": payload["source_post_rerun_broker_guard_review_hash"],
                "expected_broker_submission_guard_hash": payload["expected_broker_submission_guard_hash"],
                "source_broker_submission_response_evidence_hash": payload["source_broker_submission_response_evidence_hash"],
                "broker_response_submission_status": payload["broker_response_submission_status"],
                "broker_response_inputs": payload["broker_response_inputs"],
            }
        ).encode("utf-8")
    ).hexdigest()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_post_rerun_broker_response_submission_{payload['competition_run_id'] or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload
