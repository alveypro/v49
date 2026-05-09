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


def _lineage_matches(payload: JsonDict, expected_hash: str, *candidate_keys: str) -> bool:
    if not expected_hash:
        return False
    candidates = [_clean_text(payload.get(key)) for key in candidate_keys]
    source_artifact_hashes = payload.get("source_artifact_hashes")
    if isinstance(source_artifact_hashes, dict):
        candidates.append(_clean_text(source_artifact_hashes.get("rerun_court_rebuild_review")))
    return any(candidate == expected_hash for candidate in candidates if candidate)


def _submission_item(name: str, path: str | Path, expected_hash: str) -> JsonDict:
    payload = _load_json(path)
    artifact_hash = _hash_file(path)
    reasons: List[str] = []
    if not payload:
        reasons.append(f"{name}_missing")
    if payload and not artifact_hash:
        reasons.append(f"{name}_file_missing")
    if payload and expected_hash and not _lineage_matches(payload, expected_hash, "source_rerun_court_rebuild_review_hash"):
        reasons.append(f"{name}_rerun_court_rebuild_hash_mismatch")
    if payload and payload.get("production_candidate_allowed") is True:
        reasons.append(f"{name}_attempted_production_eligibility")
    if payload and payload.get("production_release_authorized") is True:
        reasons.append(f"{name}_attempted_release_authorization")
    if payload and payload.get("live_order_authority_granted") is True:
        reasons.append(f"{name}_attempted_live_order_authority")
    if _clean_text(payload.get("release_chain_status")) and payload.get("passed") is not True:
        reasons.append(f"{name}_not_passed")
    if name == "release_chain_adjudication" and _clean_text(payload.get("chain_status")) != "release_chain_passed_for_human_approval":
        reasons.append(f"{name}_status_invalid")
    if name == "human_release_approval" and _clean_text(payload.get("approval_status")) != "human_release_approved":
        reasons.append(f"{name}_status_invalid")
    return {
        "name": name,
        "artifact": str(path or ""),
        "artifact_hash": artifact_hash,
        "artifact_version": _clean_text(payload.get("artifact_version")),
        "status": _clean_text(payload.get("release_readiness_status"))
        or _clean_text(payload.get("chain_status"))
        or _clean_text(payload.get("approval_status"))
        or "missing",
        "passed": not reasons,
        "blocking_reasons": reasons,
    }


def build_strategy_competition_post_rerun_release_readiness_submission(
    conn: sqlite3.Connection,
    *,
    rerun_court_rebuild_review_artifact_path: str | Path,
    release_chain_adjudication_artifact_path: str | Path,
    human_release_approval_artifact_path: str | Path,
    output_dir: str | Path,
    operator_name: str = "strategy_competition_post_rerun_release_readiness_submission",
) -> JsonDict:
    """Package the post-rerun release-readiness evidence before review."""

    apply_professional_migrations(conn)
    court = _load_json(rerun_court_rebuild_review_artifact_path)
    release_chain = _load_json(release_chain_adjudication_artifact_path)
    human = _load_json(human_release_approval_artifact_path)
    court_hash = _hash_file(rerun_court_rebuild_review_artifact_path)
    release_hash = _hash_file(release_chain_adjudication_artifact_path)
    human_hash = _hash_file(human_release_approval_artifact_path)
    expected_court_hash = _clean_text(court.get("court_rebuild_review_hash"))

    submission_items = [
        _submission_item("rerun_court_rebuild_review", rerun_court_rebuild_review_artifact_path, expected_court_hash),
        _submission_item("release_chain_adjudication", release_chain_adjudication_artifact_path, expected_court_hash),
        _submission_item("human_release_approval", human_release_approval_artifact_path, expected_court_hash),
    ]

    blocking_reasons: List[str] = []
    if court.get("court_rebuild_status") != "rerun_court_rebuild_accepted" or court.get("passed") is not True:
        blocking_reasons.append("rerun_court_rebuild_review_not_accepted")
    if not release_chain:
        blocking_reasons.append("release_chain_adjudication_missing")
    elif release_chain.get("chain_status") != "release_chain_passed_for_human_approval" or release_chain.get("passed") is not True:
        blocking_reasons.append("release_chain_not_passed_for_human_approval")
    if release_chain and not _lineage_matches(release_chain, expected_court_hash, "source_rerun_court_rebuild_review_hash"):
        blocking_reasons.append("release_chain_rerun_court_hash_mismatch")
    if release_chain.get("production_candidate_allowed") is True or release_chain.get("production_release_allowed") is True:
        blocking_reasons.append("release_chain_attempted_direct_production_permission")
    if not human:
        blocking_reasons.append("human_release_approval_missing")
    elif human.get("approval_status") != "human_release_approved" or human.get("passed") is not True:
        blocking_reasons.append("human_release_approval_not_approved")
    if human and not _lineage_matches(human, expected_court_hash, "source_rerun_court_rebuild_review_hash"):
        blocking_reasons.append("human_release_rerun_court_hash_mismatch")
    if human.get("live_order_authority_granted") is True:
        blocking_reasons.append("human_release_attempted_direct_live_authority")

    ready = not blocking_reasons
    payload: JsonDict = {
        "artifact_version": "strategy_competition_post_rerun_release_readiness_submission.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": _clean_text(court.get("competition_run_id")),
        "source_rerun_court_rebuild_review_artifact": str(rerun_court_rebuild_review_artifact_path),
        "source_rerun_court_rebuild_review_hash": expected_court_hash,
        "source_rerun_court_rebuild_review_artifact_hash": court_hash,
        "source_release_chain_adjudication_artifact": str(release_chain_adjudication_artifact_path),
        "source_release_chain_adjudication_hash": release_hash,
        "source_human_release_approval_artifact": str(human_release_approval_artifact_path),
        "source_human_release_approval_hash": human_hash,
        "release_readiness_submission_status": "post_rerun_release_readiness_submission_ready" if ready else "post_rerun_release_readiness_submission_blocked",
        "passed": ready,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "release_readiness_inputs": submission_items,
        "blocking_reasons": blocking_reasons,
        "allowed_next_actions": (
            ["submit_to_post_rerun_release_readiness_review"]
            if ready
            else ["complete_rerun_court_rebuild_release_chain_and_human_release_approval"]
        ),
        "release_readiness_submission_contract": {
            "requires_accepted_rerun_court_rebuild_review": True,
            "requires_release_chain_passed_for_human_approval": True,
            "requires_human_release_approved": True,
            "requires_rerun_court_hash_lineage": True,
            "does_not_create_live_order_authority": True,
            "does_not_submit_broker_orders": True,
        },
        "hard_boundaries": [
            "post_rerun_release_readiness_submission_is_not_release_readiness_review",
            "post_rerun_release_readiness_submission_is_not_live_order_authority",
            "post_rerun_release_readiness_submission_does_not_submit_broker_orders",
            "post_rerun_release_readiness_submission_cannot_create_production_eligibility",
        ],
    }
    payload["release_readiness_submission_hash"] = sha256(
        canonical_json(
            {
                "competition_run_id": payload["competition_run_id"],
                "source_rerun_court_rebuild_review_hash": payload["source_rerun_court_rebuild_review_hash"],
                "source_rerun_court_rebuild_review_artifact_hash": payload["source_rerun_court_rebuild_review_artifact_hash"],
                "source_release_chain_adjudication_hash": payload["source_release_chain_adjudication_hash"],
                "source_human_release_approval_hash": payload["source_human_release_approval_hash"],
                "release_readiness_submission_status": payload["release_readiness_submission_status"],
                "release_readiness_inputs": payload["release_readiness_inputs"],
            }
        ).encode("utf-8")
    ).hexdigest()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_post_rerun_release_readiness_submission_{payload['competition_run_id'] or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload
