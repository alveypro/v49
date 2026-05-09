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


def _artifact_review(name: str, path: str | Path, expected_rerun_hash: str) -> JsonDict:
    payload = _load_json(path)
    artifact_hash = _hash_file(path)
    reasons: List[str] = []
    if not payload:
        reasons.append(f"{name}_missing")
    if payload and not artifact_hash:
        reasons.append(f"{name}_file_missing")
    lineage_hash = _clean_text(payload.get("source_rerun_result_review_hash")) or _clean_text(payload.get("rerun_result_review_hash"))
    if payload and expected_rerun_hash and lineage_hash and lineage_hash != expected_rerun_hash:
        reasons.append(f"{name}_rerun_result_hash_mismatch")
    if payload and expected_rerun_hash and not lineage_hash:
        reasons.append(f"{name}_rerun_result_hash_missing")
    if payload and payload.get("production_candidate_allowed") is True:
        reasons.append(f"{name}_attempted_production_eligibility")
    if payload and payload.get("production_release_authorized") is True:
        reasons.append(f"{name}_attempted_release_authorization")
    if payload and payload.get("live_order_authority_granted") is True:
        reasons.append(f"{name}_attempted_live_order_authority")
    passed = not reasons
    return {
        "name": name,
        "artifact": str(path or ""),
        "artifact_hash": artifact_hash,
        "artifact_version": _clean_text(payload.get("artifact_version")),
        "status": _clean_text(payload.get("manifest_status")) or _clean_text(payload.get("chain_status")) or "missing",
        "passed": passed,
        "blocking_reasons": reasons,
    }


def build_strategy_competition_rerun_court_rebuild_review(
    conn: sqlite3.Connection,
    *,
    rerun_result_review_artifact_path: str | Path,
    court_rebuild_submission_artifact_path: str | Path = "",
    rebuilt_manifest_artifact_path: str | Path = "",
    rebuilt_release_chain_artifact_path: str | Path = "",
    output_dir: str | Path,
    operator_name: str = "strategy_competition_rerun_court_rebuild_review",
) -> JsonDict:
    """Review whether accepted rerun outputs rebuilt the manifest and court-of-record."""

    apply_professional_migrations(conn)
    rerun_result = _load_json(rerun_result_review_artifact_path)
    submission = _load_json(court_rebuild_submission_artifact_path)
    rerun_result_hash = _hash_file(rerun_result_review_artifact_path)
    expected_lineage_hash = _clean_text(rerun_result.get("rerun_result_review_hash"))
    if submission:
        expected_lineage_hash = _clean_text(submission.get("source_rerun_result_review_hash")) or expected_lineage_hash
        rebuilt_manifest_artifact_path = _clean_text(submission.get("rebuilt_manifest_artifact_path") or submission.get("rebuilt_manifest_artifact")) or rebuilt_manifest_artifact_path
        rebuilt_release_chain_artifact_path = _clean_text(submission.get("rebuilt_release_chain_artifact_path") or submission.get("rebuilt_release_chain_artifact")) or rebuilt_release_chain_artifact_path
    manifest_review = _artifact_review("rebuilt_evidence_chain_manifest", rebuilt_manifest_artifact_path, expected_lineage_hash)
    release_review = _artifact_review("rebuilt_release_chain_adjudication", rebuilt_release_chain_artifact_path, expected_lineage_hash)
    blocking_reasons: List[str] = []
    if rerun_result.get("rerun_result_review_status") != "formal_rerun_results_accepted" or rerun_result.get("passed") is not True:
        blocking_reasons.append("formal_rerun_result_review_not_accepted")
    if court_rebuild_submission_artifact_path and not submission:
        blocking_reasons.append("rerun_court_rebuild_submission_missing")
    if submission and _clean_text(submission.get("source_rerun_result_review_hash")) and _clean_text(submission.get("source_rerun_result_review_hash")) != _clean_text(rerun_result.get("rerun_result_review_hash")):
        blocking_reasons.append("rerun_court_rebuild_submission_rerun_result_hash_mismatch")
    if submission and str(submission.get("rerun_court_rebuild_submission_status") or "") != "rerun_court_rebuild_submission_ready":
        blocking_reasons.append("rerun_court_rebuild_submission_not_ready")
    for review in (manifest_review, release_review):
        for reason in review.get("blocking_reasons") or []:
            blocking_reasons.append(f"{review['name']}:{reason}")
    accepted = not blocking_reasons
    payload: JsonDict = {
        "artifact_version": "strategy_competition_rerun_court_rebuild_review.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": _clean_text(rerun_result.get("competition_run_id")),
        "court_rebuild_status": "rerun_court_rebuild_accepted" if accepted else "rerun_court_rebuild_blocked",
        "passed": accepted,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "source_rerun_result_review_artifact": str(rerun_result_review_artifact_path),
        "source_rerun_result_review_hash": rerun_result_hash,
        "source_court_rebuild_submission_artifact": str(court_rebuild_submission_artifact_path or ""),
        "expected_rerun_result_review_hash": expected_lineage_hash,
        "artifact_reviews": [manifest_review, release_review],
        "blocking_reasons": blocking_reasons,
        "allowed_next_actions": (
            ["proceed_to_release_chain_adjudication_then_human_release_review"]
            if accepted
            else ["rebuild_manifest_and_release_chain_from_accepted_rerun_results"]
        ),
        "court_rebuild_contract": {
            "requires_accepted_formal_rerun_result_review": True,
            "requires_rebuilt_evidence_chain_manifest": True,
            "requires_rebuilt_release_chain_adjudication": True,
            "requires_rerun_result_hash_lineage": True,
            "does_not_create_production_eligibility": True,
            "does_not_create_live_order_authority": True,
        },
        "hard_boundaries": [
            "rerun_court_rebuild_review_is_not_release_approval",
            "rebuilt_manifest_or_release_chain_cannot_create_live_order_authority",
            "rerun_court_rebuild_requires_accepted_rerun_results",
            "production_still_requires_release_chain_and_human_approval",
        ],
    }
    payload["court_rebuild_review_hash"] = sha256(
        canonical_json(
            {
                "competition_run_id": payload["competition_run_id"],
                "source_rerun_result_review_hash": payload["source_rerun_result_review_hash"],
                "expected_rerun_result_review_hash": payload["expected_rerun_result_review_hash"],
                "artifact_reviews": payload["artifact_reviews"],
                "court_rebuild_status": payload["court_rebuild_status"],
            }
        ).encode("utf-8")
    ).hexdigest()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_rerun_court_rebuild_review_{payload['competition_run_id'] or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload
