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
        payload.get("source_rerun_court_rebuild_review_hash"),
        payload.get("court_rebuild_review_hash"),
        payload.get("source_artifact_hashes", {}).get("rerun_court_rebuild_review")
        if isinstance(payload.get("source_artifact_hashes"), dict)
        else "",
    )
    return any(_clean_text(item) == expected_hash for item in candidates)


def build_strategy_competition_post_rerun_release_readiness(
    conn: sqlite3.Connection,
    *,
    rerun_court_rebuild_review_artifact_path: str | Path,
    release_readiness_submission_artifact_path: str | Path = "",
    release_chain_adjudication_artifact_path: str | Path = "",
    human_release_approval_artifact_path: str | Path = "",
    output_dir: str | Path,
    operator_name: str = "strategy_competition_post_rerun_release_readiness",
) -> JsonDict:
    """Review release readiness after rerun court rebuild without granting live authority."""

    apply_professional_migrations(conn)
    court = _load_json(rerun_court_rebuild_review_artifact_path)
    submission = _load_json(release_readiness_submission_artifact_path)
    court_hash = _hash_file(rerun_court_rebuild_review_artifact_path)
    submission_hash = _hash_file(release_readiness_submission_artifact_path)
    expected_court_hash = _clean_text(court.get("court_rebuild_review_hash"))
    if submission:
        expected_court_hash = _clean_text(submission.get("source_rerun_court_rebuild_review_hash")) or expected_court_hash
        release_readiness_submission_artifact_path = _clean_text(
            submission.get("artifact_path")
            or release_readiness_submission_artifact_path
        )
        release_chain_adjudication_artifact_path = _clean_text(
            submission.get("source_release_chain_adjudication_artifact")
            or release_chain_adjudication_artifact_path
        )
        human_release_approval_artifact_path = _clean_text(
            submission.get("source_human_release_approval_artifact")
            or human_release_approval_artifact_path
        )
    release = _load_json(release_chain_adjudication_artifact_path)
    human = _load_json(human_release_approval_artifact_path)
    release_hash = _hash_file(release_chain_adjudication_artifact_path)
    human_hash = _hash_file(human_release_approval_artifact_path)
    blocking_reasons: List[str] = []

    if court.get("court_rebuild_status") != "rerun_court_rebuild_accepted" or court.get("passed") is not True:
        blocking_reasons.append("rerun_court_rebuild_review_not_accepted")
    if release_readiness_submission_artifact_path and not submission:
        blocking_reasons.append("post_rerun_release_readiness_submission_missing")
    if submission and str(submission.get("release_readiness_submission_status") or "") != "post_rerun_release_readiness_submission_ready":
        blocking_reasons.append("post_rerun_release_readiness_submission_not_ready")
    if submission and _clean_text(submission.get("source_rerun_court_rebuild_review_hash")) and _clean_text(submission.get("source_rerun_court_rebuild_review_hash")) != _clean_text(court.get("court_rebuild_review_hash")):
        blocking_reasons.append("post_rerun_release_readiness_submission_court_hash_mismatch")
    if not release:
        blocking_reasons.append("release_chain_adjudication_missing")
    elif release.get("chain_status") != "release_chain_passed_for_human_approval" or release.get("passed") is not True:
        blocking_reasons.append("release_chain_not_passed_for_human_approval")
    if release and not _lineage_matches(release, expected_court_hash):
        blocking_reasons.append("release_chain_rerun_court_hash_mismatch")
    if release.get("production_candidate_allowed") is True or release.get("production_release_allowed") is True:
        blocking_reasons.append("release_chain_attempted_direct_production_permission")
    if not human:
        blocking_reasons.append("human_release_approval_missing")
    elif human.get("approval_status") != "human_release_approved" or human.get("passed") is not True:
        blocking_reasons.append("human_release_approval_not_approved")
    if human and not _lineage_matches(human, expected_court_hash):
        blocking_reasons.append("human_release_rerun_court_hash_mismatch")
    if human.get("live_order_authority_granted") is True:
        blocking_reasons.append("human_release_attempted_direct_live_authority")

    ready = not blocking_reasons
    payload: JsonDict = {
        "artifact_version": "strategy_competition_post_rerun_release_readiness.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": _clean_text(court.get("competition_run_id")),
        "release_readiness_status": "post_rerun_release_ready_for_live_authority_check" if ready else "post_rerun_release_blocked",
        "passed": ready,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "source_rerun_court_rebuild_review_artifact": str(rerun_court_rebuild_review_artifact_path),
        "source_rerun_court_rebuild_review_hash": court_hash,
        "source_release_readiness_submission_artifact": str(release_readiness_submission_artifact_path or ""),
        "source_release_readiness_submission_hash": submission_hash,
        "expected_court_rebuild_review_hash": expected_court_hash,
        "release_chain_adjudication_artifact": str(release_chain_adjudication_artifact_path or ""),
        "release_chain_adjudication_hash": release_hash,
        "human_release_approval_artifact": str(human_release_approval_artifact_path or ""),
        "human_release_approval_hash": human_hash,
        "blocking_reasons": blocking_reasons,
        "allowed_next_actions": (
            ["run_live_order_authority_check_with_matching_human_release_hash"]
            if ready
            else ["complete_rerun_court_rebuild_release_chain_and_human_release_approval"]
        ),
        "release_readiness_contract": {
            "requires_accepted_rerun_court_rebuild_review": True,
            "requires_release_chain_passed_for_human_approval": True,
            "requires_human_release_approved": True,
            "requires_rerun_court_hash_lineage": True,
            "does_not_create_live_order_authority": True,
            "does_not_submit_broker_orders": True,
        },
        "hard_boundaries": [
            "post_rerun_release_readiness_is_not_live_order_authority",
            "human_release_approval_still_requires_live_order_authority_check",
            "release_readiness_cannot_submit_broker_orders",
            "live_trading_requires_separate_authority_broker_execution_and_post_trade_chain",
        ],
    }
    payload["release_readiness_hash"] = sha256(
        canonical_json(
            {
                "competition_run_id": payload["competition_run_id"],
                "source_rerun_court_rebuild_review_hash": payload["source_rerun_court_rebuild_review_hash"],
                "source_release_readiness_submission_hash": payload["source_release_readiness_submission_hash"],
                "release_chain_adjudication_hash": payload["release_chain_adjudication_hash"],
                "human_release_approval_hash": payload["human_release_approval_hash"],
                "release_readiness_status": payload["release_readiness_status"],
                "blocking_reasons": payload["blocking_reasons"],
            }
        ).encode("utf-8")
    ).hexdigest()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_post_rerun_release_readiness_{payload['competition_run_id'] or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload
