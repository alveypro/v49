from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from typing import Any, Dict, List

from openclaw.services.lineage_service import apply_professional_migrations, canonical_json


JsonDict = Dict[str, Any]

CHAIN_ORDER = (
    "post_rerun_release_readiness",
    "post_rerun_live_authority_review",
    "post_rerun_release_chain_adjudication",
    "post_rerun_broker_guard_review",
    "post_rerun_broker_response_review",
    "post_rerun_broker_execution_feedback_review",
    "post_rerun_post_trade_reconciliation",
    "post_rerun_trade_lifecycle_adjudication",
    "post_rerun_human_release_approval_review",
)


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


def _status_from_payload(payload: JsonDict) -> str:
    for key in (
        "release_readiness_status",
        "live_authority_review_status",
        "release_chain_status",
        "broker_guard_review_status",
        "broker_response_review_status",
        "broker_execution_feedback_review_status",
        "reconciliation_status",
        "lifecycle_status",
        "human_release_approval_review_status",
    ):
        value = _clean_text(payload.get(key))
        if value:
            return value
    return "passed" if payload.get("passed") is True else "blocked"


def _blocking_reasons(payload: JsonDict) -> List[str]:
    return [_clean_text(item) for item in payload.get("blocking_reasons") or [] if _clean_text(item)]


def _artifact_status(name: str, payload: JsonDict, path: str) -> JsonDict:
    missing = not payload
    return {
        "name": name,
        "artifact_version": _clean_text(payload.get("artifact_version")),
        "artifact": _clean_text(payload.get("artifact_path")) or _clean_text(path),
        "artifact_hash": _hash_file(path),
        "status": "missing" if missing else _status_from_payload(payload),
        "passed": payload.get("passed") is True if payload else False,
        "missing": missing,
        "blocking_reasons": [f"{name}_artifact_missing"] if missing else _blocking_reasons(payload),
    }


def _first_blocking_artifact(statuses: List[JsonDict]) -> str:
    for name in CHAIN_ORDER:
        for item in statuses:
            if item.get("name") == name and item.get("passed") is not True:
                return name
    return ""


def _allowed_next_actions(blocking_artifact: str, payloads: Dict[str, JsonDict]) -> List[str]:
    if blocking_artifact:
        payload = payloads.get(blocking_artifact) or {}
        actions = payload.get("allowed_next_actions")
        if isinstance(actions, list) and actions:
            return [_clean_text(item) for item in actions if _clean_text(item)]
        return [f"complete_or_repair_{blocking_artifact}_and_rerun_post_rerun_manifest"]
    return ["archive_post_rerun_evidence_as_complete_without_creating_new_trade_permission"]


def build_strategy_competition_post_rerun_evidence_chain_manifest(
    conn: sqlite3.Connection,
    *,
    output_dir: str | Path,
    post_rerun_release_readiness_artifact_path: str = "",
    post_rerun_live_authority_review_artifact_path: str = "",
    post_rerun_release_chain_adjudication_artifact_path: str = "",
    post_rerun_broker_guard_review_artifact_path: str = "",
    post_rerun_broker_response_review_artifact_path: str = "",
    post_rerun_broker_execution_feedback_review_artifact_path: str = "",
    post_rerun_post_trade_reconciliation_artifact_path: str = "",
    post_rerun_trade_lifecycle_adjudication_artifact_path: str = "",
    post_rerun_human_release_approval_review_artifact_path: str = "",
    operator_name: str = "strategy_competition_post_rerun_evidence_chain_manifest",
) -> JsonDict:
    """Build a post-rerun evidence inventory without granting production or trading authority."""

    apply_professional_migrations(conn)
    paths = {
        "post_rerun_release_readiness": str(post_rerun_release_readiness_artifact_path or ""),
        "post_rerun_live_authority_review": str(post_rerun_live_authority_review_artifact_path or ""),
        "post_rerun_release_chain_adjudication": str(post_rerun_release_chain_adjudication_artifact_path or ""),
        "post_rerun_broker_guard_review": str(post_rerun_broker_guard_review_artifact_path or ""),
        "post_rerun_broker_response_review": str(post_rerun_broker_response_review_artifact_path or ""),
        "post_rerun_broker_execution_feedback_review": str(post_rerun_broker_execution_feedback_review_artifact_path or ""),
        "post_rerun_post_trade_reconciliation": str(post_rerun_post_trade_reconciliation_artifact_path or ""),
        "post_rerun_trade_lifecycle_adjudication": str(post_rerun_trade_lifecycle_adjudication_artifact_path or ""),
        "post_rerun_human_release_approval_review": str(post_rerun_human_release_approval_review_artifact_path or ""),
    }
    payloads = {name: _load_json(path) for name, path in paths.items()}
    statuses = [_artifact_status(name, payloads[name], paths[name]) for name in CHAIN_ORDER]
    current_blocking_artifact = _first_blocking_artifact(statuses)
    missing_artifacts = [item["name"] for item in statuses if item.get("missing") is True]
    lifecycle = payloads["post_rerun_trade_lifecycle_adjudication"]
    complete = (
        not current_blocking_artifact
        and lifecycle.get("lifecycle_status") == "post_rerun_trade_lifecycle_complete"
        and lifecycle.get("trade_lifecycle_complete") is True
    )
    competition_run_id = next(
        (_clean_text(payload.get("competition_run_id")) for payload in payloads.values() if _clean_text(payload.get("competition_run_id"))),
        "",
    )
    root_blockers: List[str] = []
    for item in statuses:
        if item.get("passed") is True:
            continue
        reasons = item.get("blocking_reasons") or ["failed_without_reason"]
        root_blockers.extend(f"{item['name']}:{reason}" for reason in reasons)

    payload: JsonDict = {
        "artifact_version": "strategy_competition_post_rerun_evidence_chain_manifest.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": competition_run_id,
        "manifest_status": "post_rerun_evidence_chain_complete" if complete else "post_rerun_evidence_chain_blocked",
        "passed": complete,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "trade_lifecycle_complete": lifecycle.get("trade_lifecycle_complete") is True if complete else False,
        "current_blocking_artifact": current_blocking_artifact,
        "missing_artifacts": missing_artifacts,
        "chain_order": list(CHAIN_ORDER),
        "artifact_statuses": statuses,
        "root_blockers": root_blockers,
        "allowed_next_actions": _allowed_next_actions(current_blocking_artifact, payloads),
        "source_artifacts": paths,
        "source_artifact_hashes": {name: _hash_file(path) for name, path in paths.items()},
        "manifest_contract": {
            "requires_post_rerun_release_readiness": True,
            "requires_post_rerun_live_authority_review": True,
            "requires_post_rerun_release_chain_adjudication": True,
            "requires_post_rerun_broker_guard_review": True,
            "requires_post_rerun_broker_response_review": True,
            "requires_post_rerun_broker_execution_feedback_review": True,
            "requires_post_rerun_post_trade_reconciliation": True,
            "requires_post_rerun_trade_lifecycle_adjudication": True,
            "requires_post_rerun_human_release_approval_review": True,
            "does_not_create_production_eligibility": True,
            "does_not_create_live_order_authority": True,
            "does_not_mark_execution_or_reconciliation_complete": True,
        },
        "hard_boundaries": [
            "post_rerun_evidence_chain_manifest_is_inventory_not_approval",
            "manifest_cannot_create_production_or_live_order_authority",
            "blocked_or_partial_post_rerun_artifacts_cannot_be_packaged_as_passed",
            "trade_lifecycle_complete_requires_post_rerun_post_trade_reconciliation_and_lifecycle_adjudication",
        ],
    }
    payload["manifest_hash"] = sha256(
        canonical_json(
            {
                "competition_run_id": payload["competition_run_id"],
                "manifest_status": payload["manifest_status"],
                "source_artifact_hashes": payload["source_artifact_hashes"],
                "artifact_statuses": payload["artifact_statuses"],
            }
        ).encode("utf-8")
    ).hexdigest()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_post_rerun_evidence_chain_manifest_{competition_run_id or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload
