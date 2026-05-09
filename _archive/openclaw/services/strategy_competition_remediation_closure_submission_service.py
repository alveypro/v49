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


def _work_item_lookup(work_order: JsonDict) -> Dict[str, JsonDict]:
    lookup: Dict[str, JsonDict] = {}
    for item in work_order.get("work_items") or []:
        if isinstance(item, dict):
            artifact = _clean_text(item.get("artifact"))
            if artifact:
                lookup[artifact] = item
    return lookup


def _closure_items(work_order: JsonDict, closure_artifact_paths: Dict[str, str]) -> List[JsonDict]:
    lookup = _work_item_lookup(work_order)
    items: List[JsonDict] = []
    for artifact, work_item in lookup.items():
        closure_path = _clean_text(closure_artifact_paths.get(artifact))
        closure_payload = _load_json(closure_path)
        closure_hash = _hash_file(closure_path)
        validator_passed = bool(closure_payload.get("passed") is True) if closure_payload else False
        closed = bool(closure_path) and bool(closure_hash) and validator_passed
        items.append(
            {
                "artifact": artifact,
                "owner_role": _clean_text(work_item.get("owner_role")),
                "validator_tool": _clean_text(work_item.get("validator_tool")),
                "validator_artifact": closure_path,
                "validator_artifact_hash": closure_hash,
                "validator_passed": validator_passed,
                "closure_status": "closed" if closed else "open",
                "closed": closed,
                "blocking_reasons": [] if closed else ["closure_artifact_missing_or_unpassed"],
            }
        )
    return items


def _blocking_reasons(items: List[JsonDict]) -> List[str]:
    reasons: List[str] = []
    for item in items:
        if item.get("closed") is not True:
            reasons.append(f"{item.get('artifact')}:closure_missing_or_unpassed")
    return reasons


def build_strategy_competition_remediation_closure_submission(
    conn: sqlite3.Connection,
    *,
    work_order_artifact_path: str | Path,
    closure_artifact_paths: Dict[str, str],
    output_dir: str | Path,
    operator_name: str = "strategy_competition_remediation_closure_submission",
) -> JsonDict:
    """Package designated validator artifacts into a closure submission for remediation review."""

    apply_professional_migrations(conn)
    work_order = _load_json(work_order_artifact_path)
    work_order_hash = _hash_file(work_order_artifact_path)
    items = _closure_items(work_order, closure_artifact_paths)
    blocking = _blocking_reasons(items)
    submission_ready = not blocking and bool(items)
    payload: JsonDict = {
        "artifact_version": "strategy_competition_remediation_closure_submission.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": _clean_text(work_order.get("competition_run_id")),
        "source_work_order_artifact": str(work_order_artifact_path),
        "source_work_order_hash": work_order_hash,
        "source_manifest_hash": _clean_text(work_order.get("source_manifest_hash")),
        "closure_submission_status": "remediation_closure_submission_ready" if submission_ready else "remediation_closure_submission_blocked",
        "passed": submission_ready,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "item_closures": items,
        "blocking_reasons": blocking,
        "allowed_next_actions": (
            ["submit_to_remediation_closure_review"]
            if submission_ready
            else ["complete_all_work_item_closures_before_submission"]
        ),
        "closure_submission_contract": {
            "requires_work_order_hash_match": True,
            "requires_source_manifest_hash_match": True,
            "requires_designated_validator_artifacts": True,
            "does_not_create_production_eligibility": True,
            "does_not_create_live_order_authority": True,
        },
        "hard_boundaries": [
            "closure_submission_is_not_remediation_closure_review",
            "closure_submission_is_not_formal_validation_pass",
            "partial_closure_submissions_cannot_advance_release_chain",
            "closure_submission_does_not_create_production_or_live_order_authority",
        ],
    }
    payload["closure_submission_hash"] = sha256(
        canonical_json(
            {
                "competition_run_id": payload["competition_run_id"],
                "source_work_order_hash": payload["source_work_order_hash"],
                "source_manifest_hash": payload["source_manifest_hash"],
                "closure_submission_status": payload["closure_submission_status"],
                "item_closures": payload["item_closures"],
            }
        ).encode("utf-8")
    ).hexdigest()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_remediation_closure_submission_{payload['competition_run_id'] or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload
