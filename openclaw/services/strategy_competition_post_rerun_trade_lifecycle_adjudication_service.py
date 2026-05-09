from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from typing import Any, Dict, List

from openclaw.services.lineage_service import apply_professional_migrations, canonical_json


JsonDict = Dict[str, Any]

LIFECYCLE_ORDER = (
    "post_rerun_broker_guard_review",
    "post_rerun_broker_response_review",
    "post_rerun_broker_execution_feedback_review",
    "post_rerun_post_trade_reconciliation",
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


def _blocking_reasons(payload: JsonDict) -> List[str]:
    return [_clean_text(item) for item in payload.get("blocking_reasons") or [] if _clean_text(item)]


def _status(name: str, payload: JsonDict, path: str) -> JsonDict:
    status_key = {
        "post_rerun_broker_guard_review": "broker_guard_review_status",
        "post_rerun_broker_response_review": "broker_response_review_status",
        "post_rerun_broker_execution_feedback_review": "broker_execution_feedback_review_status",
        "post_rerun_post_trade_reconciliation": "reconciliation_status",
    }[name]
    reasons = _blocking_reasons(payload)
    if not payload:
        reasons = [f"{name}_artifact_missing"]
    return {
        "name": name,
        "artifact_version": _clean_text(payload.get("artifact_version")),
        "artifact": _clean_text(payload.get("artifact_path")) or _clean_text(path),
        "artifact_hash": _hash_file(path),
        "status": _clean_text(payload.get(status_key)) or ("passed" if payload.get("passed") is True else "blocked"),
        "passed": payload.get("passed") is True,
        "blocking_reasons": reasons,
    }


def _current_blocking_stage(stages: List[JsonDict]) -> str:
    for name in LIFECYCLE_ORDER:
        for stage in stages:
            if stage.get("name") == name and stage.get("passed") is not True:
                return name
    return ""


def _allowed_next_actions(stage: str) -> List[str]:
    return {
        "post_rerun_broker_guard_review": ["complete_post_rerun_broker_guard_review_and_broker_response_evidence"],
        "post_rerun_broker_response_review": ["complete_post_rerun_broker_response_review_and_broker_execution_feedback"],
        "post_rerun_broker_execution_feedback_review": ["run_post_rerun_post_trade_reconciliation_with_matching_execution_feedback_hash"],
        "post_rerun_post_trade_reconciliation": ["archive_post_rerun_trade_lifecycle_as_complete"],
        "": ["archive_post_rerun_trade_lifecycle_as_complete"],
    }.get(stage, ["repair_current_lifecycle_stage"])


def build_strategy_competition_post_rerun_trade_lifecycle_adjudication(
    conn: sqlite3.Connection,
    *,
    output_dir: str | Path,
    post_rerun_broker_guard_review_artifact_path: str = "",
    post_rerun_broker_response_review_artifact_path: str = "",
    post_rerun_broker_execution_feedback_review_artifact_path: str = "",
    post_rerun_post_trade_reconciliation_artifact_path: str = "",
    operator_name: str = "strategy_competition_post_rerun_trade_lifecycle_adjudication",
) -> JsonDict:
    """Adjudicate the post-rerun trade lifecycle without creating permissions."""

    apply_professional_migrations(conn)
    paths = {
        "post_rerun_broker_guard_review": str(post_rerun_broker_guard_review_artifact_path or ""),
        "post_rerun_broker_response_review": str(post_rerun_broker_response_review_artifact_path or ""),
        "post_rerun_broker_execution_feedback_review": str(post_rerun_broker_execution_feedback_review_artifact_path or ""),
        "post_rerun_post_trade_reconciliation": str(post_rerun_post_trade_reconciliation_artifact_path or ""),
    }
    payloads = {name: _load_json(path) for name, path in paths.items()}
    stages = [_status(name, payloads[name], paths[name]) for name in LIFECYCLE_ORDER]
    current_stage = _current_blocking_stage(stages)
    recon = payloads["post_rerun_post_trade_reconciliation"]
    complete = (
        not current_stage
        and recon.get("reconciliation_status") == "post_rerun_post_trade_reconciliation_passed"
        and recon.get("trade_lifecycle_complete") is True
    )
    competition_run_id = next(
        (_clean_text(payload.get("competition_run_id")) for payload in payloads.values() if _clean_text(payload.get("competition_run_id"))),
        "",
    )
    root_blockers = []
    for stage in stages:
        if stage.get("passed") is True:
            continue
        reasons = stage.get("blocking_reasons") or ["failed_without_reason"]
        root_blockers.extend(f"{stage['name']}:{reason}" for reason in reasons)
    payload: JsonDict = {
        "artifact_version": "strategy_competition_post_rerun_trade_lifecycle_adjudication.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": competition_run_id,
        "lifecycle_status": "post_rerun_trade_lifecycle_complete" if complete else "post_rerun_trade_lifecycle_blocked",
        "passed": complete,
        "trade_lifecycle_complete": complete,
        "current_blocking_stage": current_stage,
        "lifecycle_order": list(LIFECYCLE_ORDER),
        "lifecycle_statuses": stages,
        "root_blockers": root_blockers,
        "allowed_next_actions": _allowed_next_actions(current_stage),
        "source_artifacts": paths,
        "source_artifact_hashes": {name: _hash_file(path) for name, path in paths.items()},
        "lifecycle_contract": {
            "requires_post_rerun_broker_guard_review": True,
            "requires_post_rerun_broker_response_review": True,
            "requires_post_rerun_broker_execution_feedback_review": True,
            "requires_post_rerun_post_trade_reconciliation": True,
            "does_not_create_new_trade_permission": True,
        },
        "hard_boundaries": [
            "post_rerun_trade_lifecycle_adjudication_is_not_trade_instruction",
            "blocked_lifecycle_stage_cannot_be_skipped",
            "broker_submission_does_not_equal_execution_complete",
            "execution_feedback_does_not_equal_post_trade_reconciled",
        ],
    }
    payload["lifecycle_adjudication_hash"] = sha256(
        canonical_json(
            {
                "competition_run_id": payload["competition_run_id"],
                "lifecycle_status": payload["lifecycle_status"],
                "source_artifact_hashes": payload["source_artifact_hashes"],
                "lifecycle_statuses": payload["lifecycle_statuses"],
            }
        ).encode("utf-8")
    ).hexdigest()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_post_rerun_trade_lifecycle_adjudication_{competition_run_id or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload
