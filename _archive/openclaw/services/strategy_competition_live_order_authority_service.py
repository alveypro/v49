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
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"json payload must be object: {path}")
    return payload


def _hash_file(path: str) -> str:
    text = _clean_text(path)
    if not text:
        return ""
    file_path = Path(text)
    if not file_path.exists() or not file_path.is_file():
        return ""
    return sha256(file_path.read_bytes()).hexdigest()


def _order_intent_failures(intent: JsonDict, *, approval_hash: str, competition_run_id: str) -> List[str]:
    failures: List[str] = []
    if not intent:
        return ["live_order_intent_missing"]
    if intent.get("artifact_version") != "strategy_competition_live_order_intent.v1":
        failures.append("live_order_intent_version_invalid")
    if _clean_text(intent.get("competition_run_id")) != _clean_text(competition_run_id):
        failures.append("live_order_intent_competition_run_id_mismatch")
    if _clean_text(intent.get("human_release_approval_hash")) != _clean_text(approval_hash):
        failures.append("live_order_intent_approval_hash_mismatch")
    orders = intent.get("orders") if isinstance(intent.get("orders"), list) else []
    if not orders:
        failures.append("live_order_intent_orders_missing")
    if len(orders) > 5:
        failures.append(f"live_order_intent_order_count_exceeds_top5:{len(orders)}")
    for idx, order in enumerate(orders):
        if not isinstance(order, dict):
            failures.append(f"live_order_intent_order_invalid:{idx}")
            continue
        ts_code = _clean_text(order.get("ts_code"))
        side = _clean_text(order.get("side")).lower()
        qty = int(order.get("target_qty") or 0)
        if not ts_code:
            failures.append(f"live_order_intent_ts_code_missing:{idx}")
        if side not in {"buy", "sell", "reduce"}:
            failures.append(f"live_order_intent_side_invalid:{ts_code or idx}")
        if qty <= 0:
            failures.append(f"live_order_intent_qty_invalid:{ts_code or idx}")
    return failures


def build_strategy_competition_live_order_authority_check(
    conn: sqlite3.Connection,
    *,
    human_release_approval_artifact_path: str,
    output_dir: str | Path,
    live_order_intent_artifact_path: str = "",
    operator_name: str = "strategy_competition_live_order_authority_check",
) -> JsonDict:
    """Validate that a live order intent is backed by final human release approval."""

    apply_professional_migrations(conn)
    approval_path = str(human_release_approval_artifact_path or "")
    intent_path = str(live_order_intent_artifact_path or "")
    approval = _load_json(approval_path)
    intent = _load_json(intent_path)
    blocking: List[str] = []
    if approval.get("artifact_version") != "strategy_competition_human_release_approval.v1":
        blocking.append("human_release_approval_version_invalid")
    if (
        approval.get("approval_status") != "human_release_approved"
        or approval.get("passed") is not True
        or approval.get("production_release_authorized") is not True
        or approval.get("live_order_authority_granted") is not True
    ):
        blocking.append("human_release_approval_not_authorized")
    approval_hash = _clean_text(approval.get("approval_hash"))
    if not approval_hash:
        blocking.append("human_release_approval_hash_missing")
    competition_run_id = _clean_text(approval.get("competition_run_id"))
    blocking.extend(_order_intent_failures(intent, approval_hash=approval_hash, competition_run_id=competition_run_id))
    allowed = not blocking
    payload: JsonDict = {
        "artifact_version": "strategy_competition_live_order_authority_check.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": competition_run_id or _clean_text(intent.get("competition_run_id")),
        "authority_status": "live_order_submission_allowed" if allowed else "live_order_submission_blocked",
        "passed": allowed,
        "live_order_submission_allowed": allowed,
        "human_release_approval_artifact": approval_path,
        "live_order_intent_artifact": intent_path,
        "source_artifact_hashes": {
            "human_release_approval": _hash_file(approval_path),
            "live_order_intent": _hash_file(intent_path),
        },
        "blocking_reasons": blocking,
        "orders": intent.get("orders") if isinstance(intent.get("orders"), list) else [],
        "authority_contract": {
            "requires_human_release_approved": True,
            "requires_live_order_authority_granted": True,
            "requires_order_intent_approval_hash_match": True,
            "requires_order_intent_competition_run_match": True,
            "does_not_execute_orders": True,
        },
        "hard_boundaries": [
            "live_order_authority_check_does_not_execute_orders",
            "blocked_authority_check_cannot_submit_live_orders",
            "order_intent_must_reference_current_human_approval_hash",
            "live_submission_requires_broker_layer_after_authority_check",
        ],
    }
    payload["authority_hash"] = sha256(
        canonical_json(
            {
                "competition_run_id": payload["competition_run_id"],
                "authority_status": payload["authority_status"],
                "source_artifact_hashes": payload["source_artifact_hashes"],
                "orders": payload["orders"],
            }
        ).encode("utf-8")
    ).hexdigest()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_live_order_authority_check_{payload['competition_run_id'] or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload
