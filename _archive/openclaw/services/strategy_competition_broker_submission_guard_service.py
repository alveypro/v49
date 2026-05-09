from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from typing import Any, Dict, List

from openclaw.services.lineage_service import apply_professional_migrations, canonical_json


JsonDict = Dict[str, Any]
ALLOWED_SUBMISSION_MODES = {"dry_run", "controlled_submit"}


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


def _broker_intent_failures(intent: JsonDict, *, authority_hash: str, competition_run_id: str) -> List[str]:
    failures: List[str] = []
    if not intent:
        return ["broker_submission_intent_missing"]
    if intent.get("artifact_version") != "strategy_competition_broker_submission_intent.v1":
        failures.append("broker_submission_intent_version_invalid")
    if _clean_text(intent.get("competition_run_id")) != _clean_text(competition_run_id):
        failures.append("broker_submission_intent_competition_run_id_mismatch")
    if _clean_text(intent.get("live_order_authority_hash")) != _clean_text(authority_hash):
        failures.append("broker_submission_intent_authority_hash_mismatch")
    mode = _clean_text(intent.get("submission_mode")).lower()
    if mode not in ALLOWED_SUBMISSION_MODES:
        failures.append(f"broker_submission_mode_invalid:{mode or 'missing'}")
    if not _clean_text(intent.get("broker_adapter")):
        failures.append("broker_adapter_missing")
    if not _clean_text(intent.get("idempotency_key")):
        failures.append("broker_idempotency_key_missing")
    orders = intent.get("orders") if isinstance(intent.get("orders"), list) else []
    if not orders:
        failures.append("broker_submission_orders_missing")
    if len(orders) > 5:
        failures.append(f"broker_submission_order_count_exceeds_top5:{len(orders)}")
    return failures


def build_strategy_competition_broker_submission_guard(
    conn: sqlite3.Connection,
    *,
    live_order_authority_artifact_path: str,
    output_dir: str | Path,
    broker_submission_intent_artifact_path: str = "",
    operator_name: str = "strategy_competition_broker_submission_guard",
) -> JsonDict:
    """Validate broker submission intent after live-order authority and before broker adapter execution."""

    apply_professional_migrations(conn)
    authority_path = str(live_order_authority_artifact_path or "")
    intent_path = str(broker_submission_intent_artifact_path or "")
    authority = _load_json(authority_path)
    intent = _load_json(intent_path)
    blocking: List[str] = []
    if authority.get("artifact_version") != "strategy_competition_live_order_authority_check.v1":
        blocking.append("live_order_authority_version_invalid")
    if (
        authority.get("authority_status") != "live_order_submission_allowed"
        or authority.get("passed") is not True
        or authority.get("live_order_submission_allowed") is not True
    ):
        blocking.append("live_order_authority_not_allowed")
    authority_hash = _clean_text(authority.get("authority_hash"))
    if not authority_hash:
        blocking.append("live_order_authority_hash_missing")
    competition_run_id = _clean_text(authority.get("competition_run_id"))
    blocking.extend(_broker_intent_failures(intent, authority_hash=authority_hash, competition_run_id=competition_run_id))
    allowed = not blocking
    payload: JsonDict = {
        "artifact_version": "strategy_competition_broker_submission_guard.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": competition_run_id or _clean_text(intent.get("competition_run_id")),
        "guard_status": "broker_submission_guard_passed" if allowed else "broker_submission_guard_blocked",
        "passed": allowed,
        "broker_submission_allowed": allowed,
        "live_order_authority_artifact": authority_path,
        "broker_submission_intent_artifact": intent_path,
        "source_artifact_hashes": {
            "live_order_authority": _hash_file(authority_path),
            "broker_submission_intent": _hash_file(intent_path),
        },
        "submission_mode": _clean_text(intent.get("submission_mode")),
        "broker_adapter": _clean_text(intent.get("broker_adapter")),
        "idempotency_key": _clean_text(intent.get("idempotency_key")),
        "orders": intent.get("orders") if isinstance(intent.get("orders"), list) else [],
        "blocking_reasons": blocking,
        "broker_guard_contract": {
            "requires_live_order_authority_allowed": True,
            "requires_authority_hash_match": True,
            "requires_idempotency_key": True,
            "requires_broker_adapter_declared": True,
            "does_not_record_fills": True,
        },
        "hard_boundaries": [
            "broker_submission_guard_does_not_execute_orders",
            "blocked_broker_guard_cannot_call_broker_adapter",
            "broker_submit_requires_separate_broker_response_evidence",
            "fills_must_be_recorded_by_execution_feedback_not_guard",
        ],
    }
    payload["broker_guard_hash"] = sha256(
        canonical_json(
            {
                "competition_run_id": payload["competition_run_id"],
                "guard_status": payload["guard_status"],
                "source_artifact_hashes": payload["source_artifact_hashes"],
                "submission_mode": payload["submission_mode"],
                "broker_adapter": payload["broker_adapter"],
                "idempotency_key": payload["idempotency_key"],
            }
        ).encode("utf-8")
    ).hexdigest()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_broker_submission_guard_{payload['competition_run_id'] or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload
