from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from typing import Any, Dict, List

from openclaw.services.lineage_service import apply_professional_migrations, canonical_json


JsonDict = Dict[str, Any]
ALLOWED_BROKER_RESPONSE_STATUSES = {"dry_run_ack", "submitted", "accepted", "rejected", "adapter_error"}


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


def _guard_order_keys(guard: JsonDict) -> List[str]:
    keys: List[str] = []
    for idx, order in enumerate(guard.get("orders") or []):
        if not isinstance(order, dict):
            continue
        ts_code = _clean_text(order.get("ts_code"))
        side = _clean_text(order.get("side")).lower()
        qty = int(order.get("target_qty") or 0)
        keys.append(f"{ts_code}|{side}|{qty}|{idx}")
    return keys


def _response_order_keys(response: JsonDict) -> List[str]:
    keys: List[str] = []
    for idx, order in enumerate(response.get("order_responses") or []):
        if not isinstance(order, dict):
            continue
        ts_code = _clean_text(order.get("ts_code"))
        side = _clean_text(order.get("side")).lower()
        qty = int(order.get("target_qty") or 0)
        keys.append(f"{ts_code}|{side}|{qty}|{idx}")
    return keys


def _response_failures(response: JsonDict, *, guard: JsonDict) -> List[str]:
    failures: List[str] = []
    if not response:
        return ["broker_submission_response_missing"]
    if response.get("artifact_version") != "strategy_competition_broker_submission_response.v1":
        failures.append("broker_submission_response_version_invalid")
    if _clean_text(response.get("competition_run_id")) != _clean_text(guard.get("competition_run_id")):
        failures.append("broker_submission_response_competition_run_id_mismatch")
    if _clean_text(response.get("broker_guard_hash")) != _clean_text(guard.get("broker_guard_hash")):
        failures.append("broker_submission_response_guard_hash_mismatch")
    if _clean_text(response.get("idempotency_key")) != _clean_text(guard.get("idempotency_key")):
        failures.append("broker_submission_response_idempotency_key_mismatch")
    if _clean_text(response.get("broker_adapter")) != _clean_text(guard.get("broker_adapter")):
        failures.append("broker_submission_response_adapter_mismatch")
    if _response_order_keys(response) != _guard_order_keys(guard):
        failures.append("broker_submission_response_order_set_mismatch")
    order_responses = response.get("order_responses") if isinstance(response.get("order_responses"), list) else []
    if not order_responses:
        failures.append("broker_submission_response_orders_missing")
    for idx, order in enumerate(order_responses):
        if not isinstance(order, dict):
            failures.append(f"broker_submission_response_order_invalid:{idx}")
            continue
        status = _clean_text(order.get("status")).lower()
        ts_code = _clean_text(order.get("ts_code")) or str(idx)
        if status not in ALLOWED_BROKER_RESPONSE_STATUSES:
            failures.append(f"broker_submission_response_status_invalid:{ts_code}")
        if status in {"submitted", "accepted"} and not _clean_text(order.get("broker_order_ref")):
            failures.append(f"broker_submission_response_broker_ref_missing:{ts_code}")
        if status in {"rejected", "adapter_error"} and not _clean_text(order.get("reject_reason") or order.get("error_code")):
            failures.append(f"broker_submission_response_failure_reason_missing:{ts_code}")
        if order.get("fills"):
            failures.append(f"broker_submission_response_must_not_include_fills:{ts_code}")
    return failures


def build_strategy_competition_broker_submission_response_evidence(
    conn: sqlite3.Connection,
    *,
    broker_submission_guard_artifact_path: str,
    output_dir: str | Path,
    broker_submission_response_artifact_path: str = "",
    operator_name: str = "strategy_competition_broker_submission_response_evidence",
) -> JsonDict:
    """Review broker adapter submission response without treating it as fill evidence."""

    apply_professional_migrations(conn)
    guard_path = str(broker_submission_guard_artifact_path or "")
    response_path = str(broker_submission_response_artifact_path or "")
    guard = _load_json(guard_path)
    response = _load_json(response_path)
    blocking: List[str] = []
    if guard.get("artifact_version") != "strategy_competition_broker_submission_guard.v1":
        blocking.append("broker_submission_guard_version_invalid")
    if guard.get("guard_status") != "broker_submission_guard_passed" or guard.get("passed") is not True:
        blocking.append("broker_submission_guard_not_passed")
    blocking.extend(_response_failures(response, guard=guard))
    accepted = not blocking
    payload: JsonDict = {
        "artifact_version": "strategy_competition_broker_submission_response_evidence.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": _clean_text(guard.get("competition_run_id")) or _clean_text(response.get("competition_run_id")),
        "response_status": "broker_submission_response_accepted" if accepted else "broker_submission_response_blocked",
        "passed": accepted,
        "broker_submission_confirmed": accepted,
        "execution_fills_confirmed": False,
        "broker_submission_guard_artifact": guard_path,
        "broker_submission_response_artifact": response_path,
        "source_artifact_hashes": {
            "broker_submission_guard": _hash_file(guard_path),
            "broker_submission_response": _hash_file(response_path),
        },
        "broker_adapter": _clean_text(response.get("broker_adapter")) or _clean_text(guard.get("broker_adapter")),
        "idempotency_key": _clean_text(response.get("idempotency_key")) or _clean_text(guard.get("idempotency_key")),
        "order_responses": response.get("order_responses") if isinstance(response.get("order_responses"), list) else [],
        "blocking_reasons": blocking,
        "response_contract": {
            "requires_passed_broker_guard": True,
            "requires_guard_hash_match": True,
            "requires_idempotency_key_match": True,
            "requires_order_set_match": True,
            "does_not_confirm_fills": True,
        },
        "hard_boundaries": [
            "broker_submission_response_is_not_fill_evidence",
            "broker_submission_confirmed_does_not_mean_filled",
            "fills_require_separate_broker_execution_report",
            "blocked_broker_response_cannot_advance_execution_state",
        ],
    }
    payload["response_evidence_hash"] = sha256(
        canonical_json(
            {
                "competition_run_id": payload["competition_run_id"],
                "response_status": payload["response_status"],
                "source_artifact_hashes": payload["source_artifact_hashes"],
                "order_responses": payload["order_responses"],
            }
        ).encode("utf-8")
    ).hexdigest()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_broker_submission_response_evidence_{payload['competition_run_id'] or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload
