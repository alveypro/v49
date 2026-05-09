from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from typing import Any, Dict, List

from openclaw.services.lineage_service import apply_professional_migrations, canonical_json


JsonDict = Dict[str, Any]
TERMINAL_NO_FILL_STATUSES = {"cancelled", "rejected", "expired", "manual_override"}
FILL_STATUSES = {"filled", "partial_fill"}


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


def _response_keys(response: JsonDict) -> List[str]:
    keys: List[str] = []
    for idx, order in enumerate(response.get("order_responses") or []):
        if not isinstance(order, dict):
            continue
        ts_code = _clean_text(order.get("ts_code"))
        side = _clean_text(order.get("side")).lower()
        qty = int(order.get("target_qty") or 0)
        keys.append(f"{ts_code}|{side}|{qty}|{idx}")
    return keys


def _feedback_keys(feedback: JsonDict) -> List[str]:
    keys: List[str] = []
    for idx, report in enumerate(feedback.get("execution_reports") or []):
        if not isinstance(report, dict):
            continue
        ts_code = _clean_text(report.get("ts_code"))
        side = _clean_text(report.get("side")).lower()
        qty = int(report.get("target_qty") or 0)
        keys.append(f"{ts_code}|{side}|{qty}|{idx}")
    return keys


def _feedback_failures(feedback: JsonDict, *, response: JsonDict) -> List[str]:
    failures: List[str] = []
    if not feedback:
        return ["broker_execution_feedback_missing"]
    if feedback.get("artifact_version") != "strategy_competition_broker_execution_feedback.v1":
        failures.append("broker_execution_feedback_version_invalid")
    if _clean_text(feedback.get("competition_run_id")) != _clean_text(response.get("competition_run_id")):
        failures.append("broker_execution_feedback_competition_run_id_mismatch")
    if _clean_text(feedback.get("broker_submission_response_hash")) != _clean_text(response.get("response_evidence_hash")):
        failures.append("broker_execution_feedback_response_hash_mismatch")
    if _clean_text(feedback.get("idempotency_key")) != _clean_text(response.get("idempotency_key")):
        failures.append("broker_execution_feedback_idempotency_key_mismatch")
    if _feedback_keys(feedback) != _response_keys(response):
        failures.append("broker_execution_feedback_order_set_mismatch")
    reports = feedback.get("execution_reports") if isinstance(feedback.get("execution_reports"), list) else []
    if not reports:
        failures.append("broker_execution_feedback_reports_missing")
    for idx, report in enumerate(reports):
        if not isinstance(report, dict):
            failures.append(f"broker_execution_feedback_report_invalid:{idx}")
            continue
        ts_code = _clean_text(report.get("ts_code")) or str(idx)
        status = _clean_text(report.get("status")).lower()
        if status not in FILL_STATUSES | TERMINAL_NO_FILL_STATUSES | {"submitted", "accepted"}:
            failures.append(f"broker_execution_feedback_status_invalid:{ts_code}")
        fills = report.get("fills") if isinstance(report.get("fills"), list) else []
        if status in FILL_STATUSES and not fills:
            failures.append(f"broker_execution_feedback_fills_missing:{ts_code}")
        if status in TERMINAL_NO_FILL_STATUSES and not _clean_text(report.get("miss_reason_code")):
            failures.append(f"broker_execution_feedback_miss_reason_missing:{ts_code}")
        if status == "manual_override" and not _clean_text(report.get("cancel_reason")):
            failures.append(f"broker_execution_feedback_manual_override_reason_missing:{ts_code}")
        if status in {"submitted", "accepted"}:
            failures.append(f"broker_execution_feedback_not_terminal:{ts_code}")
        for fill_idx, fill in enumerate(fills):
            if not isinstance(fill, dict):
                failures.append(f"broker_execution_feedback_fill_invalid:{ts_code}:{fill_idx}")
                continue
            if float(fill.get("fill_price") or 0.0) <= 0:
                failures.append(f"broker_execution_feedback_fill_price_missing:{ts_code}:{fill_idx}")
            if int(fill.get("fill_qty") or 0) <= 0:
                failures.append(f"broker_execution_feedback_fill_qty_missing:{ts_code}:{fill_idx}")
            if fill.get("fill_fee") is None:
                failures.append(f"broker_execution_feedback_fill_fee_missing:{ts_code}:{fill_idx}")
            if fill.get("fill_slippage_bp") is None:
                failures.append(f"broker_execution_feedback_slippage_missing:{ts_code}:{fill_idx}")
        if float(report.get("close_price") or 0.0) <= 0:
            failures.append(f"broker_execution_feedback_close_price_missing:{ts_code}")
        if report.get("execution_attribution") not in {True, "true", "present"}:
            failures.append(f"broker_execution_feedback_attribution_missing:{ts_code}")
    return failures


def build_strategy_competition_broker_execution_feedback_review(
    conn: sqlite3.Connection,
    *,
    broker_submission_response_evidence_artifact_path: str,
    output_dir: str | Path,
    broker_execution_feedback_artifact_path: str = "",
    operator_name: str = "strategy_competition_broker_execution_feedback_review",
) -> JsonDict:
    """Review final broker execution feedback after broker submission response evidence."""

    apply_professional_migrations(conn)
    response_path = str(broker_submission_response_evidence_artifact_path or "")
    feedback_path = str(broker_execution_feedback_artifact_path or "")
    response = _load_json(response_path)
    feedback = _load_json(feedback_path)
    blocking: List[str] = []
    if response.get("artifact_version") != "strategy_competition_broker_submission_response_evidence.v1":
        blocking.append("broker_submission_response_evidence_version_invalid")
    if response.get("response_status") != "broker_submission_response_accepted" or response.get("passed") is not True:
        blocking.append("broker_submission_response_not_accepted")
    blocking.extend(_feedback_failures(feedback, response=response))
    accepted = not blocking
    payload: JsonDict = {
        "artifact_version": "strategy_competition_broker_execution_feedback_review.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": _clean_text(response.get("competition_run_id")) or _clean_text(feedback.get("competition_run_id")),
        "feedback_status": "broker_execution_feedback_accepted" if accepted else "broker_execution_feedback_blocked",
        "passed": accepted,
        "execution_feedback_complete": accepted,
        "broker_submission_response_evidence_artifact": response_path,
        "broker_execution_feedback_artifact": feedback_path,
        "source_artifact_hashes": {
            "broker_submission_response_evidence": _hash_file(response_path),
            "broker_execution_feedback": _hash_file(feedback_path),
        },
        "execution_reports": feedback.get("execution_reports") if isinstance(feedback.get("execution_reports"), list) else [],
        "blocking_reasons": blocking,
        "feedback_contract": {
            "requires_accepted_broker_submission_response": True,
            "requires_response_hash_match": True,
            "requires_terminal_order_feedback": True,
            "requires_fills_for_filled_status": True,
            "requires_miss_reason_for_unfilled_terminal_status": True,
            "requires_cost_slippage_and_attribution": True,
        },
        "hard_boundaries": [
            "execution_feedback_is_required_after_broker_submission",
            "submitted_or_accepted_orders_are_not_execution_complete",
            "filled_orders_require_fills_costs_slippage_and_attribution",
            "blocked_execution_feedback_cannot_mark_trade_complete",
        ],
    }
    payload["feedback_review_hash"] = sha256(
        canonical_json(
            {
                "competition_run_id": payload["competition_run_id"],
                "feedback_status": payload["feedback_status"],
                "source_artifact_hashes": payload["source_artifact_hashes"],
                "execution_reports": payload["execution_reports"],
            }
        ).encode("utf-8")
    ).hexdigest()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_broker_execution_feedback_review_{payload['competition_run_id'] or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload
