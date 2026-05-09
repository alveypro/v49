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


def build_strategy_competition_post_rerun_broker_execution_feedback_review(
    conn: sqlite3.Connection,
    *,
    post_rerun_broker_response_review_artifact_path: str | Path,
    broker_execution_feedback_artifact_path: str | Path = "",
    output_dir: str | Path,
    operator_name: str = "strategy_competition_post_rerun_broker_execution_feedback_review",
) -> JsonDict:
    """Review post-rerun broker execution feedback without treating it as post-trade completion."""

    apply_professional_migrations(conn)
    response_review = _load_json(post_rerun_broker_response_review_artifact_path)
    feedback = _load_json(broker_execution_feedback_artifact_path)
    response_review_hash = _hash_file(post_rerun_broker_response_review_artifact_path)
    feedback_hash = _hash_file(broker_execution_feedback_artifact_path)
    expected_response_evidence_hash = _clean_text(response_review.get("broker_submission_response_evidence_hash"))
    blocking: List[str] = []

    if response_review.get("broker_response_review_status") != "post_rerun_broker_response_ready_for_execution_feedback" or response_review.get("passed") is not True:
        blocking.append("post_rerun_broker_response_review_not_ready")
    if not feedback:
        blocking.append("broker_execution_feedback_artifact_missing")
    elif feedback.get("feedback_status") != "broker_execution_feedback_accepted" or feedback.get("passed") is not True:
        blocking.append("broker_execution_feedback_not_accepted")
    if feedback and expected_response_evidence_hash and _clean_text(feedback.get("broker_submission_response_hash")) != expected_response_evidence_hash:
        blocking.append("broker_execution_feedback_response_hash_mismatch")
    if feedback.get("execution_feedback_complete") is True and feedback.get("post_trade_reconciliation_passed") is True:
        blocking.append("broker_execution_feedback_attempted_post_trade_completion")
    blocking.extend(_feedback_failures(feedback, response={
        "competition_run_id": _clean_text(response_review.get("competition_run_id")),
        "response_evidence_hash": expected_response_evidence_hash,
        "idempotency_key": _clean_text(response_review.get("idempotency_key")),
        "broker_adapter": _clean_text(response_review.get("broker_adapter")),
        "order_responses": response_review.get("order_responses") if isinstance(response_review.get("order_responses"), list) else [],
    }))

    ready = not blocking
    payload: JsonDict = {
        "artifact_version": "strategy_competition_post_rerun_broker_execution_feedback_review.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": _clean_text(response_review.get("competition_run_id")) or _clean_text(feedback.get("competition_run_id")),
        "broker_execution_feedback_review_status": "post_rerun_broker_execution_feedback_ready_for_post_trade" if ready else "post_rerun_broker_execution_feedback_blocked",
        "passed": ready,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "broker_submission_confirmed": bool(ready and response_review.get("broker_submission_confirmed") is True),
        "execution_feedback_complete": bool(ready),
        "post_trade_reconciliation_passed": False,
        "trade_lifecycle_complete": False,
        "source_post_rerun_broker_response_review_artifact": str(post_rerun_broker_response_review_artifact_path),
        "source_post_rerun_broker_response_review_hash": response_review_hash,
        "expected_broker_submission_response_evidence_hash": expected_response_evidence_hash,
        "broker_execution_feedback_artifact": str(broker_execution_feedback_artifact_path or ""),
        "broker_execution_feedback_hash": feedback_hash,
        "blocking_reasons": blocking,
        "allowed_next_actions": (
            ["run_post_trade_reconciliation_with_matching_execution_feedback_hash"]
            if ready
            else ["complete_post_rerun_broker_response_review_and_broker_execution_feedback"]
        ),
        "broker_execution_feedback_review_contract": {
            "requires_post_rerun_broker_response_review_ready": True,
            "requires_broker_execution_feedback_accepted": True,
            "requires_response_hash_lineage": True,
            "requires_terminal_order_feedback": True,
            "requires_cost_slippage_and_attribution": True,
            "does_not_create_post_trade_reconciliation": True,
        },
        "hard_boundaries": [
            "post_rerun_broker_execution_feedback_review_is_not_post_trade",
            "submitted_or_accepted_orders_are_not_execution_complete",
            "filled_orders_require_fills_costs_slippage_and_attribution",
            "post_trade_reconciliation_still_required_after_execution_feedback",
        ],
    }
    payload["broker_execution_feedback_review_hash"] = sha256(
        canonical_json(
            {
                "competition_run_id": payload["competition_run_id"],
                "source_post_rerun_broker_response_review_hash": payload["source_post_rerun_broker_response_review_hash"],
                "broker_execution_feedback_hash": payload["broker_execution_feedback_hash"],
                "broker_execution_feedback_review_status": payload["broker_execution_feedback_review_status"],
                "blocking_reasons": payload["blocking_reasons"],
            }
        ).encode("utf-8")
    ).hexdigest()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_post_rerun_broker_execution_feedback_review_{payload['competition_run_id'] or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload
