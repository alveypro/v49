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


def _to_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _reconciliation_failures(recon: JsonDict, *, feedback: JsonDict) -> List[str]:
    failures: List[str] = []
    if not recon:
        return ["post_trade_reconciliation_input_missing"]
    if recon.get("artifact_version") != "strategy_competition_post_trade_reconciliation_input.v1":
        failures.append("post_trade_reconciliation_input_version_invalid")
    if _clean_text(recon.get("competition_run_id")) != _clean_text(feedback.get("competition_run_id")):
        failures.append("post_trade_reconciliation_competition_run_id_mismatch")
    if _clean_text(recon.get("execution_feedback_review_hash")) != _clean_text(feedback.get("feedback_review_hash")):
        failures.append("post_trade_reconciliation_feedback_hash_mismatch")
    thresholds = recon.get("thresholds") if isinstance(recon.get("thresholds"), dict) else {}
    cash_limit = _to_float(thresholds.get("cash_diff_abs_max"))
    position_limit = _to_float(thresholds.get("position_qty_diff_abs_max"))
    cost_limit = _to_float(thresholds.get("cost_slippage_bps_abs_max"))
    if cash_limit <= 0:
        failures.append("post_trade_cash_threshold_missing")
    if position_limit < 0:
        failures.append("post_trade_position_threshold_invalid")
    if cost_limit <= 0:
        failures.append("post_trade_cost_threshold_missing")
    cash = recon.get("cash_reconciliation") if isinstance(recon.get("cash_reconciliation"), dict) else {}
    cash_diff = abs(_to_float(cash.get("cash_diff")))
    if cash_limit > 0 and cash_diff > cash_limit:
        failures.append(f"post_trade_cash_diff_exceeds_threshold:{cash_diff}/{cash_limit}")
    positions = recon.get("position_reconciliation") if isinstance(recon.get("position_reconciliation"), list) else []
    if not positions:
        failures.append("post_trade_position_reconciliation_missing")
    for item in positions:
        if not isinstance(item, dict):
            failures.append("post_trade_position_reconciliation_item_invalid")
            continue
        ts_code = _clean_text(item.get("ts_code")) or "unknown"
        qty_diff = abs(_to_float(item.get("qty_diff")))
        if position_limit >= 0 and qty_diff > position_limit:
            failures.append(f"post_trade_position_qty_diff_exceeds_threshold:{ts_code}:{qty_diff}/{position_limit}")
    costs = recon.get("cost_slippage_reconciliation") if isinstance(recon.get("cost_slippage_reconciliation"), list) else []
    if not costs:
        failures.append("post_trade_cost_slippage_reconciliation_missing")
    for item in costs:
        if not isinstance(item, dict):
            failures.append("post_trade_cost_slippage_item_invalid")
            continue
        ts_code = _clean_text(item.get("ts_code")) or "unknown"
        slippage = abs(_to_float(item.get("slippage_bps")))
        if cost_limit > 0 and slippage > cost_limit:
            failures.append(f"post_trade_slippage_exceeds_threshold:{ts_code}:{slippage}/{cost_limit}")
    exceptions = recon.get("exceptions") if isinstance(recon.get("exceptions"), list) else []
    for idx, item in enumerate(exceptions):
        if not isinstance(item, dict):
            failures.append(f"post_trade_exception_invalid:{idx}")
            continue
        if not _clean_text(item.get("owner")):
            failures.append(f"post_trade_exception_owner_missing:{idx}")
        if not _clean_text(item.get("resolution_plan")):
            failures.append(f"post_trade_exception_resolution_missing:{idx}")
    if recon.get("operations_signoff") is not True:
        failures.append("post_trade_operations_signoff_missing")
    return failures


def build_strategy_competition_post_trade_reconciliation(
    conn: sqlite3.Connection,
    *,
    broker_execution_feedback_review_artifact_path: str,
    output_dir: str | Path,
    post_trade_reconciliation_input_artifact_path: str = "",
    operator_name: str = "strategy_competition_post_trade_reconciliation",
) -> JsonDict:
    """Reconcile terminal execution feedback against cash, positions, costs, and exceptions."""

    apply_professional_migrations(conn)
    feedback_path = str(broker_execution_feedback_review_artifact_path or "")
    recon_path = str(post_trade_reconciliation_input_artifact_path or "")
    feedback = _load_json(feedback_path)
    recon = _load_json(recon_path)
    blocking: List[str] = []
    if feedback.get("artifact_version") != "strategy_competition_broker_execution_feedback_review.v1":
        blocking.append("broker_execution_feedback_review_version_invalid")
    if feedback.get("feedback_status") != "broker_execution_feedback_accepted" or feedback.get("passed") is not True:
        blocking.append("broker_execution_feedback_not_accepted")
    blocking.extend(_reconciliation_failures(recon, feedback=feedback))
    passed = not blocking
    payload: JsonDict = {
        "artifact_version": "strategy_competition_post_trade_reconciliation.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": _clean_text(feedback.get("competition_run_id")) or _clean_text(recon.get("competition_run_id")),
        "reconciliation_status": "post_trade_reconciliation_passed" if passed else "post_trade_reconciliation_blocked",
        "passed": passed,
        "trade_lifecycle_complete": passed,
        "broker_execution_feedback_review_artifact": feedback_path,
        "post_trade_reconciliation_input_artifact": recon_path,
        "source_artifact_hashes": {
            "broker_execution_feedback_review": _hash_file(feedback_path),
            "post_trade_reconciliation_input": _hash_file(recon_path),
        },
        "cash_reconciliation": recon.get("cash_reconciliation") if isinstance(recon.get("cash_reconciliation"), dict) else {},
        "position_reconciliation": recon.get("position_reconciliation") if isinstance(recon.get("position_reconciliation"), list) else [],
        "cost_slippage_reconciliation": recon.get("cost_slippage_reconciliation") if isinstance(recon.get("cost_slippage_reconciliation"), list) else [],
        "exceptions": recon.get("exceptions") if isinstance(recon.get("exceptions"), list) else [],
        "blocking_reasons": blocking,
        "reconciliation_contract": {
            "requires_accepted_execution_feedback": True,
            "requires_feedback_hash_match": True,
            "requires_cash_reconciliation": True,
            "requires_position_reconciliation": True,
            "requires_cost_slippage_reconciliation": True,
            "requires_exception_owners_and_resolution": True,
            "requires_operations_signoff": True,
        },
        "hard_boundaries": [
            "post_trade_reconciliation_is_required_after_execution_feedback",
            "execution_feedback_complete_is_not_portfolio_reconciled",
            "trade_lifecycle_complete_requires_cash_position_cost_reconciliation",
            "blocked_reconciliation_cannot_mark_lifecycle_complete",
        ],
    }
    payload["reconciliation_hash"] = sha256(
        canonical_json(
            {
                "competition_run_id": payload["competition_run_id"],
                "reconciliation_status": payload["reconciliation_status"],
                "source_artifact_hashes": payload["source_artifact_hashes"],
                "cash_reconciliation": payload["cash_reconciliation"],
                "position_reconciliation": payload["position_reconciliation"],
                "cost_slippage_reconciliation": payload["cost_slippage_reconciliation"],
                "exceptions": payload["exceptions"],
            }
        ).encode("utf-8")
    ).hexdigest()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_post_trade_reconciliation_{payload['competition_run_id'] or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload
