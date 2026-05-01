from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict

from openclaw.services.lineage_service import apply_professional_migrations


REQUIRED_BACKTEST_CREDIBILITY_FLAGS = (
    "point_in_time_data",
    "suspension_and_limit_handling",
    "volume_constraint",
    "cost_model",
    "slippage_model",
    "in_sample_out_of_sample_split",
    "parameter_sensitivity",
    "failed_backtests_recorded",
)


def _safe_json_loads(value: Any) -> Dict[str, Any]:
    try:
        parsed = json.loads(str(value or "{}"))
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def evaluate_backtest_credibility(audit: Dict[str, Any]) -> Dict[str, Any]:
    payload = audit or {}
    blocking = []
    for key in REQUIRED_BACKTEST_CREDIBILITY_FLAGS:
        if payload.get(key) is not True:
            blocking.append(f"missing_or_failed:{key}")
    sample = payload.get("sample") if isinstance(payload.get("sample"), dict) else {}
    if not sample.get("in_sample"):
        blocking.append("missing_in_sample_window")
    if not sample.get("out_of_sample"):
        blocking.append("missing_out_of_sample_window")
    metrics = payload.get("metrics") if isinstance(payload.get("metrics"), dict) else {}
    if not metrics:
        blocking.append("missing_metrics")
    return {
        "passed": not blocking,
        "blocking_reasons": blocking,
        "required_flags": list(REQUIRED_BACKTEST_CREDIBILITY_FLAGS),
        "audit": payload,
    }


def extract_backtest_credibility_from_signal_run(conn: sqlite3.Connection, *, run_id: str) -> Dict[str, Any]:
    apply_professional_migrations(conn)
    row = conn.execute(
        """
        SELECT run_type, strategy, status, summary_json
        FROM signal_runs
        WHERE run_id = ?
        """,
        (str(run_id or ""),),
    ).fetchone()
    if not row:
        return {"run_id": str(run_id or ""), "passed": False, "blocking_reasons": ["signal_run_missing"]}
    run_type, strategy, status, summary_json = row
    summary = _safe_json_loads(summary_json)
    audit = summary.get("backtest_credibility") if isinstance(summary.get("backtest_credibility"), dict) else {}
    if not audit:
        nested = summary.get("backtest_audit") if isinstance(summary.get("backtest_audit"), dict) else {}
        audit = nested
    result = evaluate_backtest_credibility(audit)
    result.update(
        {
            "run_id": str(run_id or ""),
            "run_type": str(run_type or ""),
            "strategy": str(strategy or ""),
            "status": str(status or ""),
        }
    )
    if str(run_type or "").lower() != "backtest" and summary.get("governance_type") != "experiment_strategy_evidence":
        result["passed"] = False
        result["blocking_reasons"] = ["not_backtest_or_experiment_run"] + list(result.get("blocking_reasons") or [])
    if str(status or "").lower() != "success":
        result["passed"] = False
        result["blocking_reasons"] = ["signal_run_not_success"] + list(result.get("blocking_reasons") or [])
    return result
