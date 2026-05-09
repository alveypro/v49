from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Any, Dict, Iterable

from openclaw.services.lineage_service import apply_professional_migrations, canonical_json


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
    else:
        if float(metrics.get("signal_density", 0.0) or 0.0) <= 0.0:
            blocking.append("missing_positive_signal_density")
        if int(metrics.get("test_windows", 0) or 0) <= 0:
            blocking.append("missing_successful_test_windows")
    return {
        "passed": not blocking,
        "blocking_reasons": blocking,
        "required_flags": list(REQUIRED_BACKTEST_CREDIBILITY_FLAGS),
        "audit": payload,
    }


def _metric_float(source: Dict[str, Any], key: str, default: float = 0.0) -> float:
    value = source.get(key, None)
    if value is None:
        return float(default)
    try:
        return float(value)
    except Exception:
        return float(default)


def _metric_int(source: Dict[str, Any], key: str, default: int = 0) -> int:
    try:
        return int(source.get(key, default) or default)
    except Exception:
        return int(default)


def _first_dict(*items: Any) -> Dict[str, Any]:
    for item in items:
        if isinstance(item, dict):
            return item
    return {}


def build_backtest_credibility_audit(
    *,
    result: Dict[str, Any],
    params: Dict[str, Any],
    param_runs: int = 1,
    failed_runs: Iterable[Dict[str, Any]] | None = None,
    artifact_path: str = "",
) -> Dict[str, Any]:
    """Build a conservative audit payload from actual backtest output.

    This function does not certify missing controls. A flag is true only when
    the runtime result explicitly carries evidence for that control.
    """
    payload = result or {}
    result_body = _first_dict(payload.get("result"), payload)
    summary = _first_dict(result_body.get("summary"), payload.get("summary"))
    rolling = _first_dict(result_body.get("rolling"))
    failed_windows = rolling.get("failed_windows") if isinstance(rolling.get("failed_windows"), list) else []
    errors = list(failed_runs or [])
    trading_cost = summary.get("trading_cost") if isinstance(summary.get("trading_cost"), dict) else {}

    train_windows = _metric_int(rolling, "train_windows")
    test_windows = _metric_int(rolling, "test_windows")
    signal_density = _metric_float(summary, "signal_density")
    metrics = {
        "win_rate": _metric_float(summary, "win_rate"),
        "max_drawdown": _metric_float(summary, "max_drawdown", 1.0),
        "signal_density": signal_density,
        "samples": _metric_int(summary, "samples", test_windows),
        "train_windows": train_windows,
        "test_windows": test_windows,
        "failed_windows": len(failed_windows),
        "param_runs": int(param_runs or 0),
        "failed_param_runs": len(errors),
    }
    if trading_cost:
        metrics["trading_cost"] = trading_cost

    sample = {
        "in_sample": f"train_windows:{train_windows}" if train_windows > 0 else "",
        "out_of_sample": f"test_windows:{test_windows}" if test_windows > 0 else "",
    }

    return {
        "point_in_time_data": bool(rolling.get("train_test_separated") is True and test_windows > 0),
        "suspension_and_limit_handling": bool(summary.get("tradeability_filter_enabled") is True),
        "volume_constraint": bool(summary.get("volume_constraint_enabled") is True),
        "cost_model": bool(trading_cost),
        "slippage_model": bool(
            trading_cost
            and (
                "slippage_bp" in trading_cost
                or "base_round_trip_bp" in trading_cost
                or "expected_cost_bp" in trading_cost
            )
        ),
        "in_sample_out_of_sample_split": bool(train_windows > 0 and test_windows > 0),
        "parameter_sensitivity": bool(int(param_runs or 0) >= 2),
        "failed_backtests_recorded": bool(isinstance(failed_windows, list) and errors is not None),
        "sample": sample,
        "metrics": metrics,
        "artifact_path": str(artifact_path or ""),
        "source": "backtest_runtime_output",
    }


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def attach_backtest_credibility_to_signal_run(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    audit: Dict[str, Any],
    operator_name: str,
    evidence_note: str = "",
) -> Dict[str, Any]:
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
        raise ValueError(f"signal_run_missing:{run_id}")
    run_type, strategy, status, summary_json = row
    if str(run_type or "").lower() != "backtest":
        raise ValueError(f"not_backtest_run:{run_id}")
    if str(status or "").lower() != "success":
        raise ValueError(f"backtest_run_not_success:{run_id}")
    if not str(operator_name or "").strip():
        raise ValueError("missing_operator_name")
    review = evaluate_backtest_credibility(audit or {})
    if review.get("passed") is not True:
        raise ValueError("backtest_credibility_not_passed:" + ",".join(review.get("blocking_reasons") or []))

    summary = _safe_json_loads(summary_json)
    summary["backtest_credibility"] = audit or {}
    summary["backtest_credibility_attestation"] = {
        "operator_name": str(operator_name or "").strip(),
        "evidence_note": str(evidence_note or ""),
        "attached_at": _now_text(),
    }
    conn.execute(
        """
        UPDATE signal_runs
        SET summary_json = ?
        WHERE run_id = ?
        """,
        (canonical_json(summary), str(run_id or "")),
    )
    conn.commit()
    return {
        "run_id": str(run_id or ""),
        "strategy": str(strategy or ""),
        "attached": True,
        "review": review,
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
