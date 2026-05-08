from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict, Iterable, Optional

from openclaw.services.backtest_credibility_service import evaluate_backtest_credibility
from openclaw.services.decision_service import record_decision_event, upsert_decision_snapshot
from openclaw.services.execution_evidence_service import summarize_execution_evidence
from openclaw.services.lineage_service import (
    apply_professional_migrations,
    canonical_json,
    insert_signal_run,
    new_decision_id,
)
from strategies.registry import experimental_strategies, production_strategies


EXPERIMENT_GOVERNANCE_TYPE = "experiment_strategy_evidence"
EXPERIMENT_DECISION_TYPES = {
    "experiment_observe",
    "experiment_reject",
    "experiment_promote_candidate",
}


def _safe_json_loads(value: Any) -> Dict[str, Any]:
    try:
        parsed = json.loads(str(value or "{}"))
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def strategy_tier(strategy: str) -> str:
    key = str(strategy or "").lower()
    if key in production_strategies():
        return "production"
    if key in experimental_strategies():
        return "experimental"
    return "unknown"


def _has_nonempty_dict(value: Any) -> bool:
    return isinstance(value, dict) and bool(value)


def build_experiment_signal_summary(
    *,
    strategy: str,
    hypothesis: str,
    params: Dict[str, Any],
    sample_window: Dict[str, Any],
    out_of_sample_window: Dict[str, Any],
    backtest_audit: Dict[str, Any],
    execution_evidence: Dict[str, Any],
    notes: str = "",
) -> Dict[str, Any]:
    tier = strategy_tier(strategy)
    return {
        "governance_type": EXPERIMENT_GOVERNANCE_TYPE,
        "strategy": str(strategy or "").lower(),
        "strategy_tier": tier,
        "hypothesis": str(hypothesis or ""),
        "params": params or {},
        "sample_window": sample_window or {},
        "out_of_sample_window": out_of_sample_window or {},
        "backtest_audit": backtest_audit or {},
        "execution_evidence": execution_evidence or {},
        "notes": str(notes or ""),
    }


def record_experiment_signal_evidence(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    strategy: str,
    trade_date: str,
    data_version: str,
    code_version: str,
    param_version: str,
    hypothesis: str,
    params: Dict[str, Any],
    sample_window: Dict[str, Any],
    out_of_sample_window: Dict[str, Any],
    backtest_audit: Dict[str, Any],
    execution_evidence: Dict[str, Any],
    status: str = "success",
    artifact_path: str = "",
    notes: str = "",
) -> str:
    apply_professional_migrations(conn)
    tier = strategy_tier(strategy)
    if tier != "experimental":
        raise ValueError(f"strategy_not_experimental:{strategy}")
    summary = build_experiment_signal_summary(
        strategy=strategy,
        hypothesis=hypothesis,
        params=params,
        sample_window=sample_window,
        out_of_sample_window=out_of_sample_window,
        backtest_audit=backtest_audit,
        execution_evidence=execution_evidence,
        notes=notes,
    )
    return insert_signal_run(
        conn,
        run_id=run_id,
        run_type="experiment",
        strategy=strategy,
        trade_date=trade_date,
        data_version=data_version,
        code_version=code_version,
        param_version=param_version,
        status=status,
        artifact_path=artifact_path,
        summary=summary,
    )


def evaluate_experiment_promotion_readiness(conn: sqlite3.Connection, *, run_id: str) -> Dict[str, Any]:
    apply_professional_migrations(conn)
    row = conn.execute(
        """
        SELECT strategy, data_version, code_version, param_version, status, summary_json
        FROM signal_runs
        WHERE run_id = ?
        """,
        (str(run_id or ""),),
    ).fetchone()
    if not row:
        return {
            "run_id": str(run_id or ""),
            "allow_promotion_candidate": False,
            "blocking_reasons": ["signal_run_missing"],
        }

    strategy, data_version, code_version, param_version, status, summary_json = row
    summary = _safe_json_loads(summary_json)
    backtest_audit = summary.get("backtest_audit") if isinstance(summary.get("backtest_audit"), dict) else {}
    backtest_credibility = evaluate_backtest_credibility(backtest_audit)
    execution_evidence = summary.get("execution_evidence") if isinstance(summary.get("execution_evidence"), dict) else {}
    blocking: list[str] = []
    if summary.get("governance_type") != EXPERIMENT_GOVERNANCE_TYPE:
        blocking.append("not_experiment_governance_evidence")
    if strategy_tier(str(strategy or "")) != "experimental":
        blocking.append("strategy_not_experimental")
    if str(status or "").lower() != "success":
        blocking.append("signal_run_not_success")
    if not data_version or not code_version or not param_version:
        blocking.append("missing_versions")
    if not str(summary.get("hypothesis") or "").strip():
        blocking.append("missing_hypothesis")
    if not _has_nonempty_dict(summary.get("sample_window")):
        blocking.append("missing_sample_window")
    if not _has_nonempty_dict(summary.get("out_of_sample_window")):
        blocking.append("missing_out_of_sample_window")
    if backtest_credibility.get("passed") is not True:
        blocking.append("backtest_credibility_not_passed")
    if str(execution_evidence.get("mode") or "") not in {"shadow", "canary"}:
        blocking.append("missing_shadow_or_canary_execution")
    if int(execution_evidence.get("sample_count") or 0) <= 0:
        blocking.append("missing_execution_sample")
    linked_decision_ids = execution_evidence.get("linked_decision_ids")
    if not isinstance(linked_decision_ids, list) or not [item for item in linked_decision_ids if str(item or "").strip()]:
        blocking.append("missing_execution_decision_links")
        execution_review = {"passed": False, "blocking_reasons": ["missing_execution_decision_links"]}
    else:
        execution_review = summarize_execution_evidence(conn, decision_ids=[str(item) for item in linked_decision_ids])
        if execution_review.get("passed") is not True:
            blocking.append("execution_evidence_review_not_passed")

    return {
        "run_id": str(run_id or ""),
        "strategy": str(strategy or ""),
        "allow_promotion_candidate": not blocking,
        "blocking_reasons": blocking,
        "evidence": {
            "data_version": str(data_version or ""),
            "code_version": str(code_version or ""),
            "param_version": str(param_version or ""),
            "hypothesis": str(summary.get("hypothesis") or ""),
            "sample_window": summary.get("sample_window") or {},
            "out_of_sample_window": summary.get("out_of_sample_window") or {},
            "backtest_audit": backtest_audit,
            "backtest_credibility": backtest_credibility,
            "execution_evidence": execution_evidence,
            "execution_review": execution_review,
        },
    }


def record_experiment_governance_decision(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    decision_type: str,
    operator_name: str,
    approval_reason_codes: Optional[Iterable[str]] = None,
    approval_note: str = "",
    decision_id: str = "",
) -> Dict[str, Any]:
    apply_professional_migrations(conn)
    normalized_type = str(decision_type or "").lower()
    if normalized_type not in EXPERIMENT_DECISION_TYPES:
        raise ValueError(f"unsupported_experiment_decision_type:{decision_type}")
    readiness = evaluate_experiment_promotion_readiness(conn, run_id=run_id)
    if normalized_type == "experiment_promote_candidate" and not readiness.get("allow_promotion_candidate"):
        raise ValueError("experiment_promotion_not_ready:" + ",".join(readiness.get("blocking_reasons") or []))

    final_decision_id = decision_id or new_decision_id()
    decision_status = "candidate" if normalized_type == "experiment_promote_candidate" else ("rejected" if normalized_type == "experiment_reject" else "observing")
    payload = {
        "governance_type": "experiment_strategy_decision",
        "run_id": str(run_id or ""),
        "decision_type": normalized_type,
        "readiness": readiness,
    }
    record_decision_event(
        conn,
        decision_id=final_decision_id,
        decision_type=normalized_type,
        based_on_run_id=run_id,
        risk_gate_state={"source": "experiment_governance", "allow_promotion_candidate": readiness.get("allow_promotion_candidate")},
        release_gate_state={"source": "experiment_governance", "release_gate_required": False},
        approval_reason_codes=list(approval_reason_codes or readiness.get("blocking_reasons") or ["experiment_governance_review"]),
        approval_note=approval_note or canonical_json({"decision": normalized_type, "blocking_reasons": readiness.get("blocking_reasons") or []}),
        operator_name=operator_name,
        decision_payload=payload,
    )
    upsert_decision_snapshot(
        conn,
        decision_id=final_decision_id,
        decision_status=decision_status,
        selected_count=0,
        active_flag=normalized_type == "experiment_promote_candidate",
    )
    return {
        "decision_id": final_decision_id,
        "decision_status": decision_status,
        "readiness": readiness,
    }
