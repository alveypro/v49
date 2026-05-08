from __future__ import annotations

import sqlite3
from collections import Counter
from typing import Any, Dict, Iterable, List

from openclaw.services.lineage_service import apply_professional_migrations


TERMINAL_MISS_STATUSES = {"cancelled", "rejected", "expired", "manual_override"}


def _placeholders(values: List[str]) -> str:
    return ",".join("?" for _ in values)


def summarize_execution_evidence(
    conn: sqlite3.Connection,
    *,
    decision_ids: Iterable[str] = (),
    order_ids: Iterable[str] = (),
    slippage_warn_bp: float = 150.0,
) -> Dict[str, Any]:
    apply_professional_migrations(conn)
    decision_id_list = [str(item or "").strip() for item in decision_ids if str(item or "").strip()]
    order_id_list = [str(item or "").strip() for item in order_ids if str(item or "").strip()]
    filters: List[str] = []
    params: List[str] = []
    if decision_id_list:
        filters.append(f"o.decision_id IN ({_placeholders(decision_id_list)})")
        params.extend(decision_id_list)
    if order_id_list:
        filters.append(f"o.order_id IN ({_placeholders(order_id_list)})")
        params.extend(order_id_list)
    where_clause = "WHERE " + " OR ".join(filters) if filters else ""
    rows = conn.execute(
        f"""
        SELECT
            o.order_id, o.decision_id, o.ts_code, o.status, o.target_qty, o.cancel_reason,
            o.broker_ref, o.source_type,
            d.based_on_run_id,
            r.strategy,
            a.fill_ratio, a.slippage_bp, a.miss_reason_code,
            COUNT(f.fill_id) AS fill_count,
            COALESCE(SUM(f.fill_qty), 0) AS filled_qty
        FROM execution_orders o
        LEFT JOIN decision_events d ON d.decision_id = o.decision_id
        LEFT JOIN signal_runs r ON r.run_id = d.based_on_run_id
        LEFT JOIN execution_attribution a ON a.order_id = o.order_id
        LEFT JOIN execution_fills f ON f.order_id = o.order_id
        {where_clause}
        GROUP BY o.order_id
        ORDER BY o.created_at ASC, o.rowid ASC
        """,
        tuple(params),
    ).fetchall()

    statuses: Counter[str] = Counter()
    miss_reasons: Counter[str] = Counter()
    blocking: List[str] = []
    cases: List[Dict[str, Any]] = []
    high_slippage_orders: List[str] = []
    linked_run_ids = set()
    for row in rows:
        (
            order_id,
            decision_id,
            ts_code,
            status,
            target_qty,
            cancel_reason,
            broker_ref,
            source_type,
            based_on_run_id,
            strategy,
            fill_ratio,
            slippage_bp,
            miss_reason_code,
            fill_count,
            filled_qty,
        ) = row
        normalized_status = str(status or "")
        statuses[normalized_status] += 1
        reason = str(miss_reason_code or "")
        if reason:
            miss_reasons[reason] += 1
        if based_on_run_id:
            linked_run_ids.add(str(based_on_run_id))
        case = {
            "order_id": str(order_id or ""),
            "decision_id": str(decision_id or ""),
            "ts_code": str(ts_code or ""),
            "status": normalized_status,
            "target_qty": int(target_qty or 0),
            "filled_qty": int(filled_qty or 0),
            "fill_count": int(fill_count or 0),
            "fill_ratio": float(fill_ratio or 0.0),
            "slippage_bp": float(slippage_bp or 0.0),
            "miss_reason_code": reason,
            "cancel_reason": str(cancel_reason or ""),
            "broker_ref": str(broker_ref or ""),
            "source_type": str(source_type or ""),
            "based_on_run_id": str(based_on_run_id or ""),
            "strategy": str(strategy or ""),
        }
        if not case["based_on_run_id"]:
            blocking.append(f"missing_signal_lineage:{order_id}")
        if not case["broker_ref"]:
            blocking.append(f"missing_broker_ref:{order_id}")
        if not case["source_type"]:
            blocking.append(f"missing_source_type:{order_id}")
        if normalized_status in TERMINAL_MISS_STATUSES and not reason:
            blocking.append(f"missing_miss_reason:{order_id}")
        if normalized_status in {"filled", "partial_fill"} and int(fill_count or 0) <= 0:
            blocking.append(f"missing_fill:{order_id}")
        if float(slippage_bp or 0.0) >= float(slippage_warn_bp or 0.0):
            high_slippage_orders.append(str(order_id or ""))
        cases.append(case)

    if not rows:
        blocking.append("execution_evidence_empty")

    return {
        "passed": not blocking,
        "blocking_reasons": blocking,
        "total_orders": len(rows),
        "status_counts": dict(sorted(statuses.items())),
        "miss_reason_counts": dict(sorted(miss_reasons.items())),
        "linked_run_ids": sorted(linked_run_ids),
        "high_slippage_orders": high_slippage_orders,
        "cases": cases,
    }
