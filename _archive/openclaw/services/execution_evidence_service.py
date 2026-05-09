from __future__ import annotations

import sqlite3
from collections import Counter
from typing import Any, Dict, Iterable, List

from openclaw.services.lineage_service import apply_professional_migrations


TERMINAL_MISS_STATUSES = {"cancelled", "rejected", "expired", "manual_override"}
SOURCE_TIER_RANKS = {
    "simulated": 0,
    "quasi_live": 1,
    "broker": 2,
}
SOURCE_TYPE_TIERS = {
    "sim": "simulated",
    "shadow": "simulated",
    "legacy_feedback": "simulated",
    "overnight_feedback": "simulated",
    "manual": "simulated",
    "paper_broker": "quasi_live",
    "broker_rehearsal": "quasi_live",
    "rehearsal_broker": "quasi_live",
    "broker": "broker",
    "broker_api": "broker",
    "oms": "broker",
    "ems": "broker",
}


def _placeholders(values: List[str]) -> str:
    return ",".join("?" for _ in values)


def source_tier_for_execution_source(source_type: str) -> str:
    return SOURCE_TYPE_TIERS.get(str(source_type or "").strip().lower(), "")


def _source_tier(source_type: str) -> str:
    return source_tier_for_execution_source(source_type)


def _source_tier_rank(source_type: str) -> int:
    tier = _source_tier(source_type)
    return SOURCE_TIER_RANKS.get(tier, -1)


def _has_release_gate_state(value: str) -> bool:
    normalized = str(value or "").strip()
    return normalized not in {"", "{}", "null", "None"}


def summarize_execution_evidence(
    conn: sqlite3.Connection,
    *,
    decision_ids: Iterable[str] = (),
    order_ids: Iterable[str] = (),
    slippage_warn_bp: float = 150.0,
    minimum_source_tier: str = "simulated",
    block_high_slippage: bool = False,
) -> Dict[str, Any]:
    apply_professional_migrations(conn)
    required_tier = str(minimum_source_tier or "simulated").strip().lower()
    if required_tier not in SOURCE_TIER_RANKS:
        raise ValueError(f"unsupported minimum_source_tier: {minimum_source_tier}")
    required_rank = SOURCE_TIER_RANKS[required_tier]
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
            d.based_on_run_id, d.release_gate_state,
            r.strategy,
            a.fill_ratio, a.slippage_bp, a.miss_reason_code,
            MAX(CASE WHEN a.order_id IS NULL THEN 0 ELSE 1 END) AS attribution_found,
            COUNT(f.fill_id) AS fill_count,
            COALESCE(SUM(f.fill_qty), 0) AS filled_qty,
            MAX(CASE WHEN si.ts_code IS NULL THEN 0 ELSE 1 END) AS signal_item_found
        FROM execution_orders o
        LEFT JOIN decision_events d ON d.decision_id = o.decision_id
        LEFT JOIN signal_runs r ON r.run_id = d.based_on_run_id
        LEFT JOIN signal_items si ON si.run_id = d.based_on_run_id AND si.ts_code = o.ts_code
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
    source_tiers: Counter[str] = Counter()
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
            release_gate_state,
            strategy,
            fill_ratio,
            slippage_bp,
            miss_reason_code,
            attribution_found,
            fill_count,
            filled_qty,
            signal_item_found,
        ) = row
        normalized_status = str(status or "")
        normalized_source_type = str(source_type or "").strip().lower()
        source_tier = _source_tier(normalized_source_type)
        statuses[normalized_status] += 1
        if source_tier:
            source_tiers[source_tier] += 1
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
            "source_type": normalized_source_type,
            "source_tier": source_tier,
            "based_on_run_id": str(based_on_run_id or ""),
            "has_attribution": bool(attribution_found),
            "has_signal_item": bool(signal_item_found),
            "release_gate_state": str(release_gate_state or ""),
            "strategy": str(strategy or ""),
        }
        if not case["decision_id"]:
            blocking.append(f"missing_decision_id:{order_id}")
        if not case["based_on_run_id"]:
            blocking.append(f"missing_signal_lineage:{order_id}")
        elif not case["has_signal_item"]:
            blocking.append(f"missing_signal_item:{order_id}")
        if not _has_release_gate_state(case["release_gate_state"]):
            blocking.append(f"missing_release_gate_state:{order_id}")
        if not case["has_attribution"]:
            blocking.append(f"missing_attribution:{order_id}")
        if not case["broker_ref"]:
            blocking.append(f"missing_broker_ref:{order_id}")
        if not case["source_type"]:
            blocking.append(f"missing_source_type:{order_id}")
        elif not case["source_tier"]:
            blocking.append(f"unknown_source_type:{order_id}:{case['source_type']}")
        elif _source_tier_rank(case["source_type"]) < required_rank:
            blocking.append(f"source_tier_too_low:{order_id}:{case['source_type']}:{required_tier}")
        if normalized_status in TERMINAL_MISS_STATUSES and not reason:
            blocking.append(f"missing_miss_reason:{order_id}")
        if normalized_status == "manual_override" and not case["cancel_reason"]:
            blocking.append(f"missing_manual_override_reason:{order_id}")
        if normalized_status in {"filled", "partial_fill"} and int(fill_count or 0) <= 0:
            blocking.append(f"missing_fill:{order_id}")
        if float(slippage_bp or 0.0) >= float(slippage_warn_bp or 0.0):
            high_slippage_orders.append(str(order_id or ""))
            if block_high_slippage:
                blocking.append(f"high_slippage:{order_id}")
        cases.append(case)

    if not rows:
        blocking.append("execution_evidence_empty")

    return {
        "passed": not blocking,
        "blocking_reasons": blocking,
        "total_orders": len(rows),
        "status_counts": dict(sorted(statuses.items())),
        "miss_reason_counts": dict(sorted(miss_reasons.items())),
        "source_tier_counts": dict(sorted(source_tiers.items())),
        "minimum_source_tier": required_tier,
        "linked_run_ids": sorted(linked_run_ids),
        "high_slippage_orders": high_slippage_orders,
        "cases": cases,
    }
