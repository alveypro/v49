from __future__ import annotations

import sqlite3
from hashlib import sha256
from typing import Any, Dict, Iterable, List

from openclaw.services.execution_analytics_service import compute_execution_attribution, upsert_execution_attribution
from openclaw.services.execution_evidence_service import source_tier_for_execution_source, summarize_execution_evidence
from openclaw.services.execution_fill_service import record_execution_fill
from openclaw.services.execution_order_service import ALLOWED_ORDER_STATUSES, create_execution_order, update_execution_order_status
from openclaw.services.lineage_service import apply_professional_migrations, new_order_id


BROKER_SOURCE_TIERS = {"broker"}
MANUAL_BROKER_SOURCE_TYPES = {"broker"}
TERMINAL_MISS_STATUSES = {"cancelled", "rejected", "expired", "manual_override"}


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _require_text(report: Dict[str, Any], key: str) -> str:
    value = _clean_text(report.get(key))
    if not value:
        raise ValueError(f"missing_required_broker_field:{key}")
    return value


def _is_sha256(value: str) -> bool:
    text = _clean_text(value).lower()
    return len(text) == 64 and all(ch in "0123456789abcdef" for ch in text)


def _to_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _to_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _deterministic_fill_id(*, broker_ref: str, fill: Dict[str, Any], idx: int) -> str:
    explicit = _clean_text(fill.get("fill_id"))
    if explicit:
        return explicit
    fill_ref = _clean_text(fill.get("fill_ref")) or _clean_text(fill.get("broker_fill_ref")) or str(idx)
    digest = sha256(f"{broker_ref}|{fill_ref}".encode("utf-8")).hexdigest()[:18]
    return f"fill_broker_{digest}"


def _manual_broker_ref(*, evidence_ref: str, evidence_sha256: str) -> str:
    digest = _clean_text(evidence_sha256).lower()
    compact_ref = _clean_text(evidence_ref).replace(" ", "_")
    return f"manual_broker:{compact_ref}:{digest[:16]}"


def _validate_manual_broker_attestation(report: Dict[str, Any], *, broker_ref: str, source_type: str) -> Dict[str, str]:
    if source_type not in MANUAL_BROKER_SOURCE_TYPES:
        return {}
    operator_name = _require_text(report, "operator_name")
    evidence_ref = _require_text(report, "evidence_ref")
    evidence_sha256 = _require_text(report, "evidence_sha256").lower()
    if not _is_sha256(evidence_sha256):
        raise ValueError("invalid_evidence_sha256")
    if evidence_ref not in broker_ref or evidence_sha256[:16] not in broker_ref:
        raise ValueError("broker_ref_missing_manual_evidence_anchor")
    return {
        "operator_name": operator_name,
        "evidence_ref": evidence_ref,
        "evidence_sha256": evidence_sha256,
    }


def _validate_decision_lineage(conn: sqlite3.Connection, *, decision_id: str, ts_code: str) -> None:
    row = conn.execute(
        """
        SELECT based_on_run_id, release_gate_state
        FROM decision_events
        WHERE decision_id = ?
        """,
        (decision_id,),
    ).fetchone()
    if not row:
        raise ValueError(f"missing_decision_event:{decision_id}")
    based_on_run_id = _clean_text(row[0])
    release_gate_state = _clean_text(row[1])
    if not based_on_run_id:
        raise ValueError(f"missing_signal_lineage:{decision_id}")
    if release_gate_state in {"", "{}", "null", "None"}:
        raise ValueError(f"missing_release_gate_state:{decision_id}")
    signal_item = conn.execute(
        """
        SELECT 1
        FROM signal_items
        WHERE run_id = ? AND ts_code = ?
        LIMIT 1
        """,
        (based_on_run_id, ts_code),
    ).fetchone()
    if not signal_item:
        raise ValueError(f"missing_signal_item:{decision_id}:{ts_code}")


def _filled_quantity(fills: Iterable[Dict[str, Any]]) -> int:
    return sum(max(_to_int(fill.get("fill_qty")), 0) for fill in fills)


def _average_fill_price(fills: List[Dict[str, Any]]) -> float:
    qty = _filled_quantity(fills)
    if qty <= 0:
        return 0.0
    notional = sum(_to_float(fill.get("fill_price")) * max(_to_int(fill.get("fill_qty")), 0) for fill in fills)
    return notional / float(qty)


def record_broker_execution_report(
    conn: sqlite3.Connection,
    *,
    report: Dict[str, Any],
) -> Dict[str, Any]:
    """Write an authoritative broker/OMS/EMS execution report into the existing execution fact chain."""
    apply_professional_migrations(conn)
    decision_id = _require_text(report, "decision_id")
    ts_code = _require_text(report, "ts_code")
    broker_ref = _require_text(report, "broker_ref")
    source_type = _require_text(report, "source_type").lower()
    if source_tier_for_execution_source(source_type) not in BROKER_SOURCE_TIERS:
        raise ValueError(f"non_broker_execution_source:{source_type}")
    manual_attestation = _validate_manual_broker_attestation(report, broker_ref=broker_ref, source_type=source_type)

    status = _require_text(report, "status").lower()
    if status not in ALLOWED_ORDER_STATUSES:
        raise ValueError(f"unsupported_order_status:{status}")
    fills = list(report.get("fills") or [])
    if status in {"filled", "partial_fill"} and not fills:
        raise ValueError(f"missing_broker_fills:{broker_ref}")
    miss_reason_code = _clean_text(report.get("miss_reason_code"))
    cancel_reason = _clean_text(report.get("cancel_reason"))
    if status in TERMINAL_MISS_STATUSES and not miss_reason_code:
        raise ValueError(f"missing_broker_miss_reason:{broker_ref}")
    if status == "manual_override" and not cancel_reason:
        raise ValueError(f"missing_manual_override_reason:{broker_ref}")

    _validate_decision_lineage(conn, decision_id=decision_id, ts_code=ts_code)

    existing = conn.execute(
        "SELECT order_id FROM execution_orders WHERE broker_ref = ? ORDER BY created_at DESC LIMIT 1",
        (broker_ref,),
    ).fetchone()
    order_id = _clean_text(existing[0]) if existing and existing[0] else _clean_text(report.get("order_id")) or new_order_id()
    target_qty = _to_int(report.get("target_qty"))
    decision_price = _to_float(report.get("decision_price"))
    submitted_price = _to_float(report.get("submitted_price"))
    if existing:
        update_execution_order_status(
            conn,
            order_id=order_id,
            status=status,
            cancel_reason=cancel_reason,
            broker_ref=broker_ref,
        )
    else:
        create_execution_order(
            conn,
            order_id=order_id,
            decision_id=decision_id,
            ts_code=ts_code,
            side=_clean_text(report.get("side")) or "buy",
            target_qty=target_qty,
            decision_price=decision_price,
            submitted_price=submitted_price,
            submitted_at=_clean_text(report.get("submitted_at")),
            status=status,
            cancel_reason=cancel_reason,
            broker_ref=broker_ref,
            source_type=source_type,
        )

    for idx, fill in enumerate(fills, start=1):
        record_execution_fill(
            conn,
            fill_id=_deterministic_fill_id(broker_ref=broker_ref, fill=fill, idx=idx),
            order_id=order_id,
            fill_price=_to_float(fill.get("fill_price")),
            fill_qty=_to_int(fill.get("fill_qty")),
            fill_time=_clean_text(fill.get("fill_time")),
            fill_fee=_to_float(fill.get("fill_fee")),
            fill_slippage_bp=_to_float(fill.get("fill_slippage_bp")),
            venue=_clean_text(fill.get("venue")),
        )

    filled_qty = _filled_quantity(fills)
    avg_fill_price = _average_fill_price(fills)
    attribution = compute_execution_attribution(
        decision_price=decision_price,
        submit_price=submitted_price,
        avg_fill_price=avg_fill_price,
        close_price=_to_float(report.get("close_price")),
        target_qty=target_qty,
        filled_qty=filled_qty,
        delay_sec=_to_float(report.get("delay_sec")),
        miss_reason_code=miss_reason_code,
    )
    upsert_execution_attribution(conn, order_id=order_id, attribution=attribution)

    summary = summarize_execution_evidence(
        conn,
        order_ids=[order_id],
        minimum_source_tier="broker",
        block_high_slippage=bool(report.get("block_high_slippage")),
    )
    return {
        "order_id": order_id,
        "broker_ref": broker_ref,
        "status": status,
        "fill_count": len(fills),
        "filled_qty": filled_qty,
        "manual_attestation": manual_attestation,
        "evidence_summary": summary,
    }


def record_manual_broker_execution_attestation(
    conn: sqlite3.Connection,
    *,
    report: Dict[str, Any],
) -> Dict[str, Any]:
    """Record a human-attested broker app result without allowing it to masquerade as API execution."""
    evidence_ref = _require_text(report, "evidence_ref")
    evidence_sha256 = _require_text(report, "evidence_sha256").lower()
    if not _is_sha256(evidence_sha256):
        raise ValueError("invalid_evidence_sha256")
    broker_ref = _clean_text(report.get("broker_ref")) or _manual_broker_ref(
        evidence_ref=evidence_ref,
        evidence_sha256=evidence_sha256,
    )
    enriched = dict(report)
    enriched["source_type"] = "broker"
    enriched["broker_ref"] = broker_ref
    return record_broker_execution_report(conn, report=enriched)
