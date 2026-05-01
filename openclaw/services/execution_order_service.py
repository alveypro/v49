from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Optional


ALLOWED_ORDER_STATUSES = {
    "created",
    "submitted",
    "partial_fill",
    "filled",
    "cancelled",
    "rejected",
    "expired",
    "manual_override",
}


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def create_execution_order(
    conn: sqlite3.Connection,
    *,
    order_id: str,
    decision_id: str,
    ts_code: str,
    side: str = "buy",
    target_qty: int = 0,
    decision_price: float = 0.0,
    submitted_price: float = 0.0,
    submitted_at: str = "",
    status: str = "created",
    cancel_reason: str = "",
    broker_ref: str = "",
    source_type: str = "",
) -> str:
    normalized_status = str(status or "created").lower()
    if normalized_status not in ALLOWED_ORDER_STATUSES:
        raise ValueError(f"unsupported execution order status: {normalized_status}")
    conn.execute(
        """
        INSERT OR REPLACE INTO execution_orders (
            order_id, decision_id, ts_code, side, target_qty, decision_price, submitted_price,
            submitted_at, status, cancel_reason, broker_ref, source_type, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(order_id or ""),
            str(decision_id or ""),
            str(ts_code or "").upper(),
            str(side or "buy").lower(),
            int(target_qty or 0),
            float(decision_price or 0.0),
            float(submitted_price or 0.0),
            str(submitted_at or _now_text()),
            normalized_status,
            str(cancel_reason or ""),
            str(broker_ref or ""),
            str(source_type or ""),
            _now_text(),
            _now_text(),
        ),
    )
    conn.commit()
    return order_id


def update_execution_order_status(
    conn: sqlite3.Connection,
    *,
    order_id: str,
    status: str,
    cancel_reason: str = "",
    broker_ref: Optional[str] = None,
) -> str:
    normalized_status = str(status or "").lower()
    if normalized_status not in ALLOWED_ORDER_STATUSES:
        raise ValueError(f"unsupported execution order status: {normalized_status}")
    conn.execute(
        """
        UPDATE execution_orders
        SET status = ?, cancel_reason = ?, broker_ref = COALESCE(?, broker_ref), updated_at = ?
        WHERE order_id = ?
        """,
        (
            normalized_status,
            str(cancel_reason or ""),
            broker_ref,
            _now_text(),
            str(order_id or ""),
        ),
    )
    conn.commit()
    return order_id
