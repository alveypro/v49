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


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}


def _date_from_text(value: str) -> str:
    text = str(value or "").strip()
    if len(text) >= 10 and text[4:5] == "-" and text[7:8] == "-":
        return text[:10]
    return datetime.now().strftime("%Y-%m-%d")


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
    now = _now_text()
    submitted_at_text = str(submitted_at or now)
    row = {
        "order_id": str(order_id or ""),
        "decision_id": str(decision_id or ""),
        "ts_code": str(ts_code or "").upper(),
        "side": str(side or "buy").lower(),
        "target_qty": int(target_qty or 0),
        "decision_price": float(decision_price or 0.0),
        "submitted_price": float(submitted_price or 0.0),
        "submitted_at": submitted_at_text,
        "status": normalized_status,
        "cancel_reason": str(cancel_reason or ""),
        "broker_ref": str(broker_ref or ""),
        "source_type": str(source_type or ""),
        "created_at": now,
        "updated_at": now,
        # Legacy execution_orders compatibility columns.
        "decision_date": _date_from_text(submitted_at_text),
        "trade_date": _date_from_text(submitted_at_text),
        "account_id": "default",
        "intent_source": str(source_type or "professional_fact_chain"),
        "order_type": "market",
        "order_price": float(submitted_price or decision_price or 0.0),
        "order_qty": int(target_qty or 0),
        "reason": str(cancel_reason or ""),
        "source_ref_type": str(source_type or ""),
        "source_ref_id": str(broker_ref or ""),
        "metadata_json": "{}",
    }
    columns = [name for name in row if name in _table_columns(conn, "execution_orders")]
    placeholders = ", ".join("?" for _ in columns)
    conn.execute(
        f"""
        INSERT OR REPLACE INTO execution_orders ({", ".join(columns)})
        VALUES ({placeholders})
        """,
        tuple(row[name] for name in columns),
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
