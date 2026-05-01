from __future__ import annotations

import sqlite3
from datetime import datetime


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def record_execution_fill(
    conn: sqlite3.Connection,
    *,
    fill_id: str,
    order_id: str,
    fill_price: float,
    fill_qty: int,
    fill_time: str = "",
    fill_fee: float = 0.0,
    fill_slippage_bp: float = 0.0,
    venue: str = "",
) -> str:
    conn.execute(
        """
        INSERT OR REPLACE INTO execution_fills (
            fill_id, order_id, fill_price, fill_qty, fill_time, fill_fee, fill_slippage_bp, venue, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(fill_id or ""),
            str(order_id or ""),
            float(fill_price or 0.0),
            int(fill_qty or 0),
            str(fill_time or _now_text()),
            float(fill_fee or 0.0),
            float(fill_slippage_bp or 0.0),
            str(venue or ""),
            _now_text(),
        ),
    )
    conn.commit()
    return fill_id
