from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Dict


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def compute_execution_attribution(
    *,
    decision_price: float,
    submit_price: float,
    avg_fill_price: float,
    close_price: float,
    target_qty: int,
    filled_qty: int,
    delay_sec: float,
    miss_reason_code: str = "",
) -> Dict[str, float | str]:
    target = max(int(target_qty or 0), 0)
    filled = max(int(filled_qty or 0), 0)
    ratio = 0.0 if target <= 0 else min(1.0, filled / float(target))
    base_price = float(decision_price or 0.0)
    slippage_bp = 0.0
    if base_price > 0 and avg_fill_price > 0:
        slippage_bp = ((float(avg_fill_price) - base_price) / base_price) * 10000.0
    return {
        "decision_price": float(decision_price or 0.0),
        "submit_price": float(submit_price or 0.0),
        "avg_fill_price": float(avg_fill_price or 0.0),
        "close_price": float(close_price or 0.0),
        "delay_sec": float(delay_sec or 0.0),
        "fill_ratio": ratio,
        "slippage_bp": slippage_bp,
        "miss_reason_code": str(miss_reason_code or ""),
    }


def upsert_execution_attribution(
    conn: sqlite3.Connection,
    *,
    order_id: str,
    attribution: Dict[str, float | str],
) -> str:
    conn.execute(
        """
        INSERT INTO execution_attribution (
            order_id, decision_price, submit_price, avg_fill_price, close_price,
            delay_sec, fill_ratio, slippage_bp, miss_reason_code, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(order_id) DO UPDATE SET
            decision_price=excluded.decision_price,
            submit_price=excluded.submit_price,
            avg_fill_price=excluded.avg_fill_price,
            close_price=excluded.close_price,
            delay_sec=excluded.delay_sec,
            fill_ratio=excluded.fill_ratio,
            slippage_bp=excluded.slippage_bp,
            miss_reason_code=excluded.miss_reason_code,
            updated_at=excluded.updated_at
        """,
        (
            str(order_id or ""),
            float(attribution.get("decision_price", 0.0) or 0.0),
            float(attribution.get("submit_price", 0.0) or 0.0),
            float(attribution.get("avg_fill_price", 0.0) or 0.0),
            float(attribution.get("close_price", 0.0) or 0.0),
            float(attribution.get("delay_sec", 0.0) or 0.0),
            float(attribution.get("fill_ratio", 0.0) or 0.0),
            float(attribution.get("slippage_bp", 0.0) or 0.0),
            str(attribution.get("miss_reason_code", "") or ""),
            _now_text(),
        ),
    )
    conn.commit()
    return order_id
