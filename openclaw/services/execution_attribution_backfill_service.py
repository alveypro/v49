from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List

from openclaw.services.execution_analytics_service import compute_execution_attribution, upsert_execution_attribution
from openclaw.services.lineage_service import apply_professional_migrations


JsonDict = Dict[str, Any]


def _now() -> datetime:
    return datetime.now()


def _now_text() -> str:
    return _now().strftime("%Y-%m-%d %H:%M:%S")


def _parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _compact_date(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) >= 10 and text[4:5] == "-" and text[7:8] == "-":
        return text[:10].replace("-", "")
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits[:8]


def _miss_reason_for_status(status: str) -> str:
    normalized = str(status or "").strip().lower()
    mapping = {
        "created": "legacy_created_no_attribution",
        "submitted": "legacy_submitted_no_attribution",
        "cancelled": "legacy_cancelled_no_attribution",
        "rejected": "legacy_rejected_no_attribution",
        "expired": "legacy_expired_no_attribution",
        "manual_override": "legacy_manual_override_no_attribution",
    }
    return mapping.get(normalized, "legacy_missing_attribution")


def _latest_close_price(conn: sqlite3.Connection, *, ts_code: str, submitted_at: str) -> float:
    trade_date = _compact_date(submitted_at)
    if not trade_date:
        return 0.0
    row = conn.execute(
        """
        SELECT close_price
        FROM daily_trading_data
        WHERE ts_code = ?
          AND trade_date <= ?
        ORDER BY trade_date DESC
        LIMIT 1
        """,
        (str(ts_code or "").upper(), trade_date),
    ).fetchone()
    return float((row or [0.0])[0] or 0.0)


def _candidate_orders(
    conn: sqlite3.Connection,
    *,
    statuses: Iterable[str],
) -> List[JsonDict]:
    status_values = [str(item or "").strip().lower() for item in statuses if str(item or "").strip()]
    if not status_values:
        return []
    placeholders = ",".join("?" for _ in status_values)
    rows = conn.execute(
        f"""
        SELECT
            o.order_id, o.decision_id, o.ts_code, o.status, o.target_qty,
            o.decision_price, o.submitted_price, o.submitted_at, o.created_at,
            COALESCE(SUM(f.fill_qty), 0) AS filled_qty,
            COALESCE(SUM(f.fill_price * f.fill_qty), 0.0) AS fill_notional
        FROM execution_orders o
        LEFT JOIN execution_attribution a ON a.order_id = o.order_id
        LEFT JOIN execution_fills f ON f.order_id = o.order_id
        WHERE a.order_id IS NULL
          AND LOWER(COALESCE(o.status, '')) IN ({placeholders})
        GROUP BY o.order_id
        ORDER BY o.created_at ASC, o.order_id ASC
        """,
        tuple(status_values),
    ).fetchall()
    out: List[JsonDict] = []
    for row in rows:
        filled_qty = int(row[9] or 0)
        fill_notional = float(row[10] or 0.0)
        avg_fill_price = 0.0 if filled_qty <= 0 else fill_notional / float(filled_qty)
        out.append(
            {
                "order_id": str(row[0] or ""),
                "decision_id": str(row[1] or ""),
                "ts_code": str(row[2] or "").upper(),
                "status": str(row[3] or "").lower(),
                "target_qty": int(row[4] or 0),
                "decision_price": float(row[5] or 0.0),
                "submitted_price": float(row[6] or 0.0),
                "submitted_at": str(row[7] or ""),
                "created_at": str(row[8] or ""),
                "filled_qty": filled_qty,
                "avg_fill_price": float(avg_fill_price or 0.0),
            }
        )
    return out


def _is_stale(created_at: str, *, stale_minutes: int, now: datetime) -> bool:
    if stale_minutes <= 0:
        return True
    created = _parse_datetime(created_at)
    if created is None:
        return True
    return created <= now - timedelta(minutes=int(stale_minutes))


def backfill_missing_execution_attribution(
    conn: sqlite3.Connection,
    *,
    statuses: Iterable[str] = ("created", "submitted"),
    stale_minutes: int = 30,
    max_orders: int = 500,
    apply_changes: bool = False,
) -> JsonDict:
    apply_professional_migrations(conn)
    now = _now()
    candidates = _candidate_orders(conn, statuses=statuses)
    selected = [
        item
        for item in candidates
        if _is_stale(str(item.get("created_at") or ""), stale_minutes=stale_minutes, now=now)
    ][: max(0, int(max_orders or 0))]

    patched: List[JsonDict] = []
    for order in selected:
        close_price = _latest_close_price(
            conn,
            ts_code=str(order.get("ts_code") or ""),
            submitted_at=str(order.get("submitted_at") or order.get("created_at") or ""),
        )
        miss_reason_code = _miss_reason_for_status(str(order.get("status") or ""))
        attribution = compute_execution_attribution(
            decision_price=float(order.get("decision_price") or 0.0),
            submit_price=float(order.get("submitted_price") or 0.0),
            avg_fill_price=float(order.get("avg_fill_price") or 0.0),
            close_price=float(close_price or 0.0),
            target_qty=int(order.get("target_qty") or 0),
            filled_qty=int(order.get("filled_qty") or 0),
            delay_sec=0.0,
            miss_reason_code=miss_reason_code,
        )
        patched.append(
            {
                **order,
                "computed_close_price": float(close_price or 0.0),
                "computed_miss_reason_code": miss_reason_code,
                "computed_fill_ratio": float(attribution.get("fill_ratio") or 0.0),
                "computed_slippage_bp": float(attribution.get("slippage_bp") or 0.0),
            }
        )
        if apply_changes:
            upsert_execution_attribution(
                conn,
                order_id=str(order.get("order_id") or ""),
                attribution=attribution,
            )

    status_counts: Dict[str, int] = {}
    for row in patched:
        key = str(row.get("status") or "")
        status_counts[key] = int(status_counts.get(key, 0)) + 1

    return {
        "artifact_version": "execution_attribution_backfill.v1",
        "created_at": _now_text(),
        "apply_changes": bool(apply_changes),
        "stale_minutes": int(stale_minutes or 0),
        "max_orders": int(max_orders or 0),
        "candidate_count": len(candidates),
        "selected_count": len(selected),
        "patched_count": len(patched),
        "status_counts": dict(sorted(status_counts.items())),
        "patched_orders": patched,
    }

