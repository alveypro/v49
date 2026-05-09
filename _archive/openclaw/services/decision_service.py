from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any, Dict, Iterable, Optional

from openclaw.services.lineage_service import canonical_json


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def record_decision_event(
    conn: sqlite3.Connection,
    *,
    decision_id: str,
    decision_type: str,
    based_on_run_id: str = "",
    risk_gate_state: Optional[Dict[str, Any]] = None,
    release_gate_state: Optional[Dict[str, Any]] = None,
    approval_reason_codes: Optional[Iterable[str]] = None,
    approval_note: str = "",
    operator_name: str = "",
    decision_payload: Optional[Dict[str, Any]] = None,
) -> str:
    conn.execute(
        """
        INSERT OR REPLACE INTO decision_events (
            decision_id, decision_type, based_on_run_id, risk_gate_state, release_gate_state,
            approval_reason_codes, approval_note, operator_name, decision_payload_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(decision_id or ""),
            str(decision_type or "").lower(),
            str(based_on_run_id or ""),
            canonical_json(risk_gate_state or {}),
            canonical_json(release_gate_state or {}),
            canonical_json(list(approval_reason_codes or [])),
            str(approval_note or ""),
            str(operator_name or ""),
            canonical_json(decision_payload or {}),
            _now_text(),
        ),
    )
    conn.commit()
    return decision_id


def upsert_decision_snapshot(
    conn: sqlite3.Connection,
    *,
    decision_id: str,
    decision_status: str,
    effective_trade_date: str = "",
    selected_count: int = 0,
    active_flag: bool = False,
) -> str:
    conn.execute(
        """
        INSERT INTO decision_snapshot (
            decision_id, decision_status, effective_trade_date, selected_count, active_flag, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(decision_id) DO UPDATE SET
            decision_status=excluded.decision_status,
            effective_trade_date=excluded.effective_trade_date,
            selected_count=excluded.selected_count,
            active_flag=excluded.active_flag,
            updated_at=excluded.updated_at
        """,
        (
            str(decision_id or ""),
            str(decision_status or "").lower(),
            str(effective_trade_date or ""),
            int(selected_count or 0),
            1 if active_flag else 0,
            _now_text(),
        ),
    )
    conn.commit()
    return decision_id
