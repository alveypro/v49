from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any, Dict, Optional

from openclaw.services.lineage_service import canonical_json


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def record_release_event(
    conn: sqlite3.Connection,
    *,
    release_id: str,
    release_type: str,
    code_version: str,
    config_version: str = "",
    operator_name: str = "",
    gate_result: Optional[Dict[str, Any]] = None,
    payload: Optional[Dict[str, Any]] = None,
) -> str:
    conn.execute(
        """
        INSERT OR REPLACE INTO release_events (
            release_id, release_type, code_version, config_version, operator_name, gate_result, payload_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(release_id or ""),
            str(release_type or "").lower(),
            str(code_version or ""),
            str(config_version or ""),
            str(operator_name or ""),
            canonical_json(gate_result or {}),
            canonical_json(payload or {}),
            _now_text(),
        ),
    )
    conn.commit()
    return release_id


def record_release_validation(
    conn: sqlite3.Connection,
    *,
    release_id: str,
    validation_type: str,
    validation_status: str,
    validation_output_path: str = "",
) -> int:
    cur = conn.execute(
        """
        INSERT INTO release_validations (
            release_id, validation_type, validation_status, validation_output_path, created_at
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (
            str(release_id or ""),
            str(validation_type or "").lower(),
            str(validation_status or "").lower(),
            str(validation_output_path or ""),
            _now_text(),
        ),
    )
    conn.commit()
    return int(cur.lastrowid or 0)
