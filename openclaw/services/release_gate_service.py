from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional

from openclaw.services.data_version_service import build_code_version
from openclaw.services.lineage_service import apply_professional_migrations, new_release_id
from openclaw.services.professional_audit_service import audit_professional_fact_chains
from openclaw.services.release_event_service import record_release_event, record_release_validation


def run_professional_fact_audit_gate(db_path: str | Path, *, output_path: str | Path = "") -> Dict[str, Any]:
    conn = sqlite3.connect(str(db_path), timeout=20)
    try:
        apply_professional_migrations(conn)
        audit = audit_professional_fact_chains(conn)
    finally:
        conn.close()

    summary = {
        "passed": audit.get("passed") is True,
        "missing_tables": audit.get("missing_tables") or [],
        "blocking_reasons": audit.get("blocking_reasons") or [],
        "chains": audit.get("chains") or {},
    }
    if output_path:
        Path(output_path).write_text(json.dumps(summary, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")
    return summary


def load_release_gate_audit_summary(path: str | Path) -> Dict[str, Any]:
    if not path:
        return {}
    audit_path = Path(path)
    if not audit_path.exists():
        return {}
    try:
        payload = json.loads(audit_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"passed": False, "blocking_reasons": [f"audit_summary_parse_failed:{exc}"]}
    return payload if isinstance(payload, dict) else {"passed": False, "blocking_reasons": ["audit_summary_not_object"]}


def build_release_gate_payload(*, log_file: str, audit_summary: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    audit = audit_summary or {}
    return {
        "log_file": log_file,
        "tool": "tools/release_gate.sh",
        "professional_fact_audit": audit,
        "rollback_context": {
            "blocking_reasons": audit.get("blocking_reasons") or [],
            "missing_tables": audit.get("missing_tables") or [],
        },
    }


def record_release_gate_ledger(
    *,
    db_path: str | Path,
    code_root: str | Path,
    overall: str,
    rounds: int,
    skip_remote: bool,
    log_file: str,
    validation_statuses: Dict[str, str],
    audit_summary: Optional[Dict[str, Any]] = None,
    operator_name: str = "",
) -> str:
    root = Path(code_root)
    conn = sqlite3.connect(str(db_path), timeout=20)
    try:
        apply_professional_migrations(conn)
        release_id = new_release_id()
        gate_result = {
            "passed": overall == "passed",
            "overall": overall,
            "rounds": int(rounds),
            "skip_remote": bool(skip_remote),
            "validations": validation_statuses,
        }
        record_release_event(
            conn,
            release_id=release_id,
            release_type="release_gate",
            code_version=build_code_version(root=root),
            config_version="",
            operator_name=operator_name or os.environ.get("USER", "system"),
            gate_result=gate_result,
            payload=build_release_gate_payload(log_file=log_file, audit_summary=audit_summary),
        )
        for validation_type, status in validation_statuses.items():
            record_release_validation(
                conn,
                release_id=release_id,
                validation_type=validation_type,
                validation_status=status,
                validation_output_path=log_file,
            )
        return release_id
    finally:
        conn.close()
