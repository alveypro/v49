from __future__ import annotations

import sqlite3
from typing import Any, Dict, List


REQUIRED_TABLES = (
    "signal_runs",
    "signal_items",
    "decision_events",
    "decision_snapshot",
    "execution_orders",
    "execution_fills",
    "execution_attribution",
    "release_events",
    "release_validations",
)


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return bool(conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone())


def _count(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> int:
    return int((conn.execute(sql, params).fetchone() or [0])[0] or 0)


def audit_professional_fact_chains(conn: sqlite3.Connection) -> Dict[str, Any]:
    missing_tables = [table for table in REQUIRED_TABLES if not _table_exists(conn, table)]
    if missing_tables:
        return {
            "passed": False,
            "missing_tables": missing_tables,
            "chains": {},
            "blocking_reasons": [f"missing_table:{table}" for table in missing_tables],
        }

    chains = {
        "signal": _audit_signal_chain(conn),
        "decision": _audit_decision_chain(conn),
        "execution": _audit_execution_chain(conn),
        "release": _audit_release_chain(conn),
    }
    blocking: List[str] = []
    for chain, result in chains.items():
        for reason in result.get("blocking_reasons", []):
            blocking.append(f"{chain}:{reason}")
    return {
        "passed": not blocking,
        "missing_tables": [],
        "chains": chains,
        "blocking_reasons": blocking,
    }


def _audit_signal_chain(conn: sqlite3.Connection) -> Dict[str, Any]:
    total = _count(conn, "SELECT COUNT(*) FROM signal_runs")
    missing_versions = _count(
        conn,
        """
        SELECT COUNT(*)
        FROM signal_runs
        WHERE COALESCE(data_version, '') = ''
           OR COALESCE(code_version, '') = ''
           OR COALESCE(param_version, '') = ''
        """,
    )
    orphan_items = _count(
        conn,
        """
        SELECT COUNT(*)
        FROM signal_items i
        LEFT JOIN signal_runs r ON r.run_id = i.run_id
        WHERE r.run_id IS NULL
        """,
    )
    blocking = []
    if missing_versions:
        blocking.append(f"missing_versions:{missing_versions}")
    if orphan_items:
        blocking.append(f"orphan_signal_items:{orphan_items}")
    return {"total_runs": total, "missing_versions": missing_versions, "orphan_items": orphan_items, "blocking_reasons": blocking}


def _audit_decision_chain(conn: sqlite3.Connection) -> Dict[str, Any]:
    total = _count(conn, "SELECT COUNT(*) FROM decision_events")
    missing_reason = _count(
        conn,
        """
        SELECT COUNT(*)
        FROM decision_events
        WHERE COALESCE(approval_note, '') = ''
           OR approval_reason_codes IS NULL
           OR approval_reason_codes = '[]'
        """,
    )
    missing_gate_state = _count(
        conn,
        """
        SELECT COUNT(*)
        FROM decision_events
        WHERE COALESCE(risk_gate_state, '') IN ('', '{}')
           OR COALESCE(release_gate_state, '') IN ('', '{}')
        """,
    )
    missing_snapshot = _count(
        conn,
        """
        SELECT COUNT(*)
        FROM decision_events e
        LEFT JOIN decision_snapshot s ON s.decision_id = e.decision_id
        WHERE s.decision_id IS NULL
        """,
    )
    blocking = []
    if missing_reason:
        blocking.append(f"missing_reason:{missing_reason}")
    if missing_gate_state:
        blocking.append(f"missing_gate_state:{missing_gate_state}")
    if missing_snapshot:
        blocking.append(f"missing_snapshot:{missing_snapshot}")
    return {
        "total_events": total,
        "missing_reason": missing_reason,
        "missing_gate_state": missing_gate_state,
        "missing_snapshot": missing_snapshot,
        "blocking_reasons": blocking,
    }


def _audit_execution_chain(conn: sqlite3.Connection) -> Dict[str, Any]:
    total = _count(conn, "SELECT COUNT(*) FROM execution_orders")
    missing_decision = _count(
        conn,
        """
        SELECT COUNT(*)
        FROM execution_orders o
        LEFT JOIN decision_events d ON d.decision_id = o.decision_id
        WHERE d.decision_id IS NULL
        """,
    )
    missing_attribution = _count(
        conn,
        """
        SELECT COUNT(*)
        FROM execution_orders o
        LEFT JOIN execution_attribution a ON a.order_id = o.order_id
        WHERE a.order_id IS NULL
        """,
    )
    missing_miss_reason = _count(
        conn,
        """
        SELECT COUNT(*)
        FROM execution_orders o
        LEFT JOIN execution_attribution a ON a.order_id = o.order_id
        WHERE o.status IN ('cancelled', 'rejected', 'expired', 'manual_override')
          AND COALESCE(a.miss_reason_code, '') = ''
        """,
    )
    missing_signal_lineage = _count(
        conn,
        """
        SELECT COUNT(*)
        FROM execution_orders o
        LEFT JOIN decision_events d ON d.decision_id = o.decision_id
        LEFT JOIN signal_runs r ON r.run_id = d.based_on_run_id
        WHERE d.decision_id IS NOT NULL
          AND (COALESCE(d.based_on_run_id, '') = '' OR r.run_id IS NULL)
        """,
    )
    blocking = []
    if missing_decision:
        blocking.append(f"missing_decision:{missing_decision}")
    if missing_attribution:
        blocking.append(f"missing_attribution:{missing_attribution}")
    if missing_miss_reason:
        blocking.append(f"missing_miss_reason:{missing_miss_reason}")
    if missing_signal_lineage:
        blocking.append(f"missing_signal_lineage:{missing_signal_lineage}")
    return {
        "total_orders": total,
        "missing_decision": missing_decision,
        "missing_attribution": missing_attribution,
        "missing_miss_reason": missing_miss_reason,
        "missing_signal_lineage": missing_signal_lineage,
        "blocking_reasons": blocking,
    }


def _audit_release_chain(conn: sqlite3.Connection) -> Dict[str, Any]:
    total = _count(conn, "SELECT COUNT(*) FROM release_events")
    missing_code_version = _count(
        conn,
        "SELECT COUNT(*) FROM release_events WHERE COALESCE(code_version, '') = ''",
    )
    missing_validation = _count(
        conn,
        """
        SELECT COUNT(*)
        FROM release_events e
        LEFT JOIN release_validations v ON v.release_id = e.release_id
        WHERE v.release_id IS NULL
        """,
    )
    blocking = []
    if missing_code_version:
        blocking.append(f"missing_code_version:{missing_code_version}")
    if missing_validation:
        blocking.append(f"missing_validation:{missing_validation}")
    return {
        "total_events": total,
        "missing_code_version": missing_code_version,
        "missing_validation": missing_validation,
        "blocking_reasons": blocking,
    }
