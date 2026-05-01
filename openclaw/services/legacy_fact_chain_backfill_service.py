from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List

from openclaw.services.airivo_feedback_service import _fetch_feedback_fact, _upsert_execution_fact_from_feedback
from openclaw.services.data_version_service import build_code_version, build_data_version
from openclaw.services.decision_service import record_decision_event, upsert_decision_snapshot
from openclaw.services.lineage_service import apply_professional_migrations, canonical_json, insert_signal_run, replace_signal_items
from openclaw.services.release_event_service import record_release_event, record_release_validation


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (str(table_name or ""),),
    ).fetchone()
    return row is not None


def _compact_date(value: Any) -> str:
    text = str(value or "").strip()
    return "".join(ch for ch in text if ch.isdigit())[:8]


def _decision_id_for_date(decision_date: str) -> str:
    return f"dec_legacy_{_compact_date(decision_date) or 'unknown'}"


def _latest_signal_run_for_date(conn: sqlite3.Connection, trade_date: str) -> str:
    compact = _compact_date(trade_date)
    if not compact or not _table_exists(conn, "strategy_signal_tracking"):
        return ""
    row = conn.execute(
        """
        SELECT run_id
        FROM strategy_signal_tracking
        WHERE signal_trade_date <= ?
        ORDER BY signal_trade_date DESC, created_at DESC, id DESC
        LIMIT 1
        """,
        (compact,),
    ).fetchone()
    return str(row[0] or "") if row else ""


def _backfill_legacy_signal_tracking(conn: sqlite3.Connection, *, code_root: Path, limit_runs: int) -> int:
    if not _table_exists(conn, "strategy_signal_tracking"):
        return 0
    run_rows = conn.execute(
        """
        SELECT run_id, strategy, signal_trade_date, MIN(created_at), COUNT(*)
        FROM strategy_signal_tracking
        WHERE COALESCE(run_id, '') != ''
        GROUP BY run_id, strategy, signal_trade_date
        ORDER BY signal_trade_date DESC, MIN(created_at) DESC
        LIMIT ?
        """,
        (int(limit_runs or 0),),
    ).fetchall()
    inserted = 0
    data_version = build_data_version(conn)
    code_version = build_code_version(root=code_root)
    for run_id, strategy, trade_date, created_at, row_count in run_rows:
        existing = conn.execute("SELECT 1 FROM signal_runs WHERE run_id = ?", (str(run_id or ""),)).fetchone()
        if existing:
            continue
        rows = conn.execute(
            """
            SELECT ts_code, score, rank_idx, reason, source
            FROM strategy_signal_tracking
            WHERE run_id = ? AND strategy = ? AND signal_trade_date = ?
            ORDER BY rank_idx ASC, id ASC
            """,
            (run_id, strategy, trade_date),
        ).fetchall()
        insert_signal_run(
            conn,
            run_id=str(run_id or ""),
            run_type="scan",
            strategy=str(strategy or "").lower(),
            trade_date=str(trade_date or ""),
            data_version=data_version,
            code_version=code_version,
            param_version=f"legacy_signal_tracking:{run_id}",
            status="success",
            summary={
                "source": "strategy_signal_tracking",
                "row_count": int(row_count or 0),
                "legacy_created_at": str(created_at or ""),
            },
        )
        replace_signal_items(
            conn,
            run_id=str(run_id or ""),
            items=[
                {
                    "ts_code": str(row[0] or ""),
                    "score": float(row[1] or 0.0),
                    "rank_idx": int(row[2] or idx),
                    "strategy": str(strategy or "").lower(),
                    "reason_codes": [str(row[3] or "")] if str(row[3] or "").strip() else [],
                    "source": str(row[4] or "strategy_signal_tracking"),
                }
                for idx, row in enumerate(rows, start=1)
            ],
        )
        inserted += 1
    return inserted


def _backfill_overnight_decisions(conn: sqlite3.Connection) -> int:
    if not _table_exists(conn, "overnight_decision_runs"):
        return 0
    rows = conn.execute(
        """
        SELECT decision_date, trade_date, risk_level, candidate_pool_size, selected_count,
               source_type, source_run_id, source_label, release_status, release_note,
               rollback_reason, approved_by, approved_at, is_active, created_at
        FROM overnight_decision_runs
        ORDER BY created_at DESC
        """
    ).fetchall()
    inserted = 0
    for row in rows:
        decision_date = str(row[0] or "")
        decision_id = _decision_id_for_date(decision_date)
        based_on_run_id = str(row[6] or "") or _latest_signal_run_for_date(conn, decision_date)
        if not based_on_run_id:
            continue
        record_decision_event(
            conn,
            decision_id=decision_id,
            decision_type="approve",
            based_on_run_id=based_on_run_id,
            risk_gate_state={
                "risk_level": str(row[2] or ""),
                "source": "overnight_decision_runs",
            },
            release_gate_state={
                "release_status": str(row[8] or ""),
                "rollback_reason": str(row[10] or ""),
                "source": "overnight_decision_runs",
            },
            approval_reason_codes=["legacy_overnight_decision", f"release_status:{str(row[8] or 'unknown')}"],
            approval_note=str(row[9] or "") or "legacy overnight decision backfilled for release readiness rehearsal",
            operator_name=str(row[11] or "") or "legacy_system",
            decision_payload={
                "decision_date": decision_date,
                "trade_date": str(row[1] or ""),
                "candidate_pool_size": int(row[3] or 0),
                "selected_count": int(row[4] or 0),
                "source_type": str(row[5] or ""),
                "source_label": str(row[7] or ""),
                "approved_at": str(row[12] or ""),
                "legacy_created_at": str(row[14] or ""),
            },
        )
        upsert_decision_snapshot(
            conn,
            decision_id=decision_id,
            decision_status=str(row[8] or "active"),
            effective_trade_date=_compact_date(decision_date),
            selected_count=int(row[4] or 0),
            active_flag=bool(int(row[13] or 0)),
        )
        inserted += 1
    return inserted


def _backfill_overnight_execution_feedback(conn: sqlite3.Connection, *, limit_rows: int) -> int:
    if not _table_exists(conn, "overnight_execution_feedback"):
        return 0
    ids = [
        int(row[0])
        for row in conn.execute(
            """
            SELECT id
            FROM overnight_execution_feedback
            WHERE COALESCE(final_action, '') NOT IN ('', 'pending')
              AND COALESCE(execution_status, '') NOT IN ('', 'pending')
            ORDER BY updated_at DESC, id DESC
            LIMIT ?
            """,
            (int(limit_rows or 0),),
        ).fetchall()
    ]
    inserted = 0
    for row_id in ids:
        fact = _fetch_feedback_fact(conn, row_id)
        if not fact:
            continue
        decision_date = _compact_date(fact.get("decision_date"))
        fact["decision_date"] = decision_date
        decision_id = _decision_id_for_date(decision_date)
        if not conn.execute("SELECT 1 FROM decision_events WHERE decision_id = ?", (decision_id,)).fetchone():
            based_on_run_id = _latest_signal_run_for_date(conn, decision_date)
            if not based_on_run_id:
                continue
            record_decision_event(
                conn,
                decision_id=decision_id,
                decision_type="approve",
                based_on_run_id=based_on_run_id,
                risk_gate_state={"source": "overnight_execution_feedback"},
                release_gate_state={"source": "overnight_execution_feedback"},
                approval_reason_codes=["legacy_execution_feedback"],
                approval_note="legacy execution feedback decision anchor",
                operator_name=str(fact.get("operator_name") or "") or "legacy_system",
                decision_payload={"decision_date": decision_date, "source": "overnight_execution_feedback"},
            )
            upsert_decision_snapshot(
                conn,
                decision_id=decision_id,
                decision_status="active",
                effective_trade_date=_compact_date(decision_date),
                selected_count=0,
                active_flag=False,
            )
        _upsert_execution_fact_from_feedback(conn, fact)
        inserted += 1
    return inserted


def _latest_release_audit(conn: sqlite3.Connection) -> Dict[str, Any]:
    if not _table_exists(conn, "trading_kernel_governance_audit"):
        return {}
    row = conn.execute(
        """
        SELECT audit_id, decision_date, strategy, event_type, system_next_action,
               effective_next_action, final_disposition, publish_requested, publish_status,
               fallback_execution_mode, override_applied, override_reason, operator_name,
               risk_level, validation_gates_json, kernel_gates_json, context_json,
               run_summary_path, created_at
        FROM trading_kernel_governance_audit
        ORDER BY created_at DESC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return {}
    keys = [
        "audit_id",
        "decision_date",
        "strategy",
        "event_type",
        "system_next_action",
        "effective_next_action",
        "final_disposition",
        "publish_requested",
        "publish_status",
        "fallback_execution_mode",
        "override_applied",
        "override_reason",
        "operator_name",
        "risk_level",
        "validation_gates_json",
        "kernel_gates_json",
        "context_json",
        "run_summary_path",
        "created_at",
    ]
    return {key: row[idx] for idx, key in enumerate(keys)}


def _backfill_release_reference(conn: sqlite3.Connection, *, code_root: Path) -> int:
    audit = _latest_release_audit(conn)
    if not audit:
        return 0
    release_id = f"rel_legacy_{str(audit.get('audit_id') or '').replace('-', '_')}"
    record_release_event(
        conn,
        release_id=release_id,
        release_type="dry_run_rehearsal",
        code_version=build_code_version(root=code_root),
        config_version="legacy_trading_kernel_governance_audit",
        operator_name=str(audit.get("operator_name") or "") or "legacy_system",
        gate_result={
            "source": "trading_kernel_governance_audit",
            "publish_status": str(audit.get("publish_status") or ""),
            "publish_requested": int(audit.get("publish_requested") or 0),
            "effective_next_action": str(audit.get("effective_next_action") or ""),
        },
        payload={
            "rollback_context": {
                "source": "trading_kernel_governance_audit",
                "audit_id": str(audit.get("audit_id") or ""),
                "decision_date": str(audit.get("decision_date") or ""),
                "run_summary_path": str(audit.get("run_summary_path") or ""),
            },
            "legacy_audit": audit,
        },
    )
    record_release_validation(
        conn,
        release_id=release_id,
        validation_type="legacy_governance_audit",
        validation_status="passed",
        validation_output_path=str(audit.get("run_summary_path") or ""),
    )
    return 1


def backfill_legacy_fact_chains(
    conn: sqlite3.Connection,
    *,
    code_root: str | Path,
    signal_run_limit: int = 300,
    feedback_row_limit: int = 500,
) -> Dict[str, Any]:
    apply_professional_migrations(conn)
    root = Path(code_root)
    counts = {
        "signal_runs": _backfill_legacy_signal_tracking(conn, code_root=root, limit_runs=int(signal_run_limit or 0)),
        "decision_events": _backfill_overnight_decisions(conn),
        "execution_orders": _backfill_overnight_execution_feedback(conn, limit_rows=int(feedback_row_limit or 0)),
        "release_events": _backfill_release_reference(conn, code_root=root),
    }
    return {
        "source": "legacy_fact_chain_backfill_service",
        "generated_at": _now_text(),
        "counts": counts,
    }
