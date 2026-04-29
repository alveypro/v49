from __future__ import annotations

import json
import sqlite3
from typing import Any, Callable, Dict, List, Optional, Tuple


def set_active_batch(
    *,
    decision_date: str,
    approved_by: str = "",
    release_note: str = "",
    rollback_reason: str = "",
    current_primary: str = "",
    override_gate: bool = False,
    override_reason: str = "",
    connect_db: Callable[[], sqlite3.Connection],
    logs_dir: Any,
    clear_execution_queue_cache: Callable[[], None],
    clear_execution_batches_cache: Callable[[], None],
) -> Tuple[bool, str]:
    try:
        conn = connect_db()
        from openclaw.overnight_decision import set_active_decision_batch

        result = set_active_decision_batch(
            conn,
            decision_date=str(decision_date or ""),
            approved_by=str(approved_by or "").strip(),
            release_note=str(release_note or "").strip(),
            rollback_reason=str(rollback_reason or "").strip(),
            output_dir=logs_dir,
            current_primary=str(current_primary or "").strip(),
            override_gate=bool(override_gate),
            override_reason=str(override_reason or "").strip(),
        )
        conn.close()
        clear_execution_queue_cache()
        clear_execution_batches_cache()
        if result.get("ok"):
            return True, f"已切换当前生效批次：{decision_date}"
        return False, str(result.get("message") or "切换失败")
    except Exception as exc:
        return False, f"切换生效批次失败：{exc}"


def set_canary_batch(
    *,
    decision_date: str,
    approved_by: str = "",
    release_note: str = "",
    current_primary: str = "",
    allowed_buckets: Optional[List[str]] = None,
    sample_limit: int = 2,
    window_start: str = "",
    window_end: str = "",
    connect_db: Callable[[], sqlite3.Connection],
    logs_dir: Any,
    clear_execution_queue_cache: Callable[[], None],
    clear_execution_batches_cache: Callable[[], None],
) -> Tuple[bool, str]:
    try:
        conn = connect_db()
        from openclaw.overnight_decision import set_canary_decision_batch

        result = set_canary_decision_batch(
            conn,
            decision_date=str(decision_date or ""),
            approved_by=str(approved_by or "").strip(),
            release_note=str(release_note or "").strip(),
            output_dir=logs_dir,
            current_primary=str(current_primary or "").strip(),
            allowed_buckets=list(allowed_buckets or ["direct_execute", "observe"]),
            sample_limit=int(sample_limit or 2),
            window_start=str(window_start or "").strip(),
            window_end=str(window_end or "").strip(),
        )
        conn.close()
        clear_execution_queue_cache()
        clear_execution_batches_cache()
        if result.get("ok"):
            return True, f"已设为灰度批次：{decision_date}"
        return False, str(result.get("message") or "灰度发布失败")
    except Exception as exc:
        return False, f"灰度发布失败：{exc}"


def archive_batch(
    *,
    decision_date: str,
    operator_name: str = "",
    archive_note: str = "",
    connect_db: Callable[[], sqlite3.Connection],
    clear_execution_queue_cache: Callable[[], None],
    clear_execution_batches_cache: Callable[[], None],
) -> Tuple[bool, str]:
    try:
        conn = connect_db()
        from openclaw.overnight_decision import archive_decision_batch

        result = archive_decision_batch(
            conn,
            decision_date=str(decision_date or ""),
            operator_name=str(operator_name or "").strip(),
            archive_note=str(archive_note or "").strip(),
        )
        conn.close()
        clear_execution_queue_cache()
        clear_execution_batches_cache()
        if result.get("ok"):
            return True, f"已归档批次：{decision_date}"
        return False, str(result.get("message") or "归档失败")
    except Exception as exc:
        return False, f"归档批次失败：{exc}"


def evaluate_batch_release_gate(
    *,
    decision_date: str,
    current_primary: str = "",
    connect_db: Callable[[], sqlite3.Connection],
    logs_dir: Any,
) -> Dict[str, Any]:
    try:
        conn = connect_db()
        from openclaw.overnight_decision import evaluate_decision_batch_release_gate

        result = evaluate_decision_batch_release_gate(
            conn,
            decision_date=str(decision_date or ""),
            output_dir=logs_dir,
            current_primary=str(current_primary or "").strip(),
        )
        conn.close()
        return result
    except Exception as exc:
        return {
            "decision_date": str(decision_date or ""),
            "passed": False,
            "summary": f"发布门禁校验失败：{exc}",
            "gates": ["gate_eval_error"],
            "metrics": {},
        }


def get_canary_scope(
    *,
    db_path: str,
    decision_date: str,
) -> Dict[str, Any]:
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        row = conn.execute(
            """
            SELECT allowed_buckets_json, sample_limit, window_start, window_end, selected_codes_json, operator_name, scope_note, updated_at
            FROM overnight_canary_scopes
            WHERE decision_date = ?
            """,
            (str(decision_date or ""),),
        ).fetchone()
        conn.close()
        if not row:
            return {}
        return {
            "allowed_buckets": json.loads(str(row[0] or "[]")),
            "sample_limit": int(row[1] or 0),
            "window_start": str(row[2] or ""),
            "window_end": str(row[3] or ""),
            "selected_codes": json.loads(str(row[4] or "[]")),
            "operator_name": str(row[5] or ""),
            "scope_note": str(row[6] or ""),
            "updated_at": str(row[7] or ""),
        }
    except Exception:
        return {}


def get_override_audits(
    *,
    db_path: str,
    decision_date: str,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        rows = conn.execute(
            """
            SELECT requested_decision, gate_decision, gate_summary, gate_codes_json, operator_name, override_reason, override_context_json, created_at
            FROM overnight_release_override_audit
            WHERE decision_date = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (str(decision_date or ""), int(limit)),
        ).fetchall()
        conn.close()
        return [
            {
                "requested_decision": str(r[0] or ""),
                "gate_decision": str(r[1] or ""),
                "gate_summary": str(r[2] or ""),
                "gate_codes": json.loads(str(r[3] or "[]")),
                "operator_name": str(r[4] or ""),
                "override_reason": str(r[5] or ""),
                "override_context": json.loads(str(r[6] or "{}")),
                "created_at": str(r[7] or ""),
            }
            for r in rows
        ]
    except Exception:
        return []


def get_release_outcome_review(
    *,
    db_path: str,
    decision_date: str,
    connect_db: Callable[[], sqlite3.Connection],
) -> Dict[str, Any]:
    try:
        conn = connect_db()
        from openclaw.overnight_decision import evaluate_release_batch_outcome

        review = evaluate_release_batch_outcome(conn, decision_date=str(decision_date or ""))
        conn.close()
        return review
    except Exception:
        return {}
