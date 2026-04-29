from __future__ import annotations

import sqlite3
from typing import Callable, Tuple


def update_feedback_row(
    *,
    db_path: str,
    row_id: int,
    final_action: str,
    execution_status: str,
    execution_note: str,
    operator_name: str,
    system_suggested_action: str = "",
    human_override_reason: str = "",
    clear_feedback_snapshot_cache: Callable[[], None],
) -> Tuple[bool, str]:
    final_action = str(final_action or "").strip()
    execution_status = str(execution_status or "").strip()
    execution_note = str(execution_note or "").strip()
    operator_name = str(operator_name or "").strip()
    system_suggested_action = str(system_suggested_action or "").strip()
    human_override_reason = str(human_override_reason or "").strip()
    allowed_actions = {"buy", "watch", "skip", "switch", "hold", "cancelled"}
    allowed_statuses = {"done", "skipped", "cancelled", "executed", "filled"}
    if final_action not in allowed_actions:
        return False, "请选择真实最终动作。"
    if execution_status not in allowed_statuses:
        return False, "请选择真实执行状态。"
    if not operator_name:
        return False, "必须填写操作人。"
    if not execution_note:
        return False, "必须填写备注：买入价、未执行原因或切换原因至少写清一个。"
    human_override = bool(system_suggested_action and final_action != system_suggested_action)
    if human_override and not human_override_reason:
        return False, "人工动作覆盖系统建议时，必须填写覆盖原因。"
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        try:
            from openclaw.overnight_decision import ensure_tables

            ensure_tables(conn)
        except Exception:
            pass
        cur = conn.execute(
            """
            UPDATE overnight_execution_feedback
            SET final_action = ?,
                execution_status = ?,
                execution_note = ?,
                operator_name = ?,
                human_override = ?,
                human_override_reason = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                final_action,
                execution_status,
                execution_note,
                operator_name,
                1 if human_override else 0,
                human_override_reason,
                int(row_id),
            ),
        )
        conn.commit()
        conn.close()
        if cur.rowcount != 1:
            return False, "未找到对应反馈记录，页面可能已过期。"
        clear_feedback_snapshot_cache()
        return True, "执行反馈已写入数据库。"
    except Exception as exc:
        return False, f"写入失败：{exc}"


def apply_batch_feedback_action(
    *,
    db_path: str,
    bucket: str,
    operator_name: str,
    execution_note: str,
    clear_feedback_snapshot_cache: Callable[[], None],
) -> Tuple[bool, str]:
    bucket = str(bucket or "").strip()
    operator_name = str(operator_name or "").strip()
    execution_note = str(execution_note or "").strip()
    if bucket not in {"observe", "auto_reject"}:
        return False, "当前仅支持批量采纳观察/淘汰队列。直接执行仍需人工确认真实买入。"
    if not operator_name:
        return False, "批量采纳前必须填写操作人。"
    if not execution_note:
        return False, "批量采纳前必须填写统一备注。"
    try:
        conn = sqlite3.connect(db_path, timeout=20)
        try:
            from openclaw.overnight_decision import ensure_tables

            ensure_tables(conn)
        except Exception:
            pass
        if bucket == "observe":
            final_action = "watch"
            execution_status = "skipped"
        else:
            final_action = "skip"
            execution_status = "cancelled"
        cur = conn.execute(
            """
            UPDATE overnight_execution_feedback
            SET final_action = ?,
                execution_status = ?,
                execution_note = CASE
                    WHEN COALESCE(execution_note, '') = '' THEN ?
                    ELSE execution_note || ' | ' || ?
                END,
                operator_name = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE decision_bucket = ?
              AND (execution_status = 'pending' OR final_action = 'pending' OR COALESCE(final_action, '') = '')
            """,
            (
                final_action,
                execution_status,
                execution_note,
                execution_note,
                operator_name,
                bucket,
            ),
        )
        conn.commit()
        conn.close()
        clear_feedback_snapshot_cache()
        return True, f"批量采纳完成：bucket={bucket}, updated={int(cur.rowcount or 0)}"
    except Exception as exc:
        return False, f"批量采纳失败：{exc}"


def refresh_realized_outcomes(
    *,
    db_path: str,
    lookback_days: int = 120,
    clear_feedback_snapshot_cache: Callable[[], None],
) -> Tuple[bool, str]:
    try:
        from openclaw.overnight_decision import refresh_realized_outcomes as refresh_realized_outcomes_impl

        conn = sqlite3.connect(db_path, timeout=20)
        result = refresh_realized_outcomes_impl(conn, lookback_days=int(lookback_days))
        conn.close()
        clear_feedback_snapshot_cache()
        updated = int((result or {}).get("updated", 0) or 0)
        skipped = int((result or {}).get("skipped", 0) or 0)
        return True, f"结果回刷完成：updated={updated}, skipped={skipped}"
    except Exception as exc:
        return False, f"结果回刷失败：{exc}"
