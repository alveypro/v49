from __future__ import annotations

import sqlite3
from typing import Any, Dict
from typing import Callable, Tuple

from openclaw.services.decision_service import record_decision_event, upsert_decision_snapshot
from openclaw.services.execution_analytics_service import compute_execution_attribution, upsert_execution_attribution
from openclaw.services.execution_order_service import ALLOWED_ORDER_STATUSES, create_execution_order, update_execution_order_status
from openclaw.services.lineage_service import apply_professional_migrations, new_decision_id, new_order_id


def _map_feedback_status_to_order_status(*, final_action: str, execution_status: str) -> str:
    action = str(final_action or "").strip().lower()
    status = str(execution_status or "").strip().lower()
    if status in {"done", "executed", "filled"} and action in {"buy", "switch"}:
        return "filled"
    if status == "cancelled" or action in {"skip", "cancelled"}:
        return "cancelled"
    if status == "skipped" or action in {"watch", "hold"}:
        return "expired"
    return "manual_override"


def _miss_reason_for_feedback(*, final_action: str, execution_status: str, execution_note: str) -> str:
    action = str(final_action or "").strip().lower()
    status = str(execution_status or "").strip().lower()
    if status in {"done", "executed", "filled"} and action in {"buy", "switch"}:
        return ""
    if action in {"watch", "hold"}:
        return "manual_observe"
    if action in {"skip", "cancelled"} or status == "cancelled":
        return "manual_cancel"
    return str(execution_note or "").strip()[:80] or "manual_override"


def _fetch_feedback_fact(conn: sqlite3.Connection, row_id: int) -> Dict[str, Any]:
    row = conn.execute(
        """
        SELECT
            id,
            decision_date,
            trade_date,
            ts_code,
            stock_name,
            planned_action,
            final_action,
            execution_status,
            execution_note,
            operator_name,
            switched_from_ts_code,
            system_suggested_action,
            system_confidence,
            system_reason,
            decision_bucket,
            decision_gate_reason,
            human_override,
            human_override_reason
        FROM overnight_execution_feedback
        WHERE id = ?
        """,
        (int(row_id),),
    ).fetchone()
    if not row:
        return {}
    keys = [
        "id",
        "decision_date",
        "trade_date",
        "ts_code",
        "stock_name",
        "planned_action",
        "final_action",
        "execution_status",
        "execution_note",
        "operator_name",
        "switched_from_ts_code",
        "system_suggested_action",
        "system_confidence",
        "system_reason",
        "decision_bucket",
        "decision_gate_reason",
        "human_override",
        "human_override_reason",
    ]
    return {key: row[idx] for idx, key in enumerate(keys)}


def _find_or_create_feedback_decision(conn: sqlite3.Connection, fact: Dict[str, Any]) -> str:
    decision_date = str(fact.get("decision_date") or "")
    row = conn.execute(
        """
        SELECT ds.decision_id
        FROM decision_snapshot ds
        JOIN decision_events de ON de.decision_id = ds.decision_id
        WHERE ds.effective_trade_date = ?
        ORDER BY ds.active_flag DESC, ds.updated_at DESC
        LIMIT 1
        """,
        (decision_date,),
    ).fetchone()
    if row and row[0]:
        return str(row[0])

    decision_id = new_decision_id()
    record_decision_event(
        conn,
        decision_id=decision_id,
        decision_type="approve",
        based_on_run_id="",
        risk_gate_state={},
        release_gate_state={"source": "execution_feedback_bridge"},
        approval_reason_codes=["manual_override"],
        approval_note="execution feedback anchored to authoritative ledger",
        operator_name=str(fact.get("operator_name") or ""),
        decision_payload={
            "decision_date": decision_date,
            "trade_date": str(fact.get("trade_date") or ""),
            "source": "overnight_execution_feedback",
        },
    )
    upsert_decision_snapshot(
        conn,
        decision_id=decision_id,
        decision_status="active",
        effective_trade_date=decision_date,
        selected_count=0,
        active_flag=False,
    )
    return decision_id


def _upsert_execution_fact_from_feedback(conn: sqlite3.Connection, fact: Dict[str, Any]) -> None:
    if not fact:
        return
    apply_professional_migrations(conn)
    broker_ref = f"overnight_execution_feedback:{int(fact.get('id') or 0)}"
    existing = conn.execute(
        "SELECT order_id FROM execution_orders WHERE broker_ref = ? ORDER BY created_at DESC LIMIT 1",
        (broker_ref,),
    ).fetchone()
    order_id = str(existing[0]) if existing and existing[0] else new_order_id()
    decision_id = _find_or_create_feedback_decision(conn, fact)
    order_status = _map_feedback_status_to_order_status(
        final_action=str(fact.get("final_action") or ""),
        execution_status=str(fact.get("execution_status") or ""),
    )
    if order_status not in ALLOWED_ORDER_STATUSES:
        order_status = "manual_override"
    cancel_reason = ""
    if order_status in {"cancelled", "expired", "manual_override"}:
        cancel_reason = str(fact.get("execution_note") or "")[:200]
    if existing:
        update_execution_order_status(
            conn,
            order_id=order_id,
            status=order_status,
            cancel_reason=cancel_reason,
            broker_ref=broker_ref,
        )
    else:
        create_execution_order(
            conn,
            order_id=order_id,
            decision_id=decision_id,
            ts_code=str(fact.get("ts_code") or ""),
            side="buy",
            target_qty=0,
            decision_price=0.0,
            submitted_price=0.0,
            status=order_status,
            cancel_reason=cancel_reason,
            broker_ref=broker_ref,
            source_type="overnight_feedback",
        )
    attribution = compute_execution_attribution(
        decision_price=0.0,
        submit_price=0.0,
        avg_fill_price=0.0,
        close_price=0.0,
        target_qty=1 if order_status == "filled" else 0,
        filled_qty=1 if order_status == "filled" else 0,
        delay_sec=0.0,
        miss_reason_code=_miss_reason_for_feedback(
            final_action=str(fact.get("final_action") or ""),
            execution_status=str(fact.get("execution_status") or ""),
            execution_note=str(fact.get("execution_note") or ""),
        ),
    )
    upsert_execution_attribution(conn, order_id=order_id, attribution=attribution)

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
        if cur.rowcount != 1:
            conn.close()
            return False, "未找到对应反馈记录，页面可能已过期。"
        fact = _fetch_feedback_fact(conn, int(row_id))
        _upsert_execution_fact_from_feedback(conn, fact)
        conn.close()
        clear_feedback_snapshot_cache()
        return True, "执行反馈已写入权威执行账本。"
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
        target_ids = [
            int(row[0])
            for row in conn.execute(
                """
                SELECT id
                FROM overnight_execution_feedback
                WHERE decision_bucket = ?
                  AND (execution_status = 'pending' OR final_action = 'pending' OR COALESCE(final_action, '') = '')
                """,
                (bucket,),
            ).fetchall()
        ]
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
        for target_id in target_ids:
            fact = _fetch_feedback_fact(conn, target_id)
            _upsert_execution_fact_from_feedback(conn, fact)
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
