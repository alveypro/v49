from __future__ import annotations

import sqlite3

from openclaw.services.decision_service import record_decision_event, upsert_decision_snapshot
from openclaw.services.execution_attribution_backfill_service import backfill_missing_execution_attribution
from openclaw.services.execution_evidence_service import summarize_execution_evidence
from openclaw.services.execution_order_service import create_execution_order
from openclaw.services.lineage_service import (
    apply_professional_migrations,
    insert_signal_run,
    new_decision_id,
    new_order_id,
    new_run_id,
    replace_signal_items,
)


def _seed_lineage(conn: sqlite3.Connection) -> tuple[str, str]:
    apply_professional_migrations(conn)
    run_id = new_run_id("scan", "stable")
    insert_signal_run(
        conn,
        run_id=run_id,
        run_type="scan",
        strategy="stable",
        trade_date="2026-05-01",
        data_version="trade_date:20260501",
        code_version="git:test:dirty0",
        param_version="param:sha256:test",
        status="success",
    )
    replace_signal_items(conn, run_id=run_id, items=[{"ts_code": "000001.SZ", "score": 80, "rank_idx": 1}])
    decision_id = new_decision_id()
    record_decision_event(
        conn,
        decision_id=decision_id,
        decision_type="approve",
        based_on_run_id=run_id,
        risk_gate_state={"risk_level": "green"},
        release_gate_state={"passed": True},
        approval_reason_codes=["fixture"],
        approval_note="fixture",
        operator_name="pytest",
        decision_payload={"selected_count": 1},
    )
    upsert_decision_snapshot(
        conn,
        decision_id=decision_id,
        decision_status="active",
        effective_trade_date="2026-05-01",
        selected_count=1,
        active_flag=True,
    )
    return run_id, decision_id


def test_execution_attribution_backfill_dry_run_does_not_write(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    _, decision_id = _seed_lineage(conn)
    order_id = new_order_id()
    create_execution_order(
        conn,
        order_id=order_id,
        decision_id=decision_id,
        ts_code="000001.SZ",
        target_qty=100,
        decision_price=10.0,
        submitted_price=10.0,
        status="created",
        broker_ref="sim:created",
        source_type="sim",
    )

    result = backfill_missing_execution_attribution(
        conn,
        statuses=("created",),
        stale_minutes=0,
        max_orders=10,
        apply_changes=False,
    )
    row = conn.execute("SELECT 1 FROM execution_attribution WHERE order_id = ?", (order_id,)).fetchone()
    conn.close()

    assert result["patched_count"] == 1
    assert row is None


def test_execution_attribution_backfill_apply_writes_and_clears_missing_attribution(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    _, decision_id = _seed_lineage(conn)
    order_id = new_order_id()
    create_execution_order(
        conn,
        order_id=order_id,
        decision_id=decision_id,
        ts_code="000001.SZ",
        target_qty=100,
        decision_price=10.0,
        submitted_price=10.1,
        status="created",
        broker_ref="sim:created",
        source_type="sim",
    )

    before = summarize_execution_evidence(conn, order_ids=[order_id], minimum_source_tier="simulated")
    assert before["passed"] is False
    assert f"missing_attribution:{order_id}" in before["blocking_reasons"]

    result = backfill_missing_execution_attribution(
        conn,
        statuses=("created",),
        stale_minutes=0,
        max_orders=10,
        apply_changes=True,
    )
    after = summarize_execution_evidence(conn, order_ids=[order_id], minimum_source_tier="simulated")
    conn.close()

    assert result["patched_count"] == 1
    assert after["passed"] is True
    assert f"missing_attribution:{order_id}" not in after["blocking_reasons"]
