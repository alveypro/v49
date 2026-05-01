from __future__ import annotations

import sqlite3

from openclaw.services.decision_service import record_decision_event, upsert_decision_snapshot
from openclaw.services.execution_analytics_service import compute_execution_attribution, upsert_execution_attribution
from openclaw.services.execution_evidence_service import summarize_execution_evidence
from openclaw.services.execution_fill_service import record_execution_fill
from openclaw.services.execution_order_service import create_execution_order, update_execution_order_status
from openclaw.services.lineage_service import (
    apply_professional_migrations,
    insert_signal_run,
    new_decision_id,
    new_fill_id,
    new_order_id,
    new_run_id,
)


def _seed_signal_and_decision(conn: sqlite3.Connection) -> tuple[str, str]:
    apply_professional_migrations(conn)
    run_id = new_run_id("scan", "combo")
    insert_signal_run(
        conn,
        run_id=run_id,
        run_type="scan",
        strategy="combo",
        trade_date="2026-05-01",
        data_version="trade_date:20260501",
        code_version="git:test:dirty0",
        param_version="param:sha256:test",
        status="success",
    )
    decision_id = new_decision_id()
    record_decision_event(
        conn,
        decision_id=decision_id,
        decision_type="approve",
        based_on_run_id=run_id,
        risk_gate_state={"risk_level": "green"},
        release_gate_state={"passed": True},
        approval_reason_codes=["risk_pass"],
        approval_note="execution evidence fixture",
        operator_name="alice",
        decision_payload={"selected_count": 2},
    )
    upsert_decision_snapshot(
        conn,
        decision_id=decision_id,
        decision_status="active",
        effective_trade_date="2026-05-01",
        selected_count=2,
        active_flag=True,
    )
    return run_id, decision_id


def test_summarize_execution_evidence_returns_reviewable_cases(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    run_id, decision_id = _seed_signal_and_decision(conn)
    filled_order = new_order_id()
    create_execution_order(
        conn,
        order_id=filled_order,
        decision_id=decision_id,
        ts_code="000001.SZ",
        target_qty=1000,
        decision_price=10.0,
        submitted_price=10.05,
        status="submitted",
        broker_ref="sim:filled",
        source_type="shadow",
    )
    update_execution_order_status(conn, order_id=filled_order, status="filled")
    record_execution_fill(
        conn,
        fill_id=new_fill_id(),
        order_id=filled_order,
        fill_price=10.08,
        fill_qty=1000,
        fill_slippage_bp=80.0,
        venue="SIM",
    )
    upsert_execution_attribution(
        conn,
        order_id=filled_order,
        attribution=compute_execution_attribution(
            decision_price=10.0,
            submit_price=10.05,
            avg_fill_price=10.08,
            close_price=10.2,
            target_qty=1000,
            filled_qty=1000,
            delay_sec=18,
        ),
    )

    cancelled_order = new_order_id()
    create_execution_order(
        conn,
        order_id=cancelled_order,
        decision_id=decision_id,
        ts_code="000002.SZ",
        target_qty=800,
        decision_price=12.0,
        submitted_price=11.9,
        status="submitted",
        broker_ref="sim:cancelled",
        source_type="shadow",
    )
    update_execution_order_status(conn, order_id=cancelled_order, status="cancelled", cancel_reason="price not reached")
    upsert_execution_attribution(
        conn,
        order_id=cancelled_order,
        attribution=compute_execution_attribution(
            decision_price=12.0,
            submit_price=11.9,
            avg_fill_price=0.0,
            close_price=12.3,
            target_qty=800,
            filled_qty=0,
            delay_sec=120,
            miss_reason_code="manual_cancel",
        ),
    )

    summary = summarize_execution_evidence(conn, decision_ids=[decision_id])
    conn.close()

    assert summary["passed"] is True
    assert summary["blocking_reasons"] == []
    assert summary["total_orders"] == 2
    assert summary["status_counts"] == {"cancelled": 1, "filled": 1}
    assert summary["miss_reason_counts"] == {"manual_cancel": 1}
    assert summary["linked_run_ids"] == [run_id]
    assert [case["order_id"] for case in summary["cases"]] == [filled_order, cancelled_order]


def test_summarize_execution_evidence_blocks_untraceable_or_incomplete_orders(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)
    decision_id = new_decision_id()
    record_decision_event(
        conn,
        decision_id=decision_id,
        decision_type="approve",
        based_on_run_id="",
        risk_gate_state={"risk_level": "green"},
        release_gate_state={"passed": True},
        approval_reason_codes=["risk_pass"],
        approval_note="missing lineage fixture",
        operator_name="alice",
        decision_payload={"selected_count": 1},
    )
    upsert_decision_snapshot(conn, decision_id=decision_id, decision_status="active", active_flag=True)
    order_id = new_order_id()
    create_execution_order(
        conn,
        order_id=order_id,
        decision_id=decision_id,
        ts_code="000001.SZ",
        target_qty=1000,
        decision_price=10.0,
        submitted_price=10.1,
        status="cancelled",
    )
    upsert_execution_attribution(
        conn,
        order_id=order_id,
        attribution=compute_execution_attribution(
            decision_price=10.0,
            submit_price=10.1,
            avg_fill_price=0.0,
            close_price=10.2,
            target_qty=1000,
            filled_qty=0,
            delay_sec=30,
        ),
    )

    summary = summarize_execution_evidence(conn, order_ids=[order_id])
    conn.close()

    assert summary["passed"] is False
    assert f"missing_signal_lineage:{order_id}" in summary["blocking_reasons"]
    assert f"missing_broker_ref:{order_id}" in summary["blocking_reasons"]
    assert f"missing_source_type:{order_id}" in summary["blocking_reasons"]
    assert f"missing_miss_reason:{order_id}" in summary["blocking_reasons"]


def test_summarize_execution_evidence_blocks_empty_scope(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)

    summary = summarize_execution_evidence(conn, decision_ids=["missing"])
    conn.close()

    assert summary["passed"] is False
    assert summary["blocking_reasons"] == ["execution_evidence_empty"]
