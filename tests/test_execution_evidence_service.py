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
    replace_signal_items,
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
    replace_signal_items(
        conn,
        run_id=run_id,
        items=[
            {"ts_code": f"00000{idx}.SZ", "score": 90 - idx, "rank_idx": idx, "reason_codes": ["fixture"]}
            for idx in range(1, 8)
        ],
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
    assert f"missing_release_gate_state:{order_id}" not in summary["blocking_reasons"]
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


def test_summarize_execution_evidence_blocks_missing_attribution_and_decision_id(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)
    run_id = new_run_id("scan", "v8")
    insert_signal_run(
        conn,
        run_id=run_id,
        run_type="scan",
        strategy="v8",
        trade_date="2026-05-01",
        data_version="trade_date:20260501",
        code_version="git:test:dirty0",
        param_version="param:sha256:test",
        status="success",
    )
    replace_signal_items(conn, run_id=run_id, items=[{"ts_code": "000001.SZ", "score": 88, "rank_idx": 1}])
    order_id = new_order_id()
    create_execution_order(
        conn,
        order_id=order_id,
        decision_id="",
        ts_code="000001.SZ",
        target_qty=100,
        decision_price=10.0,
        submitted_price=10.0,
        status="cancelled",
        broker_ref="sim:missing-attribution",
        source_type="shadow",
    )

    summary = summarize_execution_evidence(conn, order_ids=[order_id])
    conn.close()

    assert summary["passed"] is False
    assert f"missing_decision_id:{order_id}" in summary["blocking_reasons"]
    assert f"missing_attribution:{order_id}" in summary["blocking_reasons"]


def test_summarize_execution_evidence_reviews_quasi_live_failure_paths(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    run_id, decision_id = _seed_signal_and_decision(conn)

    partial_order = new_order_id()
    create_execution_order(
        conn,
        order_id=partial_order,
        decision_id=decision_id,
        ts_code="000003.SZ",
        target_qty=1000,
        decision_price=10.0,
        submitted_price=10.0,
        status="partial_fill",
        broker_ref="paper:partial",
        source_type="paper_broker",
    )
    record_execution_fill(
        conn,
        fill_id=new_fill_id(),
        order_id=partial_order,
        fill_price=10.02,
        fill_qty=400,
        fill_slippage_bp=20.0,
        venue="PAPER",
    )
    upsert_execution_attribution(
        conn,
        order_id=partial_order,
        attribution=compute_execution_attribution(
            decision_price=10.0,
            submit_price=10.0,
            avg_fill_price=10.02,
            close_price=10.1,
            target_qty=1000,
            filled_qty=400,
            delay_sec=45,
        ),
    )

    expired_order = new_order_id()
    create_execution_order(
        conn,
        order_id=expired_order,
        decision_id=decision_id,
        ts_code="000004.SZ",
        target_qty=500,
        decision_price=20.0,
        submitted_price=19.8,
        status="expired",
        cancel_reason="time in force expired",
        broker_ref="paper:expired",
        source_type="paper_broker",
    )
    upsert_execution_attribution(
        conn,
        order_id=expired_order,
        attribution=compute_execution_attribution(
            decision_price=20.0,
            submit_price=19.8,
            avg_fill_price=0.0,
            close_price=20.4,
            target_qty=500,
            filled_qty=0,
            delay_sec=300,
            miss_reason_code="price_not_reached",
        ),
    )

    override_order = new_order_id()
    create_execution_order(
        conn,
        order_id=override_order,
        decision_id=decision_id,
        ts_code="000005.SZ",
        target_qty=300,
        decision_price=30.0,
        submitted_price=30.0,
        status="manual_override",
        cancel_reason="operator reduced exposure after decision drift",
        broker_ref="paper:override",
        source_type="paper_broker",
    )
    upsert_execution_attribution(
        conn,
        order_id=override_order,
        attribution=compute_execution_attribution(
            decision_price=30.0,
            submit_price=30.0,
            avg_fill_price=0.0,
            close_price=29.1,
            target_qty=300,
            filled_qty=0,
            delay_sec=60,
            miss_reason_code="decision_deviation",
        ),
    )

    slippage_order = new_order_id()
    create_execution_order(
        conn,
        order_id=slippage_order,
        decision_id=decision_id,
        ts_code="000006.SZ",
        target_qty=200,
        decision_price=10.0,
        submitted_price=10.2,
        status="filled",
        broker_ref="paper:slippage",
        source_type="paper_broker",
    )
    record_execution_fill(
        conn,
        fill_id=new_fill_id(),
        order_id=slippage_order,
        fill_price=10.25,
        fill_qty=200,
        fill_slippage_bp=250.0,
        venue="PAPER",
    )
    upsert_execution_attribution(
        conn,
        order_id=slippage_order,
        attribution=compute_execution_attribution(
            decision_price=10.0,
            submit_price=10.2,
            avg_fill_price=10.25,
            close_price=10.3,
            target_qty=200,
            filled_qty=200,
            delay_sec=8,
        ),
    )

    summary = summarize_execution_evidence(
        conn,
        decision_ids=[decision_id],
        minimum_source_tier="quasi_live",
    )
    strict_summary = summarize_execution_evidence(
        conn,
        order_ids=[slippage_order],
        minimum_source_tier="quasi_live",
        block_high_slippage=True,
    )
    conn.close()

    assert summary["passed"] is True
    assert summary["blocking_reasons"] == []
    assert summary["status_counts"] == {"expired": 1, "filled": 1, "manual_override": 1, "partial_fill": 1}
    assert summary["miss_reason_counts"] == {"decision_deviation": 1, "price_not_reached": 1}
    assert summary["source_tier_counts"] == {"quasi_live": 4}
    assert summary["minimum_source_tier"] == "quasi_live"
    assert summary["linked_run_ids"] == [run_id]
    assert summary["high_slippage_orders"] == [slippage_order]
    assert {case["source_tier"] for case in summary["cases"]} == {"quasi_live"}
    assert all(case["has_signal_item"] for case in summary["cases"])
    assert all(case["release_gate_state"] for case in summary["cases"])
    assert strict_summary["passed"] is False
    assert strict_summary["blocking_reasons"] == [f"high_slippage:{slippage_order}"]


def test_summarize_execution_evidence_blocks_simulated_evidence_as_quasi_live(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    _, decision_id = _seed_signal_and_decision(conn)
    order_id = new_order_id()
    create_execution_order(
        conn,
        order_id=order_id,
        decision_id=decision_id,
        ts_code="000007.SZ",
        target_qty=100,
        decision_price=10.0,
        submitted_price=10.0,
        status="filled",
        broker_ref="sim:filled",
        source_type="sim",
    )
    record_execution_fill(
        conn,
        fill_id=new_fill_id(),
        order_id=order_id,
        fill_price=10.0,
        fill_qty=100,
        fill_slippage_bp=0.0,
        venue="SIM",
    )
    upsert_execution_attribution(
        conn,
        order_id=order_id,
        attribution=compute_execution_attribution(
            decision_price=10.0,
            submit_price=10.0,
            avg_fill_price=10.0,
            close_price=10.0,
            target_qty=100,
            filled_qty=100,
            delay_sec=5,
        ),
    )

    summary = summarize_execution_evidence(conn, order_ids=[order_id], minimum_source_tier="quasi_live")
    conn.close()

    assert summary["passed"] is False
    assert summary["source_tier_counts"] == {"simulated": 1}
    assert summary["blocking_reasons"] == [f"source_tier_too_low:{order_id}:sim:quasi_live"]


def test_summarize_execution_evidence_blocks_missing_signal_item(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    _, decision_id = _seed_signal_and_decision(conn)
    order_id = new_order_id()
    create_execution_order(
        conn,
        order_id=order_id,
        decision_id=decision_id,
        ts_code="999999.SZ",
        target_qty=100,
        decision_price=10.0,
        submitted_price=10.0,
        status="submitted",
        broker_ref="paper:missing-signal-item",
        source_type="paper_broker",
    )

    summary = summarize_execution_evidence(conn, order_ids=[order_id], minimum_source_tier="quasi_live")
    conn.close()

    assert summary["passed"] is False
    assert f"missing_signal_item:{order_id}" in summary["blocking_reasons"]


def test_summarize_execution_evidence_blocks_quasi_live_as_broker_evidence(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    _, decision_id = _seed_signal_and_decision(conn)
    order_id = new_order_id()
    create_execution_order(
        conn,
        order_id=order_id,
        decision_id=decision_id,
        ts_code="000001.SZ",
        target_qty=100,
        decision_price=10.0,
        submitted_price=10.0,
        status="filled",
        broker_ref="paper:not-real-broker",
        source_type="paper_broker",
    )
    record_execution_fill(
        conn,
        fill_id=new_fill_id(),
        order_id=order_id,
        fill_price=10.0,
        fill_qty=100,
        fill_slippage_bp=0.0,
        venue="PAPER",
    )
    upsert_execution_attribution(
        conn,
        order_id=order_id,
        attribution=compute_execution_attribution(
            decision_price=10.0,
            submit_price=10.0,
            avg_fill_price=10.0,
            close_price=10.0,
            target_qty=100,
            filled_qty=100,
            delay_sec=5,
        ),
    )

    summary = summarize_execution_evidence(conn, order_ids=[order_id], minimum_source_tier="broker")
    conn.close()

    assert summary["passed"] is False
    assert summary["source_tier_counts"] == {"quasi_live": 1}
    assert summary["blocking_reasons"] == [f"source_tier_too_low:{order_id}:paper_broker:broker"]
