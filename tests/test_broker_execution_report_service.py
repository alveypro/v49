from __future__ import annotations

import sqlite3

import pytest

from openclaw.services.broker_execution_report_service import (
    record_broker_execution_report,
    record_manual_broker_execution_attestation,
)
from openclaw.services.decision_service import record_decision_event, upsert_decision_snapshot
from openclaw.services.lineage_service import (
    apply_professional_migrations,
    insert_signal_run,
    new_decision_id,
    new_run_id,
    replace_signal_items,
)


def _seed_broker_lineage(conn: sqlite3.Connection, *, ts_code: str = "000001.SZ") -> tuple[str, str]:
    apply_professional_migrations(conn)
    run_id = new_run_id("scan", "combo")
    insert_signal_run(
        conn,
        run_id=run_id,
        run_type="scan",
        strategy="combo",
        trade_date="2026-05-02",
        data_version="trade_date:20260502",
        code_version="git:test:broker",
        param_version="param:sha256:broker",
        status="success",
    )
    replace_signal_items(
        conn,
        run_id=run_id,
        items=[{"ts_code": ts_code, "score": 91.0, "rank_idx": 1, "reason_codes": ["broker_fixture"]}],
    )
    decision_id = new_decision_id()
    record_decision_event(
        conn,
        decision_id=decision_id,
        decision_type="approve",
        based_on_run_id=run_id,
        risk_gate_state={"risk_level": "green"},
        release_gate_state={"release_id": "rel_test_broker", "passed": True},
        approval_reason_codes=["release_gate_passed"],
        approval_note="broker execution report fixture",
        operator_name="release_gate",
        decision_payload={"selected_count": 1},
    )
    upsert_decision_snapshot(
        conn,
        decision_id=decision_id,
        decision_status="active",
        effective_trade_date="2026-05-02",
        selected_count=1,
        active_flag=True,
    )
    return run_id, decision_id


def test_record_broker_execution_report_writes_authoritative_execution_chain(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    run_id, decision_id = _seed_broker_lineage(conn)

    result = record_broker_execution_report(
        conn,
        report={
            "decision_id": decision_id,
            "ts_code": "000001.SZ",
            "broker_ref": "broker:ord:001",
            "source_type": "broker_api",
            "status": "filled",
            "target_qty": 1000,
            "decision_price": 10.0,
            "submitted_price": 10.05,
            "close_price": 10.2,
            "delay_sec": 12,
            "fills": [
                {
                    "broker_fill_ref": "fill-1",
                    "fill_price": 10.06,
                    "fill_qty": 600,
                    "fill_fee": 1.2,
                    "fill_slippage_bp": 60.0,
                    "venue": "BROKER",
                },
                {
                    "broker_fill_ref": "fill-2",
                    "fill_price": 10.08,
                    "fill_qty": 400,
                    "fill_fee": 0.8,
                    "fill_slippage_bp": 80.0,
                    "venue": "BROKER",
                },
            ],
        },
    )
    conn.close()

    assert result["status"] == "filled"
    assert result["fill_count"] == 2
    assert result["filled_qty"] == 1000
    assert result["evidence_summary"]["passed"] is True
    assert result["evidence_summary"]["source_tier_counts"] == {"broker": 1}
    assert result["evidence_summary"]["linked_run_ids"] == [run_id]
    assert result["evidence_summary"]["cases"][0]["has_signal_item"] is True


def test_record_broker_execution_report_rejects_quasi_live_source(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    _, decision_id = _seed_broker_lineage(conn)

    with pytest.raises(ValueError, match="non_broker_execution_source:paper_broker"):
        record_broker_execution_report(
            conn,
            report={
                "decision_id": decision_id,
                "ts_code": "000001.SZ",
                "broker_ref": "paper:ord:001",
                "source_type": "paper_broker",
                "status": "filled",
                "target_qty": 100,
                "decision_price": 10.0,
                "submitted_price": 10.0,
                "fills": [{"fill_price": 10.0, "fill_qty": 100}],
            },
        )
    conn.close()


def test_record_broker_execution_report_writes_rejected_broker_order_with_reason(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    run_id, decision_id = _seed_broker_lineage(conn)

    result = record_broker_execution_report(
        conn,
        report={
            "decision_id": decision_id,
            "ts_code": "000001.SZ",
            "broker_ref": "oms:ord:rejected",
            "source_type": "oms",
            "status": "rejected",
            "target_qty": 100,
            "decision_price": 10.0,
            "submitted_price": 10.0,
            "miss_reason_code": "broker_reject_price_limit",
            "cancel_reason": "broker rejected order because price breached limit",
        },
    )
    conn.close()

    assert result["status"] == "rejected"
    assert result["fill_count"] == 0
    assert result["filled_qty"] == 0
    assert result["evidence_summary"]["passed"] is True
    assert result["evidence_summary"]["source_tier_counts"] == {"broker": 1}
    assert result["evidence_summary"]["miss_reason_counts"] == {"broker_reject_price_limit": 1}
    assert result["evidence_summary"]["linked_run_ids"] == [run_id]


def test_record_broker_execution_report_requires_fills_for_filled_status(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    _, decision_id = _seed_broker_lineage(conn)

    with pytest.raises(ValueError, match="missing_broker_fills:broker:ord:no-fill"):
        record_broker_execution_report(
            conn,
            report={
                "decision_id": decision_id,
                "ts_code": "000001.SZ",
                "broker_ref": "broker:ord:no-fill",
                "source_type": "broker_api",
                "status": "filled",
                "target_qty": 100,
                "decision_price": 10.0,
                "submitted_price": 10.0,
            },
        )
    conn.close()


def test_record_broker_execution_report_requires_signal_item_lineage(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    _, decision_id = _seed_broker_lineage(conn, ts_code="000001.SZ")

    with pytest.raises(ValueError, match=f"missing_signal_item:{decision_id}:999999.SZ"):
        record_broker_execution_report(
            conn,
            report={
                "decision_id": decision_id,
                "ts_code": "999999.SZ",
                "broker_ref": "broker:ord:missing-signal-item",
                "source_type": "broker_api",
                "status": "submitted",
                "target_qty": 100,
                "decision_price": 10.0,
                "submitted_price": 10.0,
            },
        )
    conn.close()


def test_record_manual_broker_execution_attestation_writes_operator_evidence_anchor(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    run_id, decision_id = _seed_broker_lineage(conn)
    evidence_sha = "a" * 64

    result = record_manual_broker_execution_attestation(
        conn,
        report={
            "decision_id": decision_id,
            "ts_code": "000001.SZ",
            "operator_name": "alice",
            "evidence_ref": "broker-app-screenshot-20260502-001",
            "evidence_sha256": evidence_sha,
            "status": "filled",
            "target_qty": 100,
            "decision_price": 10.0,
            "submitted_price": 10.01,
            "fills": [{"fill_price": 10.01, "fill_qty": 100, "venue": "manual_broker_app"}],
        },
    )
    conn.close()

    assert result["status"] == "filled"
    assert result["broker_ref"] == "manual_broker:broker-app-screenshot-20260502-001:aaaaaaaaaaaaaaaa"
    assert result["manual_attestation"] == {
        "operator_name": "alice",
        "evidence_ref": "broker-app-screenshot-20260502-001",
        "evidence_sha256": evidence_sha,
    }
    assert result["evidence_summary"]["passed"] is True
    assert result["evidence_summary"]["source_tier_counts"] == {"broker": 1}
    assert result["evidence_summary"]["linked_run_ids"] == [run_id]


def test_record_broker_execution_report_blocks_manual_broker_without_attestation(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    _, decision_id = _seed_broker_lineage(conn)

    with pytest.raises(ValueError, match="missing_required_broker_field:operator_name"):
        record_broker_execution_report(
            conn,
            report={
                "decision_id": decision_id,
                "ts_code": "000001.SZ",
                "broker_ref": "manual_broker:unattested",
                "source_type": "broker",
                "status": "submitted",
                "target_qty": 100,
                "decision_price": 10.0,
                "submitted_price": 10.0,
            },
        )
    conn.close()


def test_record_manual_broker_execution_attestation_rejects_invalid_evidence_hash(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    _, decision_id = _seed_broker_lineage(conn)

    with pytest.raises(ValueError, match="invalid_evidence_sha256"):
        record_manual_broker_execution_attestation(
            conn,
            report={
                "decision_id": decision_id,
                "ts_code": "000001.SZ",
                "operator_name": "alice",
                "evidence_ref": "broker-app-screenshot-20260502-002",
                "evidence_sha256": "not-a-sha",
                "status": "cancelled",
                "target_qty": 100,
                "decision_price": 10.0,
                "submitted_price": 0,
                "miss_reason_code": "manual_not_submitted",
                "cancel_reason": "operator did not submit order",
            },
        )
    conn.close()
