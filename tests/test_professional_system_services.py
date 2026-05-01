from __future__ import annotations

import sqlite3
import sys
import types

import pandas as pd

from openclaw.services.airivo_batch_service import build_manual_scan_opportunities, publish_manual_scan_to_execution_queue
from openclaw.services.data_version_service import build_data_version, build_param_version
from openclaw.services.data_quality_service import evaluate_data_quality
from openclaw.services.decision_service import record_decision_event, upsert_decision_snapshot
from openclaw.services.execution_analytics_service import compute_execution_attribution, upsert_execution_attribution
from openclaw.services.execution_fill_service import record_execution_fill
from openclaw.services.execution_order_service import create_execution_order, update_execution_order_status
from openclaw.services.airivo_feedback_service import apply_batch_feedback_action, update_feedback_row
from openclaw.services.lineage_service import (
    apply_professional_migrations,
    insert_signal_run,
    new_decision_id,
    new_fill_id,
    new_order_id,
    new_release_id,
    new_run_id,
    record_signal_dataframe_chain,
    record_backtest_result_chain,
    replace_signal_items,
)
from openclaw.services.professional_audit_service import audit_professional_fact_chains
from openclaw.services.release_gate_service import (
    build_release_gate_payload,
    record_release_gate_ledger,
    run_professional_fact_audit_gate,
)
from openclaw.services.release_event_service import record_release_event, record_release_validation


def test_apply_professional_migrations_and_signal_chain(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)
    quality = evaluate_data_quality(conn, max_required_age_days=9999)
    assert quality["passed"] is True
    assert quality["latest_trade_date"]
    run_id = new_run_id("scan", "v9")
    insert_signal_run(
        conn,
        run_id=run_id,
        run_type="scan",
        strategy="v9",
        trade_date="2025-01-28",
        data_version=build_data_version(conn),
        code_version="git:test:dirty0",
        param_version=build_param_version({"score_threshold": 66}),
        status="success",
        summary={"count": 2},
    )
    inserted = replace_signal_items(
        conn,
        run_id=run_id,
        items=[
            {"ts_code": "000001.SZ", "score": 88, "rank_idx": 1, "reason_codes": ["risk_pass"]},
            {"ts_code": "600000.SH", "score": 76, "rank_idx": 2, "reason_codes": ["signal_consensus_weak"]},
        ],
    )
    row = conn.execute("SELECT run_type, strategy, status FROM signal_runs WHERE run_id = ?", (run_id,)).fetchone()
    assert row == ("scan", "v9", "success")
    assert inserted == 2
    count = conn.execute("SELECT COUNT(*) FROM signal_items WHERE run_id = ?", (run_id,)).fetchone()[0]
    assert count == 2
    conn.close()


def test_record_signal_dataframe_chain_writes_versions_and_items(tmp_db, tmp_path):
    run_id = new_run_id("scan", "v9")
    frame = pd.DataFrame(
        [
            {"股票代码": "000001.SZ", "综合评分": 91, "排名": 1, "理由摘要": "risk_pass"},
            {"股票代码": "600000.SH", "综合评分": 82, "排名": 2, "理由摘要": "manual_override"},
        ]
    )

    record_signal_dataframe_chain(
        connect_db=lambda: sqlite3.connect(str(tmp_db)),
        code_root=tmp_path,
        run_id=run_id,
        strategy="v9",
        params={"score_threshold": 60},
        score_col="综合评分",
        result_df=frame,
        meta={"trade_date": "2026-05-01"},
        result_csv="logs/openclaw/async_scan/out.csv",
        meta_json="logs/openclaw/async_scan/out.meta.json",
        row_count=2,
    )

    conn = sqlite3.connect(str(tmp_db))
    run = conn.execute(
        "SELECT run_type, strategy, trade_date, data_version, code_version, param_version, artifact_path FROM signal_runs WHERE run_id = ?",
        (run_id,),
    ).fetchone()
    items = conn.execute("SELECT ts_code, score, rank_idx FROM signal_items WHERE run_id = ? ORDER BY rank_idx", (run_id,)).fetchall()
    assert run[0:3] == ("scan", "v9", "2026-05-01")
    assert run[3].startswith("trade_date:")
    assert run[4].startswith("git:")
    assert run[5].startswith("param:sha256:")
    assert run[6] == "logs/openclaw/async_scan/out.csv"
    assert items == [("000001.SZ", 91.0, 1), ("600000.SH", 82.0, 2)]
    conn.close()


def test_record_backtest_result_chain_writes_signal_run(tmp_db, tmp_path):
    run_id = new_run_id("backtest", "single")
    record_backtest_result_chain(
        connect_db=lambda: sqlite3.connect(str(tmp_db)),
        code_root=tmp_path,
        run_id=run_id,
        job_kind="single",
        payload={"strategy": "v9", "sample_size": 10, "date_to": "2026-05-01"},
        result={"success": True, "result": {"stats": {"win_rate": 60}}},
    )

    conn = sqlite3.connect(str(tmp_db))
    row = conn.execute(
        "SELECT run_type, strategy, trade_date, status, data_version, code_version, param_version FROM signal_runs WHERE run_id = ?",
        (run_id,),
    ).fetchone()
    assert row[0:4] == ("backtest", "v9", "2026-05-01", "success")
    assert row[4].startswith("trade_date:")
    assert row[5].startswith("git:")
    assert row[6].startswith("param:sha256:")
    conn.close()


def test_record_backtest_result_chain_writes_failed_signal_run(tmp_db, tmp_path):
    run_id = new_run_id("backtest", "single")
    record_backtest_result_chain(
        connect_db=lambda: sqlite3.connect(str(tmp_db)),
        code_root=tmp_path,
        run_id=run_id,
        job_kind="single",
        payload={"strategy": "v9", "sample_size": 10, "date_to": "2026-05-01"},
        result={"success": False, "error": "无法获取历史数据"},
    )

    conn = sqlite3.connect(str(tmp_db))
    row = conn.execute(
        "SELECT run_type, strategy, trade_date, status, summary_json FROM signal_runs WHERE run_id = ?",
        (run_id,),
    ).fetchone()
    assert row[0:4] == ("backtest", "v9", "2026-05-01", "failed")
    assert "无法获取历史数据" in row[4]
    conn.close()


def test_decision_execution_and_release_chains(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)

    run_id = new_run_id("scan", "combo")
    insert_signal_run(conn, run_id=run_id, run_type="scan", strategy="combo", status="success")

    decision_id = new_decision_id()
    record_decision_event(
        conn,
        decision_id=decision_id,
        decision_type="approve",
        based_on_run_id=run_id,
        risk_gate_state={"risk_level": "green"},
        release_gate_state={"passed": True},
        approval_reason_codes=["risk_pass"],
        approval_note="release approved",
        operator_name="alice",
        decision_payload={"selected_count": 1},
    )
    upsert_decision_snapshot(
        conn,
        decision_id=decision_id,
        decision_status="active",
        effective_trade_date="2025-01-28",
        selected_count=1,
        active_flag=True,
    )

    order_id = new_order_id()
    create_execution_order(
        conn,
        order_id=order_id,
        decision_id=decision_id,
        ts_code="000001.SZ",
        target_qty=1000,
        decision_price=10.0,
        submitted_price=10.05,
        status="submitted",
        source_type="overnight",
    )
    update_execution_order_status(conn, order_id=order_id, status="filled")

    fill_id = new_fill_id()
    record_execution_fill(
        conn,
        fill_id=fill_id,
        order_id=order_id,
        fill_price=10.08,
        fill_qty=1000,
        fill_fee=12.0,
        fill_slippage_bp=8.0,
        venue="SIM",
    )
    attribution = compute_execution_attribution(
        decision_price=10.0,
        submit_price=10.05,
        avg_fill_price=10.08,
        close_price=10.2,
        target_qty=1000,
        filled_qty=1000,
        delay_sec=18,
    )
    upsert_execution_attribution(conn, order_id=order_id, attribution=attribution)

    release_id = new_release_id()
    record_release_event(
        conn,
        release_id=release_id,
        release_type="deploy",
        code_version="git:test:dirty0",
        operator_name="alice",
        gate_result={"passed": True},
        payload={"decision_id": decision_id},
    )
    record_release_validation(
        conn,
        release_id=release_id,
        validation_type="pytest",
        validation_status="passed",
        validation_output_path="logs/pytest.log",
    )

    decision_row = conn.execute("SELECT decision_type, operator_name FROM decision_events WHERE decision_id = ?", (decision_id,)).fetchone()
    order_row = conn.execute("SELECT status, source_type FROM execution_orders WHERE order_id = ?", (order_id,)).fetchone()
    attr_row = conn.execute("SELECT fill_ratio, slippage_bp FROM execution_attribution WHERE order_id = ?", (order_id,)).fetchone()
    release_row = conn.execute("SELECT release_type, code_version FROM release_events WHERE release_id = ?", (release_id,)).fetchone()

    assert decision_row == ("approve", "alice")
    assert order_row == ("filled", "overnight")
    assert attr_row is not None and abs(attr_row[0] - 1.0) < 1e-9 and attr_row[1] > 0
    assert release_row == ("deploy", "git:test:dirty0")
    conn.close()


def test_professional_fact_chain_audit_passes_complete_fixture(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
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
        items=[{"ts_code": "000001.SZ", "score": 88, "rank_idx": 1, "reason_codes": ["risk_pass"]}],
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
        approval_note="release approved",
        operator_name="alice",
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

    order_id = new_order_id()
    create_execution_order(
        conn,
        order_id=order_id,
        decision_id=decision_id,
        ts_code="000001.SZ",
        target_qty=1000,
        decision_price=10.0,
        submitted_price=10.05,
        status="filled",
        source_type="overnight",
    )
    upsert_execution_attribution(
        conn,
        order_id=order_id,
        attribution={
            "fill_ratio": 1.0,
            "slippage_bp": 8.0,
            "opportunity_cost_bp": 0.0,
            "delay_sec": 18,
            "miss_reason_code": "",
            "attribution_json": {"source": "test"},
        },
    )

    release_id = new_release_id()
    record_release_event(
        conn,
        release_id=release_id,
        release_type="deploy",
        code_version="git:test:dirty0",
        operator_name="alice",
        gate_result={"passed": True},
        payload={"decision_id": decision_id},
    )
    record_release_validation(
        conn,
        release_id=release_id,
        validation_type="pytest",
        validation_status="passed",
        validation_output_path="logs/pytest.log",
    )

    audit = audit_professional_fact_chains(conn)
    assert audit["passed"] is True
    assert audit["blocking_reasons"] == []
    assert audit["chains"]["signal"]["total_runs"] == 1
    assert audit["chains"]["execution"]["total_orders"] == 1
    conn.close()


def test_professional_fact_chain_audit_blocks_missing_signal_versions(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)
    insert_signal_run(
        conn,
        run_id=new_run_id("scan", "combo"),
        run_type="scan",
        strategy="combo",
        status="success",
    )

    audit = audit_professional_fact_chains(conn)
    assert audit["passed"] is False
    assert "signal:missing_versions:1" in audit["blocking_reasons"]
    conn.close()


def test_professional_fact_chain_audit_blocks_execution_without_signal_lineage(tmp_db):
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
        approval_note="decision without signal lineage",
        operator_name="alice",
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
    order_id = new_order_id()
    create_execution_order(
        conn,
        order_id=order_id,
        decision_id=decision_id,
        ts_code="000001.SZ",
        target_qty=1000,
        decision_price=10.0,
        submitted_price=10.05,
        status="filled",
        source_type="test",
    )
    upsert_execution_attribution(
        conn,
        order_id=order_id,
        attribution={"fill_ratio": 1.0, "slippage_bp": 8.0, "delay_sec": 18},
    )

    audit = audit_professional_fact_chains(conn)
    conn.close()
    assert audit["passed"] is False
    assert audit["chains"]["execution"]["missing_signal_lineage"] == 1
    assert "execution:missing_signal_lineage:1" in audit["blocking_reasons"]


def test_release_gate_ledger_embeds_professional_audit_for_rollback(tmp_db, tmp_path):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)
    insert_signal_run(
        conn,
        run_id=new_run_id("scan", "combo"),
        run_type="scan",
        strategy="combo",
        status="success",
    )
    conn.close()

    audit_path = tmp_path / "professional_audit.json"
    audit = run_professional_fact_audit_gate(tmp_db, output_path=audit_path)
    payload = build_release_gate_payload(log_file="logs/release_gate.log", audit_summary=audit)
    assert payload["rollback_context"]["blocking_reasons"] == ["signal:missing_versions:1"]
    assert "signal:missing_versions:1" in audit_path.read_text(encoding="utf-8")

    release_id = record_release_gate_ledger(
        db_path=tmp_db,
        code_root=tmp_path,
        overall="failed",
        rounds=3,
        skip_remote=True,
        log_file="logs/release_gate.log",
        validation_statuses={
            "git_hash": "passed",
            "regression_combo": "passed",
            "health_gate": "passed",
            "professional_fact_audit": "failed",
        },
        audit_summary=audit,
        operator_name="alice",
    )

    conn = sqlite3.connect(str(tmp_db))
    row = conn.execute(
        "SELECT gate_result, payload_json FROM release_events WHERE release_id = ?",
        (release_id,),
    ).fetchone()
    validation = conn.execute(
        "SELECT validation_status FROM release_validations WHERE release_id = ? AND validation_type = ?",
        (release_id, "professional_fact_audit"),
    ).fetchone()
    assert row is not None
    assert '"professional_fact_audit":"failed"' in row[0]
    assert "signal:missing_versions:1" in row[1]
    assert validation == ("failed",)
    conn.close()


def _create_overnight_feedback_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE overnight_execution_feedback (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            decision_date TEXT,
            trade_date TEXT,
            ts_code TEXT,
            stock_name TEXT,
            planned_action TEXT,
            final_action TEXT,
            execution_status TEXT,
            execution_note TEXT,
            operator_name TEXT,
            switched_from_ts_code TEXT,
            system_suggested_action TEXT,
            system_suggested_status TEXT,
            system_confidence TEXT,
            system_reason TEXT,
            decision_bucket TEXT,
            decision_gate_reason TEXT,
            needs_manual_review INTEGER DEFAULT 0,
            human_override INTEGER DEFAULT 0,
            human_override_reason TEXT,
            updated_at TEXT
        )
        """
    )


def test_feedback_update_anchors_authoritative_execution_order(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    _create_overnight_feedback_table(conn)
    conn.execute(
        """
        INSERT INTO overnight_execution_feedback (
            decision_date, trade_date, ts_code, stock_name, planned_action, final_action,
            execution_status, execution_note, operator_name, system_suggested_action,
            system_confidence, decision_bucket
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "2026-04-30",
            "2026-05-01",
            "000001.SZ",
            "平安银行",
            "buy",
            "pending",
            "pending",
            "",
            "",
            "buy",
            "0.92",
            "direct_execute",
        ),
    )
    row_id = int(conn.execute("SELECT id FROM overnight_execution_feedback").fetchone()[0])
    conn.commit()
    conn.close()

    ok, msg = update_feedback_row(
        db_path=str(tmp_db),
        row_id=row_id,
        final_action="buy",
        execution_status="done",
        execution_note="人工确认已成交，成交明细待券商回填",
        operator_name="alice",
        system_suggested_action="buy",
        clear_feedback_snapshot_cache=lambda: None,
    )

    assert ok, msg
    conn = sqlite3.connect(str(tmp_db))
    order = conn.execute(
        """
        SELECT decision_id, ts_code, status, broker_ref, source_type
        FROM execution_orders
        WHERE broker_ref = ?
        """,
        (f"overnight_execution_feedback:{row_id}",),
    ).fetchone()
    assert order is not None
    assert order[1:] == ("000001.SZ", "filled", f"overnight_execution_feedback:{row_id}", "overnight_feedback")
    snapshot = conn.execute("SELECT decision_status, effective_trade_date FROM decision_snapshot WHERE decision_id = ?", (order[0],)).fetchone()
    attribution = conn.execute("SELECT fill_ratio, miss_reason_code FROM execution_attribution WHERE order_id = (SELECT order_id FROM execution_orders WHERE broker_ref = ?)", (f"overnight_execution_feedback:{row_id}",)).fetchone()
    assert snapshot == ("active", "2026-04-30")
    assert attribution == (1.0, "")
    conn.close()


def test_batch_feedback_action_anchors_cancelled_orders(tmp_db):
    conn = sqlite3.connect(str(tmp_db))
    _create_overnight_feedback_table(conn)
    for code in ("000001.SZ", "600000.SH"):
        conn.execute(
            """
            INSERT INTO overnight_execution_feedback (
                decision_date, trade_date, ts_code, stock_name, planned_action, final_action,
                execution_status, execution_note, operator_name, system_suggested_action,
                system_confidence, decision_bucket
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("2026-04-30", "2026-05-01", code, code, "buy", "pending", "pending", "", "", "skip", "0.80", "auto_reject"),
        )
    conn.commit()
    conn.close()

    ok, msg = apply_batch_feedback_action(
        db_path=str(tmp_db),
        bucket="auto_reject",
        operator_name="alice",
        execution_note="批量采纳系统淘汰",
        clear_feedback_snapshot_cache=lambda: None,
    )

    assert ok, msg
    conn = sqlite3.connect(str(tmp_db))
    rows = conn.execute("SELECT status, cancel_reason FROM execution_orders ORDER BY ts_code").fetchall()
    attrs = conn.execute("SELECT miss_reason_code FROM execution_attribution ORDER BY order_id").fetchall()
    assert rows == [("cancelled", "批量采纳系统淘汰"), ("cancelled", "批量采纳系统淘汰")]
    assert attrs == [("manual_cancel",), ("manual_cancel",)]
    conn.close()


def test_manual_scan_publish_is_service_owned_and_audited(tmp_db, monkeypatch, tmp_path):
    fake = types.ModuleType("openclaw.overnight_decision")

    def _identity_enrich(conn, *, opportunities):
        return opportunities

    def _build_decision(*, trade_date, opportunities, active_holdings, risk, calibration, top_n):
        return {
            "trade_date": trade_date,
            "recommendations": opportunities[:top_n],
            "risk": risk,
        }

    fake.apply_feature_enrichment = _identity_enrich
    fake.apply_trade_window_analysis = lambda conn, payload, lookback_days: payload
    fake.build_overnight_decision = _build_decision
    fake.export_execution_feedback_template = lambda output_dir, decision_date, payload: str(tmp_path / "feedback.csv")
    fake.load_active_holdings = lambda conn: []
    fake.load_return_calibration = lambda conn, horizon_days, lookback_days, min_samples: {"samples": 0}
    fake.next_trade_date = lambda today: "2026-05-04"
    fake.refresh_realized_outcomes = lambda conn, lookback_days: {"updated": 0}

    def _persist(conn, **kwargs):
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS overnight_decision_runs (
                decision_date TEXT,
                trade_date TEXT,
                source_type TEXT,
                source_run_id TEXT,
                source_label TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO overnight_decision_runs
            (decision_date, trade_date, source_type, source_run_id, source_label)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                kwargs["decision_date"],
                kwargs["payload"]["trade_date"],
                kwargs["source_type"],
                kwargs["source_run_id"],
                kwargs["source_label"],
            ),
        )

    def _seed(conn, *, decision_date, payload, source_type, source_run_id):
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS overnight_execution_feedback (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                decision_date TEXT,
                trade_date TEXT,
                ts_code TEXT
            )
            """
        )
        for row in payload.get("recommendations") or []:
            conn.execute(
                "INSERT INTO overnight_execution_feedback (decision_date, trade_date, ts_code) VALUES (?, ?, ?)",
                (decision_date, payload.get("trade_date"), row.get("ts_code")),
            )

    fake.persist_overnight_decision = _persist
    fake.seed_execution_feedback = _seed
    monkeypatch.setitem(sys.modules, "openclaw.overnight_decision", fake)

    candidate = {
        "strategy": "v9",
        "score_col": "综合评分",
        "run_id": "run_scan_v9_20260501_080000_demo",
        "df": pd.DataFrame(
            [
                {"股票代码": "000001.SZ", "股票名称": "平安银行", "综合评分": 91, "排名": 1, "理由摘要": "risk_pass"},
                {"股票代码": "600000.SH", "股票名称": "浦发银行", "综合评分": 82, "排名": 2, "理由摘要": "manual_override"},
            ]
        ),
    }
    opportunities = build_manual_scan_opportunities(candidate)
    assert [x["ts_code"] for x in opportunities] == ["000001.SZ", "600000.SH"]

    cleared = {"queue": 0, "feedback": 0}
    ok, msg, batch_id = publish_manual_scan_to_execution_queue(
        candidate=candidate,
        runtime_snapshot={"risk_level": "GREEN"},
        connect_db=lambda: sqlite3.connect(str(tmp_db)),
        logs_dir=tmp_path,
        clear_execution_queue_cache=lambda: cleared.__setitem__("queue", cleared["queue"] + 1),
        clear_feedback_snapshot_cache=lambda: cleared.__setitem__("feedback", cleared["feedback"] + 1),
    )

    assert ok, msg
    assert batch_id
    assert cleared == {"queue": 1, "feedback": 1}
    conn = sqlite3.connect(str(tmp_db))
    decision = conn.execute(
        "SELECT decision_type, based_on_run_id FROM decision_events ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    release = conn.execute("SELECT release_type FROM release_events ORDER BY created_at DESC LIMIT 1").fetchone()
    feedback_count = conn.execute("SELECT COUNT(*) FROM overnight_execution_feedback WHERE decision_date = ?", (batch_id,)).fetchone()[0]
    assert decision == ("approve", "run_scan_v9_20260501_080000_demo")
    assert release == ("deploy",)
    assert feedback_count == 2
    conn.close()
