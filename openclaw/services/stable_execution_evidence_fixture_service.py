from __future__ import annotations

from datetime import datetime
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List

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
    replace_signal_items,
)


JsonDict = Dict[str, Any]


def seed_stable_shadow_execution_evidence(
    conn: sqlite3.Connection,
    *,
    linked_run_id: str,
    output_dir: str,
    operator_name: str = "stable_shadow_execution_fixture",
) -> JsonDict:
    apply_professional_migrations(conn)
    run_id = str(linked_run_id or "").strip()
    if not run_id:
        raise ValueError("missing_linked_run_id")
    _ensure_stable_signal_run(conn, run_id=run_id)
    replace_signal_items(
        conn,
        run_id=run_id,
        items=[
            {"ts_code": "000001.SZ", "score": 88, "rank_idx": 1, "reason_codes": ["stable_shadow", "filled_case"]},
            {"ts_code": "000002.SZ", "score": 84, "rank_idx": 2, "reason_codes": ["stable_shadow", "miss_case"]},
            {"ts_code": "000333.SZ", "score": 81, "rank_idx": 3, "reason_codes": ["stable_shadow", "manual_override_case"]},
            {"ts_code": "000651.SZ", "score": 79, "rank_idx": 4, "reason_codes": ["stable_shadow", "partial_fill_case"]},
            {"ts_code": "600519.SH", "score": 77, "rank_idx": 5, "reason_codes": ["stable_shadow", "cancelled_case"]},
            {"ts_code": "000858.SZ", "score": 75, "rank_idx": 6, "reason_codes": ["stable_shadow", "rejected_case"]},
            {"ts_code": "300750.SZ", "score": 73, "rank_idx": 7, "reason_codes": ["stable_shadow", "high_slippage_case"]},
        ],
    )
    decision_id = new_decision_id()
    record_decision_event(
        conn,
        decision_id=decision_id,
        decision_type="candidate_shadow_execution",
        based_on_run_id=run_id,
        risk_gate_state={"mode": "shadow", "strategy": "stable", "risk_level": "candidate_discussion"},
        release_gate_state={"passed": True, "mode": "shadow", "production_release": False},
        approval_reason_codes=["stable_candidate_discussion", "shadow_execution_evidence"],
        approval_note="stable shadow/quasi-live execution evidence fixture; not production release",
        operator_name=operator_name,
        decision_payload={"source_run_id": run_id, "production_candidate_allowed": False},
    )
    upsert_decision_snapshot(
        conn,
        decision_id=decision_id,
        decision_status="shadow",
        effective_trade_date="2026-05-05",
        selected_count=3,
        active_flag=False,
    )
    cases = [
        _record_case(
            conn,
            decision_id=decision_id,
            ts_code="000001.SZ",
            case_type="filled_baseline",
            status="filled",
            target_qty=1000,
            decision_price=10.00,
            submitted_price=10.02,
            avg_fill_price=10.04,
            close_price=10.12,
            filled_qty=1000,
            delay_sec=20,
        ),
        _record_case(
            conn,
            decision_id=decision_id,
            ts_code="000002.SZ",
            case_type="expired_price_not_reached",
            status="expired",
            target_qty=800,
            decision_price=12.00,
            submitted_price=11.95,
            close_price=12.20,
            miss_reason_code="price_not_reached",
            cancel_reason="shadow order expired without fill",
            delay_sec=300,
        ),
        _record_case(
            conn,
            decision_id=decision_id,
            ts_code="000333.SZ",
            case_type="manual_override",
            status="manual_override",
            target_qty=600,
            decision_price=18.00,
            submitted_price=18.00,
            close_price=17.80,
            miss_reason_code="manual_risk_override",
            cancel_reason="operator blocked candidate order in shadow review",
            delay_sec=60,
        ),
        _record_case(
            conn,
            decision_id=decision_id,
            ts_code="000651.SZ",
            case_type="partial_fill",
            status="partial_fill",
            target_qty=1200,
            decision_price=22.00,
            submitted_price=22.02,
            avg_fill_price=22.06,
            close_price=22.10,
            filled_qty=600,
            delay_sec=45,
        ),
        _record_case(
            conn,
            decision_id=decision_id,
            ts_code="600519.SH",
            case_type="cancelled",
            status="cancelled",
            target_qty=300,
            decision_price=1500.00,
            submitted_price=1498.00,
            close_price=1496.00,
            miss_reason_code="decision_deviation",
            cancel_reason="operator cancelled due to decision_deviation",
            delay_sec=120,
        ),
        _record_case(
            conn,
            decision_id=decision_id,
            ts_code="000858.SZ",
            case_type="rejected",
            status="rejected",
            target_qty=900,
            decision_price=180.00,
            submitted_price=180.20,
            close_price=179.80,
            miss_reason_code="broker_reject_risk_rule",
            cancel_reason="broker rejected by risk rule",
            delay_sec=30,
        ),
        _record_case(
            conn,
            decision_id=decision_id,
            ts_code="300750.SZ",
            case_type="filled_high_slippage",
            status="filled",
            target_qty=400,
            decision_price=200.00,
            submitted_price=201.00,
            avg_fill_price=204.50,
            close_price=205.00,
            filled_qty=400,
            delay_sec=50,
        ),
    ]
    evidence = summarize_execution_evidence(conn, decision_ids=[decision_id], minimum_source_tier="quasi_live")
    payload: JsonDict = {
        "artifact_version": "stable_shadow_execution_evidence.v1",
        "created_at": _now_text(),
        "strategy": "stable",
        "linked_run_id": run_id,
        "decision_id": decision_id,
        "operator_name": operator_name,
        "mode": "shadow_quasi_live",
        "production_release": False,
        "seeded_cases": cases,
        "execution_evidence": evidence,
    }
    artifacts = _write_artifacts(Path(output_dir), payload)
    payload["artifacts"] = artifacts
    _write_json(Path(artifacts["json"]), payload)
    return payload


def _ensure_stable_signal_run(conn: sqlite3.Connection, *, run_id: str) -> None:
    row = conn.execute("SELECT 1 FROM signal_runs WHERE run_id = ?", (run_id,)).fetchone()
    if row:
        return
    insert_signal_run(
        conn,
        run_id=run_id,
        run_type="backtest",
        strategy="stable",
        trade_date="2026-05-05",
        data_version="stable_shadow_fixture",
        code_version="git:stable-shadow-fixture",
        param_version="param:stable-shadow-fixture",
        status="success",
        summary={"source": "stable_shadow_execution_fixture"},
    )


def _record_case(
    conn: sqlite3.Connection,
    *,
    decision_id: str,
    ts_code: str,
    case_type: str,
    status: str,
    target_qty: int,
    decision_price: float,
    submitted_price: float,
    avg_fill_price: float = 0.0,
    close_price: float = 0.0,
    filled_qty: int = 0,
    miss_reason_code: str = "",
    cancel_reason: str = "",
    delay_sec: float = 0.0,
) -> JsonDict:
    order_id = new_order_id()
    create_execution_order(
        conn,
        order_id=order_id,
        decision_id=decision_id,
        ts_code=ts_code,
        side="buy",
        target_qty=target_qty,
        decision_price=decision_price,
        submitted_price=submitted_price,
        status="submitted",
        broker_ref=f"paper_broker://stable_shadow/{case_type}",
        source_type="paper_broker",
    )
    update_execution_order_status(conn, order_id=order_id, status=status, cancel_reason=cancel_reason)
    fill_id = ""
    if filled_qty > 0:
        fill_id = new_fill_id()
        record_execution_fill(
            conn,
            fill_id=fill_id,
            order_id=order_id,
            fill_price=avg_fill_price,
            fill_qty=filled_qty,
            fill_fee=round(avg_fill_price * filled_qty * 0.001, 2),
            fill_slippage_bp=_slippage_bp(decision_price=decision_price, avg_fill_price=avg_fill_price),
            venue="PAPER",
        )
    upsert_execution_attribution(
        conn,
        order_id=order_id,
        attribution=compute_execution_attribution(
            decision_price=decision_price,
            submit_price=submitted_price,
            avg_fill_price=avg_fill_price,
            close_price=close_price,
            target_qty=target_qty,
            filled_qty=filled_qty,
            delay_sec=delay_sec,
            miss_reason_code=miss_reason_code,
        ),
    )
    return {"case_type": case_type, "order_id": order_id, "fill_id": fill_id, "status": status}


def _slippage_bp(*, decision_price: float, avg_fill_price: float) -> float:
    if float(decision_price or 0.0) <= 0 or float(avg_fill_price or 0.0) <= 0:
        return 0.0
    return ((float(avg_fill_price) - float(decision_price)) / float(decision_price)) * 10000.0


def _write_artifacts(output_dir: Path, payload: JsonDict) -> JsonDict:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"stable_shadow_execution_evidence_{ts}.json"
    md_path = output_dir / f"stable_shadow_execution_evidence_{ts}.md"
    _write_json(json_path, payload)
    lines = [
        "# Stable Shadow Execution Evidence",
        "",
        f"- decision_id: `{payload.get('decision_id')}`",
        f"- linked_run_id: `{payload.get('linked_run_id')}`",
        f"- passed: `{(payload.get('execution_evidence') or {}).get('passed')}`",
        f"- total_orders: `{(payload.get('execution_evidence') or {}).get('total_orders')}`",
    ]
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"json": str(json_path), "markdown": str(md_path)}


def _write_json(path: Path, payload: JsonDict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
