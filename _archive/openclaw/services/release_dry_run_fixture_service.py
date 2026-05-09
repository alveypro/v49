from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List

from openclaw.services.decision_service import record_decision_event, upsert_decision_snapshot
from openclaw.services.execution_analytics_service import compute_execution_attribution, upsert_execution_attribution
from openclaw.services.execution_fill_service import record_execution_fill
from openclaw.services.execution_order_service import create_execution_order, update_execution_order_status
from openclaw.services.lineage_service import (
    apply_professional_migrations,
    insert_signal_run,
    new_decision_id,
    new_fill_id,
    new_order_id,
    new_release_id,
    new_run_id,
    replace_signal_items,
)
from openclaw.services.release_dry_run_service import run_release_dry_run_audit
from openclaw.services.release_event_service import record_release_event, record_release_validation


def _record_execution_case(
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
    fill_slippage_bp: float = 0.0,
    delay_sec: float = 0.0,
    miss_reason_code: str = "",
    cancel_reason: str = "",
) -> Dict[str, Any]:
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
        broker_ref=f"fixture://release_dry_run/{case_type}",
        source_type="release_dry_run_fixture",
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
            fill_fee=round(max(avg_fill_price, 0.0) * filled_qty * 0.001, 2),
            fill_slippage_bp=fill_slippage_bp,
            venue="SIM",
        )

    attribution = compute_execution_attribution(
        decision_price=decision_price,
        submit_price=submitted_price,
        avg_fill_price=avg_fill_price,
        close_price=close_price,
        target_qty=target_qty,
        filled_qty=filled_qty,
        delay_sec=delay_sec,
        miss_reason_code=miss_reason_code,
    )
    upsert_execution_attribution(conn, order_id=order_id, attribution=attribution)
    return {
        "case_type": case_type,
        "order_id": order_id,
        "fill_id": fill_id,
        "status": status,
        "fill_ratio": attribution["fill_ratio"],
        "slippage_bp": attribution["slippage_bp"],
        "miss_reason_code": miss_reason_code,
    }


def seed_release_dry_run_fixture(conn: sqlite3.Connection, *, operator_name: str = "dry_run") -> Dict[str, Any]:
    apply_professional_migrations(conn)
    run_id = new_run_id("scan", "combo")
    insert_signal_run(
        conn,
        run_id=run_id,
        run_type="scan",
        strategy="combo",
        trade_date="2026-05-01",
        data_version="trade_date:20260501",
        code_version="git:dry-run-fixture:dirty0",
        param_version="param:sha256:dryrun",
        status="success",
        artifact_path="fixture://release_dry_run/signal_combo.csv",
        summary={"source": "release_dry_run_fixture", "row_count": 6},
    )
    replace_signal_items(
        conn,
        run_id=run_id,
        items=[
            {
                "ts_code": "000001.SZ",
                "score": 88,
                "rank_idx": 1,
                "reason_codes": ["risk_pass", "combo_consensus"],
            },
            {"ts_code": "000002.SZ", "score": 82, "rank_idx": 2, "reason_codes": ["combo_consensus", "liquidity_watch"]},
            {"ts_code": "000333.SZ", "score": 80, "rank_idx": 3, "reason_codes": ["combo_consensus", "manual_cancel_case"]},
            {"ts_code": "600519.SH", "score": 79, "rank_idx": 4, "reason_codes": ["combo_consensus", "partial_fill_case"]},
            {"ts_code": "300750.SZ", "score": 78, "rank_idx": 5, "reason_codes": ["combo_consensus", "slippage_case"]},
            {"ts_code": "601318.SH", "score": 76, "rank_idx": 6, "reason_codes": ["combo_consensus", "decision_deviation_case"]},
        ],
    )

    decision_id = new_decision_id()
    record_decision_event(
        conn,
        decision_id=decision_id,
        decision_type="approve",
        based_on_run_id=run_id,
        risk_gate_state={"risk_level": "green", "source": "release_dry_run_fixture"},
        release_gate_state={"passed": True, "source": "release_dry_run_fixture"},
        approval_reason_codes=["risk_pass", "release_dry_run_ready"],
        approval_note="release dry-run fixture approved for audit rehearsal",
        operator_name=operator_name,
        decision_payload={"selected_count": 6, "source_run_id": run_id},
    )
    upsert_decision_snapshot(
        conn,
        decision_id=decision_id,
        decision_status="active",
        effective_trade_date="2026-05-01",
        selected_count=6,
        active_flag=True,
    )

    execution_cases: List[Dict[str, Any]] = []
    execution_cases.append(
        _record_execution_case(
            conn,
            decision_id=decision_id,
            ts_code="000001.SZ",
            case_type="filled_baseline",
            status="filled",
            target_qty=1000,
            decision_price=10.0,
            submitted_price=10.05,
            avg_fill_price=10.08,
            close_price=10.2,
            filled_qty=1000,
            fill_slippage_bp=80.0,
            delay_sec=18,
        )
    )
    execution_cases.append(
        _record_execution_case(
            conn,
            decision_id=decision_id,
            ts_code="000002.SZ",
            case_type="unfilled_expired",
            status="expired",
            target_qty=800,
            decision_price=12.0,
            submitted_price=11.88,
            close_price=12.25,
            delay_sec=300,
            miss_reason_code="no_fill_price_not_reached",
            cancel_reason="fixture expired without fill",
        )
    )
    execution_cases.append(
        _record_execution_case(
            conn,
            decision_id=decision_id,
            ts_code="000333.SZ",
            case_type="cancelled_manual",
            status="cancelled",
            target_qty=600,
            decision_price=18.0,
            submitted_price=18.0,
            close_price=17.6,
            delay_sec=45,
            miss_reason_code="manual_cancel",
            cancel_reason="fixture operator cancelled before venue ack",
        )
    )
    execution_cases.append(
        _record_execution_case(
            conn,
            decision_id=decision_id,
            ts_code="600519.SH",
            case_type="partial_fill",
            status="partial_fill",
            target_qty=100,
            decision_price=1500.0,
            submitted_price=1502.0,
            avg_fill_price=1504.5,
            close_price=1510.0,
            filled_qty=40,
            fill_slippage_bp=30.0,
            delay_sec=120,
            miss_reason_code="partial_liquidity",
        )
    )
    execution_cases.append(
        _record_execution_case(
            conn,
            decision_id=decision_id,
            ts_code="300750.SZ",
            case_type="slippage_anomaly",
            status="filled",
            target_qty=500,
            decision_price=220.0,
            submitted_price=221.0,
            avg_fill_price=224.4,
            close_price=223.0,
            filled_qty=500,
            fill_slippage_bp=200.0,
            delay_sec=35,
            miss_reason_code="slippage_anomaly",
        )
    )
    execution_cases.append(
        _record_execution_case(
            conn,
            decision_id=decision_id,
            ts_code="601318.SH",
            case_type="decision_deviation",
            status="manual_override",
            target_qty=700,
            decision_price=52.0,
            submitted_price=51.8,
            close_price=50.9,
            delay_sec=60,
            miss_reason_code="decision_deviation",
            cancel_reason="fixture operator overrode approved decision",
        )
    )

    baseline_case = execution_cases[0]
    order_id = str(baseline_case["order_id"])
    fill_id = str(baseline_case["fill_id"])

    release_id = new_release_id()
    record_release_event(
        conn,
        release_id=release_id,
        release_type="deploy",
        code_version="git:dry-run-fixture:dirty0",
        operator_name=operator_name,
        gate_result={"passed": True, "source": "release_dry_run_fixture"},
        payload={
            "rollback_context": {
                "previous_release_id": "rel_fixture_previous",
                "restore_code_version": "git:dry-run-fixture:previous",
                "restore_decision_id": decision_id,
            }
        },
    )
    record_release_validation(
        conn,
        release_id=release_id,
        validation_type="fixture_release_reference",
        validation_status="passed",
        validation_output_path="fixture://release_dry_run/report.md",
    )
    return {
        "run_id": run_id,
        "decision_id": decision_id,
        "order_id": order_id,
        "fill_id": fill_id,
        "release_id": release_id,
        "execution_cases": execution_cases,
    }


def render_release_dry_run_report(payload: Dict[str, Any], *, fixture_ids: Dict[str, Any]) -> str:
    validations = payload.get("validation_statuses") if isinstance(payload.get("validation_statuses"), dict) else {}
    execution_cases = fixture_ids.get("execution_cases") if isinstance(fixture_ids.get("execution_cases"), list) else []
    lines = [
        "# Airivo Release Dry-Run Report",
        "",
        f"- decision: `{payload.get('decision')}`",
        f"- allow_release_gate: `{payload.get('allow_release_gate')}`",
        f"- run_id: `{fixture_ids.get('run_id', '')}`",
        f"- decision_id: `{fixture_ids.get('decision_id', '')}`",
        f"- order_id: `{fixture_ids.get('order_id', '')}`",
        f"- rollback_release_id: `{fixture_ids.get('release_id', '')}`",
        "",
        "## Validations",
    ]
    for name in sorted(validations):
        lines.append(f"- `{name}`: `{validations[name]}`")
    lines.extend(["", "## Execution Evidence Cases"])
    for case in execution_cases:
        if not isinstance(case, dict):
            continue
        lines.append(
            "- "
            f"`{case.get('case_type', '')}` "
            f"status=`{case.get('status', '')}` "
            f"fill_ratio=`{case.get('fill_ratio', '')}` "
            f"slippage_bp=`{case.get('slippage_bp', '')}` "
            f"miss_reason=`{case.get('miss_reason_code', '')}`"
        )
    blockers = payload.get("blocking_reasons") or []
    lines.extend(["", "## Blocking Reasons"])
    if blockers:
        lines.extend([f"- `{reason}`" for reason in blockers])
    else:
        lines.append("- none")
    rollback = payload.get("rollback_context") if isinstance(payload.get("rollback_context"), dict) else {}
    reference = rollback.get("reference") if isinstance(rollback.get("reference"), dict) else {}
    lines.extend(
        [
            "",
            "## Rollback Reference",
            f"- available: `{rollback.get('available')}`",
            f"- release_id: `{reference.get('release_id', '')}`",
            f"- code_version: `{reference.get('code_version', '')}`",
            "",
        ]
    )
    return "\n".join(lines)


def build_release_dry_run_fixture(
    *,
    db_path: str | Path,
    code_root: str | Path,
    report_path: str | Path,
    payload_path: str | Path,
    operator_name: str = "dry_run",
    overwrite: bool = False,
) -> Dict[str, Any]:
    db = Path(db_path)
    if db.exists() and not overwrite:
        raise FileExistsError(f"fixture_db_exists:{db}")
    db.parent.mkdir(parents=True, exist_ok=True)
    if db.exists():
        db.unlink()
    conn = sqlite3.connect(str(db), timeout=20)
    try:
        fixture_ids = seed_release_dry_run_fixture(conn, operator_name=operator_name)
    finally:
        conn.close()

    payload = run_release_dry_run_audit(
        db_path=db,
        code_root=code_root,
        output_path=payload_path,
        operator_name=operator_name,
    )
    report = render_release_dry_run_report(payload, fixture_ids=fixture_ids)
    report_file = Path(report_path)
    report_file.parent.mkdir(parents=True, exist_ok=True)
    report_file.write_text(report, encoding="utf-8")
    return {
        "db_path": str(db),
        "payload_path": str(payload_path),
        "report_path": str(report_file),
        "fixture_ids": fixture_ids,
        "payload": payload,
    }
