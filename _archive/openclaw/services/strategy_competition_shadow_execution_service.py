from __future__ import annotations

from datetime import datetime
import json
import math
from pathlib import Path
import sqlite3
from typing import Any, Dict, List

from openclaw.services.decision_service import record_decision_event, upsert_decision_snapshot
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


JsonDict = Dict[str, Any]


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _load_json(path: str | Path) -> JsonDict:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("competition_audit_payload_not_object")
    return payload


def _round_lot_qty(*, order_value: float, price: float, lot_size: int = 100) -> int:
    if float(price or 0.0) <= 0 or float(order_value or 0.0) <= 0:
        return 0
    raw = int(math.floor(float(order_value) / float(price)))
    if raw < int(lot_size or 1):
        return raw
    return int(math.floor(raw / int(lot_size or 100)) * int(lot_size or 100))


def build_strategy_competition_shadow_execution_plan(
    conn: sqlite3.Connection,
    *,
    competition_audit_artifact_path: str,
    output_dir: str | Path,
    operator_name: str = "strategy_competition_shadow_execution_plan",
) -> JsonDict:
    """Create traceable shadow orders from a blocked competition audit.

    This service creates a plan and pending shadow orders only. It does not
    create fills or attribution, so the resulting shadow execution evidence
    remains blocked until actual shadow execution feedback is recorded.
    """

    apply_professional_migrations(conn)
    audit_path = str(competition_audit_artifact_path or "")
    audit = _load_json(audit_path)
    competition_run_id = str(audit.get("competition_run_id") or "")
    top5 = [dict(item) for item in audit.get("top5_portfolio_audit") or [] if isinstance(item, dict)]
    pre_trade = audit.get("pre_trade_risk_controls") if isinstance(audit.get("pre_trade_risk_controls"), dict) else {}
    blocking: List[str] = []
    if str(audit.get("artifact_version") or "") != "strategy_competition_portfolio_audit.v1":
        blocking.append("invalid_competition_audit_artifact")
    if not competition_run_id:
        blocking.append("competition_run_id_missing")
    if len(top5) != 5:
        blocking.append(f"top5_count_invalid:{len(top5)}")
    if pre_trade.get("passed") is not True:
        blocking.append("pre_trade_controls_not_passed")
    if audit.get("production_candidate_allowed") is True:
        blocking.append("competition_audit_already_claimed_production")

    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    plan_run_id = new_run_id("shadow_execution_plan", "strategy_competition_top5")
    decision_id = new_decision_id()
    order_rows: List[JsonDict] = []

    if not blocking:
        insert_signal_run(
            conn,
            run_id=plan_run_id,
            run_type="shadow_execution_plan",
            strategy="strategy_competition_top5",
            trade_date=str(audit.get("trade_date") or ""),
            data_version="competition_audit:" + str(audit.get("ranking_method_hash") or ""),
            code_version="competition_audit:" + str(audit.get("competition_run_id") or ""),
            param_version="competition_audit:" + str(audit.get("ranking_method_hash") or ""),
            status="created",
            artifact_path=audit_path,
            summary={
                "source": "strategy_competition_shadow_execution_plan",
                "competition_run_id": competition_run_id,
                "pre_trade_risk_controls": pre_trade,
            },
        )
        replace_signal_items(
            conn,
            run_id=plan_run_id,
            items=[
                {
                    "ts_code": str(item.get("ts_code") or ""),
                    "score": float(((item.get("source") or {}).get("final_stock_score") or 0.0)),
                    "rank_idx": idx,
                    "reason_codes": ["strategy_competition_top5_shadow_plan"],
                    "source_signal_refs": (item.get("source") or {}).get("signal_refs") or [],
                }
                for idx, item in enumerate(top5, start=1)
            ],
        )
        record_decision_event(
            conn,
            decision_id=decision_id,
            decision_type="strategy_competition_shadow_execution_plan",
            based_on_run_id=plan_run_id,
            risk_gate_state={
                "passed": False,
                "state": "shadow_execution_plan_only",
                "production_candidate_allowed": False,
                "pre_trade_risk_controls_passed": pre_trade.get("passed") is True,
            },
            release_gate_state={
                "passed": False,
                "state": "shadow_execution_pending",
                "production_release_allowed": False,
            },
            approval_reason_codes=["shadow_execution_plan_created", "not_production_approval"],
            approval_note="Plan-only shadow execution orders; fills and attribution must be collected separately.",
            operator_name=operator_name,
            decision_payload={
                "competition_run_id": competition_run_id,
                "competition_audit_artifact": audit_path,
                "top5_symbols": [str(item.get("ts_code") or "") for item in top5],
            },
        )
        upsert_decision_snapshot(
            conn,
            decision_id=decision_id,
            decision_status="shadow_pending",
            effective_trade_date=str(audit.get("trade_date") or ""),
            selected_count=len(top5),
            active_flag=False,
        )
        for item in top5:
            ts_code = str(item.get("ts_code") or "")
            risk = item.get("risk") if isinstance(item.get("risk"), dict) else {}
            pre_order = _pre_trade_order_for_symbol(pre_trade, ts_code)
            latest = pre_order.get("latest_facts") if isinstance(pre_order.get("latest_facts"), dict) else {}
            order_value = float(pre_order.get("estimated_order_value") or 0.0)
            price = float(latest.get("close_price") or risk.get("close_price") or 0.0)
            qty = _round_lot_qty(order_value=order_value, price=price)
            order_id = new_order_id()
            create_execution_order(
                conn,
                order_id=order_id,
                decision_id=decision_id,
                ts_code=ts_code,
                side="buy",
                target_qty=qty,
                decision_price=price,
                submitted_price=price,
                status="created",
                broker_ref=f"shadow_plan:{competition_run_id}:{ts_code}",
                source_type="shadow",
            )
            order_rows.append(
                {
                    "order_id": order_id,
                    "decision_id": decision_id,
                    "ts_code": ts_code,
                    "side": "buy",
                    "target_qty": qty,
                    "decision_price": price,
                    "estimated_order_value": order_value,
                    "status": "created",
                    "source_type": "shadow",
                    "next_required_action": "record_shadow_execution_fill_or_terminal_miss_with_attribution",
                }
            )

    evidence = summarize_execution_evidence(conn, decision_ids=[decision_id]) if order_rows else {
        "passed": False,
        "blocking_reasons": blocking or ["shadow_execution_plan_not_created"],
        "total_orders": 0,
        "cases": [],
        "linked_run_ids": [],
    }
    shadow_execution = {
        "passed": evidence.get("passed") is True,
        "sample_count": int(evidence.get("total_orders") or 0),
        "mode": "shadow",
        "artifact": "",
        "decision_id": decision_id if order_rows else "",
        "plan_run_id": plan_run_id if order_rows else "",
        "linked_run_ids": list(evidence.get("linked_run_ids") or []),
        "blocking_reasons": list(evidence.get("blocking_reasons") or []),
        "cases": list(evidence.get("cases") or []),
    }
    payload: JsonDict = {
        "artifact_version": "strategy_competition_shadow_execution_plan.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": competition_run_id,
        "competition_audit_artifact": audit_path,
        "plan_status": "shadow_execution_pending" if order_rows else "shadow_execution_plan_blocked",
        "passed": False,
        "production_candidate_allowed": False,
        "blocking_reasons": blocking,
        "plan_run_id": plan_run_id if order_rows else "",
        "decision_id": decision_id if order_rows else "",
        "orders": order_rows,
        "shadow_execution": shadow_execution,
        "hard_boundaries": [
            "shadow_execution_plan_is_not_shadow_execution_pass",
            "no_fills_or_attribution_are_created_by_this_plan",
            "production_requires_evidence_summary_passed_and_independent_validation",
        ],
    }
    json_path = output / f"strategy_competition_shadow_execution_plan_{competition_run_id or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(json_path)
    payload["shadow_execution"]["artifact"] = str(json_path)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def _pre_trade_order_for_symbol(pre_trade: JsonDict, ts_code: str) -> JsonDict:
    for item in pre_trade.get("orders") or []:
        if isinstance(item, dict) and str(item.get("ts_code") or "") == str(ts_code or ""):
            return item
    return {}
