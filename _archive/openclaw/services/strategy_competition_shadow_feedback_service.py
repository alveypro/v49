from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from typing import Any, Dict, Iterable, List

from openclaw.services.execution_analytics_service import compute_execution_attribution, upsert_execution_attribution
from openclaw.services.execution_evidence_service import summarize_execution_evidence
from openclaw.services.execution_fill_service import record_execution_fill
from openclaw.services.execution_order_service import ALLOWED_ORDER_STATUSES, update_execution_order_status
from openclaw.services.lineage_service import apply_professional_migrations


JsonDict = Dict[str, Any]
TERMINAL_MISS_STATUSES = {"cancelled", "rejected", "expired", "manual_override"}
SHADOW_FEEDBACK_SOURCE_TYPES = {"shadow", "paper_broker", "broker_rehearsal", "rehearsal_broker"}


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _to_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _to_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _load_json(path: str | Path) -> JsonDict:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"json payload must be object: {path}")
    return payload


def _feedback_reports(payload: JsonDict) -> List[JsonDict]:
    reports = payload.get("reports")
    if not isinstance(reports, list):
        raise ValueError("shadow_feedback_reports_missing")
    return [dict(item) for item in reports if isinstance(item, dict)]


def _deterministic_shadow_fill_id(*, order_id: str, fill: JsonDict, idx: int) -> str:
    explicit = _clean_text(fill.get("fill_id"))
    if explicit:
        return explicit
    fill_ref = _clean_text(fill.get("fill_ref")) or _clean_text(fill.get("shadow_fill_ref")) or str(idx)
    digest = sha256(f"{order_id}|{fill_ref}".encode("utf-8")).hexdigest()[:18]
    return f"fill_shadow_{digest}"


def _plan_order_ids(plan: JsonDict) -> set[str]:
    orders = plan.get("orders") if isinstance(plan.get("orders"), list) else []
    return {_clean_text(item.get("order_id")) for item in orders if isinstance(item, dict) and _clean_text(item.get("order_id"))}


def _order_row(conn: sqlite3.Connection, order_id: str) -> JsonDict:
    row = conn.execute(
        """
        SELECT order_id, decision_id, ts_code, side, target_qty, decision_price, submitted_price,
               status, broker_ref, source_type
        FROM execution_orders
        WHERE order_id = ?
        """,
        (order_id,),
    ).fetchone()
    if not row:
        raise ValueError(f"shadow_order_not_found:{order_id}")
    return {
        "order_id": _clean_text(row[0]),
        "decision_id": _clean_text(row[1]),
        "ts_code": _clean_text(row[2]),
        "side": _clean_text(row[3]),
        "target_qty": _to_int(row[4]),
        "decision_price": _to_float(row[5]),
        "submitted_price": _to_float(row[6]),
        "status": _clean_text(row[7]),
        "broker_ref": _clean_text(row[8]),
        "source_type": _clean_text(row[9]),
    }


def _filled_quantity(fills: Iterable[JsonDict]) -> int:
    return sum(max(_to_int(fill.get("fill_qty")), 0) for fill in fills)


def _average_fill_price(fills: List[JsonDict]) -> float:
    qty = _filled_quantity(fills)
    if qty <= 0:
        return 0.0
    notional = sum(_to_float(fill.get("fill_price")) * max(_to_int(fill.get("fill_qty")), 0) for fill in fills)
    return notional / float(qty)


def _validate_feedback_report(*, report: JsonDict, plan_order_ids: set[str], existing: JsonDict) -> None:
    order_id = _clean_text(report.get("order_id"))
    if not order_id:
        raise ValueError("shadow_feedback_order_id_missing")
    if order_id not in plan_order_ids:
        raise ValueError(f"shadow_feedback_order_not_in_plan:{order_id}")
    ts_code = _clean_text(report.get("ts_code"))
    if ts_code and ts_code != existing["ts_code"]:
        raise ValueError(f"shadow_feedback_ts_code_mismatch:{order_id}:{ts_code}:{existing['ts_code']}")
    status = _clean_text(report.get("status")).lower()
    if status not in ALLOWED_ORDER_STATUSES:
        raise ValueError(f"unsupported_shadow_feedback_status:{status}")
    source_type = (_clean_text(report.get("source_type")) or "shadow").lower()
    if source_type not in SHADOW_FEEDBACK_SOURCE_TYPES:
        raise ValueError(f"unsupported_shadow_feedback_source_type:{source_type}")
    fills = [dict(item) for item in report.get("fills") or [] if isinstance(item, dict)]
    if status in {"filled", "partial_fill"} and not fills:
        raise ValueError(f"missing_shadow_fills:{order_id}")
    if status in TERMINAL_MISS_STATUSES and not _clean_text(report.get("miss_reason_code")):
        raise ValueError(f"missing_shadow_miss_reason:{order_id}")
    if status == "manual_override" and not _clean_text(report.get("cancel_reason")):
        raise ValueError(f"missing_shadow_manual_override_reason:{order_id}")
    if _to_float(report.get("close_price")) <= 0:
        raise ValueError(f"missing_shadow_close_price:{order_id}")


def _record_one_shadow_feedback(conn: sqlite3.Connection, *, report: JsonDict, plan_order_ids: set[str]) -> JsonDict:
    order_id = _clean_text(report.get("order_id"))
    existing = _order_row(conn, order_id)
    _validate_feedback_report(report=report, plan_order_ids=plan_order_ids, existing=existing)

    status = _clean_text(report.get("status")).lower()
    source_type = (_clean_text(report.get("source_type")) or "shadow").lower()
    broker_ref = _clean_text(report.get("broker_ref")) or f"shadow_feedback:{order_id}"
    miss_reason = _clean_text(report.get("miss_reason_code"))
    cancel_reason = _clean_text(report.get("cancel_reason")) or miss_reason
    fills = [dict(item) for item in report.get("fills") or [] if isinstance(item, dict)]

    update_execution_order_status(
        conn,
        order_id=order_id,
        status=status,
        cancel_reason=cancel_reason,
        broker_ref=broker_ref,
    )
    # Keep source_type explicit for shadow evidence; update_execution_order_status preserves legacy source refs only.
    conn.execute(
        "UPDATE execution_orders SET source_type = ?, updated_at = ? WHERE order_id = ?",
        (source_type, _now_text(), order_id),
    )
    conn.commit()

    for idx, fill in enumerate(fills, start=1):
        record_execution_fill(
            conn,
            fill_id=_deterministic_shadow_fill_id(order_id=order_id, fill=fill, idx=idx),
            order_id=order_id,
            fill_price=_to_float(fill.get("fill_price")),
            fill_qty=_to_int(fill.get("fill_qty")),
            fill_time=_clean_text(fill.get("fill_time")),
            fill_fee=_to_float(fill.get("fill_fee")),
            fill_slippage_bp=_to_float(fill.get("fill_slippage_bp")),
            venue=_clean_text(fill.get("venue")) or "shadow",
        )

    filled_qty = _filled_quantity(fills)
    attribution = compute_execution_attribution(
        decision_price=_to_float(report.get("decision_price")) or existing["decision_price"],
        submit_price=_to_float(report.get("submitted_price")) or existing["submitted_price"],
        avg_fill_price=_average_fill_price(fills),
        close_price=_to_float(report.get("close_price")),
        target_qty=_to_int(report.get("target_qty")) or existing["target_qty"],
        filled_qty=filled_qty,
        delay_sec=_to_float(report.get("delay_sec")),
        miss_reason_code=miss_reason,
    )
    upsert_execution_attribution(conn, order_id=order_id, attribution=attribution)
    return {
        "order_id": order_id,
        "ts_code": existing["ts_code"],
        "status": status,
        "source_type": source_type,
        "fill_count": len(fills),
        "filled_qty": filled_qty,
        "miss_reason_code": miss_reason,
    }


def build_strategy_competition_shadow_execution_evidence(
    conn: sqlite3.Connection,
    *,
    shadow_plan_artifact_path: str,
    output_dir: str | Path,
    shadow_feedback_artifact_path: str = "",
    operator_name: str = "strategy_competition_shadow_feedback",
    minimum_source_tier: str = "simulated",
) -> JsonDict:
    """Record shadow feedback when supplied and emit a shadow execution evidence artifact.

    The function intentionally does not invent fills, misses, or attribution. Without a
    feedback artifact it only summarizes the existing pending orders and remains blocked.
    """

    apply_professional_migrations(conn)
    plan_path = str(shadow_plan_artifact_path or "")
    plan = _load_json(plan_path)
    if plan.get("artifact_version") != "strategy_competition_shadow_execution_plan.v1":
        raise ValueError("invalid_shadow_execution_plan_artifact")
    plan_order_ids = _plan_order_ids(plan)
    decision_id = _clean_text(plan.get("decision_id"))
    if not decision_id or not plan_order_ids:
        raise ValueError("shadow_execution_plan_has_no_orders")

    feedback_path = str(shadow_feedback_artifact_path or "")
    recorded_reports: List[JsonDict] = []
    if feedback_path:
        feedback = _load_json(feedback_path)
        if feedback.get("artifact_version") not in {None, "", "strategy_competition_shadow_execution_feedback.v1"}:
            raise ValueError("invalid_shadow_feedback_artifact_version")
        for report in _feedback_reports(feedback):
            recorded_reports.append(_record_one_shadow_feedback(conn, report=report, plan_order_ids=plan_order_ids))

    evidence = summarize_execution_evidence(
        conn,
        decision_ids=[decision_id],
        minimum_source_tier=minimum_source_tier,
    )
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    competition_run_id = _clean_text(plan.get("competition_run_id"))
    payload: JsonDict = {
        "artifact_version": "strategy_competition_shadow_execution_evidence.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": competition_run_id,
        "source_plan_artifact": plan_path,
        "source_feedback_artifact": feedback_path,
        "plan_run_id": _clean_text(plan.get("plan_run_id")),
        "decision_id": decision_id,
        "status": "shadow_execution_passed" if evidence.get("passed") is True else "shadow_execution_blocked",
        "passed": evidence.get("passed") is True,
        "production_candidate_allowed": False,
        "recorded_report_count": len(recorded_reports),
        "recorded_reports": recorded_reports,
        "shadow_execution": {
            "passed": evidence.get("passed") is True,
            "sample_count": int(evidence.get("total_orders") or 0),
            "mode": "shadow",
            "artifact": "",
            "decision_id": decision_id,
            "plan_run_id": _clean_text(plan.get("plan_run_id")),
            "minimum_source_tier": minimum_source_tier,
            "source_tier_counts": dict(evidence.get("source_tier_counts") or {}),
            "status_counts": dict(evidence.get("status_counts") or {}),
            "miss_reason_counts": dict(evidence.get("miss_reason_counts") or {}),
            "linked_run_ids": list(evidence.get("linked_run_ids") or []),
            "blocking_reasons": list(evidence.get("blocking_reasons") or []),
            "cases": list(evidence.get("cases") or []),
        },
        "execution_evidence": evidence,
        "hard_boundaries": [
            "shadow_feedback_must_reference_existing_plan_order",
            "filled_or_partial_shadow_orders_require_fills",
            "terminal_shadow_misses_require_miss_reason",
            "shadow_evidence_does_not_replace_independent_validation",
            "production_candidate_allowed_remains_false_until_competition_audit_passes_all_gates",
        ],
    }
    json_path = output / f"strategy_competition_shadow_execution_evidence_{competition_run_id or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(json_path)
    payload["shadow_execution"]["artifact"] = str(json_path)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload
