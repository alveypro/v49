from __future__ import annotations

from datetime import datetime
import json
import sqlite3
import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

from openclaw.services.decision_service import record_decision_event, upsert_decision_snapshot
from openclaw.services.lineage_service import apply_professional_migrations, new_decision_id, new_release_id
from openclaw.services.release_event_service import record_release_event, record_release_validation


def _safe_gate_status(passed: Any) -> str:
    return "passed" if bool(passed) else "failed"


def _reason_codes_from_gate(gate_result: Dict[str, Any], *, fallback: Optional[List[str]] = None) -> List[str]:
    codes = list(gate_result.get("gates") or [])
    codes.extend(list(gate_result.get("blocking_gates") or []))
    codes.extend(list(gate_result.get("review_gates") or []))
    codes.extend(list(gate_result.get("canary_gates") or []))
    deduped = [str(code or "").strip() for code in codes if str(code or "").strip()]
    if deduped:
        return list(dict.fromkeys(deduped))
    return list(fallback or [])


def _parse_market_cap_yi(value: Any) -> float:
    text = str(value or "").strip().replace("亿元", "").replace("亿", "")
    if not text or text == "-":
        return 0.0
    try:
        return float(text)
    except Exception:
        return 0.0


def build_manual_scan_opportunities(candidate: Dict[str, Any]) -> List[Dict[str, Any]]:
    df = candidate.get("df")
    if not isinstance(df, pd.DataFrame) or df.empty:
        return []
    strategy = str(candidate.get("strategy") or "manual").strip().lower()
    score_col = str(candidate.get("score_col") or "综合评分")
    work = df.copy()
    code_col = "股票代码" if "股票代码" in work.columns else ("TS代码" if "TS代码" in work.columns else ("ts_code" if "ts_code" in work.columns else ""))
    name_col = "股票名称" if "股票名称" in work.columns else ("name" if "name" in work.columns else "")
    industry_col = "行业" if "行业" in work.columns else ("industry" if "industry" in work.columns else "")
    if not code_col:
        return []
    work["_ts_code"] = work[code_col].astype(str).str.strip()
    work["_stock_name"] = work[name_col].astype(str).str.strip() if name_col else ""
    work["_industry"] = work[industry_col].astype(str).str.strip() if industry_col else ""
    work["_score"] = pd.to_numeric(work.get(score_col), errors="coerce")
    if "排名" in work.columns:
        work["_rank"] = pd.to_numeric(work["排名"], errors="coerce")
    else:
        work["_rank"] = pd.Series(range(1, len(work) + 1), index=work.index, dtype=float)
    work["_circ_mv_yi"] = work["流通市值"].map(_parse_market_cap_yi) if "流通市值" in work.columns else 0.0
    work = work.dropna(subset=["_score"])
    work = work[work["_ts_code"] != ""].sort_values(by=["_score", "_rank"], ascending=[False, True], kind="stable").head(30)
    opportunities: List[Dict[str, Any]] = []
    for idx, row in enumerate(work.to_dict(orient="records"), 1):
        rank = int(float(row.get("_rank") or idx))
        target_weight = max(0.10, round(0.45 - (idx - 1) * 0.03, 2))
        reason = str(row.get("理由摘要") or row.get("筛选理由") or row.get("reason") or "").strip()
        opportunities.append(
            {
                "ts_code": str(row.get("_ts_code") or ""),
                "stock_name": str(row.get("_stock_name") or ""),
                "industry": str(row.get("_industry") or ""),
                "score": float(row.get("_score") or 0.0),
                "rank_idx": rank,
                "weighted_score": float(row.get("_score") or 0.0),
                "strategies": [strategy],
                "strategy": strategy,
                "target_weight": target_weight,
                "circ_mv": float(row.get("_circ_mv_yi") or 0.0) * 10000.0,
                "reason": reason,
            }
        )
    return opportunities


def _record_decision_and_release(
    conn: sqlite3.Connection,
    *,
    decision_type: str,
    decision_status: str,
    decision_date: str,
    selected_count: int,
    active_flag: bool,
    operator_name: str,
    approval_note: str,
    release_type: str,
    gate_result: Optional[Dict[str, Any]],
    payload: Optional[Dict[str, Any]],
    config_version: str = "",
) -> Tuple[str, str]:
    apply_professional_migrations(conn)
    gate_state = gate_result or {}
    decision_id = new_decision_id()
    record_decision_event(
        conn,
        decision_id=decision_id,
        decision_type=decision_type,
        based_on_run_id=str((payload or {}).get("based_on_run_id") or ""),
        risk_gate_state=gate_state.get("risk_gate_state") or {},
        release_gate_state=gate_state,
        approval_reason_codes=_reason_codes_from_gate(gate_state, fallback=["manual_override"] if decision_type == "approve" else []),
        approval_note=approval_note,
        operator_name=operator_name,
        decision_payload=payload or {},
    )
    upsert_decision_snapshot(
        conn,
        decision_id=decision_id,
        decision_status=decision_status,
        effective_trade_date=decision_date,
        selected_count=selected_count,
        active_flag=active_flag,
    )
    release_id = new_release_id()
    release_payload = dict(payload or {})
    release_payload["decision_id"] = decision_id
    record_release_event(
        conn,
        release_id=release_id,
        release_type=release_type,
        code_version=str((payload or {}).get("code_version") or ""),
        config_version=config_version,
        operator_name=operator_name,
        gate_result=gate_state,
        payload=release_payload,
    )
    record_release_validation(
        conn,
        release_id=release_id,
        validation_type="release_gate",
        validation_status=_safe_gate_status(gate_state.get("passed", decision_type != "archive")),
        validation_output_path=str((payload or {}).get("validation_output_path") or ""),
    )
    return decision_id, release_id


def publish_manual_scan_to_execution_queue(
    *,
    candidate: Dict[str, Any],
    runtime_snapshot: Dict[str, Any],
    connect_db: Callable[[], sqlite3.Connection],
    logs_dir: Any,
    clear_execution_queue_cache: Callable[[], None],
    clear_feedback_snapshot_cache: Callable[[], None],
) -> Tuple[bool, str, str]:
    opportunities = build_manual_scan_opportunities(candidate)
    if not opportunities:
        return False, "当前扫描结果无法解析为执行候选。请先完成一次有效扫描。", ""
    try:
        from openclaw.overnight_decision import (
            apply_feature_enrichment,
            apply_trade_window_analysis,
            build_overnight_decision,
            export_execution_feedback_template,
            load_active_holdings,
            load_return_calibration,
            next_trade_date,
            persist_overnight_decision,
            refresh_realized_outcomes,
            seed_execution_feedback,
        )

        strategy = str(candidate.get("strategy") or "manual").strip().lower()
        source_run_id = str(candidate.get("run_id") or candidate.get("source_run_id") or "").strip()
        batch_id = f"{datetime.now().strftime('%Y-%m-%d')}#manual#{uuid.uuid4().hex[:6]}"
        trade_date = next_trade_date(datetime.now().strftime("%Y-%m-%d"))
        risk_level = str(runtime_snapshot.get("risk_level") or "YELLOW").lower()
        conn = connect_db()
        try:
            opportunities = apply_feature_enrichment(conn, opportunities=opportunities)
            active_holdings = load_active_holdings(conn)
            calibration = load_return_calibration(conn, horizon_days=1, lookback_days=180, min_samples=6)
            payload = build_overnight_decision(
                trade_date=trade_date,
                opportunities=opportunities,
                active_holdings=active_holdings,
                risk={"risk_level": risk_level},
                calibration=calibration,
                top_n=2,
            )
            payload = apply_trade_window_analysis(conn, payload=payload, lookback_days=20)
            persist_overnight_decision(
                conn,
                decision_date=batch_id,
                payload=payload,
                source_type="manual_scan",
                source_run_id=source_run_id or batch_id,
                source_label=strategy,
                activate=True,
                approved_by="system",
                release_note=f"manual_scan auto publish:{strategy}",
                output_dir=logs_dir,
                current_primary=strategy,
            )
            seed_execution_feedback(
                conn,
                decision_date=batch_id,
                payload=payload,
                source_type="manual_scan",
                source_run_id=source_run_id or batch_id,
            )
            refresh_realized_outcomes(conn, lookback_days=60)
            export_execution_feedback_template(
                output_dir=logs_dir,
                decision_date=batch_id,
                payload=payload,
            )
            selected_count = len(payload.get("recommendations") or opportunities)
            decision_payload = {
                "decision_date": batch_id,
                "trade_date": trade_date,
                "strategy": strategy,
                "selected_count": selected_count,
                "based_on_run_id": source_run_id,
                "runtime_snapshot": runtime_snapshot,
                "source": "manual_scan",
            }
            _record_decision_and_release(
                conn,
                decision_type="approve",
                decision_status="active",
                decision_date=batch_id,
                selected_count=selected_count,
                active_flag=True,
                operator_name="system",
                approval_note=f"manual_scan auto publish:{strategy}",
                release_type="deploy",
                gate_result={"passed": True, "risk_gate_state": {"risk_level": risk_level}, "source": "manual_scan"},
                payload=decision_payload,
                config_version=strategy,
            )
        finally:
            conn.close()
        clear_execution_queue_cache()
        clear_feedback_snapshot_cache()
        return True, f"已将手动扫描发布为今日执行队列：source=manual_scan/{strategy}, batch={batch_id}", batch_id
    except Exception as exc:
        return False, f"发布手动扫描队列失败：{exc}", ""


def set_active_batch(
    *,
    decision_date: str,
    approved_by: str = "",
    release_note: str = "",
    rollback_reason: str = "",
    current_primary: str = "",
    override_gate: bool = False,
    override_reason: str = "",
    connect_db: Callable[[], sqlite3.Connection],
    logs_dir: Any,
    clear_execution_queue_cache: Callable[[], None],
    clear_execution_batches_cache: Callable[[], None],
) -> Tuple[bool, str]:
    try:
        conn = connect_db()
        from openclaw.overnight_decision import set_active_decision_batch

        result = set_active_decision_batch(
            conn,
            decision_date=str(decision_date or ""),
            approved_by=str(approved_by or "").strip(),
            release_note=str(release_note or "").strip(),
            rollback_reason=str(rollback_reason or "").strip(),
            output_dir=logs_dir,
            current_primary=str(current_primary or "").strip(),
            override_gate=bool(override_gate),
            override_reason=str(override_reason or "").strip(),
        )
        if result.get("ok"):
            payload = {
                "decision_date": str(decision_date or ""),
                "approved_by": str(approved_by or "").strip(),
                "release_note": str(release_note or "").strip(),
                "rollback_reason": str(rollback_reason or "").strip(),
                "current_primary": str(current_primary or "").strip(),
                "override_gate": bool(override_gate),
                "override_reason": str(override_reason or "").strip(),
                "legacy_result": result,
                "selected_count": int(result.get("selected_count") or 0),
                "validation_output_path": str(result.get("gate_output_path") or ""),
            }
            _record_decision_and_release(
                conn,
                decision_type="approve",
                decision_status="active",
                decision_date=str(decision_date or ""),
                selected_count=int(result.get("selected_count") or 0),
                active_flag=True,
                operator_name=str(approved_by or "").strip(),
                approval_note=str(release_note or rollback_reason or "").strip(),
                release_type="deploy",
                gate_result=result.get("gate_result") if isinstance(result.get("gate_result"), dict) else {"passed": True},
                payload=payload,
                config_version=str(current_primary or ""),
            )
        conn.close()
        clear_execution_queue_cache()
        clear_execution_batches_cache()
        if result.get("ok"):
            return True, f"已切换当前生效批次：{decision_date}"
        return False, str(result.get("message") or "切换失败")
    except Exception as exc:
        return False, f"切换生效批次失败：{exc}"


def set_canary_batch(
    *,
    decision_date: str,
    approved_by: str = "",
    release_note: str = "",
    current_primary: str = "",
    allowed_buckets: Optional[List[str]] = None,
    sample_limit: int = 2,
    window_start: str = "",
    window_end: str = "",
    connect_db: Callable[[], sqlite3.Connection],
    logs_dir: Any,
    clear_execution_queue_cache: Callable[[], None],
    clear_execution_batches_cache: Callable[[], None],
) -> Tuple[bool, str]:
    try:
        conn = connect_db()
        from openclaw.overnight_decision import set_canary_decision_batch

        result = set_canary_decision_batch(
            conn,
            decision_date=str(decision_date or ""),
            approved_by=str(approved_by or "").strip(),
            release_note=str(release_note or "").strip(),
            output_dir=logs_dir,
            current_primary=str(current_primary or "").strip(),
            allowed_buckets=list(allowed_buckets or ["direct_execute", "observe"]),
            sample_limit=int(sample_limit or 2),
            window_start=str(window_start or "").strip(),
            window_end=str(window_end or "").strip(),
        )
        if result.get("ok"):
            payload = {
                "decision_date": str(decision_date or ""),
                "approved_by": str(approved_by or "").strip(),
                "release_note": str(release_note or "").strip(),
                "current_primary": str(current_primary or "").strip(),
                "allowed_buckets": list(allowed_buckets or ["direct_execute", "observe"]),
                "sample_limit": int(sample_limit or 2),
                "window_start": str(window_start or "").strip(),
                "window_end": str(window_end or "").strip(),
                "legacy_result": result,
                "selected_count": int(result.get("selected_count") or 0),
                "validation_output_path": str(result.get("gate_output_path") or ""),
            }
            _record_decision_and_release(
                conn,
                decision_type="canary",
                decision_status="canary",
                decision_date=str(decision_date or ""),
                selected_count=int(result.get("selected_count") or 0),
                active_flag=False,
                operator_name=str(approved_by or "").strip(),
                approval_note=str(release_note or "").strip(),
                release_type="canary",
                gate_result=result.get("gate_result") if isinstance(result.get("gate_result"), dict) else {"passed": True},
                payload=payload,
                config_version=str(current_primary or ""),
            )
        conn.close()
        clear_execution_queue_cache()
        clear_execution_batches_cache()
        if result.get("ok"):
            return True, f"已设为灰度批次：{decision_date}"
        return False, str(result.get("message") or "灰度发布失败")
    except Exception as exc:
        return False, f"灰度发布失败：{exc}"


def archive_batch(
    *,
    decision_date: str,
    operator_name: str = "",
    archive_note: str = "",
    connect_db: Callable[[], sqlite3.Connection],
    clear_execution_queue_cache: Callable[[], None],
    clear_execution_batches_cache: Callable[[], None],
) -> Tuple[bool, str]:
    try:
        conn = connect_db()
        from openclaw.overnight_decision import archive_decision_batch

        result = archive_decision_batch(
            conn,
            decision_date=str(decision_date or ""),
            operator_name=str(operator_name or "").strip(),
            archive_note=str(archive_note or "").strip(),
        )
        if result.get("ok"):
            payload = {
                "decision_date": str(decision_date or ""),
                "archive_note": str(archive_note or "").strip(),
                "legacy_result": result,
            }
            _record_decision_and_release(
                conn,
                decision_type="rollback",
                decision_status="archived",
                decision_date=str(decision_date or ""),
                selected_count=int(result.get("selected_count") or 0),
                active_flag=False,
                operator_name=str(operator_name or "").strip(),
                approval_note=str(archive_note or "").strip(),
                release_type="rollback",
                gate_result={"passed": True, "summary": str(archive_note or "")},
                payload=payload,
            )
        conn.close()
        clear_execution_queue_cache()
        clear_execution_batches_cache()
        if result.get("ok"):
            return True, f"已归档批次：{decision_date}"
        return False, str(result.get("message") or "归档失败")
    except Exception as exc:
        return False, f"归档批次失败：{exc}"


def evaluate_batch_release_gate(
    *,
    decision_date: str,
    current_primary: str = "",
    connect_db: Callable[[], sqlite3.Connection],
    logs_dir: Any,
) -> Dict[str, Any]:
    try:
        conn = connect_db()
        from openclaw.overnight_decision import evaluate_decision_batch_release_gate

        result = evaluate_decision_batch_release_gate(
            conn,
            decision_date=str(decision_date or ""),
            output_dir=logs_dir,
            current_primary=str(current_primary or "").strip(),
        )
        conn.close()
        return result
    except Exception as exc:
        return {
            "decision_date": str(decision_date or ""),
            "passed": False,
            "summary": f"发布门禁校验失败：{exc}",
            "gates": ["gate_eval_error"],
            "metrics": {},
        }


def get_canary_scope(
    *,
    db_path: str,
    decision_date: str,
) -> Dict[str, Any]:
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        row = conn.execute(
            """
            SELECT allowed_buckets_json, sample_limit, window_start, window_end, selected_codes_json, operator_name, scope_note, updated_at
            FROM overnight_canary_scopes
            WHERE decision_date = ?
            """,
            (str(decision_date or ""),),
        ).fetchone()
        conn.close()
        if not row:
            return {}
        return {
            "allowed_buckets": json.loads(str(row[0] or "[]")),
            "sample_limit": int(row[1] or 0),
            "window_start": str(row[2] or ""),
            "window_end": str(row[3] or ""),
            "selected_codes": json.loads(str(row[4] or "[]")),
            "operator_name": str(row[5] or ""),
            "scope_note": str(row[6] or ""),
            "updated_at": str(row[7] or ""),
        }
    except Exception:
        return {}


def get_override_audits(
    *,
    db_path: str,
    decision_date: str,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        rows = conn.execute(
            """
            SELECT requested_decision, gate_decision, gate_summary, gate_codes_json, operator_name, override_reason, override_context_json, created_at
            FROM overnight_release_override_audit
            WHERE decision_date = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (str(decision_date or ""), int(limit)),
        ).fetchall()
        conn.close()
        return [
            {
                "requested_decision": str(r[0] or ""),
                "gate_decision": str(r[1] or ""),
                "gate_summary": str(r[2] or ""),
                "gate_codes": json.loads(str(r[3] or "[]")),
                "operator_name": str(r[4] or ""),
                "override_reason": str(r[5] or ""),
                "override_context": json.loads(str(r[6] or "{}")),
                "created_at": str(r[7] or ""),
            }
            for r in rows
        ]
    except Exception:
        return []


def get_release_outcome_review(
    *,
    db_path: str,
    decision_date: str,
    connect_db: Callable[[], sqlite3.Connection],
) -> Dict[str, Any]:
    try:
        conn = connect_db()
        from openclaw.overnight_decision import evaluate_release_batch_outcome

        review = evaluate_release_batch_outcome(conn, decision_date=str(decision_date or ""))
        conn.close()
        return review
    except Exception:
        return {}
