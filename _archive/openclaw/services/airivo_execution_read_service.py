from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Tuple

import pandas as pd


def feedback_snapshot(db_path: str) -> Dict[str, Any]:
    out = {
        "feedback_rows": 0,
        "outcome_rows": 0,
        "latest_decision_date": "",
        "latest_trade_date": "",
        "pending_rows": 0,
        "done_rows": 0,
        "skipped_rows": 0,
        "cancelled_rows": 0,
        "system_suggested_rows": 0,
        "human_override_rows": 0,
        "missing_override_reason_rows": 0,
        "execution_rate": None,
    }
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        for table, key in [("overnight_execution_feedback", "feedback_rows"), ("overnight_realized_outcomes", "outcome_rows")]:
            exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
            if exists:
                out[key] = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] or 0)
        exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='overnight_execution_feedback'").fetchone()
        if exists:
            latest = conn.execute("SELECT MAX(decision_date) FROM overnight_execution_feedback").fetchone()
            out["latest_decision_date"] = str((latest or [""])[0] or "")
            latest_trade = conn.execute("SELECT MAX(trade_date) FROM overnight_execution_feedback").fetchone()
            out["latest_trade_date"] = str((latest_trade or [""])[0] or "")
            total = int(conn.execute("SELECT COUNT(*) FROM overnight_execution_feedback").fetchone()[0] or 0)
            done = int(conn.execute("SELECT COUNT(*) FROM overnight_execution_feedback WHERE execution_status IN ('done', 'executed', 'filled')").fetchone()[0] or 0)
            skipped = int(conn.execute("SELECT COUNT(*) FROM overnight_execution_feedback WHERE execution_status='skipped'").fetchone()[0] or 0)
            cancelled = int(conn.execute("SELECT COUNT(*) FROM overnight_execution_feedback WHERE execution_status='cancelled'").fetchone()[0] or 0)
            pending = int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM overnight_execution_feedback
                    WHERE execution_status = 'pending'
                       OR final_action = 'pending'
                       OR COALESCE(final_action, '') = ''
                    """
                ).fetchone()[0]
                or 0
            )
            out["done_rows"] = done
            out["skipped_rows"] = skipped
            out["cancelled_rows"] = cancelled
            out["pending_rows"] = pending
            try:
                out["system_suggested_rows"] = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM overnight_execution_feedback WHERE COALESCE(system_suggested_action,'') <> ''"
                    ).fetchone()[0]
                    or 0
                )
                out["human_override_rows"] = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM overnight_execution_feedback
                        WHERE COALESCE(system_suggested_action,'') <> ''
                          AND final_action NOT IN ('', 'pending')
                          AND final_action <> system_suggested_action
                        """
                    ).fetchone()[0]
                    or 0
                )
                out["missing_override_reason_rows"] = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM overnight_execution_feedback
                        WHERE COALESCE(system_suggested_action,'') <> ''
                          AND final_action NOT IN ('', 'pending')
                          AND final_action <> system_suggested_action
                          AND COALESCE(human_override_reason,'') = ''
                        """
                    ).fetchone()[0]
                    or 0
                )
            except Exception:
                pass
            out["execution_rate"] = round(done / total * 100.0, 2) if total else None
        conn.close()
    except Exception:
        pass
    return out


def latest_execution_queue(db_path: str, limit: int = 80) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    meta: Dict[str, Any] = {"decision_date": "", "trade_date": "", "risk_level": "", "selected_count": 0, "source_type": "", "source_label": "", "is_active": 0}
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        try:
            from openclaw.overnight_decision import ensure_tables

            ensure_tables(conn)
        except Exception:
            pass
        latest = conn.execute(
            """
            SELECT decision_date, trade_date, risk_level, selected_count, source_type, source_label, is_active, release_status, approved_by, approved_at
            FROM overnight_decision_runs
            ORDER BY is_active DESC, created_at DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
        if not latest:
            conn.close()
            return pd.DataFrame(), meta
        meta = {
            "decision_date": str(latest[0] or ""),
            "trade_date": str(latest[1] or ""),
            "risk_level": str(latest[2] or ""),
            "selected_count": int(latest[3] or 0),
            "source_type": str(latest[4] or ""),
            "source_label": str(latest[5] or ""),
            "is_active": int(latest[6] or 0),
            "release_status": str(latest[7] or ""),
            "approved_by": str(latest[8] or ""),
            "approved_at": str(latest[9] or ""),
        }
        df = pd.read_sql_query(
            """
            SELECT
                ef.id,
                ef.decision_date,
                ef.trade_date,
                ef.ts_code,
                COALESCE(ef.stock_name, r.stock_name, '') AS stock_name,
                COALESCE(r.strategy, '') AS strategy,
                COALESCE(r.rank_idx, 999999) AS rank_idx,
                ef.planned_action,
                ef.system_suggested_action,
                ef.system_confidence,
                ef.system_reason,
                ef.decision_bucket,
                ef.decision_gate_reason,
                ef.needs_manual_review,
                ef.final_action,
                ef.execution_status,
                ef.execution_note,
                ef.switched_from_ts_code
            FROM overnight_execution_feedback ef
            LEFT JOIN overnight_recommendations r
              ON r.decision_date = ef.decision_date
             AND r.ts_code = ef.ts_code
            WHERE ef.decision_date = ?
            ORDER BY
              CASE ef.decision_bucket
                WHEN 'manual_review' THEN 1
                WHEN 'direct_execute' THEN 2
                WHEN 'observe' THEN 3
                WHEN 'auto_reject' THEN 4
                ELSE 9
              END,
              COALESCE(r.rank_idx, 999999),
              ef.ts_code
            LIMIT ?
            """,
            conn,
            params=(meta["decision_date"], int(limit)),
        )
        conn.close()
        return df, meta
    except Exception:
        return pd.DataFrame(), meta


def recent_execution_batches(db_path: str, limit: int = 3) -> pd.DataFrame:
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        try:
            from openclaw.overnight_decision import ensure_tables

            ensure_tables(conn)
        except Exception:
            pass
        df = pd.read_sql_query(
            """
            SELECT
                decision_date,
                trade_date,
                source_type,
                source_label,
                risk_level,
                selected_count,
                release_status,
                release_note,
                rollback_reason,
                approved_by,
                approved_at,
                replaces_decision_date,
                replaced_by_decision_date,
                is_active,
                activated_at,
                created_at
            FROM overnight_decision_runs
            ORDER BY is_active DESC, created_at DESC, id DESC
            LIMIT ?
            """,
            conn,
            params=(int(limit),),
        )
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


def load_queue_batch_rows(db_path: str, decision_date: str) -> pd.DataFrame:
    if not str(decision_date or "").strip():
        return pd.DataFrame()
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        df = pd.read_sql_query(
            """
            SELECT
                ef.ts_code,
                COALESCE(ef.stock_name, r.stock_name, '') AS stock_name,
                COALESCE(r.strategy, '') AS strategy,
                ef.decision_bucket,
                ef.system_suggested_action,
                ef.system_confidence,
                ef.decision_gate_reason
            FROM overnight_execution_feedback ef
            LEFT JOIN overnight_recommendations r
              ON r.decision_date = ef.decision_date
             AND r.ts_code = ef.ts_code
            WHERE ef.decision_date = ?
            """,
            conn,
            params=(str(decision_date or ""),),
        )
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


def compare_queue_batches(current_df: pd.DataFrame, previous_df: pd.DataFrame) -> pd.DataFrame:
    if current_df is None:
        current_df = pd.DataFrame()
    if previous_df is None:
        previous_df = pd.DataFrame()
    curr = current_df.copy()
    prev = previous_df.copy()
    if curr.empty and prev.empty:
        return pd.DataFrame()
    curr_map = {}
    prev_map = {}
    for row in curr.to_dict(orient="records"):
        curr_map[str(row.get("ts_code") or "")] = row
    for row in prev.to_dict(orient="records"):
        prev_map[str(row.get("ts_code") or "")] = row
    codes = sorted(set(curr_map) | set(prev_map))
    diff_rows: List[Dict[str, Any]] = []
    for code in codes:
        c = curr_map.get(code)
        p = prev_map.get(code)
        if c and not p:
            change = "新增"
        elif p and not c:
            change = "移除"
        else:
            curr_bucket = str((c or {}).get("decision_bucket") or "")
            prev_bucket = str((p or {}).get("decision_bucket") or "")
            if curr_bucket == prev_bucket:
                continue
            order = {"manual_review": 1, "direct_execute": 2, "observe": 3, "auto_reject": 4, "closed": 9}
            change = "升级" if order.get(curr_bucket, 9) < order.get(prev_bucket, 9) else "降级"
        ref = c or p or {}
        diff_rows.append(
            {
                "代码": code,
                "名称": str(ref.get("stock_name") or ""),
                "策略": str(ref.get("strategy") or ""),
                "变化": change,
                "当前队列": str((c or {}).get("decision_bucket") or "-"),
                "上一批队列": str((p or {}).get("decision_bucket") or "-"),
            }
        )
    return pd.DataFrame(diff_rows)


def load_feedback_rows(db_path: str, status_filter: str = "pending", limit: int = 50) -> pd.DataFrame:
    expected_columns = [
        "id",
        "decision_date",
        "trade_date",
        "ts_code",
        "stock_name",
        "planned_action",
        "final_action",
        "execution_status",
        "execution_note",
        "operator_name",
        "switched_from_ts_code",
        "system_suggested_action",
        "system_suggested_status",
        "system_confidence",
        "system_reason",
        "decision_bucket",
        "decision_gate_reason",
        "needs_manual_review",
        "human_override",
        "human_override_reason",
        "updated_at",
    ]
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        try:
            from openclaw.overnight_decision import ensure_tables

            ensure_tables(conn)
        except Exception:
            pass
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='overnight_execution_feedback'"
        ).fetchone()
        if not exists:
            conn.close()
            return pd.DataFrame(columns=expected_columns)
        where_sql = ""
        if status_filter == "pending":
            where_sql = """
            WHERE execution_status = 'pending'
               OR final_action = 'pending'
               OR COALESCE(final_action, '') = ''
            """
        elif status_filter in {"done", "skipped", "cancelled"}:
            where_sql = "WHERE execution_status = ?"
        params: Tuple[Any, ...] = (status_filter,) if status_filter in {"done", "skipped", "cancelled"} else tuple()
        df = pd.read_sql_query(
            f"""
            SELECT
                id,
                decision_date,
                trade_date,
                ts_code,
                stock_name,
                planned_action,
                final_action,
                execution_status,
                execution_note,
                operator_name,
                switched_from_ts_code,
                system_suggested_action,
                system_suggested_status,
                system_confidence,
                system_reason,
                decision_bucket,
                decision_gate_reason,
                needs_manual_review,
                human_override,
                human_override_reason,
                updated_at
            FROM overnight_execution_feedback
            {where_sql}
            ORDER BY trade_date DESC, decision_date DESC, id ASC
            LIMIT ?
            """,
            conn,
            params=params + (int(limit),),
        )
        conn.close()
        return df.reindex(columns=expected_columns)
    except Exception:
        return pd.DataFrame(columns=expected_columns)


def parse_confidence(value: Any) -> float:
    try:
        text = str(value or "").strip().replace("%", "")
        if not text:
            return 0.0
        num = float(text)
        if num > 1.0:
            num = num / 100.0
        return max(0.0, min(1.0, num))
    except Exception:
        return 0.0


def bucket_feedback_rows(rows: pd.DataFrame) -> pd.DataFrame:
    if rows is None or rows.empty:
        return pd.DataFrame(
            columns=[
                "id",
                "decision_date",
                "trade_date",
                "ts_code",
                "stock_name",
                "planned_action",
                "final_action",
                "execution_status",
                "execution_note",
                "operator_name",
                "switched_from_ts_code",
                "system_suggested_action",
                "system_suggested_status",
                "system_confidence",
                "system_reason",
                "decision_bucket",
                "decision_bucket_label",
                "decision_gate_reason",
                "needs_manual_review",
                "human_override",
                "human_override_reason",
                "updated_at",
                "review_priority",
            ]
        )
    out = rows.copy()
    if "decision_bucket" in out.columns and out["decision_bucket"].fillna("").astype(str).str.strip().ne("").any():
        label_map = {
            "manual_review": "需要人工确认",
            "direct_execute": "系统直接执行",
            "observe": "系统观察等待",
            "auto_reject": "系统自动淘汰",
            "closed": "已完成闭环",
        }
        out["decision_bucket"] = out["decision_bucket"].fillna("").astype(str)
        out["decision_bucket_label"] = out["decision_bucket"].map(label_map).fillna("需要人工确认")
        if "decision_gate_reason" not in out.columns:
            out["decision_gate_reason"] = ""
        if "needs_manual_review" not in out.columns:
            out["needs_manual_review"] = out["decision_bucket"].eq("manual_review").astype(int)
        out["review_priority"] = out["decision_bucket"].map(
            {"manual_review": 1, "direct_execute": 2, "observe": 3, "auto_reject": 4, "closed": 9}
        ).fillna(5)
        return out.sort_values(
            by=["review_priority", "trade_date", "decision_date", "id"],
            ascending=[True, False, False, True],
            kind="stable",
        )
    buckets: List[str] = []
    labels: List[str] = []
    reasons: List[str] = []
    priorities: List[int] = []
    for row in out.to_dict(orient="records"):
        final_action = str(row.get("final_action") or "").strip().lower()
        execution_status = str(row.get("execution_status") or "").strip().lower()
        system_action = str(row.get("system_suggested_action") or "").strip().lower()
        system_reason = str(row.get("system_reason") or "").strip()
        override_reason = str(row.get("human_override_reason") or "").strip()
        planned_action = str(row.get("planned_action") or "").strip().lower()
        confidence = parse_confidence(row.get("system_confidence"))
        switched_from = str(row.get("switched_from_ts_code") or "").strip()
        done_status = execution_status in {"done", "executed", "filled", "skipped", "cancelled"}

        bucket = "manual_review"
        reason = "系统尚未替代人工判断"
        priority = 1
        if done_status:
            bucket = "closed"
            reason = "已完成真实执行记录"
            priority = 9
        elif not system_action:
            bucket = "manual_review"
            reason = "缺少系统建议，必须人工判断"
            priority = 1
        elif bool(row.get("human_override")) and not override_reason:
            bucket = "manual_review"
            reason = "人工覆盖未填写原因，不能进入评估闭环"
            priority = 1
        elif switched_from:
            bucket = "manual_review"
            reason = "涉及换仓/持仓冲突，优先人工确认"
            priority = 1
        elif system_action in {"buy", "switch"} and confidence >= 0.75:
            bucket = "direct_execute"
            reason = "高置信度生产建议，可直接进入执行清单"
            priority = 2
        elif system_action in {"watch", "hold"} and confidence >= 0.55:
            bucket = "observe"
            reason = "系统建议观察，不占用人工逐条决策"
            priority = 3
        elif system_action in {"skip", "cancelled"}:
            bucket = "auto_reject"
            reason = "系统建议自动淘汰"
            priority = 4
        elif planned_action and system_action and planned_action != system_action and confidence < 0.55:
            bucket = "manual_review"
            reason = "计划动作与系统建议分歧较大，需要人工确认"
            priority = 1
        elif confidence >= 0.6 and system_action in {"buy", "watch", "hold"}:
            bucket = "observe"
            reason = "中等置信度，先进入观察队列"
            priority = 3
        else:
            bucket = "manual_review"
            reason = system_reason or "系统置信度不足，保留人工判断"
            priority = 1

        label = {
            "manual_review": "需要人工确认",
            "direct_execute": "系统直接执行",
            "observe": "系统观察等待",
            "auto_reject": "系统自动淘汰",
            "closed": "已完成闭环",
        }.get(bucket, "需要人工确认")
        if not reason and system_reason:
            reason = system_reason
        buckets.append(bucket)
        labels.append(label)
        reasons.append(reason)
        priorities.append(priority)

    out["decision_bucket"] = buckets
    out["decision_bucket_label"] = labels
    out["decision_gate_reason"] = reasons
    out["review_priority"] = priorities
    return out.sort_values(
        by=["review_priority", "trade_date", "decision_date", "id"],
        ascending=[True, False, False, True],
        kind="stable",
    )


def feedback_bucket_summary(rows: pd.DataFrame) -> Dict[str, int]:
    counts = {
        "manual_review": 0,
        "direct_execute": 0,
        "observe": 0,
        "auto_reject": 0,
        "closed": 0,
    }
    if rows is None or rows.empty or "decision_bucket" not in rows.columns:
        return counts
    raw = rows["decision_bucket"].value_counts(dropna=False).to_dict()
    for key in counts:
        counts[key] = int(raw.get(key, 0) or 0)
    return counts
