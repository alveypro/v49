from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from uuid import uuid4

from openclaw.paths import project_root
from openclaw.services.data_version_service import build_code_version, build_data_version, build_param_version


MIGRATION_FILES = (
    "scripts/migrations/001_lineage.sql",
    "scripts/migrations/002_decision.sql",
    "scripts/migrations/003_execution.sql",
    "scripts/migrations/004_release.sql",
)


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _date_parts() -> tuple[str, str]:
    now = datetime.now()
    return now.strftime("%Y%m%d"), now.strftime("%H%M%S")


def _short_uuid() -> str:
    return uuid4().hex[:8]


def canonical_json(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def new_run_id(run_type: str, strategy: str) -> str:
    ymd, hms = _date_parts()
    return f"run_{str(run_type or 'unknown').lower()}_{str(strategy or 'unknown').lower()}_{ymd}_{hms}_{_short_uuid()}"


def new_decision_id() -> str:
    ymd, hms = _date_parts()
    return f"dec_{ymd}_{hms}_{_short_uuid()}"


def new_order_id() -> str:
    ymd, hms = _date_parts()
    return f"ord_{ymd}_{hms}_{_short_uuid()}"


def new_fill_id() -> str:
    ymd, hms = _date_parts()
    return f"fill_{ymd}_{hms}_{_short_uuid()}"


def new_release_id() -> str:
    ymd, hms = _date_parts()
    return f"rel_{ymd}_{hms}_{_short_uuid()}"


def apply_professional_migrations(conn: sqlite3.Connection, *, root: Optional[Path] = None) -> None:
    base = root or project_root()
    for rel in MIGRATION_FILES:
        sql = (base / rel).read_text(encoding="utf-8")
        conn.executescript(sql)
    conn.commit()


def insert_signal_run(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    run_type: str,
    strategy: str,
    trade_date: str = "",
    data_version: str = "",
    code_version: str = "",
    param_version: str = "",
    parent_run_id: str = "",
    status: str = "created",
    artifact_path: str = "",
    summary: Optional[Dict[str, Any]] = None,
) -> str:
    conn.execute(
        """
        INSERT OR REPLACE INTO signal_runs (
            run_id, run_type, strategy, trade_date, data_version, code_version,
            param_version, parent_run_id, status, artifact_path, summary_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            str(run_type or "").lower(),
            str(strategy or "").lower(),
            str(trade_date or ""),
            str(data_version or ""),
            str(code_version or ""),
            str(param_version or ""),
            str(parent_run_id or ""),
            str(status or "created").lower(),
            str(artifact_path or ""),
            canonical_json(summary or {}),
            _now_text(),
        ),
    )
    conn.commit()
    return run_id


def replace_signal_items(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    items: Iterable[Dict[str, Any]],
) -> int:
    conn.execute("DELETE FROM signal_items WHERE run_id = ?", (str(run_id or ""),))
    rows: List[tuple[Any, ...]] = []
    for idx, item in enumerate(items or [], start=1):
        rows.append(
            (
                str(run_id or ""),
                str(item.get("ts_code", "") or ""),
                float(item.get("score", 0.0) or 0.0),
                int(item.get("rank_idx", idx) or idx),
                canonical_json(item.get("reason_codes", [])),
                canonical_json(item),
                _now_text(),
            )
        )
    if rows:
        conn.executemany(
            """
            INSERT INTO signal_items (
                run_id, ts_code, score, rank_idx, reason_codes, raw_payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    conn.commit()
    return len(rows)


def dataframe_to_signal_items(
    df: Any,
    *,
    strategy: str,
    score_col: str = "",
    limit: int = 300,
) -> List[Dict[str, Any]]:
    if df is None or not hasattr(df, "empty") or bool(getattr(df, "empty")):
        return []
    columns = list(getattr(df, "columns", []))
    code_col = next((c for c in ("股票代码", "TS代码", "ts_code", "code") if c in columns), "")
    if not code_col:
        return []
    resolved_score_col = score_col if score_col in columns else next(
        (c for c in ("综合评分", "最终评分", "总分", "评分", "final_score", "score") if c in columns),
        "",
    )
    rank_col = next((c for c in ("排名", "rank_idx", "rank") if c in columns), "")
    reason_col = next((c for c in ("理由摘要", "筛选理由", "推荐理由", "reason") if c in columns), "")
    work = df.copy()
    if resolved_score_col:
        try:
            work = work.sort_values(resolved_score_col, ascending=False, kind="stable")
        except Exception:
            pass
    items: List[Dict[str, Any]] = []
    for idx, row in enumerate(work.head(max(1, int(limit))).to_dict(orient="records"), start=1):
        ts_code = str(row.get(code_col) or "").strip()
        if not ts_code:
            continue
        try:
            score = float(row.get(resolved_score_col, 0.0) or 0.0) if resolved_score_col else 0.0
        except Exception:
            score = 0.0
        try:
            rank_idx = int(float(row.get(rank_col, idx) or idx)) if rank_col else idx
        except Exception:
            rank_idx = idx
        reason = str(row.get(reason_col, "") or "").strip() if reason_col else ""
        item = {
            "ts_code": ts_code,
            "score": score,
            "rank_idx": rank_idx,
            "strategy": str(strategy or "").lower(),
            "reason_codes": [reason] if reason else [],
        }
        item.update({k: v for k, v in row.items() if k not in item})
        items.append(item)
    return items


def record_signal_dataframe_chain(
    *,
    connect_db: Any,
    code_root: Path,
    run_id: str,
    strategy: str,
    params: Dict[str, Any],
    score_col: str,
    result_df: Any,
    meta: Dict[str, Any],
    result_csv: str,
    meta_json: str,
    row_count: int,
) -> None:
    conn = connect_db()
    try:
        apply_professional_migrations(conn)
        insert_signal_run(
            conn,
            run_id=run_id,
            run_type="scan",
            strategy=strategy,
            trade_date=str((meta or {}).get("trade_date") or (params or {}).get("trade_date") or ""),
            data_version=build_data_version(conn),
            code_version=build_code_version(root=code_root),
            param_version=build_param_version(params or {}),
            parent_run_id=str((params or {}).get("parent_run_id") or ""),
            status="success",
            artifact_path=str(result_csv or meta_json or ""),
            summary={
                "params": params or {},
                "meta": meta or {},
                "row_count": int(row_count or 0),
                "score_col": score_col,
                "result_csv": result_csv,
                "meta_json": meta_json,
            },
        )
        replace_signal_items(
            conn,
            run_id=run_id,
            items=dataframe_to_signal_items(result_df, strategy=strategy, score_col=score_col),
        )
    finally:
        conn.close()


def record_backtest_result_chain(
    *,
    connect_db: Any,
    code_root: Path,
    run_id: str,
    job_kind: str,
    payload: Dict[str, Any],
    result: Dict[str, Any],
) -> None:
    conn = connect_db()
    try:
        apply_professional_migrations(conn)
        strategy = str(payload.get("strategy") or job_kind or "backtest").strip().lower()
        trade_date = str(payload.get("date_to") or payload.get("trade_date") or "")
        summary = {
            "job_kind": str(job_kind or ""),
            "payload": payload or {},
            "result": result or {},
        }
        insert_signal_run(
            conn,
            run_id=run_id,
            run_type="backtest",
            strategy=strategy,
            trade_date=trade_date,
            data_version=build_data_version(conn),
            code_version=build_code_version(root=code_root),
            param_version=build_param_version(payload or {}),
            parent_run_id=str((payload or {}).get("parent_run_id") or ""),
            status="success" if bool((result or {}).get("success")) else "failed",
            artifact_path=str((payload or {}).get("artifact_path") or ""),
            summary=summary,
        )
    finally:
        conn.close()
