from __future__ import annotations

from datetime import datetime
import sqlite3
from typing import Any, Dict, Optional, Tuple

import pandas as pd


def parse_yyyymmdd(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text[:10] if "-" in text or "/" in text else text[:8], fmt)
        except Exception:
            continue
    return None


def table_latest(conn: sqlite3.Connection, table: str, date_col: str = "trade_date") -> Dict[str, Any]:
    try:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        if not exists:
            return {"table": table, "latest": "", "rows": 0, "ok": False, "reason": "missing"}
        row = conn.execute(f"SELECT MAX({date_col}), COUNT(*) FROM {table}").fetchone()
        latest = str(row[0] or "") if row else ""
        rows = int(row[1] or 0) if row else 0
        return {"table": table, "latest": latest, "rows": rows, "ok": bool(latest), "reason": ""}
    except Exception as exc:
        return {"table": table, "latest": "", "rows": 0, "ok": False, "reason": str(exc)}


def data_freshness_snapshot(db_path: str) -> Dict[str, Any]:
    tables = [
        ("daily_trading_data", "trade_date", "行情"),
        ("moneyflow_daily", "trade_date", "资金流"),
        ("top_list", "trade_date", "龙虎榜"),
        ("margin_detail", "trade_date", "融资融券"),
        ("stock_concept_map", "updated_at", "概念映射"),
    ]
    out: Dict[str, Any] = {
        "ok": False,
        "mode": "research",
        "risk_level": "RED",
        "latest_trade_date": "",
        "days_old": 999,
        "tables": [],
        "warnings": [],
    }
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        for table, col, label in tables:
            item = table_latest(conn, table, col)
            item["label"] = label
            out["tables"].append(item)
        conn.close()
    except Exception as exc:
        out["warnings"].append(f"数据库不可用: {exc}")
        return out

    daily = next((x for x in out["tables"] if x.get("table") == "daily_trading_data"), {})
    latest = str(daily.get("latest", "") or "")
    out["latest_trade_date"] = latest
    dt = parse_yyyymmdd(latest)
    if dt:
        out["days_old"] = max(0, (datetime.now() - dt).days)
    else:
        out["warnings"].append("行情最新日期缺失")

    stale_tables = []
    for item in out["tables"]:
        if not item.get("ok"):
            stale_tables.append(str(item.get("label") or item.get("table")))
            continue
        if item.get("table") in {"daily_trading_data", "moneyflow_daily", "top_list", "margin_detail"}:
            item_dt = parse_yyyymmdd(item.get("latest"))
            if item_dt and (datetime.now() - item_dt).days > 7:
                stale_tables.append(str(item.get("label") or item.get("table")))

    if out["days_old"] <= 3 and not stale_tables:
        out.update({"ok": True, "mode": "production", "risk_level": "GREEN"})
    elif out["days_old"] <= 7:
        out.update({"ok": False, "mode": "review", "risk_level": "YELLOW"})
        out["warnings"].append("部分数据需要复核，生产建议需人工确认")
    else:
        out.update({"ok": False, "mode": "research", "risk_level": "RED"})
        out["warnings"].append("行情数据过期，系统降级为研究模式")

    if stale_tables:
        out["warnings"].append("需检查数据表: " + "、".join(sorted(set(stale_tables))))
    return out


def latest_candidate_snapshot(db_path: str, limit: int = 5) -> Tuple[pd.DataFrame, str]:
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        fact_exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='signal_runs'",
        ).fetchone()
        if fact_exists:
            latest_row = conn.execute(
                """
                SELECT run_id, trade_date
                FROM signal_runs
                WHERE run_type = 'scan' AND status = 'success'
                ORDER BY created_at DESC
                LIMIT 1
                """
            ).fetchone()
            latest_run_id = str((latest_row or ["", ""])[0] or "")
            latest_date = str((latest_row or ["", ""])[1] or "")
            if latest_run_id:
                raw_df = pd.read_sql_query(
                    """
                    SELECT
                        r.trade_date AS 信号日期,
                        r.strategy AS 策略,
                        i.ts_code AS TS代码,
                        COALESCE(b.name, i.ts_code) AS 股票名称,
                        COALESCE(b.industry, '') AS 行业,
                        i.score AS 评分,
                        i.rank_idx AS 排名,
                        i.reason_codes AS 理由
                    FROM signal_items i
                    JOIN signal_runs r ON r.run_id = i.run_id
                    LEFT JOIN stock_basic b ON b.ts_code = i.ts_code
                    WHERE i.run_id = ?
                    ORDER BY COALESCE(i.rank_idx, 999999), i.score DESC
                    LIMIT ?
                    """,
                    conn,
                    params=(latest_run_id, int(limit) * 3),
                )
                conn.close()
                if not raw_df.empty:
                    raw_df["理由"] = raw_df["理由"].fillna("").astype(str)
                    raw_df["理由"] = raw_df["理由"].str.replace('["', "", regex=False).str.replace('"]', "", regex=False)
                    raw_df["理由"] = raw_df["理由"].str.replace('","', "；", regex=False)
                    raw_df["评分"] = pd.to_numeric(raw_df["评分"], errors="coerce")
                    raw_df["排名"] = pd.to_numeric(raw_df["排名"], errors="coerce")
                    return _dedup_candidate_snapshot(raw_df, latest_date, limit)
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='strategy_signal_tracking'",
        ).fetchone()
        if not exists:
            conn.close()
            return pd.DataFrame(), ""
        latest_row = conn.execute(
            "SELECT MAX(signal_trade_date) FROM strategy_signal_tracking"
        ).fetchone()
        latest_date = str((latest_row or [""])[0] or "")
        if not latest_date:
            conn.close()
            return pd.DataFrame(), ""
        raw_df = pd.read_sql_query(
            """
            SELECT
                s.signal_trade_date AS 信号日期,
                s.strategy AS 策略,
                s.ts_code AS TS代码,
                COALESCE(b.name, s.ts_code) AS 股票名称,
                COALESCE(b.industry, '') AS 行业,
                s.score AS 评分,
                s.rank_idx AS 排名,
                s.reason AS 理由
            FROM strategy_signal_tracking s
            LEFT JOIN stock_basic b ON b.ts_code = s.ts_code
            WHERE s.signal_trade_date = ?
            ORDER BY COALESCE(s.rank_idx, 999999), s.score DESC
            LIMIT ?
            """,
            conn,
            params=(latest_date, int(limit)),
        )
        conn.close()
        if raw_df.empty:
            return raw_df, latest_date

        return _dedup_candidate_snapshot(raw_df, latest_date, limit)
    except Exception:
        return pd.DataFrame(), ""


def _dedup_candidate_snapshot(raw_df: pd.DataFrame, latest_date: str, limit: int) -> Tuple[pd.DataFrame, str]:
    raw_df["评分"] = pd.to_numeric(raw_df["评分"], errors="coerce")
    raw_df["排名"] = pd.to_numeric(raw_df["排名"], errors="coerce")
    raw_df["理由"] = raw_df["理由"].fillna("").astype(str)
    raw_df["策略"] = raw_df["策略"].fillna("").astype(str)

    dedup_rows = []
    for ts_code, group in raw_df.groupby("TS代码", sort=False):
        ranked = group.sort_values(by=["评分", "排名"], ascending=[False, True], na_position="last", kind="stable")
        best = ranked.iloc[0]
        strategy_values = [str(x).strip() for x in ranked["策略"].tolist() if str(x).strip()]
        strategy_values = list(dict.fromkeys(strategy_values))
        reason_values = [str(x).strip() for x in ranked["理由"].tolist() if str(x).strip()]
        reason_values = list(dict.fromkeys(reason_values))
        dedup_rows.append(
            {
                "信号日期": best["信号日期"],
                "TS代码": ts_code,
                "股票名称": best["股票名称"],
                "行业": best["行业"],
                "最高评分": best["评分"],
                "最佳排名": best["排名"],
                "主策略": strategy_values[0] if strategy_values else "",
                "贡献策略数": len(strategy_values),
                "贡献策略": " / ".join(strategy_values[:3]),
                "理由摘要": "；".join(reason_values[:2]),
            }
        )

    df = pd.DataFrame(dedup_rows).sort_values(
        by=["最高评分", "最佳排名", "TS代码"],
        ascending=[False, True, True],
        na_position="last",
        kind="stable",
    ).head(int(limit)).reset_index(drop=True)
    return df, latest_date
