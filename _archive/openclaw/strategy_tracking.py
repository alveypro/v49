from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from data.dao import db_conn, latest_trade_date, resolve_db_path


BENCHMARK_TS_CODE = "000001.SH"
DEFAULT_HORIZONS = (1, 5, 10, 20)
PRODUCTION_STRATEGIES = ("v9", "v8", "v5", "combo")
SCOREBOARD_CORE_HORIZONS = (1, 5)


def _norm_date(s: Any) -> str:
    v = str(s or "").strip()
    if not v:
        return ""
    if "-" in v:
        return v[:10].replace("-", "")
    return v[:8]


def ensure_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_signal_tracking (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            strategy TEXT NOT NULL,
            signal_trade_date TEXT NOT NULL,
            ts_code TEXT NOT NULL,
            score REAL DEFAULT 0,
            rank_idx INTEGER DEFAULT 0,
            target_weight REAL DEFAULT 0,
            reason TEXT DEFAULT '',
            source TEXT DEFAULT 'oc_daily',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(run_id, strategy, ts_code)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_signal_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id INTEGER NOT NULL,
            strategy TEXT NOT NULL,
            ts_code TEXT NOT NULL,
            signal_trade_date TEXT NOT NULL,
            horizon_days INTEGER NOT NULL,
            entry_trade_date TEXT NOT NULL,
            exit_trade_date TEXT NOT NULL,
            entry_price REAL NOT NULL,
            exit_price REAL NOT NULL,
            ret_pct REAL NOT NULL,
            bench_ret_pct REAL,
            excess_ret_pct REAL,
            evaluated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(signal_id, horizon_days)
        )
        """
    )
    conn.commit()


def _extract_picks(summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    picks: List[Dict[str, Any]] = []
    scan_picks = (((summary.get("scan") or {}).get("result") or {}).get("picks") or [])
    if isinstance(scan_picks, list) and scan_picks:
        picks = [x for x in scan_picks if isinstance(x, dict) and x.get("ts_code")]
    if not picks:
        opp = summary.get("opportunities") or []
        if isinstance(opp, list):
            picks = [x for x in opp if isinstance(x, dict) and x.get("ts_code")]
    return picks


def record_signals_from_summary(
    run_summary_path: str | Path,
    db_path: Optional[str] = None,
    source: str = "oc_daily",
) -> Dict[str, Any]:
    p = Path(run_summary_path)
    if not p.exists():
        return {"ok": False, "error": f"missing_run_summary:{p}"}

    summary = json.loads(p.read_text(encoding="utf-8"))
    strategy = str(summary.get("strategy") or "unknown")
    run_id = str(((summary.get("scan") or {}).get("run_id")) or p.stem)
    picks = _extract_picks(summary)
    if not picks:
        return {"ok": True, "inserted": 0, "duplicates": 0, "reason": "no_picks"}

    resolved_db = str(resolve_db_path(db_path)) if db_path else str(resolve_db_path())
    with db_conn(resolved_db) as conn:
        ensure_tables(conn)
        td = _norm_date(latest_trade_date(conn) or "")
        if not td:
            td = datetime.now().strftime("%Y%m%d")
        inserted = 0
        candidates = 0
        for idx, item in enumerate(picks, 1):
            ts_code = str(item.get("ts_code") or "").strip()
            if not ts_code:
                continue
            candidates += 1
            score = float(item.get("score") or 0.0)
            target_weight = float(item.get("target_weight") or 0.0)
            reason = str(item.get("reason") or "")
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO strategy_signal_tracking
                (run_id, strategy, signal_trade_date, ts_code, score, rank_idx, target_weight, reason, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (run_id, strategy, td, ts_code, score, idx, target_weight, reason, source),
            )
            inserted += int(cur.rowcount or 0)
        conn.commit()
    return {
        "ok": True,
        "inserted": inserted,
        "duplicates": max(0, candidates - inserted),
        "strategy": strategy,
        "run_id": run_id,
    }


def _load_price_map(conn: sqlite3.Connection, ts_codes: Iterable[str]) -> Dict[str, pd.DataFrame]:
    code_list = sorted({c for c in ts_codes if c})
    if BENCHMARK_TS_CODE not in code_list:
        code_list.append(BENCHMARK_TS_CODE)
    ph = ",".join(["?"] * len(code_list))
    df = pd.read_sql_query(
        f"""
        SELECT ts_code, trade_date, close_price
        FROM daily_trading_data
        WHERE ts_code IN ({ph})
        ORDER BY ts_code, trade_date
        """,
        conn,
        params=code_list,
    )
    out: Dict[str, pd.DataFrame] = {}
    for code, g in df.groupby("ts_code"):
        gg = g.copy()
        gg["trade_date"] = gg["trade_date"].astype(str).str.replace("-", "").str[:8]
        gg = gg.dropna(subset=["close_price"])
        out[str(code)] = gg.reset_index(drop=True)
    return out


def _entry_exit(df: pd.DataFrame, signal_date: str, horizon: int) -> Optional[Tuple[str, float, str, float]]:
    if df is None or df.empty:
        return None
    pos = df.index[df["trade_date"] >= signal_date].tolist()
    if not pos:
        return None
    i0 = int(pos[0])
    i1 = i0 + int(horizon)
    if i1 >= len(df):
        return None
    d0 = str(df.iloc[i0]["trade_date"])
    d1 = str(df.iloc[i1]["trade_date"])
    p0 = float(df.iloc[i0]["close_price"])
    p1 = float(df.iloc[i1]["close_price"])
    if p0 <= 0:
        return None
    return d0, p0, d1, p1


def refresh_performance(
    db_path: Optional[str] = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    lookback_days: int = 240,
) -> Dict[str, Any]:
    resolved_db = str(resolve_db_path(db_path)) if db_path else str(resolve_db_path())
    cutoff = (datetime.now() - timedelta(days=int(lookback_days))).strftime("%Y%m%d")
    with db_conn(resolved_db) as conn:
        ensure_tables(conn)
        sig = pd.read_sql_query(
            """
            SELECT id, strategy, ts_code, signal_trade_date
            FROM strategy_signal_tracking
            WHERE signal_trade_date >= ?
            ORDER BY id DESC
            """,
            conn,
            params=(cutoff,),
        )
        if sig.empty:
            return {"ok": True, "evaluated": 0, "inserted": 0, "reason": "no_signals"}

        perf_exist = pd.read_sql_query(
            "SELECT signal_id, horizon_days FROM strategy_signal_performance",
            conn,
        )
        done = {(int(r.signal_id), int(r.horizon_days)) for r in perf_exist.itertuples(index=False)}
        price_map = _load_price_map(conn, sig["ts_code"].tolist())
        bench = price_map.get(BENCHMARK_TS_CODE)

        attempted = 0
        inserted = 0
        evaluated = 0
        skipped_existing = 0
        skipped_no_price = 0
        for r in sig.itertuples(index=False):
            s_id = int(r.id)
            ts_code = str(r.ts_code)
            sdate = _norm_date(r.signal_trade_date)
            s_df = price_map.get(ts_code)
            for h in horizons:
                h = int(h)
                if (s_id, h) in done:
                    skipped_existing += 1
                    continue
                ee = _entry_exit(s_df, sdate, h)
                if not ee:
                    skipped_no_price += 1
                    continue
                attempted += 1
                entry_d, entry_p, exit_d, exit_p = ee
                ret = (exit_p / entry_p - 1.0) * 100.0
                bench_ret = None
                excess = None
                if bench is not None and not bench.empty:
                    b0 = bench[bench["trade_date"] == entry_d]
                    b1 = bench[bench["trade_date"] == exit_d]
                    if not b0.empty and not b1.empty:
                        bp0 = float(b0.iloc[0]["close_price"])
                        bp1 = float(b1.iloc[0]["close_price"])
                        if bp0 > 0:
                            bench_ret = (bp1 / bp0 - 1.0) * 100.0
                            excess = ret - bench_ret

                cur = conn.execute(
                    """
                    INSERT OR IGNORE INTO strategy_signal_performance
                    (signal_id, strategy, ts_code, signal_trade_date, horizon_days,
                     entry_trade_date, exit_trade_date, entry_price, exit_price,
                     ret_pct, bench_ret_pct, excess_ret_pct)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        s_id,
                        str(r.strategy),
                        ts_code,
                        sdate,
                        h,
                        entry_d,
                        exit_d,
                        entry_p,
                        exit_p,
                        ret,
                        bench_ret,
                        excess,
                    ),
                )
                rc = int(cur.rowcount or 0)
                inserted += rc
                if rc > 0:
                    evaluated += 1
        conn.commit()
    return {
        "ok": True,
        "evaluated": evaluated,
        "attempted": attempted,
        "inserted": inserted,
        "skipped_existing": skipped_existing,
        "skipped_no_price": skipped_no_price,
    }


def generate_scoreboard(
    db_path: Optional[str] = None,
    output_dir: str | Path = "logs/openclaw",
    lookback_days: int = 120,
) -> Dict[str, Any]:
    resolved_db = str(resolve_db_path(db_path)) if db_path else str(resolve_db_path())
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    cutoff = (datetime.now() - timedelta(days=int(lookback_days))).strftime("%Y%m%d")

    with db_conn(resolved_db) as conn:
        df = pd.read_sql_query(
            """
            SELECT strategy, ts_code, signal_trade_date, horizon_days, ret_pct, bench_ret_pct, excess_ret_pct
            FROM strategy_signal_performance
            WHERE signal_trade_date >= ?
            """,
            conn,
            params=(cutoff,),
        )
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = out_dir / f"strategy_scoreboard_{ts}.csv"
    md_path = out_dir / f"strategy_scoreboard_{ts}.md"
    if df.empty:
        rows = []
        for st in PRODUCTION_STRATEGIES:
            for h in SCOREBOARD_CORE_HORIZONS:
                rows.append(
                    {
                        "strategy": st,
                        "horizon_days": int(h),
                        "samples": 0,
                        "win_rate_pct": None,
                        "avg_ret_pct": None,
                        "median_ret_pct": None,
                        "avg_excess_ret_pct": None,
                    }
                )
        pd.DataFrame(rows).to_csv(csv_path, index=False, encoding="utf-8-sig")
        lines = [
            f"# 策略评分看板（{ts}）",
            "",
            f"- 回看天数: {lookback_days}",
            f"- 行数: {len(rows)}",
            "- 说明: 当前回看窗口内暂无已结算绩效样本（生产策略已补齐占位行）",
            "",
            "| 策略 | 期限(天) | 样本数 | 胜率(%) | 平均收益(%) | 中位收益(%) | 平均超额收益(%) |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
        for r in rows:
            lines.append(f"| {r['strategy']} | {r['horizon_days']} | 0 |  |  |  |  |")
        md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return {
            "ok": True,
            "reason": "no_performance_rows",
            "csv": str(csv_path),
            "markdown": str(md_path),
            "rows": 0,
        }

    rows: List[Dict[str, Any]] = []
    for (strategy, horizon), g in df.groupby(["strategy", "horizon_days"]):
        n = int(len(g))
        win = float((g["ret_pct"] > 0).mean() * 100.0) if n else 0.0
        avg_ret = float(g["ret_pct"].mean()) if n else 0.0
        med_ret = float(g["ret_pct"].median()) if n else 0.0
        avg_excess = float(g["excess_ret_pct"].dropna().mean()) if g["excess_ret_pct"].notna().any() else None
        rows.append(
            {
                "strategy": strategy,
                "horizon_days": int(horizon),
                "samples": n,
                "win_rate_pct": round(win, 2),
                "avg_ret_pct": round(avg_ret, 3),
                "median_ret_pct": round(med_ret, 3),
                "avg_excess_ret_pct": round(avg_excess, 3) if avg_excess is not None else None,
            }
        )

    existing_pairs = {(str(r.get("strategy")), int(r.get("horizon_days", 0))) for r in rows}
    for st in PRODUCTION_STRATEGIES:
        for h in SCOREBOARD_CORE_HORIZONS:
            key = (st, int(h))
            if key in existing_pairs:
                continue
            rows.append(
                {
                    "strategy": st,
                    "horizon_days": int(h),
                    "samples": 0,
                    "win_rate_pct": None,
                    "avg_ret_pct": None,
                    "median_ret_pct": None,
                    "avg_excess_ret_pct": None,
                }
            )

    score_df = pd.DataFrame(rows).sort_values(["horizon_days", "samples", "avg_excess_ret_pct", "avg_ret_pct"], ascending=[True, False, False, False])
    score_df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    lines = [
        f"# 策略评分看板（{ts}）",
        "",
        f"- 回看天数: {lookback_days}",
        f"- 行数: {len(score_df)}",
        "",
        "| 策略 | 期限(天) | 样本数 | 胜率(%) | 平均收益(%) | 中位收益(%) | 平均超额收益(%) |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    def _fmt(v: Any) -> str:
        if pd.isna(v):
            return ""
        return str(v)
    for r in score_df.itertuples(index=False):
        lines.append(
            f"| {r.strategy} | {int(r.horizon_days)} | {int(r.samples)} | {_fmt(r.win_rate_pct)} | {_fmt(r.avg_ret_pct)} | {_fmt(r.median_ret_pct)} | {_fmt(r.avg_excess_ret_pct)} |"
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    return {"ok": True, "csv": str(csv_path), "markdown": str(md_path), "rows": int(len(score_df))}
