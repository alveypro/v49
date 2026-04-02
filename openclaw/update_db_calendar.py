#!/usr/bin/env python3
"""Update A-share daily data using trade-calendar aware rules.

Rules:
- Use SSE trade calendar (`trade_cal`) as source of truth.
- On trade day, data is considered expected only after `close_hour + delay_hours`.
- On non-trade day, expected date is the last open date.
- Backfill missing open dates between DB latest and expected date.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import tushare as ts

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data.dao import resolve_db_path

CONFIG_PATH = ROOT / "config.json"
TOKEN_PATH = ROOT / "tushare_token.txt"


@dataclass
class CalendarContext:
    expected_trade_date: str
    is_today_trade_day: bool
    is_ready_window: bool
    now_cn: str
    ready_time_cn: str


def _load_token() -> Optional[str]:
    env = (os.getenv("TUSHARE_TOKEN") or "").strip()
    if env:
        return env
    if TOKEN_PATH.exists():
        t = TOKEN_PATH.read_text(encoding="utf-8").strip()
        if t:
            return t
    if CONFIG_PATH.exists():
        try:
            cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
            t = (cfg.get("TUSHARE_TOKEN") or "").strip()
            if t:
                return t
        except Exception:
            pass
    return None


def _cn_now() -> datetime:
    try:
        from zoneinfo import ZoneInfo

        return datetime.now(ZoneInfo("Asia/Shanghai"))
    except Exception:
        return datetime.now()


def _to_yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def _parse_trade_date(s: str) -> datetime:
    s = str(s)
    if "-" in s:
        return datetime.strptime(s[:10], "%Y-%m-%d")
    return datetime.strptime(s[:8], "%Y%m%d")


def _db_latest_trade_date(conn: sqlite3.Connection) -> Optional[str]:
    cur = conn.cursor()
    cur.execute("SELECT MAX(trade_date) FROM daily_trading_data")
    row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return str(row[0]).replace("-", "")[:8]


def _trade_open_dates(pro, start_date: str, end_date: str) -> List[str]:
    df = pro.trade_cal(exchange="SSE", start_date=start_date, end_date=end_date, is_open="1")
    if df is None or df.empty:
        return []
    vals = [str(x) for x in df["cal_date"].tolist()]
    vals.sort()
    return vals


def _build_calendar_context(pro, close_hour: int, delay_hours: int) -> CalendarContext:
    now = _cn_now()
    today = _to_yyyymmdd(now)

    cal = pro.trade_cal(exchange="SSE", start_date=today, end_date=today)
    is_today_trade_day = bool(not cal.empty and str(cal["is_open"].iloc[0]) == "1")

    ready_time = now.replace(hour=close_hour + delay_hours, minute=0, second=0, microsecond=0)
    is_ready_window = now >= ready_time

    recent_open = _trade_open_dates(
        pro,
        start_date=_to_yyyymmdd(now - timedelta(days=30)),
        end_date=today,
    )
    expected_trade_date = recent_open[-1] if recent_open else today

    # Trade day but before ready window: expected date should still be previous open day.
    if is_today_trade_day and not is_ready_window and len(recent_open) >= 2:
        expected_trade_date = recent_open[-2]

    return CalendarContext(
        expected_trade_date=expected_trade_date,
        is_today_trade_day=is_today_trade_day,
        is_ready_window=is_ready_window,
        now_cn=now.strftime("%Y-%m-%d %H:%M:%S"),
        ready_time_cn=ready_time.strftime("%Y-%m-%d %H:%M:%S"),
    )


def _normalize_daily_table(df_daily: pd.DataFrame, df_basic: Optional[pd.DataFrame]) -> pd.DataFrame:
    if df_daily is None or df_daily.empty:
        return pd.DataFrame()

    d = df_daily.copy()
    d = d.rename(
        columns={
            "open": "open_price",
            "high": "high_price",
            "low": "low_price",
            "close": "close_price",
            "change": "change_amount",
        }
    )

    if df_basic is not None and not df_basic.empty:
        keep_cols = ["ts_code", "turnover_rate", "pe", "pb"]
        b = df_basic[[c for c in keep_cols if c in df_basic.columns]].copy()
        d = d.merge(b, on="ts_code", how="left")

    required = [
        "ts_code",
        "trade_date",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "pre_close",
        "change_amount",
        "pct_chg",
        "vol",
        "amount",
        "turnover_rate",
        "volume_ratio",
        "pe",
        "pb",
    ]
    for c in required:
        if c not in d.columns:
            d[c] = None

    d["trade_date"] = d["trade_date"].astype(str).str.replace("-", "").str[:8]
    return d[required]


def _upsert_trade_date(conn: sqlite3.Connection, trade_date: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    cur = conn.cursor()
    cur.execute("DELETE FROM daily_trading_data WHERE trade_date = ?", (trade_date,))
    rows = [
        tuple(x)
        for x in df[
            [
                "ts_code",
                "trade_date",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "pre_close",
                "change_amount",
                "pct_chg",
                "vol",
                "amount",
                "turnover_rate",
                "volume_ratio",
                "pe",
                "pb",
            ]
        ].itertuples(index=False, name=None)
    ]
    cur.executemany(
        """
        INSERT INTO daily_trading_data
        (ts_code, trade_date, open_price, high_price, low_price, close_price, pre_close,
         change_amount, pct_chg, vol, amount, turnover_rate, volume_ratio, pe, pb)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    return len(rows)


def run_update(
    db_path: Path,
    close_hour: int,
    delay_hours: int,
    max_backfill_days: int,
    dry_run: bool,
) -> Dict[str, object]:
    token = _load_token()
    if not token:
        return {"ok": False, "status": "no_token", "message": "missing Tushare token"}

    pro = ts.pro_api(token)
    ctx = _build_calendar_context(pro, close_hour=close_hour, delay_hours=delay_hours)

    conn = sqlite3.connect(str(db_path))
    db_latest = _db_latest_trade_date(conn)
    conn.close()

    start_probe = (_cn_now() - timedelta(days=max(90, max_backfill_days * 4))).strftime("%Y%m%d")
    all_open_dates = _trade_open_dates(pro, start_date=start_probe, end_date=ctx.expected_trade_date)
    if not all_open_dates:
        return {
            "ok": True,
            "status": "no_open_dates",
            "db_latest": db_latest,
            "expected_trade_date": ctx.expected_trade_date,
            "calendar": ctx.__dict__,
        }

    if db_latest:
        missing = [d for d in all_open_dates if d > db_latest]
    else:
        missing = all_open_dates[-max_backfill_days:]

    # Hard cap to avoid accidental huge backfill in daily task.
    if len(missing) > max_backfill_days:
        missing = missing[-max_backfill_days:]

    if not missing:
        return {
            "ok": True,
            "status": "up_to_date",
            "db_latest": db_latest,
            "expected_trade_date": ctx.expected_trade_date,
            "calendar": ctx.__dict__,
            "updated_dates": [],
            "updated_rows": 0,
        }

    if dry_run:
        return {
            "ok": True,
            "status": "dry_run",
            "db_latest": db_latest,
            "expected_trade_date": ctx.expected_trade_date,
            "calendar": ctx.__dict__,
            "pending_dates": missing,
        }

    conn = sqlite3.connect(str(db_path))
    updated_dates: List[str] = []
    updated_rows = 0
    failed: Dict[str, str] = {}

    for d in missing:
        try:
            df_daily = pro.daily(trade_date=d)
            df_basic = pro.daily_basic(
                trade_date=d,
                fields="ts_code,turnover_rate,pe,pb",
            )
            merged = _normalize_daily_table(df_daily, df_basic)
            rows = _upsert_trade_date(conn, d, merged)
            if rows > 0:
                updated_dates.append(d)
                updated_rows += rows
            else:
                failed[d] = "empty_daily"
        except Exception as e:
            failed[d] = str(e)

    conn.close()
    return {
        "ok": True,
        "status": "updated",
        "db_latest": db_latest,
        "expected_trade_date": ctx.expected_trade_date,
        "calendar": ctx.__dict__,
        "updated_dates": updated_dates,
        "updated_rows": updated_rows,
        "failed_dates": failed,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Trade-calendar aware DB updater for A-share daily data.")
    parser.add_argument("--db-path", default=None, help="Optional DB path override.")
    parser.add_argument("--close-hour", type=int, default=int(os.getenv("OPENCLAW_TRADE_CLOSE_HOUR", "15")))
    parser.add_argument("--delay-hours", type=int, default=int(os.getenv("OPENCLAW_DATA_READY_DELAY_HOURS", "2")))
    parser.add_argument(
        "--max-backfill-days",
        type=int,
        default=int(os.getenv("OPENCLAW_AUTO_UPDATE_MAX_DAYS", "15")),
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    try:
        db_path = Path(args.db_path).resolve() if args.db_path else resolve_db_path()
    except Exception as e:
        print(json.dumps({"ok": False, "status": "no_db", "error": str(e)}, ensure_ascii=False))
        return 2

    try:
        result = run_update(
            db_path=db_path,
            close_hour=args.close_hour,
            delay_hours=args.delay_hours,
            max_backfill_days=args.max_backfill_days,
            dry_run=args.dry_run,
        )
    except Exception as e:
        result = {"ok": False, "status": "updater_exception", "error": str(e)}
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
