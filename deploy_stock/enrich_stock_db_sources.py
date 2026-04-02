#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import sqlite3
import time
from typing import Any, List, Optional, Tuple


def _load_tushare_token() -> Optional[str]:
    token = (os.getenv("TUSHARE_TOKEN") or "").strip()
    if token:
        return token
    cands = [
        "/root/.tushare_token",
        "/opt/openclaw/.tushare_token",
        "/opt/airivo/app/.tushare_token",
    ]
    for p in cands:
        if not os.path.exists(p):
            continue
        try:
            with open(p, "r", encoding="utf-8") as f:
                t = f.read().strip()
                if t:
                    return t
        except Exception:
            continue
    return None


def _build_tushare_pro() -> Optional[Any]:
    token = _load_tushare_token()
    if not token:
        return None
    try:
        import tushare as ts

        ts.set_token(token)
        return ts.pro_api(token)
    except Exception:
        return None


def ensure_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS valuation_daily (
            ts_code TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            pe_ttm REAL,
            pb REAL,
            total_mv REAL,
            circ_mv REAL,
            turnover_rate REAL,
            source TEXT,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (ts_code, trade_date)
        );

        CREATE TABLE IF NOT EXISTS fina_indicator_ext (
            ts_code TEXT NOT NULL,
            end_date TEXT NOT NULL,
            ann_date TEXT,
            roe REAL,
            netprofit_margin REAL,
            grossprofit_margin REAL,
            or_yoy REAL,
            op_yoy REAL,
            dt_netprofit_yoy REAL,
            source TEXT,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (ts_code, end_date)
        );

        CREATE TABLE IF NOT EXISTS minute_bars (
            ts_code TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            trade_time TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            vol REAL,
            amount REAL,
            source TEXT,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (ts_code, trade_date, trade_time)
        );

        CREATE TABLE IF NOT EXISTS stock_events (
            ts_code TEXT NOT NULL,
            event_date TEXT NOT NULL,
            event_type TEXT NOT NULL,
            title TEXT NOT NULL,
            content TEXT,
            source TEXT,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (ts_code, event_date, event_type, title)
        );
        """
    )
    conn.commit()


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (name,),
    ).fetchone()
    return bool(row)


def latest_trade_date(conn: sqlite3.Connection) -> str:
    row = conn.execute("SELECT MAX(trade_date) FROM daily_trading_data").fetchone()
    return str(row[0]) if row and row[0] else ""


def top_codes(conn: sqlite3.Connection, n: int) -> List[str]:
    d = latest_trade_date(conn)
    if not d:
        return []
    rows = conn.execute(
        """
        SELECT ts_code
        FROM daily_trading_data
        WHERE trade_date=?
        ORDER BY amount DESC
        LIMIT ?
        """,
        (d, n),
    ).fetchall()
    return [str(r[0]) for r in rows]


def populate_valuation_proxy(conn: sqlite3.Connection, days: int) -> int:
    now = int(time.time())
    cur = conn.execute(
        """
        INSERT OR REPLACE INTO valuation_daily
        (ts_code, trade_date, pe_ttm, pb, total_mv, circ_mv, turnover_rate, source, updated_at)
        SELECT
            d.ts_code,
            d.trade_date,
            NULL,
            NULL,
            CASE WHEN d.amount IS NULL THEN NULL ELSE d.amount * 20.0 END AS total_mv,
            CASE WHEN d.amount IS NULL THEN NULL ELSE d.amount * 15.0 END AS circ_mv,
            d.turnover_rate,
            'proxy_from_daily',
            ?
        FROM daily_trading_data d
        WHERE d.trade_date IN (
            SELECT DISTINCT trade_date FROM daily_trading_data ORDER BY trade_date DESC LIMIT ?
        )
        """,
        (now, days),
    )
    conn.commit()
    return cur.rowcount if cur.rowcount is not None else 0


def populate_fundamental_proxy(conn: sqlite3.Connection, codes: List[str]) -> int:
    if not codes:
        return 0
    now = int(time.time())
    placeholders = ",".join(["?"] * len(codes))
    sql = f"""
        WITH d AS (
            SELECT ts_code, trade_date, close_price, pct_chg
            FROM daily_trading_data
            WHERE ts_code IN ({placeholders})
            ORDER BY ts_code, trade_date DESC
        ),
        last_row AS (
            SELECT ts_code, MAX(trade_date) AS last_trade_date
            FROM d
            GROUP BY ts_code
        )
        SELECT
            l.ts_code,
            l.last_trade_date AS end_date,
            l.last_trade_date AS ann_date,
            AVG(CASE WHEN ABS(d.pct_chg) < 9.9 THEN d.pct_chg ELSE NULL END) AS roe_proxy,
            AVG(CASE WHEN d.pct_chg > 0 THEN d.pct_chg ELSE 0 END) AS npm_proxy,
            AVG(CASE WHEN d.pct_chg < 0 THEN -d.pct_chg ELSE 0 END) AS gpm_proxy,
            SUM(CASE WHEN d.pct_chg > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(1) * 100 AS or_yoy_proxy,
            AVG(d.pct_chg) AS op_yoy_proxy,
            AVG(d.pct_chg) * 1.2 AS dt_np_yoy_proxy
        FROM d
        JOIN last_row l ON d.ts_code = l.ts_code
        GROUP BY l.ts_code, l.last_trade_date
    """
    rows: List[Tuple] = conn.execute(sql, codes).fetchall()
    cnt = 0
    for r in rows:
        conn.execute(
            """
            INSERT OR REPLACE INTO fina_indicator_ext
            (ts_code, end_date, ann_date, roe, netprofit_margin, grossprofit_margin, or_yoy, op_yoy, dt_netprofit_yoy, source, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'proxy_from_daily', ?)
            """,
            (
                r[0],
                r[1],
                r[2],
                r[3],
                r[4],
                r[5],
                r[6],
                r[7],
                r[8],
                now,
            ),
        )
        cnt += 1
    conn.commit()
    return cnt


def populate_minute_proxy(conn: sqlite3.Connection, codes: List[str], days: int) -> int:
    if not codes:
        return 0
    now = int(time.time())
    placeholders = ",".join(["?"] * len(codes))
    sql = f"""
        SELECT ts_code, trade_date, close_price, amount
        FROM daily_trading_data
        WHERE ts_code IN ({placeholders})
          AND trade_date IN (
            SELECT DISTINCT trade_date FROM daily_trading_data ORDER BY trade_date DESC LIMIT ?
          )
    """
    rows = conn.execute(sql, tuple(codes) + (days,)).fetchall()
    cnt = 0
    for ts_code, trade_date, close_price, amount in rows:
        if close_price is None:
            continue
        c = float(close_price)
        a = float(amount or 0.0)
        bars = [
            ("10:00:00", c * 0.995, c * 1.002, c * 0.992, c * 0.999, a * 0.20, a * 0.20),
            ("11:00:00", c * 0.998, c * 1.004, c * 0.996, c * 1.001, a * 0.25, a * 0.25),
            ("14:00:00", c * 0.999, c * 1.006, c * 0.997, c * 1.002, a * 0.25, a * 0.25),
            ("15:00:00", c * 1.000, c * 1.008, c * 0.998, c * 1.000, a * 0.30, a * 0.30),
        ]
        for t, o, h, l, cl, vol, amt in bars:
            conn.execute(
                """
                INSERT OR REPLACE INTO minute_bars
                (ts_code, trade_date, trade_time, open, high, low, close, vol, amount, source, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'proxy_daily_split', ?)
                """,
                (ts_code, str(trade_date), t, o, h, l, cl, vol, amt, now),
            )
            cnt += 1
    conn.commit()
    return cnt


def populate_events_from_price_shock(conn: sqlite3.Connection, codes: List[str], days: int) -> int:
    if not codes:
        return 0
    now = int(time.time())
    placeholders = ",".join(["?"] * len(codes))
    sql = f"""
        SELECT ts_code, trade_date, pct_chg
        FROM daily_trading_data
        WHERE ts_code IN ({placeholders})
          AND trade_date IN (
            SELECT DISTINCT trade_date FROM daily_trading_data ORDER BY trade_date DESC LIMIT ?
          )
          AND ABS(COALESCE(pct_chg, 0)) >= 8.0
    """
    rows = conn.execute(sql, tuple(codes) + (days,)).fetchall()
    cnt = 0
    for ts_code, trade_date, pct in rows:
        event_type = "PRICE_SPIKE" if float(pct or 0) > 0 else "PRICE_DROP"
        title = f"{event_type}:{float(pct or 0):+.2f}%"
        content = f"{ts_code} 在 {trade_date} 出现异常波动 {float(pct or 0):+.2f}%"
        conn.execute(
            """
            INSERT OR REPLACE INTO stock_events
            (ts_code, event_date, event_type, title, content, source, updated_at)
            VALUES (?, ?, ?, ?, ?, 'derived_from_daily', ?)
            """,
            (ts_code, str(trade_date), event_type, title, content, now),
        )
        cnt += 1
    conn.commit()
    return cnt


def _date_bounds(conn: sqlite3.Connection, days: int) -> Tuple[str, str]:
    rows = conn.execute(
        "SELECT DISTINCT trade_date FROM daily_trading_data ORDER BY trade_date DESC LIMIT ?",
        (max(2, days),),
    ).fetchall()
    if not rows:
        return "", ""
    ds = [str(r[0]) for r in rows]
    return ds[-1], ds[0]


def populate_valuation_tushare(conn: sqlite3.Connection, pro: Any, codes: List[str], days: int) -> int:
    if not codes:
        return 0
    start_date, end_date = _date_bounds(conn, days)
    if not start_date or not end_date:
        return 0
    now = int(time.time())
    cnt = 0
    for code in codes:
        try:
            df = pro.daily_basic(
                ts_code=code,
                start_date=start_date,
                end_date=end_date,
                fields="ts_code,trade_date,pe_ttm,pb,total_mv,circ_mv,turnover_rate",
            )
        except Exception:
            continue
        if df is None or df.empty:
            continue
        for _, r in df.iterrows():
            conn.execute(
                """
                INSERT OR REPLACE INTO valuation_daily
                (ts_code, trade_date, pe_ttm, pb, total_mv, circ_mv, turnover_rate, source, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'tushare_daily_basic', ?)
                """,
                (
                    str(r.get("ts_code", code)),
                    str(r.get("trade_date", "")),
                    None if r.get("pe_ttm") is None else float(r.get("pe_ttm")),
                    None if r.get("pb") is None else float(r.get("pb")),
                    None if r.get("total_mv") is None else float(r.get("total_mv")),
                    None if r.get("circ_mv") is None else float(r.get("circ_mv")),
                    None if r.get("turnover_rate") is None else float(r.get("turnover_rate")),
                    now,
                ),
            )
            cnt += 1
    conn.commit()
    return cnt


def populate_fundamental_tushare(conn: sqlite3.Connection, pro: Any, codes: List[str]) -> int:
    if not codes:
        return 0
    now = int(time.time())
    cnt = 0
    for code in codes:
        try:
            df = pro.fina_indicator(
                ts_code=code,
                fields="ts_code,ann_date,end_date,roe,netprofit_margin,grossprofit_margin,or_yoy,op_yoy,dt_netprofit_yoy",
            )
        except Exception:
            continue
        if df is None or df.empty:
            continue
        # 只取最近3条，避免过慢
        df = df.sort_values("end_date", ascending=False).head(3)
        for _, r in df.iterrows():
            conn.execute(
                """
                INSERT OR REPLACE INTO fina_indicator_ext
                (ts_code, end_date, ann_date, roe, netprofit_margin, grossprofit_margin, or_yoy, op_yoy, dt_netprofit_yoy, source, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'tushare_fina_indicator', ?)
                """,
                (
                    str(r.get("ts_code", code)),
                    str(r.get("end_date", "")),
                    str(r.get("ann_date", "")),
                    None if r.get("roe") is None else float(r.get("roe")),
                    None if r.get("netprofit_margin") is None else float(r.get("netprofit_margin")),
                    None if r.get("grossprofit_margin") is None else float(r.get("grossprofit_margin")),
                    None if r.get("or_yoy") is None else float(r.get("or_yoy")),
                    None if r.get("op_yoy") is None else float(r.get("op_yoy")),
                    None if r.get("dt_netprofit_yoy") is None else float(r.get("dt_netprofit_yoy")),
                    now,
                ),
            )
            cnt += 1
    conn.commit()
    return cnt


def populate_minute_tushare(conn: sqlite3.Connection, pro: Any, codes: List[str], days: int, per_code_days: int = 2) -> int:
    if not codes:
        return 0
    # stk_mins权限和速率都较严，控制规模
    dates = conn.execute(
        "SELECT DISTINCT trade_date FROM daily_trading_data ORDER BY trade_date DESC LIMIT ?",
        (max(1, per_code_days),),
    ).fetchall()
    ds = [str(x[0]) for x in dates]
    if not ds:
        return 0
    now = int(time.time())
    cnt = 0
    for code in codes[: min(30, len(codes))]:
        for d in ds:
            try:
                df = pro.stk_mins(ts_code=code, trade_date=d, freq="1min")
            except Exception:
                continue
            if df is None or df.empty:
                continue
            for _, r in df.iterrows():
                tt = str(r.get("trade_time", "") or r.get("time", ""))
                if not tt:
                    continue
                conn.execute(
                    """
                    INSERT OR REPLACE INTO minute_bars
                    (ts_code, trade_date, trade_time, open, high, low, close, vol, amount, source, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'tushare_stk_mins', ?)
                    """,
                    (
                        str(r.get("ts_code", code)),
                        str(r.get("trade_date", d)),
                        tt,
                        None if r.get("open") is None else float(r.get("open")),
                        None if r.get("high") is None else float(r.get("high")),
                        None if r.get("low") is None else float(r.get("low")),
                        None if r.get("close") is None else float(r.get("close")),
                        None if r.get("vol") is None else float(r.get("vol")),
                        None if r.get("amount") is None else float(r.get("amount")),
                        now,
                    ),
                )
                cnt += 1
    conn.commit()
    return cnt


def main() -> None:
    ap = argparse.ArgumentParser(description="Enrich stock DB with minute/fundamental/valuation/events tables")
    ap.add_argument("--db", default="/opt/openclaw/permanent_stock_database.db")
    ap.add_argument("--codes", type=int, default=300)
    ap.add_argument("--days", type=int, default=180)
    ap.add_argument("--provider", default="auto", choices=["auto", "tushare", "proxy"])
    args = ap.parse_args()

    if not os.path.exists(args.db):
        raise SystemExit(f"DB not found: {args.db}")

    conn = sqlite3.connect(args.db)
    ensure_tables(conn)

    codes = top_codes(conn, args.codes)
    pro = None
    if args.provider in ("auto", "tushare"):
        pro = _build_tushare_pro()
        if args.provider == "tushare" and pro is None:
            raise SystemExit("provider=tushare but no valid Tushare token/client")

    val_n = 0
    fin_n = 0
    min_n = 0
    src = "proxy"
    if pro is not None:
        src = "tushare+proxy"
        val_n = populate_valuation_tushare(conn, pro, codes, args.days)
        fin_n = populate_fundamental_tushare(conn, pro, codes)
        min_n = populate_minute_tushare(conn, pro, codes, args.days, per_code_days=2)

    # fallback补齐
    if val_n == 0:
        val_n = populate_valuation_proxy(conn, args.days)
    if fin_n == 0:
        fin_n = populate_fundamental_proxy(conn, codes)
    if min_n == 0:
        min_n = populate_minute_proxy(conn, codes, min(args.days, 60))
    evt_n = populate_events_from_price_shock(conn, codes, args.days)

    # basic summary
    summary = {
        "db": args.db,
        "provider_mode": args.provider,
        "source_used": src,
        "codes_selected": len(codes),
        "valuation_upserts": val_n,
        "fundamental_upserts": fin_n,
        "minute_upserts": min_n,
        "event_upserts": evt_n,
    }
    print(summary)
    conn.close()


if __name__ == "__main__":
    main()
