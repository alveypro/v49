from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data.dao import resolve_db_path


def _load_daily(conn: sqlite3.Connection, lookback_days: int = 180) -> pd.DataFrame:
    dates_df = pd.read_sql_query(
        "SELECT DISTINCT trade_date FROM daily_trading_data ORDER BY trade_date DESC LIMIT ?",
        conn,
        params=(max(60, int(lookback_days)),),
    )
    if dates_df.empty:
        return pd.DataFrame()
    dates = [str(x) for x in dates_df["trade_date"].tolist()]
    cutoff = min(dates)
    sql = """
    SELECT ts_code, trade_date, close_price, vol, amount, turnover_rate, pb, pe, pct_chg
    FROM daily_trading_data
    WHERE trade_date >= ?
    ORDER BY trade_date ASC, ts_code ASC
    """
    df = pd.read_sql_query(sql, conn, params=(cutoff,))
    if df.empty:
        return df
    df = df.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)
    return df


def _build_factor_frame(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    g = work.groupby("ts_code", group_keys=False)
    work["ret_1"] = g["close_price"].pct_change()
    work["mom_5"] = g["close_price"].pct_change(5)
    work["mom_10"] = g["close_price"].pct_change(10)
    work["volatility_10"] = g["ret_1"].rolling(10).std().reset_index(level=0, drop=True)
    work["turnover_5"] = g["turnover_rate"].rolling(5).mean().reset_index(level=0, drop=True)
    work["pb_inv"] = 1.0 / work["pb"].replace(0, pd.NA)
    work["pe_inv"] = 1.0 / work["pe"].replace(0, pd.NA)
    work["future_ret_5"] = g["close_price"].shift(-5) / work["close_price"] - 1.0
    work["future_ret_10"] = g["close_price"].shift(-10) / work["close_price"] - 1.0
    return work


def _cross_sectional_ic(
    df: pd.DataFrame,
    factor: str,
    target: str,
    max_dates: int = 80,
    sample_per_day: int = 800,
) -> float:
    rows: List[float] = []
    trade_dates = sorted(df["trade_date"].dropna().astype(str).unique().tolist())[-max_dates:]
    for td in trade_dates:
        g = df[df["trade_date"] == td]
        gg = g[[factor, target]].dropna()
        if len(gg) > sample_per_day:
            gg = gg.sample(sample_per_day, random_state=42)
        if len(gg) < 30:
            continue
        c = gg[factor].corr(gg[target], method="spearman")
        if pd.notna(c):
            rows.append(float(c))
    if not rows:
        return 0.0
    return float(sum(rows) / len(rows))


def calibrate_v4_weights(
    db_path: str | None = None,
    output_dir: str | Path = "logs/openclaw",
    lookback_days: int = 180,
) -> Dict[str, Any]:
    db = resolve_db_path(db_path) if db_path else resolve_db_path()
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(str(db)) as conn:
        raw = _load_daily(conn, lookback_days=lookback_days)
    if raw.empty:
        return {"ok": False, "reason": "empty_daily_trading_data"}

    fac = _build_factor_frame(raw)
    factors = ["mom_5", "mom_10", "volatility_10", "turnover_5", "pb_inv", "pe_inv"]
    scores = {}
    for f in factors:
        ic = _cross_sectional_ic(fac, f, "future_ret_5")
        scores[f] = ic

    abs_sum = sum(abs(v) for v in scores.values()) or 1.0
    weights = {k: round(v / abs_sum, 6) for k, v in scores.items()}

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = out_dir / f"v4_ic_weights_{ts}.json"
    out_json.write_text(
        json.dumps(
            {
                "generated_at": datetime.now().isoformat(),
                "target": "future_ret_5",
                "ic": scores,
                "weights": weights,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    latest = Path("openclaw/config/v4_factor_weights.json")
    latest.parent.mkdir(parents=True, exist_ok=True)
    latest.write_text(out_json.read_text(encoding="utf-8"), encoding="utf-8")
    return {"ok": True, "weights_path": str(latest), "snapshot_path": str(out_json), "weights": weights}


def analyze_factor_decay(
    db_path: str | None = None,
    output_dir: str | Path = "logs/openclaw",
    lookback_days: int = 180,
) -> Dict[str, Any]:
    db = resolve_db_path(db_path) if db_path else resolve_db_path()
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(str(db)) as conn:
        raw = _load_daily(conn, lookback_days=lookback_days)
    if raw.empty:
        return {"ok": False, "reason": "empty_daily_trading_data"}

    fac = _build_factor_frame(raw)
    factors = ["mom_5", "mom_10", "volatility_10", "turnover_5", "pb_inv", "pe_inv"]
    horizons = {1: "future_ret_5", 2: "future_ret_10"}
    rows = []
    for factor in factors:
        base = abs(_cross_sectional_ic(fac, factor, "future_ret_5"))
        for h, target in horizons.items():
            ic_h = abs(_cross_sectional_ic(fac, factor, target))
            decay = 1.0 if base == 0 else ic_h / base
            rows.append(
                {
                    "factor": factor,
                    "horizon_bucket": h,
                    "target": target,
                    "abs_ic": round(ic_h, 6),
                    "decay_ratio": round(decay, 6),
                }
            )
    df = pd.DataFrame(rows)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_csv = out_dir / f"factor_decay_{ts}.csv"
    out_md = out_dir / f"factor_decay_{ts}.md"
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    lines = ["# Factor Decay Analysis", "", f"- generated_at: {datetime.now().isoformat()}", "", df.to_markdown(index=False)]
    out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"ok": True, "csv": str(out_csv), "markdown": str(out_md), "rows": int(len(df))}


def main() -> int:
    parser = argparse.ArgumentParser(description="v4 IC calibration and factor decay analysis")
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--output-dir", default="logs/openclaw")
    parser.add_argument("--lookback-days", type=int, default=180)
    parser.add_argument("--mode", default="all", choices=["ic", "decay", "all"])
    args = parser.parse_args()

    if args.mode == "ic":
        out = {"ic": calibrate_v4_weights(db_path=args.db_path, output_dir=args.output_dir, lookback_days=args.lookback_days)}
    elif args.mode == "decay":
        out = {"decay": analyze_factor_decay(db_path=args.db_path, output_dir=args.output_dir, lookback_days=args.lookback_days)}
    else:
        out = {
            "ic": calibrate_v4_weights(db_path=args.db_path, output_dir=args.output_dir, lookback_days=args.lookback_days),
            "decay": analyze_factor_decay(db_path=args.db_path, output_dir=args.output_dir, lookback_days=args.lookback_days),
        }
    print(json.dumps(out, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
