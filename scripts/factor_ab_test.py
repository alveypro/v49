#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data.dao import resolve_db_path
from data.factor_bonus import load_factor_bonus_maps


@dataclass
class PortfolioResult:
    name: str
    avg_return_pct: float
    win_rate_pct: float
    samples: int
    annualized_return_pct: float
    sharpe_like: float


def _load_data(conn: sqlite3.Connection, start_date: str, end_date: str) -> Tuple[pd.DataFrame, Dict[str, str]]:
    price_df = pd.read_sql_query(
        """
        SELECT ts_code, trade_date, close_price
        FROM daily_trading_data
        WHERE trade_date >= ? AND trade_date <= ?
        ORDER BY trade_date, ts_code
        """,
        conn,
        params=(start_date, end_date),
    )
    ind_df = pd.read_sql_query("SELECT ts_code, industry FROM stock_basic", conn)
    ind_map = {str(r["ts_code"]): str(r["industry"] or "") for _, r in ind_df.iterrows()}
    return price_df, ind_map


def _metrics(name: str, series: List[float], hold_days: int) -> PortfolioResult:
    if not series:
        return PortfolioResult(name, 0.0, 0.0, 0, 0.0, 0.0)
    arr = np.array(series, dtype=float)
    mean = float(arr.mean())
    std = float(arr.std(ddof=1)) if len(arr) > 1 else 0.0
    sharpe_like = (mean / std) * np.sqrt(252 / max(1, hold_days)) if std > 0 else 0.0
    annualized = (1.0 + mean) ** (252 / max(1, hold_days)) - 1.0
    win_rate = float((arr > 0).mean())
    return PortfolioResult(
        name=name,
        avg_return_pct=mean * 100.0,
        win_rate_pct=win_rate * 100.0,
        samples=len(arr),
        annualized_return_pct=annualized * 100.0,
        sharpe_like=float(sharpe_like),
    )


def run_ab_test(
    db_path: str,
    lookback_days: int = 180,
    hold_days: int = 5,
    top_k: int = 20,
    min_universe: int = 500,
) -> Dict[str, object]:
    conn = sqlite3.connect(db_path)
    try:
        dates_df = pd.read_sql_query(
            "SELECT DISTINCT trade_date FROM daily_trading_data ORDER BY trade_date DESC LIMIT ?",
            conn,
            params=(lookback_days + hold_days + 30,),
        )
        if dates_df is None or dates_df.empty:
            return {"ok": False, "error": "no trade dates"}
        all_dates = sorted(dates_df["trade_date"].astype(str).tolist())
        start_date, end_date = all_dates[0], all_dates[-1]
        price_df, industry_map = _load_data(conn, start_date, end_date)
        if price_df is None or price_df.empty:
            return {"ok": False, "error": "no prices"}

        piv = price_df.pivot(index="trade_date", columns="ts_code", values="close_price").sort_index()
        ret1 = piv.pct_change(fill_method=None)
        ret10 = piv / piv.shift(10) - 1.0
        vol20 = ret1.rolling(20).std()

        eval_dates = list(piv.index)
        if len(eval_dates) <= hold_days + 25:
            return {"ok": False, "error": "insufficient history for AB"}

        base_daily: List[float] = []
        enh_daily: List[float] = []
        eval_used = 0

        for i in range(20, len(eval_dates) - hold_days):
            d0 = eval_dates[i]
            d1 = eval_dates[i + hold_days]

            base_score = ret10.loc[d0] - vol20.loc[d0] * 1.5
            future_ret = piv.loc[d1] / piv.loc[d0] - 1.0

            valid = base_score.replace([np.inf, -np.inf], np.nan).dropna().index
            valid = valid.intersection(future_ret.replace([np.inf, -np.inf], np.nan).dropna().index)
            if len(valid) < min_universe:
                continue

            base_s = base_score.loc[valid].astype(float)
            future_s = future_ret.loc[valid].astype(float)

            # 因子加分
            fg, fs_map, fi_map, _meta = load_factor_bonus_maps(conn, trade_date=str(d0))
            bonus = pd.Series(0.0, index=valid, dtype=float)
            if fg:
                bonus += float(fg)
            if fs_map:
                b1 = pd.Series({k: float(v) for k, v in fs_map.items()})
                bonus = bonus.add(b1, fill_value=0.0)
            if fi_map:
                ind_bonus = pd.Series(
                    [float(fi_map.get(industry_map.get(ts, ""), 0.0)) for ts in valid],
                    index=valid,
                    dtype=float,
                )
                bonus = bonus.add(ind_bonus, fill_value=0.0)

            enh_s = base_s + bonus

            base_pick = base_s.sort_values(ascending=False).head(top_k).index
            enh_pick = enh_s.sort_values(ascending=False).head(top_k).index

            base_daily.append(float(future_s.loc[base_pick].mean()))
            enh_daily.append(float(future_s.loc[enh_pick].mean()))
            eval_used += 1

        base_metrics = _metrics("baseline", base_daily, hold_days=hold_days)
        enh_metrics = _metrics("enhanced", enh_daily, hold_days=hold_days)

        delta = {
            "avg_return_delta_pct": enh_metrics.avg_return_pct - base_metrics.avg_return_pct,
            "win_rate_delta_pct": enh_metrics.win_rate_pct - base_metrics.win_rate_pct,
            "sharpe_delta": enh_metrics.sharpe_like - base_metrics.sharpe_like,
        }

        return {
            "ok": True,
            "db_path": db_path,
            "eval_days": eval_used,
            "hold_days": hold_days,
            "top_k": top_k,
            "baseline": base_metrics.__dict__,
            "enhanced": enh_metrics.__dict__,
            "delta": delta,
        }
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="AB test: baseline vs factor-enhanced ranking")
    parser.add_argument("--db-path", default="", help="SQLite db path, default auto-resolve")
    parser.add_argument("--lookback-days", type=int, default=180)
    parser.add_argument("--hold-days", type=int, default=5)
    parser.add_argument("--top-k", type=int, default=20)
    parser.add_argument("--min-universe", type=int, default=500)
    args = parser.parse_args()

    db_path = args.db_path or str(resolve_db_path())
    out = run_ab_test(
        db_path=db_path,
        lookback_days=int(args.lookback_days),
        hold_days=int(args.hold_days),
        top_k=int(args.top_k),
        min_universe=int(args.min_universe),
    )
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0 if out.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
