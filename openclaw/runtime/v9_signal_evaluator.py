from __future__ import annotations

from typing import Any, Dict

import pandas as pd

from openclaw.runtime.dataframe_utils import ensure_price_aliases


def calculate_v9_score_from_history(hist: pd.DataFrame, *, industry_strength: float = 0.0) -> Dict[str, Any]:
    if hist is None or hist.empty or len(hist) < 80:
        return {"score": 0.0, "details": {}}

    h = ensure_price_aliases(hist).sort_values("trade_date")
    close_col = "close_price" if "close_price" in h.columns else ("close" if "close" in h.columns else "")
    if not close_col:
        return {"score": 0.0, "details": {}}
    close = pd.to_numeric(h[close_col], errors="coerce").ffill()
    vol = pd.to_numeric(h.get("vol", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
    amount = pd.to_numeric(h.get("amount", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
    pct = pd.to_numeric(h.get("pct_chg", pd.Series(dtype=float)), errors="coerce")
    if pct.isna().all():
        pct = close.pct_change() * 100

    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(60).mean()
    ma120 = close.rolling(120).mean()

    trend_strong = bool(ma20.iloc[-1] > ma60.iloc[-1] > ma120.iloc[-1])
    trend_ok = bool((ma20.iloc[-1] > ma60.iloc[-1]) and (ma20.iloc[-1] > ma20.iloc[-5]) and (ma60.iloc[-1] >= ma60.iloc[-5]))

    momentum_20 = (close.iloc[-1] / close.iloc[-21] - 1.0) if len(close) > 21 else 0.0
    momentum_60 = (close.iloc[-1] / close.iloc[-61] - 1.0) if len(close) > 61 else 0.0
    vol_ratio = (vol.iloc[-1] / vol.tail(20).mean()) if vol.tail(20).mean() > 0 else 0.0

    flow_sign = pct.fillna(0).apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    flow_val = (amount * flow_sign).tail(20).sum()
    flow_base = amount.tail(20).sum() if amount.tail(20).sum() > 0 else 1.0
    flow_ratio = flow_val / flow_base

    vol_20 = pct.tail(20).std() / 100.0 if pct.tail(20).std() is not None else 0.0

    fund_score = max(0.0, min(20.0, (flow_ratio + 0.03) / 0.12 * 20.0))
    volume_score = max(0.0, min(15.0, (vol_ratio - 0.5) / 1.0 * 15.0))
    momentum_score = max(0.0, min(8.0, momentum_20 * 100 / 8.0 * 8.0)) + max(0.0, min(7.0, momentum_60 * 100 / 16.0 * 7.0))
    sector_score = max(0.0, min(15.0, (industry_strength + 2.0) / 6.0 * 15.0))

    if vol_20 <= 0.03:
        volatility_score = 12.0
    elif vol_20 <= 0.06:
        volatility_score = 15.0
    elif vol_20 <= 0.10:
        volatility_score = 8.0
    else:
        volatility_score = 0.0

    trend_score = 15.0 if trend_strong else (10.0 if trend_ok else 0.0)

    rolling_peak = close.cummax()
    drawdown = (rolling_peak - close) / rolling_peak
    max_dd = float(drawdown.tail(60).max())
    dd_penalty = 0.0
    if max_dd > 0.15:
        dd_penalty = min(10.0, (max_dd - 0.15) / 0.15 * 10.0)

    total_score = fund_score + volume_score + momentum_score + sector_score + volatility_score + trend_score - dd_penalty
    if total_score < 0:
        total_score = 0.0

    return {
        "score": round(total_score, 2),
        "details": {
            "fund_score": round(fund_score, 2),
            "volume_score": round(volume_score, 2),
            "momentum_score": round(momentum_score, 2),
            "sector_score": round(sector_score, 2),
            "volatility_score": round(volatility_score, 2),
            "trend_score": round(trend_score, 2),
            "flow_ratio": round(flow_ratio, 4),
            "vol_ratio": round(vol_ratio, 3),
            "momentum_20": round(momentum_20 * 100, 2),
            "momentum_60": round(momentum_60 * 100, 2),
            "vol_20": round(vol_20 * 100, 2),
        },
    }
