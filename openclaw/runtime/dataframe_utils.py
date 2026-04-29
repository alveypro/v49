from __future__ import annotations

import pandas as pd


def ensure_price_aliases(df: pd.DataFrame) -> pd.DataFrame:
    """Keep close/close_price and common OHLCV aliases both available."""
    if df is None:
        return df
    out = df.copy()
    alias_pairs = [
        ("close_price", "close"),
        ("open_price", "open"),
        ("high_price", "high"),
        ("low_price", "low"),
        ("vol", "volume"),
    ]
    for left, right in alias_pairs:
        if left in out.columns and right not in out.columns:
            out[right] = out[left]
        elif right in out.columns and left not in out.columns:
            out[left] = out[right]
    return out


def normalize_stock_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = ensure_price_aliases(df)
    if "trade_date" in out.columns:
        out = out.sort_values("trade_date").reset_index(drop=True)
    for col in (
        "close_price",
        "close",
        "open_price",
        "open",
        "high_price",
        "high",
        "low_price",
        "low",
        "vol",
        "volume",
        "pct_chg",
        "amount",
        "turnover_rate",
    ):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.ffill().bfill()
    out = ensure_price_aliases(out)
    return out
