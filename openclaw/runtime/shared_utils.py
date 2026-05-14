from __future__ import annotations

import os
from datetime import datetime
from typing import Optional

import pandas as pd


def now_ts() -> float:
    return datetime.now().timestamp()


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    """Return UTF-8 CSV bytes with BOM for Excel compatibility."""
    csv_text = df.to_csv(index=False)
    return ("\ufeff" + csv_text).encode("utf-8")


def safe_file_mtime(path: str) -> float:
    try:
        return float(os.path.getmtime(path))
    except Exception:
        return 0.0


def fmt_file_mtime(path: str) -> str:
    try:
        if os.path.exists(path):
            return datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        pass
    return ""


def safe_parse_dt(text: str) -> Optional[datetime]:
    raw = str(text or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y%m%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(raw[:19], fmt)
        except Exception:
            continue
    return None
