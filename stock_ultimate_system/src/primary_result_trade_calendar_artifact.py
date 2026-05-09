from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_TRADE_CALENDAR_ARTIFACT_VERSION = "primary_result_trade_calendar_artifact.v1"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_date(value: str) -> str:
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        return text
    raise ValueError(f"unsupported trade date format: {value!r}")


def _to_tushare_date(value: str) -> str:
    return _normalize_date(value).replace("-", "")


def _read_token_candidates(project_root: Path) -> list[str]:
    candidates = [
        project_root / ".tushare_token",
        project_root.parent / ".tushare_token",
        Path.home() / ".tushare_token",
        project_root / ".env",
        project_root.parent / ".env",
    ]
    tokens: list[str] = []
    env_token = os.getenv("TUSHARE_TOKEN", "").strip()
    if env_token:
        tokens.append(env_token)
    for p in candidates:
        if not p.exists() or not p.is_file():
            continue
        try:
            text = p.read_text(encoding="utf-8").strip()
        except Exception:
            continue
        for raw in text.splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, value = line.split("=", 1)
                if key.strip().upper() not in {"TUSHARE_TOKEN", "TS_TOKEN"}:
                    continue
                token = value.strip().strip('"').strip("'")
            else:
                token = line.strip().strip('"').strip("'")
            if token and token not in tokens:
                tokens.append(token)
    return tokens


def fetch_tushare_open_trade_dates(
    *,
    start_date: str,
    end_date: str,
    exchange: str = "SSE",
    project_root: str | Path | None = None,
) -> list[str]:
    import tushare as ts

    root = resolve_project_path(".") if project_root is None else resolve_project_path(project_root)
    tokens = _read_token_candidates(root)
    if not tokens:
        raise RuntimeError("Tushare token not found")
    last_error: Exception | None = None
    for token in tokens:
        try:
            ts.set_token(token)
            pro = ts.pro_api(token)
            cal = pro.trade_cal(exchange=exchange, start_date=_to_tushare_date(start_date), end_date=_to_tushare_date(end_date))
            if cal is None or cal.empty:
                return []
            opened = cal[cal["is_open"] == 1].copy()
            return sorted(_normalize_date(item) for item in opened["cal_date"].astype(str).tolist())
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"Tushare trade calendar fetch failed: {last_error}")


def build_primary_result_trade_calendar_artifact(
    *,
    start_date: str,
    end_date: str,
    output_path: str | Path = "artifacts/primary_result_trade_calendar_latest.json",
    exchange: str = "SSE",
    source: str = "tushare_trade_cal",
    fetch_open_trade_dates: Callable[..., Iterable[str]] | None = None,
) -> dict[str, Any]:
    normalized_start = _normalize_date(start_date)
    normalized_end = _normalize_date(end_date)
    if normalized_start > normalized_end:
        raise ValueError("start_date must be <= end_date")
    fetcher = fetch_open_trade_dates or fetch_tushare_open_trade_dates
    trade_dates = sorted({_normalize_date(item) for item in fetcher(start_date=normalized_start, end_date=normalized_end, exchange=exchange)})
    if not trade_dates:
        raise ValueError("trade calendar contains no open trade dates")
    payload: dict[str, Any] = {
        "calendar_version": PRIMARY_RESULT_TRADE_CALENDAR_ARTIFACT_VERSION,
        "generated_at": _utc_now_iso(),
        "source": source,
        "exchange": exchange,
        "start_date": normalized_start,
        "end_date": normalized_end,
        "trade_dates": trade_dates,
        "trade_date_total": len(trade_dates),
        "production_boundary": (
            "local trade calendar artifact for observation window scheduling only; "
            "it does not select stocks, trade, or certify market holidays beyond the source data"
        ),
    }
    resolved_output = resolve_project_path(output_path)
    resolved_output.parent.mkdir(parents=True, exist_ok=True)
    resolved_output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return payload
