"""Forward return labeling for Top5 competition audit artifacts (offline research).

Computes close-to-close gross returns from ``daily_trading_data`` using the first
trading row on or after the artifact ``trade_date`` for each symbol. This matches
a minimal point-in-time replay contract; it is **not** realized PnL (no fills,
fees except an optional round-trip bps haircut, suspension handling beyond missing
bars, or borrow).

Institutional analogue: post-hoc label generation for signal snapshots + TCA
reports — kept separate from production release gates on purpose.
"""

from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence

JsonDict = Dict[str, Any]


def compact_trade_date(raw: str | None) -> str:
    """Normalize ``YYYY-MM-DD``, ``YYYYMMDD``, etc. to 8-digit compact form."""
    digits = "".join(ch for ch in str(raw or "") if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ""


def infer_as_of_trade_date_compact_from_signal_refs(artifact: Mapping[str, Any]) -> str:
    """Best-effort PIT anchor when ``trade_date`` was not persisted on older artifacts.

    Parses YYYYMMDD tokens embedded in ``run_id`` strings inside ``signal_refs``.
    """
    best = ""
    for row in artifact.get("top5_portfolio_audit") or []:
        if not isinstance(row, dict):
            continue
        src = row.get("source") if isinstance(row.get("source"), dict) else {}
        for ref in src.get("signal_refs") or []:
            if not isinstance(ref, dict):
                continue
            rid = str(ref.get("run_id") or "")
            for m in re.finditer(r"(20\d{6})", rid):
                cand = m.group(1)
                if cand > best:
                    best = cand
    return best


def resolve_as_of_trade_date_compact(
    artifact: Mapping[str, Any],
    *,
    override_as_of_compact: str = "",
) -> tuple[str, List[str]]:
    notes: List[str] = []
    o = compact_trade_date(str(override_as_of_compact or "").strip())
    if len(o) == 8:
        return o, notes
    td = compact_trade_date(str(artifact.get("trade_date") or ""))
    if len(td) == 8:
        return td, notes
    inferred = infer_as_of_trade_date_compact_from_signal_refs(artifact)
    if len(inferred) == 8:
        notes.append("as_of_inferred_from_signal_ref_run_ids")
        return inferred, notes
    return "", notes


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (str(name),),
    ).fetchone()
    return row is not None


def _forward_window_rows(
    conn: sqlite3.Connection,
    *,
    ts_code: str,
    as_of_compact: str,
    max_holding_days: int,
) -> List[tuple[str, float]]:
    """Rows ordered ascending trade_date from first session on/after as_of."""
    row = conn.execute(
        """
        SELECT trade_date, close_price
        FROM daily_trading_data
        WHERE ts_code = ?
          AND REPLACE(REPLACE(trade_date, '-', ''), '/', '') >= ?
          AND close_price IS NOT NULL
        ORDER BY REPLACE(REPLACE(trade_date, '-', ''), '/', '') ASC
        LIMIT ?
        """,
        (str(ts_code or "").strip(), str(as_of_compact), int(max_holding_days) + 1),
    ).fetchall()
    return [(str(r[0] or ""), float(r[1] or 0.0)) for r in row]


def forward_returns_for_symbol(
    conn: sqlite3.Connection,
    *,
    ts_code: str,
    as_of_compact: str,
    horizons: Sequence[int],
) -> JsonDict:
    if not _table_exists(conn, "daily_trading_data"):
        return {
            str(h): {"available": False, "blocking_reason": "missing_daily_trading_data_table"}
            for h in horizons
        }
    pos = [int(h) for h in horizons if int(h) > 0]
    if not pos:
        return {}
    max_h = max(pos)
    rows = _forward_window_rows(conn, ts_code=ts_code, as_of_compact=as_of_compact, max_holding_days=max_h)
    out: JsonDict = {}
    for h in pos:
        if len(rows) < h + 1:
            out[str(h)] = {
                "available": False,
                "blocking_reason": "insufficient_forward_price_window",
                "price_count": len(rows),
            }
            continue
        entry_px = float(rows[0][1] or 0.0)
        exit_px = float(rows[h][1] or 0.0)
        if entry_px <= 0.0 or exit_px <= 0.0:
            out[str(h)] = {"available": False, "blocking_reason": "invalid_close_price"}
            continue
        out[str(h)] = {
            "available": True,
            "entry_trade_date": rows[0][0],
            "exit_trade_date": rows[h][0],
            "return_pct": (exit_px / entry_px - 1.0) * 100.0,
        }
    return out


def load_audit_artifact(path: str | Path) -> JsonDict:
    p = Path(path)
    payload = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("audit_artifact_not_object")
    return payload


def evaluate_top5_forward_returns(
    conn: sqlite3.Connection,
    *,
    artifact: Mapping[str, Any],
    horizons: Sequence[int],
    subtract_roundtrip_cost_bps: float = 0.0,
    override_as_of_compact: str = "",
) -> JsonDict:
    """Return per-symbol forward labels and portfolio aggregates for one audit snapshot."""
    blocking: List[str] = []
    as_of, as_of_notes = resolve_as_of_trade_date_compact(artifact, override_as_of_compact=str(override_as_of_compact or "").strip())
    if not as_of:
        blocking.append("missing_as_of_trade_date")

    top5_rows = [dict(r) for r in (artifact.get("top5_portfolio_audit") or []) if isinstance(r, dict)]
    if len(top5_rows) < 1:
        blocking.append("empty_top5_portfolio_audit")

    pos_horizons = sorted({int(h) for h in horizons if int(h) > 0})
    per_symbol: List[JsonDict] = []
    weight_sum = 0.0
    for row in top5_rows:
        ts_code = str(row.get("ts_code") or "").strip()
        weight = float(row.get("weight") or 0.0)
        if ts_code:
            weight_sum += weight
        fwd = (
            forward_returns_for_symbol(conn, ts_code=ts_code, as_of_compact=as_of, horizons=pos_horizons)
            if as_of
            else {str(h): {"available": False, "blocking_reason": "missing_as_of_trade_date"} for h in pos_horizons}
        )
        per_symbol.append({"ts_code": ts_code, "weight": weight, "forward": fwd})

    portfolio_by_h: Dict[str, JsonDict] = {}
    equal_weight_by_h: Dict[str, JsonDict] = {}
    cost_frac = max(0.0, float(subtract_roundtrip_cost_bps or 0.0)) / 10000.0 * 2.0

    for h in pos_horizons:
        key = str(h)
        gross_weighted = 0.0
        gross_ew = 0.0
        w_used = 0.0
        ew_n = 0
        all_avail = True
        for ps in per_symbol:
            ts_code = str(ps.get("ts_code") or "")
            if not ts_code:
                all_avail = False
                continue
            cell = (ps.get("forward") or {}).get(key) if isinstance(ps.get("forward"), dict) else {}
            if not isinstance(cell, dict) or cell.get("available") is not True:
                all_avail = False
                continue
            r = float(cell.get("return_pct") or 0.0) / 100.0
            w = float(ps.get("weight") or 0.0)
            if w > 0.0:
                gross_weighted += w * r
                w_used += w
            gross_ew += r
            ew_n += 1
        net_weighted = gross_weighted - cost_frac * w_used if w_used > 0 else gross_weighted
        net_ew = (gross_ew / ew_n - cost_frac) if ew_n else None

        portfolio_by_h[key] = {
            "horizon_trading_days": h,
            "gross_return_weighted_pct": (gross_weighted * 100.0) if w_used > 0 else None,
            "net_return_weighted_pct_after_roundtrip_cost_pct": (net_weighted * 100.0) if w_used > 0 else None,
            "weight_coverage": w_used,
            "all_symbols_available": bool(all_avail and ew_n == len([p for p in per_symbol if p.get("ts_code")])),
            "symbol_count_with_prices": ew_n,
        }
        equal_weight_by_h[key] = {
            "horizon_trading_days": h,
            "gross_return_equal_weight_pct": (gross_ew / ew_n * 100.0) if ew_n else None,
            "net_return_equal_weight_pct_after_roundtrip_cost_pct": (net_ew * 100.0) if ew_n and net_ew is not None else None,
            "symbol_count": ew_n,
        }

    return {
        "artifact_version": str(artifact.get("artifact_version") or ""),
        "competition_run_id": str(artifact.get("competition_run_id") or ""),
        "ranking_method_hash": str(artifact.get("ranking_method_hash") or ""),
        "as_of_trade_date_compact": as_of,
        "as_of_resolution_notes": as_of_notes,
        "horizons_trading_days": pos_horizons,
        "subtract_roundtrip_cost_bps": float(subtract_roundtrip_cost_bps or 0.0),
        "weight_sum_in_artifact": weight_sum,
        "methodology": {
            "price_field": "close_price",
            "entry_timing": "first_row_trade_date_gte_as_of",
            "return_type": "close_to_close_simple_gross",
            "limitations": [
                "Not realized execution; no partial fills or limit suspension modeling.",
                "Optional cost is a naive round-trip bps haircut on portfolio weights.",
            ],
        },
        "blocking_reasons": blocking,
        "per_symbol": per_symbol,
        "portfolio": portfolio_by_h,
        "equal_weight_portfolio": equal_weight_by_h,
    }


def forward_evaluation_expected_symbol_count(per_symbol: Sequence[Mapping[str, Any]]) -> int:
    return sum(
        1 for row in per_symbol if isinstance(row, dict) and str(row.get("ts_code") or "").strip()
    )


def _count_forward_available(per_symbol: Sequence[Mapping[str, Any]], horizon_key: str) -> int:
    n = 0
    for row in per_symbol:
        if not isinstance(row, dict):
            continue
        if not str(row.get("ts_code") or "").strip():
            continue
        fwd = row.get("forward")
        if not isinstance(fwd, dict):
            continue
        cell = fwd.get(horizon_key)
        if isinstance(cell, dict) and cell.get("available") is True:
            n += 1
    return n


def forward_evaluation_gate_failures(
    evaluation: Mapping[str, Any],
    *,
    horizons: Sequence[int],
    fail_on_eval_blocking: bool = False,
    fail_on_inferred_as_of: bool = False,
    min_available_symbols_per_horizon: int = 0,
    min_available_ratio_per_horizon: float = 0.0,
) -> List[str]:
    """Return human-readable failure tokens for CI / audit gates (empty list => pass)."""
    failures: List[str] = []
    if fail_on_eval_blocking:
        br = evaluation.get("blocking_reasons")
        if isinstance(br, list) and len(br) > 0:
            failures.append("eval_blocking_reasons_non_empty")

    notes = evaluation.get("as_of_resolution_notes")
    note_list: List[str] = [str(x) for x in notes] if isinstance(notes, list) else []
    if fail_on_inferred_as_of and "as_of_inferred_from_signal_ref_run_ids" in note_list:
        failures.append("as_of_inferred_from_signal_ref_run_ids")

    per_symbol = evaluation.get("per_symbol")
    rows = per_symbol if isinstance(per_symbol, list) else []
    expected = forward_evaluation_expected_symbol_count(rows)
    if expected <= 0:
        failures.append("no_symbols_to_score_coverage")
        return failures

    min_sym = max(0, int(min_available_symbols_per_horizon or 0))
    min_ratio = max(0.0, float(min_available_ratio_per_horizon or 0.0))
    for h in horizons:
        hi = int(h)
        if hi <= 0:
            continue
        key = str(hi)
        avail = _count_forward_available(rows, key)
        if min_sym > 0 and avail < min_sym:
            failures.append(f"coverage_symbols_horizon_{key}:{avail}<{min_sym}")
        if min_ratio > 0.0 and avail < expected * min_ratio - 1e-12:
            failures.append(f"coverage_ratio_horizon_{key}:{avail}/{expected}<{min_ratio:.4f}")

    return failures
