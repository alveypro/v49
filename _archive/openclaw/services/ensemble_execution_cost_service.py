from __future__ import annotations

import sqlite3
from typing import Any, Dict, Sequence


JsonDict = Dict[str, Any]


def build_ensemble_execution_cost_replay(
    conn: sqlite3.Connection,
    shadow_portfolio: JsonDict | None,
    *,
    as_of_date: str,
    holding_days: int = 5,
    commission_bps: float = 3.0,
    stamp_tax_bps: float = 5.0,
    base_slippage_bps: float = 8.0,
    impact_bps_per_10pct_adv: float = 12.0,
    max_capacity_usage: float = 0.1,
) -> JsonDict:
    """Replay research-only shadow portfolio after costs and tradeability gates."""

    portfolio = shadow_portfolio if isinstance(shadow_portfolio, dict) else {}
    weights = portfolio.get("shadow_weights") if isinstance(portfolio.get("shadow_weights"), list) else []
    normalized_date = _compact_date(as_of_date)
    blocking = []
    if portfolio.get("research_only") is not True:
        blocking.append("shadow_portfolio_not_research_only")
    if portfolio.get("not_for_production") is not True:
        blocking.append("shadow_portfolio_missing_not_for_production")
    if not weights:
        blocking.append("missing_shadow_weights")

    capacity_blocked: list[str] = []
    limit_blocked: list[str] = []
    suspension_blocked: list[str] = []
    rows: list[JsonDict] = []
    gross_return = 0.0
    traded_weight = 0.0
    cost_bps_weighted = 0.0
    slippage_bps_weighted = 0.0

    for item in weights:
        code = str(item.get("ts_code") or "")
        weight = max(0.0, float(item.get("weight", 0.0) or 0.0))
        if not code or weight <= 0.0:
            continue
        replay = _price_replay(conn, ts_code=code, as_of_date=normalized_date, holding_days=int(holding_days or 0))
        capacity_usage = _capacity_usage(portfolio, item)
        blocked_reason = ""
        if replay.get("available") is not True:
            blocked_reason = str(replay.get("blocking_reason") or "price_replay_unavailable")
            suspension_blocked.append(code)
        elif replay.get("entry_limit_up") is True or replay.get("exit_limit_down") is True:
            blocked_reason = "limit_state_blocked"
            limit_blocked.append(code)
        elif capacity_usage > float(max_capacity_usage):
            blocked_reason = "capacity_usage_exceeds_limit"
            capacity_blocked.append(code)

        if blocked_reason:
            rows.append(
                {
                    "ts_code": code,
                    "weight": round(weight, 6),
                    "traded": False,
                    "blocking_reason": blocked_reason,
                    "capacity_usage": round(capacity_usage, 6),
                    "price_replay": replay,
                }
            )
            continue

        ret = float(replay.get("return_pct", 0.0) or 0.0)
        impact = float(impact_bps_per_10pct_adv) * (capacity_usage / 0.1) if capacity_usage > 0.0 else 0.0
        slippage_bps = float(base_slippage_bps) + impact
        cost_bps = float(commission_bps) * 2.0 + float(stamp_tax_bps)
        gross_return += weight * ret
        traded_weight += weight
        cost_bps_weighted += weight * cost_bps
        slippage_bps_weighted += weight * slippage_bps
        rows.append(
            {
                "ts_code": code,
                "weight": round(weight, 6),
                "traded": True,
                "gross_return_pct": round(ret, 6),
                "cost_bps": round(cost_bps, 6),
                "slippage_bps": round(slippage_bps, 6),
                "capacity_usage": round(capacity_usage, 6),
                "price_replay": replay,
            }
        )

    total_bps = cost_bps_weighted + slippage_bps_weighted
    return {
        "execution_replay_version": "ensemble_execution_cost_replay.v1",
        "research_only": True,
        "not_for_production": True,
        "as_of_date": normalized_date,
        "holding_days": int(holding_days or 0),
        "gross_return": round(gross_return, 6),
        "cost_bps": round(cost_bps_weighted, 6),
        "slippage_bps": round(slippage_bps_weighted, 6),
        "capacity_blocked_names": sorted(set(capacity_blocked)),
        "limit_state_blocked_names": sorted(set(limit_blocked)),
        "suspension_blocked_names": sorted(set(suspension_blocked)),
        "net_return": round(gross_return - total_bps / 100.0, 6),
        "turnover": round(traded_weight, 6),
        "trade_replay": rows,
        "blocking_reasons": blocking,
        "tradeability_source": "daily_trading_data_pct_chg_limit_proxy",
        "hard_boundaries": [
            "do_not_treat_after_cost_replay_as_alpha_evidence",
            "do_not_promote_without_formal_pool_shadow_benchmark",
            "do_not_ignore_limit_or_capacity_blocks",
        ],
    }


def _price_replay(conn: sqlite3.Connection, *, ts_code: str, as_of_date: str, holding_days: int) -> JsonDict:
    if not _table_exists(conn, "daily_trading_data"):
        return {"available": False, "blocking_reason": "missing_daily_trading_data"}
    try:
        rows = conn.execute(
            """
            SELECT trade_date, close_price, pct_chg, amount
            FROM daily_trading_data
            WHERE ts_code = ?
              AND REPLACE(trade_date, '-', '') >= ?
              AND close_price IS NOT NULL
            ORDER BY REPLACE(trade_date, '-', '') ASC
            LIMIT ?
            """,
            (str(ts_code), str(as_of_date), int(holding_days) + 1),
        ).fetchall()
    except sqlite3.Error:
        return {"available": False, "blocking_reason": "price_replay_query_failed"}
    if len(rows) < int(holding_days) + 1:
        return {"available": False, "blocking_reason": "insufficient_forward_price_window", "price_count": len(rows)}
    entry_close = float(rows[0][1] or 0.0)
    exit_close = float(rows[int(holding_days)][1] or 0.0)
    if entry_close <= 0.0 or exit_close <= 0.0:
        return {"available": False, "blocking_reason": "invalid_close_price"}
    entry_pct = float(rows[0][2] or 0.0)
    exit_pct = float(rows[int(holding_days)][2] or 0.0)
    return {
        "available": True,
        "entry_trade_date": str(rows[0][0] or ""),
        "exit_trade_date": str(rows[int(holding_days)][0] or ""),
        "entry_limit_up": entry_pct >= 9.5,
        "exit_limit_down": exit_pct <= -9.5,
        "entry_amount": float(rows[0][3] or 0.0),
        "exit_amount": float(rows[int(holding_days)][3] or 0.0),
        "return_pct": (exit_close / entry_close - 1.0) * 100.0,
    }


def _capacity_usage(portfolio: JsonDict, item: JsonDict) -> float:
    usage = portfolio.get("capacity_usage") if isinstance(portfolio.get("capacity_usage"), dict) else {}
    code = str(item.get("ts_code") or "")
    if code in usage:
        return max(0.0, float(usage.get(code, 0.0) or 0.0))
    amount = float(item.get("latest_amount", 0.0) or 0.0)
    weight = float(item.get("weight", 0.0) or 0.0)
    return weight / amount if amount > 0.0 else 0.0


def _compact_date(value: Any) -> str:
    return str(value or "").strip().replace("-", "")


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    try:
        row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (str(table),)).fetchone()
    except sqlite3.Error:
        return False
    return bool(row)
