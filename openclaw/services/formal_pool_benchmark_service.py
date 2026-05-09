from __future__ import annotations

import sqlite3
from typing import Any, Dict, Sequence


JsonDict = Dict[str, Any]


def build_formal_pool_benchmark_return_series(
    conn: sqlite3.Connection,
    *,
    strategies: Sequence[str],
    as_of_date: str,
    holding_days: int,
    top_n_per_strategy: int = 5,
) -> JsonDict:
    """Build a replayable benchmark return series from formal strategy signals."""

    strategy_list = _unique_strategies(strategies)
    normalized_as_of_date = _compact_date(as_of_date)
    contract: JsonDict = {
        "benchmark_version": "formal_pool_benchmark_return_series.v1",
        "method": "equal_weight_forward_return_from_formal_strategy_scan_items",
        "as_of_date": normalized_as_of_date,
        "holding_days": int(holding_days or 0),
        "top_n_per_strategy": int(top_n_per_strategy or 0),
        "strategies": strategy_list,
        "no_proxy_metrics": True,
    }
    blocking: list[str] = []
    if not strategy_list:
        blocking.append("missing_formal_pool_strategy_list")
    if not normalized_as_of_date:
        blocking.append("missing_as_of_date")
    if int(holding_days or 0) <= 0:
        blocking.append("invalid_holding_days")
    if not _table_exists(conn, "signal_runs"):
        blocking.append("missing_signal_runs_table")
    if not _table_exists(conn, "signal_items"):
        blocking.append("missing_signal_items_table")
    if not _table_exists(conn, "daily_trading_data"):
        blocking.append("missing_daily_trading_data_table")
    if blocking:
        return _blocked(contract=contract, blocking=blocking)

    runs = []
    missing_runs = []
    for strategy in strategy_list:
        run = _latest_scan_run(conn, strategy=strategy, as_of_date=normalized_as_of_date)
        if not run:
            missing_runs.append(strategy)
            continue
        runs.append(run)
    if missing_runs:
        blocking.append("missing_formal_pool_scan_runs:" + ",".join(missing_runs))

    signal_rows = []
    for run in runs:
        items = _top_signal_items(conn, run_id=str(run["run_id"]), limit=int(top_n_per_strategy or 0))
        if not items:
            blocking.append(f"missing_signal_items:{run['strategy']}")
            continue
        for item in items:
            item["strategy"] = run["strategy"]
            item["run_id"] = run["run_id"]
            item["run_trade_date"] = run["trade_date"]
            signal_rows.append(item)

    returns = []
    replay_rows = []
    return_blocking_counts: Dict[str, int] = {}
    for item in signal_rows:
        replay = _forward_return_pct(
            conn,
            ts_code=str(item.get("ts_code") or ""),
            as_of_date=normalized_as_of_date,
            holding_days=int(holding_days),
        )
        replay_row = {**item, **replay}
        replay_rows.append(replay_row)
        if replay.get("available") is True:
            returns.append(float(replay.get("return_pct", 0.0) or 0.0))
        else:
            reason = str(replay.get("blocking_reason") or "unknown_return_replay_failure")
            return_blocking_counts[reason] = int(return_blocking_counts.get(reason, 0) or 0) + 1

    if not returns:
        blocking.append("no_replayable_formal_pool_forward_returns")
    if return_blocking_counts:
        blocking.append(
            "return_replay_blocking_counts:"
            + ",".join(f"{key}={value}" for key, value in sorted(return_blocking_counts.items()))
        )

    return {
        "available": bool(returns),
        "contract": contract,
        "blocking_reasons": sorted(set(blocking)),
        "run_count": len(runs),
        "signal_count": len(signal_rows),
        "replayable_signal_count": len(returns),
        "return_series_pct": returns,
        "avg_return_pct": (sum(returns) / float(len(returns))) if returns else None,
        "replay_rows": replay_rows[:100],
    }


def _blocked(*, contract: JsonDict, blocking: Sequence[str]) -> JsonDict:
    return {
        "available": False,
        "contract": contract,
        "blocking_reasons": sorted(set(str(item) for item in blocking if str(item or ""))),
        "run_count": 0,
        "signal_count": 0,
        "replayable_signal_count": 0,
        "return_series_pct": [],
        "avg_return_pct": None,
        "replay_rows": [],
    }


def _unique_strategies(strategies: Sequence[str]) -> list[str]:
    out: list[str] = []
    for raw in strategies or []:
        strategy = str(raw or "").strip().lower()
        if strategy and strategy not in out:
            out.append(strategy)
    return out


def _latest_scan_run(conn: sqlite3.Connection, *, strategy: str, as_of_date: str) -> JsonDict:
    rows = conn.execute(
        """
        SELECT run_id, strategy, trade_date, created_at
        FROM signal_runs
        WHERE run_type = 'scan'
          AND strategy = ?
          AND status = 'success'
          AND trade_date != ''
          AND REPLACE(trade_date, '-', '') <= ?
        ORDER BY REPLACE(trade_date, '-', '') DESC, created_at DESC
        LIMIT 1
        """,
        (str(strategy), str(as_of_date)),
    ).fetchall()
    if not rows:
        return {}
    row = rows[0]
    return {
        "run_id": str(row[0] or ""),
        "strategy": str(row[1] or ""),
        "trade_date": str(row[2] or ""),
        "created_at": str(row[3] or ""),
    }


def _top_signal_items(conn: sqlite3.Connection, *, run_id: str, limit: int) -> list[JsonDict]:
    if limit <= 0:
        return []
    rows = conn.execute(
        """
        SELECT ts_code, score, rank_idx
        FROM signal_items
        WHERE run_id = ?
        ORDER BY rank_idx ASC, score DESC
        LIMIT ?
        """,
        (str(run_id), int(limit)),
    ).fetchall()
    return [
        {
            "ts_code": str(row[0] or ""),
            "score": float(row[1] or 0.0),
            "rank_idx": int(row[2] or 0),
        }
        for row in rows
    ]


def _forward_return_pct(
    conn: sqlite3.Connection,
    *,
    ts_code: str,
    as_of_date: str,
    holding_days: int,
) -> JsonDict:
    if not ts_code:
        return {"available": False, "blocking_reason": "missing_ts_code"}
    rows = conn.execute(
        """
        SELECT trade_date, close_price
        FROM daily_trading_data
        WHERE ts_code = ?
          AND trade_date >= ?
          AND close_price IS NOT NULL
        ORDER BY trade_date ASC
        LIMIT ?
        """,
        (str(ts_code), _compact_date(as_of_date), int(holding_days) + 1),
    ).fetchall()
    if len(rows) < int(holding_days) + 1:
        return {
            "available": False,
            "blocking_reason": "insufficient_forward_price_window",
            "price_count": len(rows),
        }
    entry = rows[0]
    exit_row = rows[int(holding_days)]
    entry_price = float(entry[1] or 0.0)
    exit_price = float(exit_row[1] or 0.0)
    if entry_price <= 0.0 or exit_price <= 0.0:
        return {"available": False, "blocking_reason": "invalid_forward_price"}
    return {
        "available": True,
        "entry_trade_date": str(entry[0] or ""),
        "exit_trade_date": str(exit_row[0] or ""),
        "entry_close": entry_price,
        "exit_close": exit_price,
        "return_pct": (exit_price / entry_price - 1.0) * 100.0,
    }


def _compact_date(value: Any) -> str:
    return str(value or "").strip().replace("-", "")


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    try:
        row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (str(table),)).fetchone()
    except sqlite3.Error:
        return False
    return bool(row)
