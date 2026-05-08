from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REALISTIC_BACKTEST_VERSION = "candidate_realistic_backtest.v1"
TRANSACTION_COST_BREAKDOWN_VERSION = "candidate_transaction_cost_breakdown.v1"
CAPACITY_CONSTRAINT_REPORT_VERSION = "candidate_capacity_constraint_report.v1"


@dataclass(frozen=True)
class RealisticBacktestConfig:
    hold_trade_days: int = 5
    main_board_limit: float = 0.10
    chinext_limit: float = 0.20
    star_limit: float = 0.20
    min_amount: float = 300_000.0
    portfolio_notional: float = 1_000_000.0
    commission_rate: float = 0.0003
    min_commission: float = 5.0
    slippage_rate: float = 0.0005
    stamp_tax_rate: float = 0.001
    impact_cost_coefficient: float = 0.001
    capacity_warn_participation_rate: float = 0.05
    capacity_block_participation_rate: float = 0.10


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _limit_ratio(ts_code: str, config: RealisticBacktestConfig) -> float:
    code = str(ts_code or "").strip().upper()
    if code.startswith("300") or code.startswith("301"):
        return config.chinext_limit
    if code.startswith("688") or code.startswith("689"):
        return config.star_limit
    return config.main_board_limit


def _is_limit_up(row: dict[str, Any], config: RealisticBacktestConfig) -> bool:
    pre_close = _to_float(row.get("pre_close"))
    close_price = _to_float(row.get("close_price"))
    if pre_close <= 0 or close_price <= 0:
        return False
    return close_price / pre_close - 1.0 >= _limit_ratio(str(row.get("ts_code", "")), config) - 1e-6


def _is_limit_down(row: dict[str, Any], config: RealisticBacktestConfig) -> bool:
    pre_close = _to_float(row.get("pre_close"))
    close_price = _to_float(row.get("close_price"))
    if pre_close <= 0 or close_price <= 0:
        return False
    return close_price / pre_close - 1.0 <= -_limit_ratio(str(row.get("ts_code", "")), config) + 1e-6


def _is_tradeable_row(row: dict[str, Any] | None, config: RealisticBacktestConfig) -> tuple[bool, str]:
    if not row:
        return False, "missing_market_row"
    if _to_float(row.get("open_price")) <= 0 or _to_float(row.get("close_price")) <= 0:
        return False, "invalid_price"
    if _to_float(row.get("vol")) <= 0 or _to_float(row.get("amount")) <= 0:
        return False, "suspended_or_no_trade"
    if _to_float(row.get("amount")) < config.min_amount:
        return False, "low_liquidity"
    return True, "ok"


def _transaction_costs(
    *,
    notional: float,
    buy_amount: float,
    sell_amount: float,
    gross_return: float,
    config: RealisticBacktestConfig,
) -> dict[str, float]:
    buy_commission = max(notional * config.commission_rate, config.min_commission)
    sell_notional = notional * (1.0 + gross_return)
    sell_commission = max(max(sell_notional, 0.0) * config.commission_rate, config.min_commission)
    stamp_tax = max(sell_notional, 0.0) * config.stamp_tax_rate
    slippage_cost = notional * config.slippage_rate + max(sell_notional, 0.0) * config.slippage_rate
    buy_participation = notional / buy_amount if buy_amount > 0 else 1.0
    sell_participation = max(sell_notional, 0.0) / sell_amount if sell_amount > 0 else 1.0
    impact_cost = notional * config.impact_cost_coefficient * buy_participation
    impact_cost += max(sell_notional, 0.0) * config.impact_cost_coefficient * sell_participation
    total_cost = buy_commission + sell_commission + stamp_tax + slippage_cost + impact_cost
    return {
        "buy_commission": round(buy_commission, 4),
        "sell_commission": round(sell_commission, 4),
        "stamp_tax": round(stamp_tax, 4),
        "slippage_cost": round(slippage_cost, 4),
        "impact_cost": round(impact_cost, 4),
        "total_transaction_cost": round(total_cost, 4),
        "cost_return_drag": round(total_cost / notional if notional > 0 else 0.0, 8),
        "buy_participation_rate": round(buy_participation, 8),
        "sell_participation_rate": round(sell_participation, 8),
    }


def _capacity_state(max_participation: float, config: RealisticBacktestConfig) -> tuple[str, list[str]]:
    if max_participation > config.capacity_block_participation_rate:
        return "blocked", ["capacity_participation_exceeds_block_limit"]
    if max_participation > config.capacity_warn_participation_rate:
        return "review", ["capacity_participation_exceeds_watch_limit"]
    return "passed", []


def _fetch_trade_dates(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"SELECT DISTINCT trade_date FROM {table} ORDER BY trade_date").fetchall()
    return [str(row[0]) for row in rows if row and row[0]]


def _fetch_rows(
    conn: sqlite3.Connection,
    table: str,
    ts_codes: list[str],
    trade_dates: list[str],
) -> dict[tuple[str, str], dict[str, Any]]:
    if not ts_codes or not trade_dates:
        return {}
    code_placeholders = ",".join("?" for _ in ts_codes)
    date_placeholders = ",".join("?" for _ in trade_dates)
    cursor = conn.execute(
        f"""
        SELECT ts_code, trade_date, open_price, high_price, low_price, close_price,
               pre_close, pct_chg, vol, amount
        FROM {table}
        WHERE ts_code IN ({code_placeholders})
          AND trade_date IN ({date_placeholders})
        ORDER BY ts_code, trade_date
        """,
        [*ts_codes, *trade_dates],
    )
    columns = [desc[0] for desc in cursor.description]
    return {
        (str(item["ts_code"]), str(item["trade_date"])): item
        for item in (dict(zip(columns, row)) for row in cursor.fetchall())
    }


def _candidate_codes_from_lineage(lineage: dict[str, Any]) -> list[str]:
    codes: list[str] = []
    seen: set[str] = set()
    for item in lineage.get("candidates", []) or []:
        code = str(item.get("ts_code", "") or "").strip()
        if code and code not in seen:
            seen.add(code)
            codes.append(code)
    return codes


def build_realistic_backtest(
    *,
    db_path: str | Path,
    lineage: dict[str, Any],
    table: str = "daily_trading_data",
    config: RealisticBacktestConfig | None = None,
) -> dict[str, Any]:
    config = config or RealisticBacktestConfig()
    resolved_db_path = Path(db_path)
    if not resolved_db_path.exists():
        return {
            "schema_version": REALISTIC_BACKTEST_VERSION,
            "status": "failed",
            "generated_at": _utc_now(),
            "blocking_reasons": ["database_missing"],
            "db_path": str(resolved_db_path),
            "table": table,
            "candidates": [],
            "summary": {},
        }

    data_as_of = str(lineage.get("data_as_of", "") or "").strip()
    candidate_codes = _candidate_codes_from_lineage(lineage)
    if not data_as_of:
        return {
            "schema_version": REALISTIC_BACKTEST_VERSION,
            "status": "failed",
            "generated_at": _utc_now(),
            "blocking_reasons": ["missing_data_as_of"],
            "db_path": str(resolved_db_path),
            "table": table,
            "candidates": [],
            "summary": {},
        }

    conn = sqlite3.connect(str(resolved_db_path))
    try:
        trade_dates = _fetch_trade_dates(conn, table)
        after_dates = [date for date in trade_dates if date > data_as_of]
        needed_dates = after_dates[: max(config.hold_trade_days + 3, 1)]
        rows_by_key = _fetch_rows(conn, table, candidate_codes, needed_dates)
    finally:
        conn.close()

    candidate_results: list[dict[str, Any]] = []
    blocked_reasons: dict[str, int] = {}

    for code in candidate_codes:
        per_candidate_notional = config.portfolio_notional / max(len(candidate_codes), 1)
        if len(after_dates) <= config.hold_trade_days:
            result = {
                "ts_code": code,
                "status": "blocked",
                "blocking_reasons": ["insufficient_future_trade_dates"],
                "buy_trade_date": None,
                "sell_trade_date": None,
                "ideal_return": None,
                "realistic_return": None,
            }
            candidate_results.append(result)
            continue
        buy_date = after_dates[0]
        sell_date = after_dates[config.hold_trade_days]
        buy_row = rows_by_key.get((code, buy_date))
        sell_row = rows_by_key.get((code, sell_date))
        buy_ok, buy_reason = _is_tradeable_row(buy_row, config)
        sell_ok, sell_reason = _is_tradeable_row(sell_row, config)
        reasons: list[str] = []
        if buy_date not in trade_dates or sell_date not in trade_dates:
            reasons.append("non_trade_date")
        if not buy_ok:
            reasons.append(f"buy_{buy_reason}")
        elif _is_limit_up(buy_row or {}, config):
            reasons.append("buy_limit_up")
        if not sell_ok:
            reasons.append(f"sell_{sell_reason}")
        elif _is_limit_down(sell_row or {}, config):
            reasons.append("sell_limit_down")

        if reasons:
            candidate_results.append(
                {
                    "ts_code": code,
                    "status": "blocked",
                    "blocking_reasons": reasons,
                    "buy_trade_date": buy_date,
                    "sell_trade_date": sell_date,
                    "ideal_return": None,
                    "realistic_return": None,
                }
            )
            continue

        buy_price = _to_float((buy_row or {}).get("open_price"))
        sell_price = _to_float((sell_row or {}).get("close_price"))
        gross_realistic_return = sell_price / buy_price - 1.0 if buy_price > 0 else 0.0
        ideal_buy = _to_float((buy_row or {}).get("close_price"))
        ideal_return = sell_price / ideal_buy - 1.0 if ideal_buy > 0 else gross_realistic_return
        buy_amount = _to_float((buy_row or {}).get("amount"))
        sell_amount = _to_float((sell_row or {}).get("amount"))
        costs = _transaction_costs(
            notional=per_candidate_notional,
            buy_amount=buy_amount,
            sell_amount=sell_amount,
            gross_return=gross_realistic_return,
            config=config,
        )
        realistic_return = gross_realistic_return - float(costs["cost_return_drag"])
        max_participation = max(float(costs["buy_participation_rate"]), float(costs["sell_participation_rate"]))
        capacity_state, capacity_reasons = _capacity_state(max_participation, config)
        candidate_results.append(
            {
                "ts_code": code,
                "status": capacity_state,
                "blocking_reasons": capacity_reasons if capacity_state == "blocked" else [],
                "review_reasons": capacity_reasons if capacity_state == "review" else [],
                "buy_trade_date": buy_date,
                "sell_trade_date": sell_date,
                "buy_price": round(buy_price, 4),
                "sell_price": round(sell_price, 4),
                "ideal_return": round(ideal_return, 6),
                "gross_realistic_return": round(gross_realistic_return, 6),
                "realistic_return": round(realistic_return, 6),
                "transaction_costs": costs,
                "capacity": {
                    "state": capacity_state,
                    "notional": round(per_candidate_notional, 4),
                    "buy_amount": round(buy_amount, 4),
                    "sell_amount": round(sell_amount, 4),
                    "max_participation_rate": round(max_participation, 8),
                    "warn_participation_rate": config.capacity_warn_participation_rate,
                    "block_participation_rate": config.capacity_block_participation_rate,
                    "reasons": capacity_reasons,
                },
            }
        )

    passed = [item for item in candidate_results if item["status"] == "passed"]
    review = [item for item in candidate_results if item["status"] == "review"]
    blocked = [item for item in candidate_results if item["status"] == "blocked"]
    for item in review:
        for reason in item.get("review_reasons", []) or []:
            blocked_reasons[reason] = blocked_reasons.get(reason, 0) + 1
    for item in blocked:
        for reason in item.get("blocking_reasons", []) or []:
            blocked_reasons[reason] = blocked_reasons.get(reason, 0) + 1
    status = "failed" if blocked else "review" if review else "passed"
    evaluable = [item for item in candidate_results if item["status"] in {"passed", "review"}]
    avg_realistic = (
        sum(float(item["realistic_return"]) for item in evaluable) / len(evaluable)
        if evaluable
        else 0.0
    )
    avg_ideal = (
        sum(float(item["ideal_return"]) for item in evaluable) / len(evaluable)
        if evaluable
        else 0.0
    )
    total_cost = sum(float((item.get("transaction_costs", {}) or {}).get("total_transaction_cost", 0.0)) for item in evaluable)
    return {
        "schema_version": REALISTIC_BACKTEST_VERSION,
        "status": status,
        "generated_at": _utc_now(),
        "blocking_reasons": sorted(blocked_reasons),
        "db_path": str(resolved_db_path),
        "table": table,
        "lineage_run_id": lineage.get("run_id"),
        "data_as_of": data_as_of,
        "hold_trade_days": int(config.hold_trade_days),
        "summary": {
            "candidate_count": len(candidate_results),
            "passed_count": len(passed),
            "review_count": len(review),
            "blocked_count": len(blocked),
            "avg_realistic_return": round(avg_realistic, 6),
            "avg_ideal_return": round(avg_ideal, 6),
            "return_decay": round(avg_ideal - avg_realistic, 6),
            "total_transaction_cost": round(total_cost, 4),
            "rule_block_stats": blocked_reasons,
        },
        "candidates": candidate_results,
    }


def build_transaction_cost_breakdown(backtest: dict[str, Any]) -> dict[str, Any]:
    rows = []
    totals = {
        "buy_commission": 0.0,
        "sell_commission": 0.0,
        "stamp_tax": 0.0,
        "slippage_cost": 0.0,
        "impact_cost": 0.0,
        "total_transaction_cost": 0.0,
    }
    for item in backtest.get("candidates", []) or []:
        costs = dict(item.get("transaction_costs", {}) or {})
        if not costs:
            continue
        for key in totals:
            totals[key] += float(costs.get(key, 0.0) or 0.0)
        rows.append(
            {
                "ts_code": item.get("ts_code"),
                "status": item.get("status"),
                "buy_trade_date": item.get("buy_trade_date"),
                "sell_trade_date": item.get("sell_trade_date"),
                **costs,
                "gross_realistic_return": item.get("gross_realistic_return"),
                "realistic_return": item.get("realistic_return"),
            }
        )
    return {
        "schema_version": TRANSACTION_COST_BREAKDOWN_VERSION,
        "status": "passed" if rows else "failed",
        "generated_at": _utc_now(),
        "lineage_run_id": backtest.get("lineage_run_id"),
        "data_as_of": backtest.get("data_as_of"),
        "summary": {key: round(value, 4) for key, value in totals.items()},
        "candidates": rows,
    }


def build_capacity_constraint_report(backtest: dict[str, Any]) -> dict[str, Any]:
    rows = []
    state_counts: dict[str, int] = {}
    worst_participation = 0.0
    for item in backtest.get("candidates", []) or []:
        capacity = dict(item.get("capacity", {}) or {})
        if not capacity:
            continue
        state = str(capacity.get("state", "unknown") or "unknown")
        state_counts[state] = state_counts.get(state, 0) + 1
        worst_participation = max(worst_participation, float(capacity.get("max_participation_rate", 0.0) or 0.0))
        rows.append({"ts_code": item.get("ts_code"), "status": item.get("status"), **capacity})
    if state_counts.get("blocked", 0):
        status = "failed"
    elif state_counts.get("review", 0):
        status = "review"
    elif rows:
        status = "passed"
    else:
        status = "failed"
    return {
        "schema_version": CAPACITY_CONSTRAINT_REPORT_VERSION,
        "status": status,
        "generated_at": _utc_now(),
        "lineage_run_id": backtest.get("lineage_run_id"),
        "data_as_of": backtest.get("data_as_of"),
        "summary": {
            "candidate_count": len(rows),
            "state_counts": state_counts,
            "worst_participation_rate": round(worst_participation, 8),
        },
        "candidates": rows,
    }


def write_realistic_backtest(
    payload: dict[str, Any],
    *,
    output_dir: str | Path,
    filename: str = "realistic_backtest_latest.json",
) -> str:
    resolved_output_dir = Path(output_dir)
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    path = resolved_output_dir / filename
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)


def write_transaction_cost_breakdown(
    payload: dict[str, Any],
    *,
    output_dir: str | Path,
    filename: str = "transaction_cost_breakdown_latest.json",
) -> str:
    resolved_output_dir = Path(output_dir)
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    path = resolved_output_dir / filename
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)


def write_capacity_constraint_report(
    payload: dict[str, Any],
    *,
    output_dir: str | Path,
    filename: str = "capacity_constraint_report_latest.json",
) -> str:
    resolved_output_dir = Path(output_dir)
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    path = resolved_output_dir / filename
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)
