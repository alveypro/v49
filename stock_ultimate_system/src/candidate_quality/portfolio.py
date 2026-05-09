from __future__ import annotations

import csv
import json
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CANDIDATE_PORTFOLIO_VERSION = "candidate_portfolio.v1"
PORTFOLIO_EXPOSURE_REPORT_VERSION = "portfolio_exposure_report.v1"
PORTFOLIO_CAPACITY_REPORT_VERSION = "portfolio_capacity_report.v1"
CANDIDATE_PORTFOLIO_QUALITY_VERSION = "candidate_portfolio_quality.v1"


@dataclass(frozen=True)
class CandidatePortfolioConfig:
    max_single_weight: float = 0.35
    target_max_industry_weight: float = 0.50
    hard_max_industry_weight: float = 0.65
    high_correlation_threshold: float = 0.85
    correlation_lookback_trade_days: int = 60
    min_correlation_points: int = 20
    high_correlation_trim_ratio: float = 0.20
    portfolio_notional: float = 1_000_000.0
    min_amount: float = 300_000.0
    capacity_warn_participation_rate: float = 0.05
    capacity_block_participation_rate: float = 0.10
    impact_cost_coefficient: float = 0.001
    min_portfolio_quality_score: float = 60.0


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _read_csv_rows(path: str | Path | None) -> list[dict[str, str]]:
    if not path or not Path(path).exists():
        return []
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        parsed = float(value)
        if math.isnan(parsed) or math.isinf(parsed):
            return default
        return parsed
    except Exception:
        return default


def _date_key(value: object) -> str:
    return str(value or "").strip()[:10].replace("-", "")


def _normalize_weights(weights: list[float]) -> list[float]:
    cleaned = [max(float(weight or 0.0), 0.0) for weight in weights]
    total = sum(cleaned)
    if total <= 0:
        return [round(1.0 / len(cleaned), 6) for _ in cleaned] if cleaned else []
    normalized = [round(weight / total, 6) for weight in cleaned]
    if normalized:
        normalized[-1] = round(1.0 - sum(normalized[:-1]), 6)
    return normalized


def _cap_single_weights(weights: list[float], max_single_weight: float) -> list[float]:
    if not weights:
        return []
    capped = [min(max(weight, 0.0), max_single_weight) for weight in weights]
    for _ in range(8):
        leftover = 1.0 - sum(capped)
        if leftover <= 1e-9:
            break
        headroom = [max(max_single_weight - weight, 0.0) for weight in capped]
        total_headroom = sum(headroom)
        if total_headroom <= 1e-12:
            break
        capped = [weight + leftover * room / total_headroom for weight, room in zip(capped, headroom)]
        capped = [min(weight, max_single_weight) for weight in capped]
    return _normalize_weights(capped)


def _industry_weights(items: list[dict[str, Any]]) -> dict[str, float]:
    weights: dict[str, float] = {}
    for item in items:
        industry = str(item.get("industry") or "unknown")
        weights[industry] = weights.get(industry, 0.0) + float(item.get("weight", 0.0) or 0.0)
    return {industry: round(weight, 6) for industry, weight in sorted(weights.items(), key=lambda pair: pair[1], reverse=True)}


def _apply_industry_hard_cap(
    items: list[dict[str, Any]],
    *,
    hard_max_industry_weight: float,
    max_single_weight: float,
) -> tuple[list[dict[str, Any]], list[str]]:
    if not items:
        return items, []
    adjustments: list[str] = []
    for _ in range(8):
        industry_map = _industry_weights(items)
        overweight = [(industry, weight) for industry, weight in industry_map.items() if weight > hard_max_industry_weight + 1e-9]
        if not overweight:
            break
        industry, industry_weight = overweight[0]
        excess = industry_weight - hard_max_industry_weight
        donor_indexes = [idx for idx, item in enumerate(items) if str(item.get("industry") or "unknown") == industry]
        receiver_indexes = [idx for idx, item in enumerate(items) if str(item.get("industry") or "unknown") != industry]
        receiver_headroom = [
            max(max_single_weight - float(items[idx].get("weight", 0.0) or 0.0), 0.0) for idx in receiver_indexes
        ]
        movable = min(excess, sum(receiver_headroom))
        if movable <= 1e-9:
            adjustments.append(f"industry_cap_unresolved:{industry}")
            break
        donor_weight = sum(float(items[idx].get("weight", 0.0) or 0.0) for idx in donor_indexes)
        for idx in donor_indexes:
            current = float(items[idx].get("weight", 0.0) or 0.0)
            trim = movable * current / donor_weight if donor_weight > 0 else 0.0
            items[idx]["weight"] = round(max(current - trim, 0.0), 6)
            items[idx]["portfolio_adjustments"].append("industry_hard_cap_trim")
        total_headroom = sum(receiver_headroom)
        for idx, room in zip(receiver_indexes, receiver_headroom):
            add = movable * room / total_headroom if total_headroom > 0 else 0.0
            items[idx]["weight"] = round(float(items[idx].get("weight", 0.0) or 0.0) + add, 6)
            items[idx]["portfolio_adjustments"].append("industry_hard_cap_redistribute")
        adjustments.append(f"industry_hard_cap_applied:{industry}")
    normalized = _normalize_weights([float(item.get("weight", 0.0) or 0.0) for item in items])
    for item, weight in zip(items, normalized):
        item["weight"] = weight
    return items, adjustments


def _fetch_trade_dates(conn: sqlite3.Connection, table: str, snapshot_date: str, lookback: int) -> list[str]:
    rows = conn.execute(
        f"""
        SELECT DISTINCT trade_date
        FROM {table}
        WHERE trade_date <= ?
        ORDER BY trade_date DESC
        LIMIT ?
        """,
        (snapshot_date, max(int(lookback) + 1, 2)),
    ).fetchall()
    return [str(row[0]) for row in reversed(rows) if row and row[0]]


def _fetch_close_series(
    conn: sqlite3.Connection,
    table: str,
    ts_codes: list[str],
    trade_dates: list[str],
) -> dict[str, list[float | None]]:
    if not ts_codes or not trade_dates:
        return {}
    code_placeholders = ",".join("?" for _ in ts_codes)
    date_placeholders = ",".join("?" for _ in trade_dates)
    rows = conn.execute(
        f"""
        SELECT ts_code, trade_date, close_price
        FROM {table}
        WHERE ts_code IN ({code_placeholders})
          AND trade_date IN ({date_placeholders})
        """,
        [*ts_codes, *trade_dates],
    ).fetchall()
    by_key = {(str(code), str(date)): _to_float(close, 0.0) for code, date, close in rows}
    series: dict[str, list[float | None]] = {}
    for code in ts_codes:
        values: list[float | None] = []
        for date in trade_dates:
            close = by_key.get((code, date), 0.0)
            values.append(close if close > 0 else None)
        series[code] = values
    return series


def _fetch_capacity_rows(
    conn: sqlite3.Connection,
    table: str,
    ts_codes: list[str],
    snapshot_date: str,
) -> dict[str, dict[str, Any]]:
    if not ts_codes:
        return {}
    placeholders = ",".join("?" for _ in ts_codes)
    cursor = conn.execute(
        f"""
        SELECT ts_code, trade_date, close_price, vol, amount
        FROM {table}
        WHERE ts_code IN ({placeholders})
          AND trade_date = (
              SELECT MAX(trade_date)
              FROM {table} AS inner_t
              WHERE inner_t.ts_code = {table}.ts_code
                AND inner_t.trade_date <= ?
          )
        """,
        [*ts_codes, snapshot_date],
    )
    columns = [desc[0] for desc in cursor.description]
    return {str(row[0]): dict(zip(columns, row)) for row in cursor.fetchall()}


def _returns_from_closes(closes: list[float | None]) -> list[float | None]:
    returns: list[float | None] = []
    previous: float | None = None
    for close in closes:
        if previous is None or close is None or previous <= 0:
            returns.append(None)
        else:
            returns.append(close / previous - 1.0)
        if close is not None and close > 0:
            previous = close
    return returns


def _pearson(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) != len(ys) or len(xs) < 2:
        return None
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    var_x = sum((x - mean_x) ** 2 for x in xs)
    var_y = sum((y - mean_y) ** 2 for y in ys)
    denom = math.sqrt(var_x * var_y)
    if denom <= 0:
        return None
    return cov / denom


def _build_correlation_report(
    *,
    sqlite_db_path: str | Path,
    sqlite_table: str,
    snapshot_date: str,
    items: list[dict[str, Any]],
    config: CandidatePortfolioConfig,
) -> dict[str, Any]:
    if not Path(sqlite_db_path).exists():
        return {
            "status": "review",
            "blocking_reasons": ["correlation_price_db_missing"],
            "pair_count": 0,
            "high_correlation_pairs": [],
            "pairs": [],
        }
    codes = [str(item.get("ts_code") or "") for item in items if item.get("ts_code")]
    conn = sqlite3.connect(str(sqlite_db_path))
    try:
        trade_dates = _fetch_trade_dates(conn, sqlite_table, snapshot_date, config.correlation_lookback_trade_days)
        close_series = _fetch_close_series(conn, sqlite_table, codes, trade_dates)
    finally:
        conn.close()
    return_series = {code: _returns_from_closes(values) for code, values in close_series.items()}
    pairs: list[dict[str, Any]] = []
    high_pairs: list[dict[str, Any]] = []
    data_gap = False
    for left_idx, left_code in enumerate(codes):
        for right_code in codes[left_idx + 1 :]:
            xs_raw = return_series.get(left_code, [])
            ys_raw = return_series.get(right_code, [])
            xs: list[float] = []
            ys: list[float] = []
            for x, y in zip(xs_raw, ys_raw):
                if x is None or y is None:
                    continue
                xs.append(float(x))
                ys.append(float(y))
            corr = _pearson(xs, ys) if len(xs) >= config.min_correlation_points else None
            if corr is None:
                data_gap = True
            pair = {
                "left_ts_code": left_code,
                "right_ts_code": right_code,
                "sample_points": len(xs),
                "correlation": round(float(corr), 6) if corr is not None else None,
                "status": "data_gap" if corr is None else ("high" if abs(corr) >= config.high_correlation_threshold else "passed"),
            }
            pairs.append(pair)
            if corr is not None and abs(corr) >= config.high_correlation_threshold:
                high_pairs.append(pair)
    status = "review" if data_gap or high_pairs else "passed"
    reasons = []
    if data_gap:
        reasons.append("correlation_data_insufficient")
    if high_pairs:
        reasons.append("high_correlation_detected")
    return {
        "status": status,
        "blocking_reasons": reasons,
        "lookback_trade_days": config.correlation_lookback_trade_days,
        "min_correlation_points": config.min_correlation_points,
        "trade_date_count": len(trade_dates),
        "pair_count": len(pairs),
        "high_correlation_pairs": high_pairs,
        "pairs": pairs,
    }


def _apply_high_correlation_trim(
    items: list[dict[str, Any]],
    correlation_report: dict[str, Any],
    config: CandidatePortfolioConfig,
) -> tuple[list[dict[str, Any]], list[str]]:
    adjustments: list[str] = []
    by_code = {str(item.get("ts_code") or ""): item for item in items}
    for pair in correlation_report.get("high_correlation_pairs", []) or []:
        left = by_code.get(str(pair.get("left_ts_code") or ""))
        right = by_code.get(str(pair.get("right_ts_code") or ""))
        if not left or not right:
            continue
        target = left if float(left.get("weight", 0.0) or 0.0) <= float(right.get("weight", 0.0) or 0.0) else right
        current = float(target.get("weight", 0.0) or 0.0)
        target["weight"] = round(current * (1.0 - config.high_correlation_trim_ratio), 6)
        target["portfolio_adjustments"].append("high_correlation_trim")
        adjustments.append(f"high_correlation_trim:{target.get('ts_code')}")
    if adjustments:
        normalized = _normalize_weights([float(item.get("weight", 0.0) or 0.0) for item in items])
        for item, weight in zip(items, normalized):
            item["weight"] = weight
    return items, adjustments


def build_candidate_portfolio(
    *,
    snapshot_path: str | Path,
    candidates_csv_path: str | Path | None = None,
    sqlite_db_path: str | Path | None = None,
    sqlite_table: str = "daily_trading_data",
    config: CandidatePortfolioConfig | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    cfg = config or CandidatePortfolioConfig()
    snapshot = _read_json(snapshot_path)
    snapshot_date = _date_key(snapshot.get("snapshot_date"))
    csv_by_code = {str(row.get("ts_code") or ""): row for row in _read_csv_rows(candidates_csv_path)}
    raw_items = list(snapshot.get("items", []) or [])
    weights = []
    for item in raw_items:
        row = csv_by_code.get(str(item.get("ts_code") or ""), {})
        weights.append(
            _to_float(
                row.get("portfolio_weight_after_risk")
                or row.get("basket_weight_pct")
                or row.get("position_pct")
                or item.get("portfolio_weight_after_risk"),
                0.0,
            )
        )
    weights = _cap_single_weights(_normalize_weights(weights), cfg.max_single_weight)
    items: list[dict[str, Any]] = []
    for item, weight in zip(raw_items, weights):
        code = str(item.get("ts_code") or "")
        row = csv_by_code.get(code, {})
        items.append(
            {
                "observation_id": item.get("observation_id"),
                "ts_code": code,
                "stock_name": item.get("stock_name") or row.get("stock_name", ""),
                "industry": item.get("industry") or row.get("industry", "") or "unknown",
                "rank": item.get("rank"),
                "final_score": _to_float(item.get("final_score") or row.get("final_score"), 0.0),
                "weight": weight,
                "portfolio_role": "core" if weight >= 0.24 else ("satellite" if weight >= 0.12 else "tactical"),
                "portfolio_adjustments": [],
                "data_quality_level": item.get("data_quality_level"),
                "lineage_hash": item.get("lineage_hash"),
            }
        )
    items, industry_adjustments = _apply_industry_hard_cap(
        items,
        hard_max_industry_weight=cfg.hard_max_industry_weight,
        max_single_weight=cfg.max_single_weight,
    )
    correlation_report = (
        _build_correlation_report(
            sqlite_db_path=sqlite_db_path,
            sqlite_table=sqlite_table,
            snapshot_date=snapshot_date,
            items=items,
            config=cfg,
        )
        if sqlite_db_path
        else {
            "status": "review",
            "blocking_reasons": ["correlation_price_db_not_configured"],
            "pair_count": 0,
            "high_correlation_pairs": [],
            "pairs": [],
        }
    )
    items, correlation_adjustments = _apply_high_correlation_trim(items, correlation_report, cfg)
    weights_after = [float(item.get("weight", 0.0) or 0.0) for item in items]
    industry_map = _industry_weights(items)
    max_industry = max(industry_map.values(), default=0.0)
    max_single = max(weights_after, default=0.0)
    hhi = sum(weight**2 for weight in weights_after)
    status = "passed"
    blocking_reasons: list[str] = []
    review_reasons: list[str] = []
    if not items:
        status = "blocked"
        blocking_reasons.append("no_observation_candidates")
    if abs(sum(weights_after) - 1.0) > 1e-5:
        status = "blocked"
        blocking_reasons.append("weight_sum_not_one")
    if max_single > cfg.max_single_weight + 1e-6:
        status = "blocked"
        blocking_reasons.append("single_weight_exceeds_limit")
    if max_industry > cfg.hard_max_industry_weight + 1e-6:
        status = "blocked"
        blocking_reasons.append("industry_hard_limit_exceeded")
    elif max_industry > cfg.target_max_industry_weight + 1e-6:
        if status == "passed":
            status = "review"
        review_reasons.append("industry_target_limit_exceeded")
    if correlation_report.get("status") == "review":
        if status == "passed":
            status = "review"
        review_reasons.extend(str(reason) for reason in correlation_report.get("blocking_reasons", []) or [])
    summary = {
        "candidate_count": len(items),
        "weight_sum": round(sum(weights_after), 6),
        "max_single_weight": round(max_single, 6),
        "industry_weights": industry_map,
        "top_industry": next(iter(industry_map.keys()), ""),
        "top_industry_weight": round(max_industry, 6),
        "concentration_hhi": round(hhi, 6),
        "industry_adjustments": industry_adjustments,
        "correlation_adjustments": correlation_adjustments,
    }
    portfolio = {
        "schema_version": CANDIDATE_PORTFOLIO_VERSION,
        "status": status,
        "generated_at": _utc_now(),
        "snapshot_date": snapshot_date,
        "lineage_run_id": snapshot.get("lineage_run_id"),
        "policy": {
            "max_single_weight": cfg.max_single_weight,
            "target_max_industry_weight": cfg.target_max_industry_weight,
            "hard_max_industry_weight": cfg.hard_max_industry_weight,
            "high_correlation_threshold": cfg.high_correlation_threshold,
        },
        "blocking_reasons": blocking_reasons,
        "review_reasons": sorted(set(review_reasons)),
        "summary": summary,
        "items": items,
    }
    exposure_report = {
        "schema_version": PORTFOLIO_EXPOSURE_REPORT_VERSION,
        "status": status,
        "generated_at": _utc_now(),
        "snapshot_date": snapshot_date,
        "blocking_reasons": blocking_reasons,
        "review_reasons": sorted(set(review_reasons)),
        "industry_exposure": {
            "status": "blocked"
            if max_industry > cfg.hard_max_industry_weight + 1e-6
            else ("review" if max_industry > cfg.target_max_industry_weight + 1e-6 else "passed"),
            "target_max_industry_weight": cfg.target_max_industry_weight,
            "hard_max_industry_weight": cfg.hard_max_industry_weight,
            "industry_weights": industry_map,
            "top_industry": summary["top_industry"],
            "top_industry_weight": summary["top_industry_weight"],
            "adjustments": industry_adjustments,
        },
        "correlation_exposure": correlation_report,
        "portfolio_summary": summary,
    }
    return portfolio, exposure_report


def build_portfolio_capacity_report(
    *,
    portfolio: dict[str, Any],
    sqlite_db_path: str | Path,
    sqlite_table: str = "daily_trading_data",
    config: CandidatePortfolioConfig | None = None,
) -> dict[str, Any]:
    cfg = config or CandidatePortfolioConfig()
    snapshot_date = _date_key(portfolio.get("snapshot_date"))
    items = list(portfolio.get("items", []) or [])
    codes = [str(item.get("ts_code") or "") for item in items if item.get("ts_code")]
    if not Path(sqlite_db_path).exists():
        return {
            "schema_version": PORTFOLIO_CAPACITY_REPORT_VERSION,
            "status": "blocked",
            "generated_at": _utc_now(),
            "snapshot_date": snapshot_date,
            "blocking_reasons": ["capacity_price_db_missing"],
            "summary": {"candidate_count": 0, "state_counts": {}, "worst_participation_rate": 0.0},
            "items": [],
        }
    conn = sqlite3.connect(str(sqlite_db_path))
    try:
        rows_by_code = _fetch_capacity_rows(conn, sqlite_table, codes, snapshot_date)
    finally:
        conn.close()
    rows: list[dict[str, Any]] = []
    state_counts: dict[str, int] = {}
    worst_participation = 0.0
    total_estimated_impact_cost = 0.0
    total_capacity_notional = 0.0
    for item in items:
        code = str(item.get("ts_code") or "")
        weight = _to_float(item.get("weight"), 0.0)
        target_notional = cfg.portfolio_notional * weight
        market_row = rows_by_code.get(code, {})
        amount = _to_float(market_row.get("amount"), 0.0)
        vol = _to_float(market_row.get("vol"), 0.0)
        reasons: list[str] = []
        if not market_row:
            reasons.append("capacity_market_row_missing")
        if amount <= 0 or vol <= 0:
            reasons.append("capacity_no_trade_or_amount_missing")
        elif amount < cfg.min_amount:
            reasons.append("capacity_amount_below_minimum")
        participation = target_notional / amount if amount > 0 else 1.0
        estimated_impact_cost = target_notional * cfg.impact_cost_coefficient * participation
        estimated_impact_bps = estimated_impact_cost / target_notional * 10_000 if target_notional > 0 else 0.0
        if participation > cfg.capacity_block_participation_rate:
            reasons.append("capacity_participation_exceeds_block_limit")
        elif participation > cfg.capacity_warn_participation_rate:
            reasons.append("capacity_participation_exceeds_watch_limit")
        state = "blocked" if any("block" in reason or "missing" in reason or "minimum" in reason for reason in reasons) else (
            "review" if reasons else "passed"
        )
        state_counts[state] = state_counts.get(state, 0) + 1
        worst_participation = max(worst_participation, participation)
        total_estimated_impact_cost += estimated_impact_cost
        if amount > 0:
            total_capacity_notional += min(amount * cfg.capacity_block_participation_rate, cfg.portfolio_notional * weight)
        rows.append(
            {
                "ts_code": code,
                "stock_name": item.get("stock_name"),
                "industry": item.get("industry"),
                "weight": round(weight, 6),
                "target_notional": round(target_notional, 4),
                "capacity_trade_date": market_row.get("trade_date"),
                "amount": round(amount, 4),
                "vol": round(vol, 4),
                "participation_rate": round(participation, 8),
                "warn_participation_rate": cfg.capacity_warn_participation_rate,
                "block_participation_rate": cfg.capacity_block_participation_rate,
                "estimated_impact_cost": round(estimated_impact_cost, 4),
                "estimated_impact_bps": round(estimated_impact_bps, 4),
                "state": state,
                "reasons": reasons,
            }
        )
    status = "blocked" if state_counts.get("blocked", 0) else "review" if state_counts.get("review", 0) else "passed"
    blocking_reasons = sorted(
        {
            reason
            for row in rows
            if row.get("state") == "blocked"
            for reason in row.get("reasons", []) or []
        }
    )
    review_reasons = sorted(
        {
            reason
            for row in rows
            if row.get("state") == "review"
            for reason in row.get("reasons", []) or []
        }
    )
    return {
        "schema_version": PORTFOLIO_CAPACITY_REPORT_VERSION,
        "status": status,
        "generated_at": _utc_now(),
        "snapshot_date": snapshot_date,
        "portfolio_notional": cfg.portfolio_notional,
        "blocking_reasons": blocking_reasons,
        "review_reasons": review_reasons,
        "summary": {
            "candidate_count": len(rows),
            "state_counts": state_counts,
            "worst_participation_rate": round(worst_participation, 8),
            "total_estimated_impact_cost": round(total_estimated_impact_cost, 4),
            "estimated_impact_cost_bps": round(
                total_estimated_impact_cost / cfg.portfolio_notional * 10_000 if cfg.portfolio_notional > 0 else 0.0,
                4,
            ),
            "capacity_notional_at_block_limit": round(total_capacity_notional, 4),
        },
        "items": rows,
    }


def build_candidate_portfolio_quality(
    *,
    portfolio: dict[str, Any],
    exposure_report: dict[str, Any],
    capacity_report: dict[str, Any],
    transaction_cost_report: dict[str, Any] | None = None,
    realistic_backtest: dict[str, Any] | None = None,
    config: CandidatePortfolioConfig | None = None,
) -> dict[str, Any]:
    cfg = config or CandidatePortfolioConfig()
    summary = dict(portfolio.get("summary", {}) or {})
    capacity_summary = dict(capacity_report.get("summary", {}) or {})
    cost_summary = dict((transaction_cost_report or {}).get("summary", {}) or {})
    backtest_summary = dict((realistic_backtest or {}).get("summary", {}) or {})
    score = 100.0
    penalties: list[dict[str, Any]] = []

    def add_penalty(name: str, points: float, detail: str) -> None:
        nonlocal score
        value = max(float(points), 0.0)
        score -= value
        penalties.append({"name": name, "points": round(value, 4), "detail": detail})

    if portfolio.get("status") == "blocked":
        add_penalty("portfolio_blocked", 35.0, "portfolio has hard blocking reasons")
    elif portfolio.get("status") == "review":
        add_penalty("portfolio_review", 12.0, ",".join(portfolio.get("review_reasons", []) or []))
    if capacity_report.get("status") == "blocked":
        add_penalty("capacity_blocked", 35.0, ",".join(capacity_report.get("blocking_reasons", []) or []))
    elif capacity_report.get("status") == "review":
        add_penalty("capacity_review", 14.0, ",".join(capacity_report.get("review_reasons", []) or []))
    top_industry_weight = _to_float(summary.get("top_industry_weight"), 0.0)
    if top_industry_weight > cfg.target_max_industry_weight:
        add_penalty(
            "industry_concentration",
            min((top_industry_weight - cfg.target_max_industry_weight) * 80.0, 18.0),
            f"top_industry_weight={round(top_industry_weight, 6)}",
        )
    hhi = _to_float(summary.get("concentration_hhi"), 0.0)
    if hhi > 0.30:
        add_penalty("portfolio_hhi", min((hhi - 0.30) * 60.0, 10.0), f"concentration_hhi={round(hhi, 6)}")
    worst_participation = _to_float(capacity_summary.get("worst_participation_rate"), 0.0)
    if worst_participation > cfg.capacity_warn_participation_rate:
        add_penalty(
            "capacity_participation",
            min(worst_participation / cfg.capacity_block_participation_rate * 20.0, 24.0),
            f"worst_participation_rate={round(worst_participation, 8)}",
        )
    impact_bps = _to_float(capacity_summary.get("estimated_impact_cost_bps"), 0.0)
    if impact_bps > 5.0:
        add_penalty("impact_cost", min((impact_bps - 5.0) * 1.2, 12.0), f"estimated_impact_cost_bps={impact_bps}")
    total_cost = _to_float(cost_summary.get("total_transaction_cost"), 0.0)
    if total_cost <= 0 and (transaction_cost_report or {}).get("status") == "failed":
        add_penalty("transaction_cost_unavailable", 8.0, "transaction cost report unavailable")
    return_decay = _to_float(backtest_summary.get("return_decay"), 0.0)
    if return_decay > 0:
        add_penalty("return_decay", min(return_decay * 500.0, 10.0), f"return_decay={return_decay}")
    blocked_count = int(_to_float(backtest_summary.get("blocked_count"), 0.0))
    if blocked_count > 0:
        add_penalty("realistic_backtest_blocked", min(blocked_count * 6.0, 18.0), f"blocked_count={blocked_count}")

    final_score = max(round(score, 4), 0.0)
    blocking_reasons = []
    review_reasons = []
    if capacity_report.get("status") == "blocked":
        blocking_reasons.append("portfolio_capacity_blocked")
    if portfolio.get("status") == "blocked":
        blocking_reasons.append("portfolio_construction_blocked")
    if final_score < cfg.min_portfolio_quality_score:
        blocking_reasons.append("portfolio_quality_score_below_minimum")
    if portfolio.get("status") == "review":
        review_reasons.extend(portfolio.get("review_reasons", []) or [])
    if capacity_report.get("status") == "review":
        review_reasons.extend(capacity_report.get("review_reasons", []) or [])
    if (transaction_cost_report or {}).get("status") == "failed":
        review_reasons.append("transaction_cost_unavailable")
    if (realistic_backtest or {}).get("status") == "failed":
        review_reasons.append("realistic_backtest_not_evaluable")
    status = "blocked" if blocking_reasons else "review" if review_reasons or final_score < 75.0 else "passed"
    return {
        "schema_version": CANDIDATE_PORTFOLIO_QUALITY_VERSION,
        "status": status,
        "generated_at": _utc_now(),
        "snapshot_date": portfolio.get("snapshot_date"),
        "quality_score": final_score,
        "minimum_quality_score": cfg.min_portfolio_quality_score,
        "blocking_reasons": sorted(set(blocking_reasons)),
        "review_reasons": sorted(set(str(reason) for reason in review_reasons if reason)),
        "penalties": penalties,
        "inputs": {
            "portfolio_status": portfolio.get("status"),
            "exposure_status": exposure_report.get("status"),
            "capacity_status": capacity_report.get("status"),
            "transaction_cost_status": (transaction_cost_report or {}).get("status"),
            "realistic_backtest_status": (realistic_backtest or {}).get("status"),
        },
        "summary": {
            "top_industry_weight": summary.get("top_industry_weight"),
            "concentration_hhi": summary.get("concentration_hhi"),
            "worst_participation_rate": capacity_summary.get("worst_participation_rate"),
            "estimated_impact_cost_bps": capacity_summary.get("estimated_impact_cost_bps"),
            "transaction_cost": total_cost,
            "realistic_backtest_blocked_count": blocked_count,
        },
    }


def write_candidate_portfolio_payload(payload: dict[str, Any], *, output_path: str | Path) -> str:
    resolved = Path(output_path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(resolved)
