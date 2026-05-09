from __future__ import annotations

import sqlite3
import math
from pathlib import Path
from typing import Any

import yaml

from src.utils.project_paths import resolve_project_path


def load_settings(config_dir: str | Path) -> dict[str, Any]:
    settings_path = resolve_project_path(config_dir) / "settings.yaml"
    with settings_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _resolve_liquidity_threshold(settings: dict[str, Any]) -> float:
    market_rules = settings.get("market_rules", {}) or {}
    risk_rules = settings.get("risk_rules", {}) or {}
    settings_risk = settings.get("risk", {}) or {}
    for source in (market_rules, risk_rules, settings_risk):
        value = source.get("liquidity_min_turnover")
        if value is not None:
            try:
                return max(float(value), 0.0)
            except Exception:
                continue
    return 1_000_000.0


def _resolve_research_pool_dynamic_params(
    settings: dict[str, Any],
    requested_size: int,
) -> dict[str, float | bool | int]:
    data_cfg = settings.get("data", {}) or {}
    research_cfg = data_cfg.get("research_pool", {}) or {}
    configured = _resolve_liquidity_threshold(settings)
    floor_default = min(max(configured, 0.0), 300_000.0) if configured > 0 else 300_000.0

    enabled = bool(research_cfg.get("dynamic_liquidity_enabled", True))
    try:
        floor = float(research_cfg.get("dynamic_liquidity_floor_turnover", floor_default) or floor_default)
    except Exception:
        floor = floor_default
    try:
        multiplier = float(research_cfg.get("dynamic_liquidity_target_multiplier", 3.0) or 3.0)
    except Exception:
        multiplier = 3.0
    try:
        min_eligible = int(research_cfg.get("dynamic_liquidity_min_eligible_count", max(int(requested_size or 0), 50)) or max(int(requested_size or 0), 50))
    except Exception:
        min_eligible = max(int(requested_size or 0), 50)

    return {
        "enabled": enabled,
        "floor_turnover": max(floor, 0.0),
        "target_multiplier": max(multiplier, 1.0),
        "min_eligible_count": max(min_eligible, max(int(requested_size or 0), 1)),
    }


def _resolve_effective_research_pool_liquidity_threshold(
    amounts: list[float],
    configured_threshold: float,
    requested_size: int,
    dynamic_params: dict[str, float | bool | int],
) -> float:
    configured = max(float(configured_threshold or 0.0), 0.0)
    floor = max(float(dynamic_params.get("floor_turnover", 0.0) or 0.0), 0.0)
    if not bool(dynamic_params.get("enabled", True)):
        return max(configured, floor)

    positive_amounts = sorted((float(v) for v in amounts if float(v) > 0.0), reverse=True)
    if not positive_amounts:
        return max(configured, floor)

    min_eligible_count = int(dynamic_params.get("min_eligible_count", max(int(requested_size or 0), 1)) or max(int(requested_size or 0), 1))
    if len(positive_amounts) < min_eligible_count:
        return max(configured, floor)

    multiplier = max(float(dynamic_params.get("target_multiplier", 3.0) or 3.0), 1.0)
    target_count = max(min_eligible_count, int(math.ceil(max(int(requested_size or 0), 1) * multiplier)))
    target_count = min(target_count, len(positive_amounts))
    dynamic_threshold = positive_amounts[target_count - 1]
    return min(max(configured, floor), max(dynamic_threshold, floor))


def resolve_research_pool_with_meta(
    config_dir: str | Path,
    *,
    size_override: int | None = None,
    min_total_mv_yi_override: float | None = None,
    max_total_mv_yi_override: float | None = None,
) -> tuple[list[str], dict[str, Any]]:
    settings = load_settings(config_dir)
    data_cfg = settings.get("data", {})
    research_cfg = data_cfg.get("research_pool", {}) or {}

    explicit_pool = [
        str(code).strip()
        for code in data_cfg.get("stock_pool", [])
        if str(code).strip()
    ]
    meta: dict[str, Any] = {
        "enabled": bool(research_cfg.get("enabled")),
        "source": "explicit_pool",
        "size": len(explicit_pool),
        "requested_size": int(size_override if size_override is not None else (research_cfg.get("size", 50) or 50)),
        "min_total_mv_yi": float(min_total_mv_yi_override if min_total_mv_yi_override is not None else (research_cfg.get("min_total_mv_yi", 0) or 0)),
        "max_total_mv_yi": float(max_total_mv_yi_override if max_total_mv_yi_override is not None else (research_cfg.get("max_total_mv_yi", 1_000_000) or 1_000_000)),
        "liquidity_min_turnover": _resolve_liquidity_threshold(settings),
        "configured_liquidity_min_turnover": _resolve_liquidity_threshold(settings),
        "effective_liquidity_min_turnover": _resolve_liquidity_threshold(settings),
        "latest_trade_date": "",
        "liquidity_filtered_out": 0,
        "market_value_filtered_out": 0,
        "eligible_before_limit": 0,
    }
    if not research_cfg.get("enabled"):
        return explicit_pool, meta

    db_path = resolve_project_path(str(data_cfg.get("sqlite_db_path", "")).strip())
    if not db_path.exists():
        meta["source"] = "explicit_pool_missing_db"
        return explicit_pool, meta

    size = meta["requested_size"]
    min_total_mv_wan = meta["min_total_mv_yi"] * 10_000
    max_total_mv_wan = meta["max_total_mv_yi"] * 10_000

    dynamic_params = _resolve_research_pool_dynamic_params(settings, size)

    query = """
    WITH latest AS (
        SELECT MAX(trade_date) AS trade_date
        FROM daily_trading_data
    ),
    scoped AS (
        SELECT
            d.ts_code,
            d.trade_date,
            d.amount,
            b.total_mv,
            CASE WHEN b.total_mv BETWEEN ? AND ? THEN 1 ELSE 0 END AS mv_ok
        FROM daily_trading_data d
        JOIN latest l ON d.trade_date = l.trade_date
        JOIN stock_basic b ON b.ts_code = d.ts_code
    )
    SELECT
        ts_code,
        trade_date,
        amount,
        total_mv
    FROM scoped
    WHERE mv_ok = 1
    ORDER BY amount DESC, total_mv DESC
    """
    stats_query = """
    WITH latest AS (
        SELECT MAX(trade_date) AS trade_date
        FROM daily_trading_data
    ),
    scoped AS (
        SELECT
            d.ts_code,
            d.trade_date,
            d.amount,
            b.total_mv,
            CASE WHEN b.total_mv BETWEEN ? AND ? THEN 1 ELSE 0 END AS mv_ok
        FROM daily_trading_data d
        JOIN latest l ON d.trade_date = l.trade_date
        JOIN stock_basic b ON b.ts_code = d.ts_code
    )
    SELECT
        MAX(trade_date) AS latest_trade_date,
        SUM(CASE WHEN mv_ok = 1 THEN 1 ELSE 0 END) AS mv_eligible_count,
        SUM(CASE WHEN mv_ok = 0 THEN 1 ELSE 0 END) AS market_value_filtered_out
    FROM scoped
    """
    try:
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        stats = cur.execute(
            stats_query,
            (min_total_mv_wan, max_total_mv_wan),
        ).fetchone()
        rows = cur.execute(
            query,
            (min_total_mv_wan, max_total_mv_wan),
        ).fetchall()
        conn.close()
        amounts = [float(row[2] or 0.0) for row in rows if row]
        effective_liquidity = _resolve_effective_research_pool_liquidity_threshold(
            amounts,
            meta["configured_liquidity_min_turnover"],
            size,
            dynamic_params,
        )
        meta["liquidity_min_turnover"] = float(effective_liquidity)
        meta["effective_liquidity_min_turnover"] = float(effective_liquidity)
        if stats:
            meta["latest_trade_date"] = str(stats[0] or "")
            mv_eligible_count = int(stats[1] or 0)
            meta["eligible_before_limit"] = mv_eligible_count
            meta["liquidity_filtered_out"] = 0
            meta["market_value_filtered_out"] = int(stats[2] or 0)
        pool = [str(row[0]).strip() for row in rows[:size] if row and str(row[0]).strip()]
        meta["source"] = "sqlite_research_pool"
        meta["size"] = len(pool)
        return (pool or explicit_pool), meta
    except Exception:
        meta["source"] = "explicit_pool_query_failed"
        return explicit_pool, meta


def resolve_research_pool(
    config_dir: str | Path,
    *,
    size_override: int | None = None,
    min_total_mv_yi_override: float | None = None,
    max_total_mv_yi_override: float | None = None,
) -> list[str]:
    pool, _ = resolve_research_pool_with_meta(
        config_dir,
        size_override=size_override,
        min_total_mv_yi_override=min_total_mv_yi_override,
        max_total_mv_yi_override=max_total_mv_yi_override,
    )
    return pool
