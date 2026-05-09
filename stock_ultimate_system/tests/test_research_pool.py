import sqlite3
from pathlib import Path

import yaml

from src.utils.research_pool import resolve_research_pool, resolve_research_pool_with_meta


def _write_settings(
    config_dir: Path,
    db_path: Path,
    enabled: bool = True,
    *,
    research_pool_overrides: dict | None = None,
    risk_liquidity_min_turnover: float = 2_500,
) -> None:
    research_pool = {
        "enabled": enabled,
        "size": 2,
        "min_total_mv_yi": 100,
        "max_total_mv_yi": 15000,
    }
    if research_pool_overrides:
        research_pool.update(research_pool_overrides)
    settings = {
        "data": {
            "sqlite_db_path": str(db_path),
            "stock_pool": ["000001.SZ"],
            "research_pool": research_pool,
        },
        "risk": {
            "liquidity_min_turnover": risk_liquidity_min_turnover,
        },
    }
    (config_dir / "settings.yaml").write_text(yaml.safe_dump(settings), encoding="utf-8")


def _prepare_db(db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE stock_basic (ts_code TEXT, total_mv REAL)")
    conn.execute("CREATE TABLE daily_trading_data (ts_code TEXT, trade_date TEXT, amount REAL)")
    conn.executemany(
        "INSERT INTO stock_basic (ts_code, total_mv) VALUES (?, ?)",
        [
            ("AAA.SZ", 900_000),
            ("BBB.SZ", 2_000_000),
            ("CCC.SZ", 5_000_000),
            ("DDD.SZ", 200_000_000),
        ],
    )
    conn.executemany(
        "INSERT INTO daily_trading_data (ts_code, trade_date, amount) VALUES (?, ?, ?)",
        [
            ("AAA.SZ", "20260320", 500),
            ("BBB.SZ", "20260320", 3000),
            ("CCC.SZ", "20260320", 2000),
            ("DDD.SZ", "20260320", 9000),
        ],
    )
    conn.commit()
    conn.close()


def _prepare_dynamic_db(db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE stock_basic (ts_code TEXT, total_mv REAL)")
    conn.execute("CREATE TABLE daily_trading_data (ts_code TEXT, trade_date TEXT, amount REAL)")
    stock_rows = []
    trading_rows = []
    for idx in range(1, 11):
        code = f"S{idx:03d}.SZ"
        stock_rows.append((code, 2_000_000))
        trading_rows.append((code, "20260320", 1_000 - idx * 50))
    conn.executemany("INSERT INTO stock_basic (ts_code, total_mv) VALUES (?, ?)", stock_rows)
    conn.executemany("INSERT INTO daily_trading_data (ts_code, trade_date, amount) VALUES (?, ?, ?)", trading_rows)
    conn.commit()
    conn.close()


def test_resolve_research_pool_filters_by_market_value_and_amount(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    db_path = tmp_path / "pool.db"
    _prepare_db(db_path)
    _write_settings(config_dir, db_path, enabled=True)

    pool = resolve_research_pool(config_dir)

    assert pool == ["BBB.SZ", "CCC.SZ"]


def test_resolve_research_pool_with_meta_tracks_liquidity_filter(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    db_path = tmp_path / "pool.db"
    _prepare_db(db_path)
    _write_settings(config_dir, db_path, enabled=True)

    pool, meta = resolve_research_pool_with_meta(config_dir)

    assert pool == ["BBB.SZ", "CCC.SZ"]
    assert meta["source"] == "sqlite_research_pool"
    assert meta["liquidity_min_turnover"] == 2500.0
    assert meta["liquidity_filtered_out"] == 0
    assert meta["eligible_before_limit"] == 2
    assert meta["market_value_filtered_out"] == 2


def test_resolve_research_pool_falls_back_to_static_pool_when_disabled(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    db_path = tmp_path / "pool.db"
    _prepare_db(db_path)
    _write_settings(config_dir, db_path, enabled=False)

    pool = resolve_research_pool(config_dir)

    assert pool == ["000001.SZ"]


def test_resolve_research_pool_supports_dynamic_liquidity_threshold(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    db_path = tmp_path / "pool_dynamic.db"
    _prepare_dynamic_db(db_path)
    _write_settings(
        config_dir,
        db_path,
        enabled=True,
        research_pool_overrides={
            "size": 3,
            "dynamic_liquidity_enabled": True,
            "dynamic_liquidity_floor_turnover": 300,
            "dynamic_liquidity_min_eligible_count": 5,
            "dynamic_liquidity_target_multiplier": 2.0,
        },
        risk_liquidity_min_turnover=10_000,
    )

    pool, meta = resolve_research_pool_with_meta(config_dir)

    assert pool == ["S001.SZ", "S002.SZ", "S003.SZ"]
    assert meta["configured_liquidity_min_turnover"] == 10_000.0
    assert meta["effective_liquidity_min_turnover"] == 700.0
    assert meta["liquidity_filtered_out"] == 0
    assert meta["eligible_before_limit"] == 10
