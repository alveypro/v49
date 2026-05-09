import json
import sqlite3
from pathlib import Path

from run_top_candidates import (
    _write_candidate_run_status,
    _annotate_selected_subset,
    _diversify_top_candidates,
    _rank_candidate_frame,
    _rebalance_candidate_basket,
    _average_abs_correlation,
    _apply_candidate_strategy_profile,
    apply_validation_guardrail,
    evaluate_basket_capacity_pressure,
    evolve_candidate_strategy_profile,
    load_stock_basic_map,
    load_candidate_strategy_profile,
    rank_candidates,
    select_universe_for_trade_date,
    select_universe_from_sqlite,
    write_candidate_strategy_profile,
    write_candidate_outputs,
)


def _build_sqlite_fixture(db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE daily_trading_data (
            ts_code TEXT,
            trade_date TEXT,
            close_price REAL,
            amount REAL,
            turnover_rate REAL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE stock_basic (
            ts_code TEXT,
            name TEXT,
            industry TEXT,
            market TEXT,
            area TEXT
        )
        """
    )
    rows = []
    for day in range(1, 131):
        trade_date = f"{20260000 + day:08d}"
        rows.append(("000001.SH", trade_date, 3200.0, 9_000_000 + day, 0.5))
        rows.append(("000001.SZ", trade_date, 10.0, 1_000_000 + day, 1.0))
    for day in range(1, 41):
        trade_date = f"{20260000 + day:08d}"
        rows.append(("688818.SH", trade_date, 20.0, 2_000_000 + day, 1.2))
    conn.executemany("INSERT INTO daily_trading_data VALUES (?, ?, ?, ?, ?)", rows)
    conn.executemany(
        "INSERT INTO stock_basic VALUES (?, ?, ?, ?, ?)",
        [
            ("000001.SZ", "平安银行", "银行", "主板", "深圳"),
            ("688818.SH", "富创精密", "半导体", "科创板", "辽宁"),
        ],
    )
    conn.commit()
    conn.close()


def test_select_universe_filters_short_history(tmp_path):
    db_path = tmp_path / "candidates.db"
    _build_sqlite_fixture(db_path)
    settings = {
        "data": {
            "sqlite_db_path": str(db_path),
            "sqlite_table": "daily_trading_data",
            "candidate_min_history_rows": 60,
            "stock_pool": ["fallback.SZ"],
        }
    }
    universe = select_universe_from_sqlite(settings, universe_size=10)
    assert "000001.SZ" in universe
    assert "688818.SH" not in universe


def test_select_universe_for_trade_date_respects_history_threshold(tmp_path):
    db_path = tmp_path / "candidates.db"
    _build_sqlite_fixture(db_path)
    settings = {
        "data": {
            "sqlite_db_path": str(db_path),
            "sqlite_table": "daily_trading_data",
            "candidate_min_history_rows": 120,
            "stock_pool": ["fallback.SZ"],
        }
    }
    universe = select_universe_for_trade_date(settings, trade_date="20260120", universe_size=10)
    assert universe == ["000001.SZ"]


def test_select_universe_excludes_index_code_even_when_liquid(tmp_path):
    db_path = tmp_path / "candidates.db"
    _build_sqlite_fixture(db_path)
    settings = {
        "data": {
            "sqlite_db_path": str(db_path),
            "sqlite_table": "daily_trading_data",
            "candidate_min_history_rows": 120,
            "stock_pool": ["fallback.SZ"],
        }
    }

    universe = select_universe_for_trade_date(settings, trade_date="20260120", universe_size=10)

    assert "000001.SH" not in universe
    assert "000001.SZ" in universe


def test_select_universe_for_trade_date_keeps_candidates_under_soft_liquidity_constraint(tmp_path):
    db_path = tmp_path / "candidates.db"
    _build_sqlite_fixture(db_path)
    settings = {
        "data": {
            "sqlite_db_path": str(db_path),
            "sqlite_table": "daily_trading_data",
            "candidate_min_history_rows": 120,
            "stock_pool": ["fallback.SZ"],
        },
        "risk": {
            "liquidity_min_turnover": 1_500_000,
        },
    }
    universe = select_universe_for_trade_date(settings, trade_date="20260120", universe_size=10)
    assert universe == ["000001.SZ"]


def test_select_universe_prefers_stable_liquidity_over_single_day_spike(tmp_path):
    db_path = tmp_path / "candidates.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE daily_trading_data (
            ts_code TEXT,
            trade_date TEXT,
            close_price REAL,
            amount REAL,
            turnover_rate REAL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE stock_basic (
            ts_code TEXT,
            name TEXT,
            industry TEXT,
            market TEXT,
            area TEXT
        )
        """
    )
    rows = []
    for day in range(1, 131):
        trade_date = f"{20260000 + day:08d}"
        rows.append(("STABLE.SZ", trade_date, 10.0, 2_500_000, 1.5))
        spike_amount = 12_000_000 if day == 130 else 1_000_000
        rows.append(("SPIKE.SZ", trade_date, 10.0, spike_amount, 1.1))
    conn.executemany("INSERT INTO daily_trading_data VALUES (?, ?, ?, ?, ?)", rows)
    conn.executemany(
        "INSERT INTO stock_basic VALUES (?, ?, ?, ?, ?)",
        [
            ("STABLE.SZ", "稳定样本", "工业", "主板", "深圳"),
            ("SPIKE.SZ", "脉冲样本", "工业", "主板", "上海"),
        ],
    )
    conn.commit()
    conn.close()

    settings = {
        "data": {
            "sqlite_db_path": str(db_path),
            "sqlite_table": "daily_trading_data",
            "candidate_min_history_rows": 120,
            "stock_pool": ["fallback.SZ"],
        },
        "risk": {
            "liquidity_min_turnover": 1_000_000,
        },
        "market_rules": {
            "candidate_liquidity_lookback_days": 20,
        },
    }

    universe = select_universe_for_trade_date(settings, trade_date="20260130", universe_size=2)
    assert universe[0] == "STABLE.SZ"


def test_select_universe_prefers_healthier_trend_when_liquidity_similar(tmp_path):
    db_path = tmp_path / "candidates.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE daily_trading_data (
            ts_code TEXT,
            trade_date TEXT,
            close_price REAL,
            amount REAL,
            turnover_rate REAL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE stock_basic (
            ts_code TEXT,
            name TEXT,
            industry TEXT,
            market TEXT,
            area TEXT
        )
        """
    )
    rows = []
    uptrend_prices = [10.0 + 0.08 * day for day in range(130)]
    downtrend_prices = [20.0 - 0.07 * day for day in range(130)]
    for day in range(1, 131):
        trade_date = f"{20260000 + day:08d}"
        rows.append(("UPTREND.SZ", trade_date, uptrend_prices[day - 1], 2_400_000, 1.6))
        rows.append(("DOWNTREND.SZ", trade_date, downtrend_prices[day - 1], 2_450_000, 1.6))
    conn.executemany("INSERT INTO daily_trading_data VALUES (?, ?, ?, ?, ?)", rows)
    conn.executemany(
        "INSERT INTO stock_basic VALUES (?, ?, ?, ?, ?)",
        [
            ("UPTREND.SZ", "上行样本", "工业", "主板", "深圳"),
            ("DOWNTREND.SZ", "下行样本", "工业", "主板", "上海"),
        ],
    )
    conn.commit()
    conn.close()

    settings = {
        "data": {
            "sqlite_db_path": str(db_path),
            "sqlite_table": "daily_trading_data",
            "candidate_min_history_rows": 120,
            "stock_pool": ["fallback.SZ"],
        },
        "risk": {
            "liquidity_min_turnover": 1_000_000,
        },
        "market_rules": {
            "candidate_liquidity_lookback_days": 20,
            "candidate_trend_lookback_days": 60,
        },
    }

    universe = select_universe_for_trade_date(settings, trade_date="20260130", universe_size=2)
    assert universe[0] == "UPTREND.SZ"


def test_select_universe_writes_prefilter_snapshot(tmp_path):
    db_path = tmp_path / "candidates.db"
    _build_sqlite_fixture(db_path)
    cache_dir = tmp_path / "experiments"
    settings = {
        "data": {
            "sqlite_db_path": str(db_path),
            "sqlite_table": "daily_trading_data",
            "candidate_min_history_rows": 60,
            "stock_pool": ["fallback.SZ"],
        },
        "market_rules": {
            "candidate_prefilter_cache_dir": str(cache_dir),
        },
    }

    universe = select_universe_for_trade_date(settings, trade_date="20260120", universe_size=10)
    assert "000001.SZ" in universe
    assert (cache_dir / "candidate_prefilter_universe_20260120.json").exists()
    assert (cache_dir / "candidate_prefilter_universe_20260120.csv").exists()
    assert (cache_dir / "candidate_prefilter_universe_20260120.md").exists()
    csv_text = (cache_dir / "candidate_prefilter_universe_20260120.csv").read_text(encoding="utf-8")
    md_text = (cache_dir / "candidate_prefilter_universe_20260120.md").read_text(encoding="utf-8")
    assert "prefilter_reason" in csv_text
    assert "Top 20 预筛结果" in md_text


def test_historical_prefilter_snapshot_does_not_overwrite_latest_trade_date(tmp_path):
    db_path = tmp_path / "candidates.db"
    _build_sqlite_fixture(db_path)
    cache_dir = tmp_path / "experiments"
    settings = {
        "data": {
            "sqlite_db_path": str(db_path),
            "sqlite_table": "daily_trading_data",
            "candidate_min_history_rows": 60,
            "stock_pool": ["fallback.SZ"],
        },
        "market_rules": {
            "candidate_prefilter_cache_dir": str(cache_dir),
        },
    }

    latest_universe = select_universe_from_sqlite(settings, universe_size=10)
    assert "000001.SZ" in latest_universe
    latest_payload_before = json.loads((cache_dir / "candidate_prefilter_universe_latest.json").read_text(encoding="utf-8"))
    assert latest_payload_before["trade_date"] == "20260130"

    historical_universe = select_universe_for_trade_date(settings, trade_date="20260120", universe_size=10)
    assert "000001.SZ" in historical_universe

    latest_payload_after = json.loads((cache_dir / "candidate_prefilter_universe_latest.json").read_text(encoding="utf-8"))
    assert latest_payload_after["trade_date"] == "20260130"
    assert (cache_dir / "candidate_prefilter_universe_20260120.json").exists()
    assert (cache_dir / "candidate_prefilter_universe_20260120.csv").exists()


def test_select_universe_can_reuse_prefilter_snapshot_when_db_unavailable(tmp_path):
    db_path = tmp_path / "candidates.db"
    _build_sqlite_fixture(db_path)
    cache_dir = tmp_path / "experiments"
    settings = {
        "data": {
            "sqlite_db_path": str(db_path),
            "sqlite_table": "daily_trading_data",
            "candidate_min_history_rows": 60,
            "stock_pool": ["fallback.SZ"],
        },
        "market_rules": {
            "candidate_prefilter_cache_dir": str(cache_dir),
            "candidate_liquidity_lookback_days": 20,
            "candidate_trend_lookback_days": 60,
        },
        "risk": {
            "liquidity_min_turnover": 1_000_000,
        },
    }

    first = select_universe_for_trade_date(settings, trade_date="20260120", universe_size=10)
    assert first == ["000001.SZ"]
    db_path.unlink()

    reused = select_universe_for_trade_date(settings, trade_date="20260120", universe_size=10)
    assert reused == ["000001.SZ"]


def test_prefilter_snapshot_records_exclusion_reasons(tmp_path):
    db_path = tmp_path / "candidates.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE daily_trading_data (
            ts_code TEXT,
            trade_date TEXT,
            close_price REAL,
            amount REAL,
            turnover_rate REAL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE stock_basic (
            ts_code TEXT,
            name TEXT,
            industry TEXT,
            market TEXT,
            area TEXT
        )
        """
    )
    rows = []
    for day in range(1, 131):
        trade_date = f"{20260000 + day:08d}"
        rows.append(("PASS.SZ", trade_date, 10.0 + day * 0.01, 2_500_000, 1.5))
        rows.append(("RANKOUT.SZ", trade_date, 9.8 + day * 0.005, 2_100_000, 1.4))
        rows.append(("LOWLIQ.SZ", trade_date, 8.0, 400_000, 0.8))
        rows.append(("LOWPRICE.SZ", trade_date, 1.8, 2_200_000, 1.1))
    conn.executemany("INSERT INTO daily_trading_data VALUES (?, ?, ?, ?, ?)", rows)
    conn.executemany(
        "INSERT INTO stock_basic VALUES (?, ?, ?, ?, ?)",
        [
            ("PASS.SZ", "通过样本", "工业", "主板", "深圳"),
            ("RANKOUT.SZ", "排序落选", "工业", "主板", "上海"),
            ("LOWLIQ.SZ", "流动性不足", "工业", "主板", "北京"),
            ("LOWPRICE.SZ", "低价样本", "工业", "主板", "广州"),
        ],
    )
    conn.commit()
    conn.close()

    cache_dir = tmp_path / "experiments"
    settings = {
        "data": {
            "sqlite_db_path": str(db_path),
            "sqlite_table": "daily_trading_data",
            "candidate_min_history_rows": 120,
            "stock_pool": ["fallback.SZ"],
        },
        "market_rules": {
            "candidate_prefilter_cache_dir": str(cache_dir),
            "candidate_liquidity_lookback_days": 20,
            "candidate_trend_lookback_days": 60,
        },
        "risk": {
            "liquidity_min_turnover": 1_000_000,
        },
    }

    universe = select_universe_for_trade_date(settings, trade_date="20260130", universe_size=1)
    assert universe == ["PASS.SZ"]

    payload = json.loads((cache_dir / "candidate_prefilter_universe_latest.json").read_text(encoding="utf-8"))
    reasons = {item["reason"]: item["count"] for item in payload.get("exclusion_summary", [])}
    assert reasons["价格低于预筛门槛"] == 1
    assert reasons["预筛排序未进入当前计算预算"] == 2


def test_select_universe_adapts_excessive_liquidity_threshold(tmp_path):
    db_path = tmp_path / "candidates.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE daily_trading_data (
            ts_code TEXT,
            trade_date TEXT,
            close_price REAL,
            amount REAL,
            turnover_rate REAL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE stock_basic (
            ts_code TEXT,
            name TEXT,
            industry TEXT,
            market TEXT,
            area TEXT
        )
        """
    )
    rows = []
    for idx in range(1, 1001):
        code = f"{idx:06d}.SZ"
        base_amount = 200_000 + idx * 1_000
        for day in range(1, 41):
            trade_date = f"{20260000 + day:08d}"
            rows.append((code, trade_date, 8.0 + idx * 0.001, base_amount, 1.0))
    conn.executemany("INSERT INTO daily_trading_data VALUES (?, ?, ?, ?, ?)", rows)
    conn.executemany(
        "INSERT INTO stock_basic VALUES (?, ?, ?, ?, ?)",
        [(f"{idx:06d}.SZ", f"样本{idx}", "工业", "主板", "深圳") for idx in range(1, 1001)],
    )
    conn.commit()
    conn.close()

    cache_dir = tmp_path / "experiments"
    settings = {
        "data": {
            "sqlite_db_path": str(db_path),
            "sqlite_table": "daily_trading_data",
            "candidate_min_history_rows": 30,
            "stock_pool": ["fallback.SZ"],
        },
        "market_rules": {
            "candidate_prefilter_cache_dir": str(cache_dir),
            "candidate_prefilter_min_pass_count": 800,
            "candidate_prefilter_floor_turnover": 300_000,
        },
        "risk": {
            "liquidity_min_turnover": 10_000_000,
        },
    }

    universe = select_universe_for_trade_date(settings, trade_date="20260040", universe_size=300)
    assert len(universe) == 300

    payload = json.loads((cache_dir / "candidate_prefilter_universe_latest.json").read_text(encoding="utf-8"))
    assert payload["configured_liquidity_min_turnover"] == 10_000_000.0
    assert payload["effective_liquidity_min_turnover"] < 10_000_000.0
    assert payload["row_count"] >= 800


def test_load_stock_basic_map_batches_large_inputs(tmp_path):
    db_path = tmp_path / "candidates.db"
    _build_sqlite_fixture(db_path)
    settings = {
        "data": {
            "sqlite_db_path": str(db_path),
            "sqlite_table": "daily_trading_data",
            "stock_pool": ["fallback.SZ"],
        }
    }
    ts_codes = [f"CODE{i:04d}.SZ" for i in range(1200)] + ["000001.SZ", "688818.SH"]
    meta = load_stock_basic_map(settings, ts_codes)
    assert meta["000001.SZ"]["stock_name"] == "平安银行"
    assert meta["688818.SH"]["industry"] == "半导体"


def test_average_abs_correlation_detects_highly_aligned_series():
    corr = _average_abs_correlation(
        [0.01, 0.02, 0.03, 0.01, 0.02, 0.03],
        [[0.011, 0.019, 0.031, 0.009, 0.021, 0.029]],
    )
    assert corr > 0.9


def test_diversify_top_candidates_adds_correlation_penalty():
    import pandas as pd

    df = pd.DataFrame([
        {"ts_code": "AAA", "industry": "Tech", "market": "主板", "area": "深圳", "final_score": 100.0, "recent_returns": [0.01, 0.02, 0.03, 0.01, 0.02, 0.03]},
        {"ts_code": "BBB", "industry": "医药", "market": "主板", "area": "上海", "final_score": 99.0, "recent_returns": [0.011, 0.019, 0.031, 0.009, 0.021, 0.029]},
        {"ts_code": "CCC", "industry": "军工", "market": "主板", "area": "北京", "final_score": 97.0, "recent_returns": [-0.01, 0.02, -0.015, 0.01, -0.005, 0.012]},
    ])

    diversified = _diversify_top_candidates(df, top_n=3)
    annotated = _annotate_selected_subset(diversified)

    assert "correlation_penalty" in annotated.columns
    penalty_bbb = float(annotated.loc[annotated["ts_code"] == "BBB", "correlation_penalty"].iloc[0])
    penalty_ccc = float(annotated.loc[annotated["ts_code"] == "CCC", "correlation_penalty"].iloc[0])
    assert penalty_bbb > penalty_ccc


def test_diversify_top_candidates_prefers_industry_headroom_when_alternatives_exist():
    import pandas as pd

    df = pd.DataFrame([
        {"ts_code": "AAA", "industry": "通信设备", "market": "主板", "area": "深圳", "final_score": 110.0, "recent_returns": [0.01, 0.02, 0.03]},
        {"ts_code": "BBB", "industry": "通信设备", "market": "主板", "area": "上海", "final_score": 109.0, "recent_returns": [0.011, 0.019, 0.031]},
        {"ts_code": "CCC", "industry": "通信设备", "market": "主板", "area": "北京", "final_score": 108.0, "recent_returns": [0.012, 0.018, 0.03]},
        {"ts_code": "DDD", "industry": "银行", "market": "主板", "area": "深圳", "final_score": 104.0, "recent_returns": [-0.01, 0.005, 0.012]},
        {"ts_code": "EEE", "industry": "医药", "market": "主板", "area": "上海", "final_score": 103.0, "recent_returns": [0.008, -0.004, 0.01]},
    ])

    diversified = _diversify_top_candidates(df, top_n=4)

    industry_counts = diversified["industry"].value_counts().to_dict()
    assert industry_counts.get("通信设备", 0) <= 2


def test_rebalance_candidate_basket_searches_replacement_that_reduces_registration_pressure():
    import pandas as pd

    candidate_pool = pd.DataFrame([
        {"ts_code": "A1", "stock_name": "A1", "industry": "通信设备", "market": "主板", "area": "深圳", "final_score": 115.0, "selection_score": 115.0, "pred_return": 0.05, "calibrated_avg_return": 0.03, "calibrated_upside_win_rate": 0.62, "confidence": 0.82, "model_agreement": 0.8, "prediction_dispersion": 0.03, "risk_level": "medium", "style_volatility": 0.01, "style_relative_strength": 0.01, "recent_returns": [0.01, 0.02, 0.03]},
        {"ts_code": "A2", "stock_name": "A2", "industry": "通信设备", "market": "主板", "area": "上海", "final_score": 114.0, "selection_score": 114.0, "pred_return": 0.045, "calibrated_avg_return": 0.028, "calibrated_upside_win_rate": 0.61, "confidence": 0.80, "model_agreement": 0.78, "prediction_dispersion": 0.03, "risk_level": "medium", "style_volatility": 0.01, "style_relative_strength": 0.01, "recent_returns": [0.011, 0.019, 0.031]},
        {"ts_code": "A3", "stock_name": "A3", "industry": "通信设备", "market": "主板", "area": "北京", "final_score": 113.0, "selection_score": 113.0, "pred_return": 0.044, "calibrated_avg_return": 0.027, "calibrated_upside_win_rate": 0.60, "confidence": 0.79, "model_agreement": 0.77, "prediction_dispersion": 0.03, "risk_level": "medium", "style_volatility": 0.01, "style_relative_strength": 0.01, "recent_returns": [0.012, 0.018, 0.03]},
        {"ts_code": "A4", "stock_name": "A4", "industry": "通信设备", "market": "主板", "area": "广州", "final_score": 112.0, "selection_score": 112.0, "pred_return": 0.043, "calibrated_avg_return": 0.026, "calibrated_upside_win_rate": 0.59, "confidence": 0.78, "model_agreement": 0.76, "prediction_dispersion": 0.03, "risk_level": "medium", "style_volatility": 0.01, "style_relative_strength": 0.01, "recent_returns": [0.013, 0.017, 0.029]},
        {"ts_code": "B1", "stock_name": "B1", "industry": "银行", "market": "主板", "area": "深圳", "final_score": 101.0, "selection_score": 101.0, "pred_return": 0.03, "calibrated_avg_return": 0.02, "calibrated_upside_win_rate": 0.57, "confidence": 0.74, "model_agreement": 0.70, "prediction_dispersion": 0.04, "risk_level": "medium", "style_volatility": 0.01, "style_relative_strength": 0.0, "recent_returns": [-0.01, 0.005, 0.012]},
        {"ts_code": "C1", "stock_name": "C1", "industry": "医药", "market": "主板", "area": "上海", "final_score": 100.0, "selection_score": 100.0, "pred_return": 0.029, "calibrated_avg_return": 0.019, "calibrated_upside_win_rate": 0.56, "confidence": 0.73, "model_agreement": 0.69, "prediction_dispersion": 0.04, "risk_level": "medium", "style_volatility": 0.01, "style_relative_strength": 0.0, "recent_returns": [0.008, -0.004, 0.01]},
    ])

    basket, summary = _rebalance_candidate_basket(candidate_pool, top_n=4)

    assert len(basket) == 4
    assert summary["top_industry_weight"] <= 0.65
    assert basket["industry"].nunique() >= 2


def test_rank_candidate_frame_can_backfill_high_score_watch_names_for_diversification():
    import pandas as pd

    df = pd.DataFrame([
        {"ts_code": "C1", "stock_name": "C1", "industry": "通信设备", "market": "主板", "area": "深圳", "signal": "strong_buy", "final_score": 150.0, "pred_return": 0.05, "calibrated_avg_return": 0.03, "calibrated_upside_win_rate": 0.61, "confidence": 0.81, "model_agreement": 0.8, "prediction_dispersion": 0.03, "risk_level": "medium", "style_volatility": 0.01, "style_relative_strength": 0.02, "recent_returns": [0.01, 0.02, 0.03], "cross_section_score": 60.0, "industry_rank_pct": 1.0, "robustness_score": 40.0},
        {"ts_code": "C2", "stock_name": "C2", "industry": "通信设备", "market": "主板", "area": "上海", "signal": "strong_buy", "final_score": 149.0, "pred_return": 0.049, "calibrated_avg_return": 0.029, "calibrated_upside_win_rate": 0.61, "confidence": 0.80, "model_agreement": 0.79, "prediction_dispersion": 0.03, "risk_level": "medium", "style_volatility": 0.01, "style_relative_strength": 0.02, "recent_returns": [0.011, 0.019, 0.031], "cross_section_score": 59.0, "industry_rank_pct": 0.9, "robustness_score": 39.0},
        {"ts_code": "C3", "stock_name": "C3", "industry": "通信设备", "market": "主板", "area": "北京", "signal": "strong_buy", "final_score": 148.0, "pred_return": 0.048, "calibrated_avg_return": 0.028, "calibrated_upside_win_rate": 0.60, "confidence": 0.79, "model_agreement": 0.78, "prediction_dispersion": 0.03, "risk_level": "medium", "style_volatility": 0.01, "style_relative_strength": 0.02, "recent_returns": [0.012, 0.018, 0.03], "cross_section_score": 58.0, "industry_rank_pct": 0.8, "robustness_score": 38.0},
        {"ts_code": "C4", "stock_name": "C4", "industry": "通信设备", "market": "主板", "area": "广州", "signal": "strong_buy", "final_score": 147.0, "pred_return": 0.047, "calibrated_avg_return": 0.027, "calibrated_upside_win_rate": 0.59, "confidence": 0.78, "model_agreement": 0.77, "prediction_dispersion": 0.03, "risk_level": "medium", "style_volatility": 0.01, "style_relative_strength": 0.02, "recent_returns": [0.013, 0.017, 0.029], "cross_section_score": 57.0, "industry_rank_pct": 0.7, "robustness_score": 37.0},
        {"ts_code": "C5", "stock_name": "C5", "industry": "通信设备", "market": "主板", "area": "杭州", "signal": "strong_buy", "final_score": 146.0, "pred_return": 0.046, "calibrated_avg_return": 0.026, "calibrated_upside_win_rate": 0.58, "confidence": 0.77, "model_agreement": 0.76, "prediction_dispersion": 0.03, "risk_level": "medium", "style_volatility": 0.01, "style_relative_strength": 0.02, "recent_returns": [0.014, 0.016, 0.028], "cross_section_score": 56.0, "industry_rank_pct": 0.6, "robustness_score": 36.0},
        {"ts_code": "C6", "stock_name": "C6", "industry": "通信设备", "market": "主板", "area": "苏州", "signal": "strong_buy", "final_score": 145.0, "pred_return": 0.045, "calibrated_avg_return": 0.025, "calibrated_upside_win_rate": 0.57, "confidence": 0.76, "model_agreement": 0.75, "prediction_dispersion": 0.03, "risk_level": "medium", "style_volatility": 0.01, "style_relative_strength": 0.02, "recent_returns": [0.015, 0.015, 0.027], "cross_section_score": 55.0, "industry_rank_pct": 0.5, "robustness_score": 35.0},
        {"ts_code": "A1", "stock_name": "A1", "industry": "电气设备", "market": "创业板", "area": "福建", "signal": "strong_buy", "final_score": 144.0, "pred_return": 0.04, "calibrated_avg_return": 0.024, "calibrated_upside_win_rate": 0.57, "confidence": 0.75, "model_agreement": 0.74, "prediction_dispersion": 0.03, "risk_level": "medium", "style_volatility": 0.01, "style_relative_strength": 0.01, "recent_returns": [-0.01, 0.005, 0.012], "cross_section_score": 54.0, "industry_rank_pct": 1.0, "robustness_score": 34.0},
        {"ts_code": "A2", "stock_name": "A2", "industry": "专用机械", "market": "创业板", "area": "江苏", "signal": "strong_buy", "final_score": 143.0, "pred_return": 0.039, "calibrated_avg_return": 0.023, "calibrated_upside_win_rate": 0.56, "confidence": 0.74, "model_agreement": 0.73, "prediction_dispersion": 0.03, "risk_level": "medium", "style_volatility": 0.01, "style_relative_strength": 0.01, "recent_returns": [0.008, -0.004, 0.01], "cross_section_score": 53.0, "industry_rank_pct": 1.0, "robustness_score": 33.0},
        {"ts_code": "W1", "stock_name": "W1", "industry": "元器件", "market": "主板", "area": "深圳", "signal": "watch", "final_score": 142.5, "pred_return": 0.038, "calibrated_avg_return": 0.022, "calibrated_upside_win_rate": 0.56, "confidence": 0.74, "model_agreement": 0.72, "prediction_dispersion": 0.03, "risk_level": "medium", "style_volatility": 0.01, "style_relative_strength": 0.01, "recent_returns": [0.004, -0.002, 0.009], "cross_section_score": 52.0, "industry_rank_pct": 1.0, "robustness_score": 32.0},
    ])

    ranked = _rank_candidate_frame(df, 10, selection_mode="diversified")

    assert "W1" in set(ranked["ts_code"])
    assert ranked["industry"].nunique() >= 3


def test_rank_candidate_frame_skips_blocked_watch_names_when_backfilling_diversification():
    import pandas as pd

    df = pd.DataFrame([
        {"ts_code": "C1", "stock_name": "C1", "industry": "通信设备", "market": "主板", "area": "深圳", "signal": "strong_buy", "final_score": 150.0, "pred_return": 0.05, "calibrated_avg_return": 0.03, "calibrated_upside_win_rate": 0.61, "confidence": 0.81, "model_agreement": 0.8, "prediction_dispersion": 0.03, "risk_level": "medium", "style_volatility": 0.01, "style_relative_strength": 0.02, "recent_returns": [0.01, 0.02, 0.03], "cross_section_score": 60.0, "industry_rank_pct": 1.0, "robustness_score": 40.0, "reason": "factor_strong(55)"},
        {"ts_code": "C2", "stock_name": "C2", "industry": "通信设备", "market": "主板", "area": "上海", "signal": "strong_buy", "final_score": 149.0, "pred_return": 0.049, "calibrated_avg_return": 0.029, "calibrated_upside_win_rate": 0.61, "confidence": 0.80, "model_agreement": 0.79, "prediction_dispersion": 0.03, "risk_level": "medium", "style_volatility": 0.01, "style_relative_strength": 0.02, "recent_returns": [0.011, 0.019, 0.031], "cross_section_score": 59.0, "industry_rank_pct": 0.9, "robustness_score": 39.0, "reason": "factor_strong(50)"},
        {"ts_code": "C3", "stock_name": "C3", "industry": "通信设备", "market": "主板", "area": "北京", "signal": "strong_buy", "final_score": 148.0, "pred_return": 0.048, "calibrated_avg_return": 0.028, "calibrated_upside_win_rate": 0.60, "confidence": 0.79, "model_agreement": 0.78, "prediction_dispersion": 0.03, "risk_level": "medium", "style_volatility": 0.01, "style_relative_strength": 0.02, "recent_returns": [0.012, 0.018, 0.03], "cross_section_score": 58.0, "industry_rank_pct": 0.8, "robustness_score": 38.0, "reason": "factor_strong(48)"},
        {"ts_code": "C4", "stock_name": "C4", "industry": "通信设备", "market": "主板", "area": "广州", "signal": "strong_buy", "final_score": 147.0, "pred_return": 0.047, "calibrated_avg_return": 0.027, "calibrated_upside_win_rate": 0.59, "confidence": 0.78, "model_agreement": 0.77, "prediction_dispersion": 0.03, "risk_level": "medium", "style_volatility": 0.01, "style_relative_strength": 0.02, "recent_returns": [0.013, 0.017, 0.029], "cross_section_score": 57.0, "industry_rank_pct": 0.7, "robustness_score": 37.0, "reason": "factor_strong(46)"},
        {"ts_code": "C5", "stock_name": "C5", "industry": "通信设备", "market": "主板", "area": "杭州", "signal": "strong_buy", "final_score": 146.0, "pred_return": 0.046, "calibrated_avg_return": 0.026, "calibrated_upside_win_rate": 0.58, "confidence": 0.77, "model_agreement": 0.76, "prediction_dispersion": 0.03, "risk_level": "medium", "style_volatility": 0.01, "style_relative_strength": 0.02, "recent_returns": [0.014, 0.016, 0.028], "cross_section_score": 56.0, "industry_rank_pct": 0.6, "robustness_score": 36.0, "reason": "factor_strong(44)"},
        {"ts_code": "C6", "stock_name": "C6", "industry": "通信设备", "market": "主板", "area": "苏州", "signal": "strong_buy", "final_score": 145.0, "pred_return": 0.045, "calibrated_avg_return": 0.025, "calibrated_upside_win_rate": 0.57, "confidence": 0.76, "model_agreement": 0.75, "prediction_dispersion": 0.03, "risk_level": "medium", "style_volatility": 0.01, "style_relative_strength": 0.02, "recent_returns": [0.015, 0.015, 0.027], "cross_section_score": 55.0, "industry_rank_pct": 0.5, "robustness_score": 35.0, "reason": "factor_strong(42)"},
        {"ts_code": "A1", "stock_name": "A1", "industry": "电气设备", "market": "创业板", "area": "福建", "signal": "strong_buy", "final_score": 144.0, "pred_return": 0.04, "calibrated_avg_return": 0.024, "calibrated_upside_win_rate": 0.57, "confidence": 0.75, "model_agreement": 0.74, "prediction_dispersion": 0.03, "risk_level": "medium", "style_volatility": 0.01, "style_relative_strength": 0.01, "recent_returns": [-0.01, 0.005, 0.012], "cross_section_score": 54.0, "industry_rank_pct": 1.0, "robustness_score": 34.0, "reason": "factor_strong(40)"},
        {"ts_code": "A2", "stock_name": "A2", "industry": "专用机械", "market": "创业板", "area": "江苏", "signal": "strong_buy", "final_score": 143.0, "pred_return": 0.039, "calibrated_avg_return": 0.023, "calibrated_upside_win_rate": 0.56, "confidence": 0.74, "model_agreement": 0.73, "prediction_dispersion": 0.03, "risk_level": "medium", "style_volatility": 0.01, "style_relative_strength": 0.01, "recent_returns": [0.008, -0.004, 0.01], "cross_section_score": 53.0, "industry_rank_pct": 1.0, "robustness_score": 33.0, "reason": "factor_strong(38)"},
        {"ts_code": "W1", "stock_name": "W1", "industry": "元器件", "market": "主板", "area": "深圳", "signal": "watch", "final_score": 142.5, "pred_return": 0.038, "calibrated_avg_return": 0.022, "calibrated_upside_win_rate": 0.56, "confidence": 0.74, "model_agreement": 0.72, "prediction_dispersion": 0.03, "risk_level": "medium", "style_volatility": 0.01, "style_relative_strength": 0.01, "recent_returns": [0.004, -0.002, 0.009], "cross_section_score": 52.0, "industry_rank_pct": 1.0, "robustness_score": 32.0, "reason": "market_rule_blocked(limit_up)"},
        {"ts_code": "W2", "stock_name": "W2", "industry": "小金属", "market": "主板", "area": "陕西", "signal": "watch", "final_score": 141.5, "pred_return": 0.037, "calibrated_avg_return": 0.021, "calibrated_upside_win_rate": 0.55, "confidence": 0.73, "model_agreement": 0.71, "prediction_dispersion": 0.03, "risk_level": "medium", "style_volatility": 0.01, "style_relative_strength": 0.01, "recent_returns": [0.003, -0.001, 0.008], "cross_section_score": 51.0, "industry_rank_pct": 1.0, "robustness_score": 31.0, "reason": "score=45.0"},
    ])

    ranked = _rank_candidate_frame(df, 10, selection_mode="diversified")

    assert "W1" not in set(ranked["ts_code"])
    assert "W2" in set(ranked["ts_code"])


def test_write_candidate_outputs_can_refresh_latest_without_history(tmp_path):
    import pandas as pd
    import json

    df = pd.DataFrame([
        {
            "rank": 1,
            "ts_code": "000001.SZ",
            "stock_name": "平安银行",
            "industry": "银行",
            "signal": "buy",
            "basket_role": "core",
            "basket_weight_pct": 1.0,
            "portfolio_weight_after_risk": 1.0,
            "basket_risk_flag": "ok",
            "final_score": 100.0,
            "direction_prob_up": 0.6,
            "calibrated_upside_win_rate": 0.58,
            "pred_return": 0.02,
            "calibrated_avg_return": 0.015,
            "confidence": 0.7,
            "risk_level": "low",
            "position_pct": 0.2,
            "stop_loss": 9.5,
            "take_profit": 11.0,
            "reason": "test",
            "risk_overlay_penalty": 0.0,
            "diversification_penalty": 0.0,
            "style_volatility": 0.02,
            "style_relative_strength": 0.03,
            "liquidity_score": 0.74,
            "basket_risk_flag": "ok",
        }
    ])

    out = write_candidate_outputs(
        df,
        tmp_path,
        validation_result={"summary": {}, "records": []},
        guardrail={"mode": "interim", "reasons": ["partial_generation"]},
        generation_meta={"degraded": True, "reason": "interim_partial_generation", "effective_universe_size": 1},
        persist_history=False,
    )

    assert Path(out["latest_csv"]).exists()
    assert Path(out["latest_md"]).exists()
    summary = json.loads(Path(out["latest_summary"]).read_text(encoding="utf-8"))
    assert "weighted_liquidity_score" in summary
    assert "liquidity_capacity_weight" in summary
    history_files = [p for p in tmp_path.glob("candidates_top_*.csv") if p.name != "candidates_top_latest.csv"]
    assert history_files == []


def test_write_candidate_outputs_writes_data_quality_gate_from_latest_report(tmp_path):
    import pandas as pd
    import json

    (tmp_path / "data_quality_report_latest.json").write_text(
        json.dumps(
            {
                "schema_version": "candidate_data_quality_report.v1",
                "expected_latest_trade_date": "20260506",
                "stocks": [
                    {
                        "ts_code": "000001.SZ",
                        "quality_level": "pass",
                        "quality_score": 98.0,
                        "blocking_reasons": [],
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    df = pd.DataFrame([
        {
            "rank": 1,
            "ts_code": "000001.SZ",
            "stock_name": "平安银行",
            "industry": "银行",
            "signal": "buy",
            "basket_role": "core",
            "basket_weight_pct": 1.0,
            "portfolio_weight_after_risk": 1.0,
            "basket_risk_flag": "ok",
            "final_score": 100.0,
            "direction_prob_up": 0.6,
            "calibrated_upside_win_rate": 0.58,
            "pred_return": 0.02,
            "calibrated_avg_return": 0.015,
            "confidence": 0.7,
            "risk_level": "low",
            "position_pct": 0.2,
            "stop_loss": 9.5,
            "take_profit": 11.0,
            "reason": "test",
            "risk_overlay_penalty": 0.0,
            "diversification_penalty": 0.0,
            "style_volatility": 0.02,
            "style_relative_strength": 0.03,
        }
    ])

    out = write_candidate_outputs(df, tmp_path, validation_result={"summary": {}, "records": []}, persist_history=False)

    gate = json.loads(Path(out["latest_data_quality_gate"]).read_text(encoding="utf-8"))
    lineage = json.loads(Path(out["latest_lineage"]).read_text(encoding="utf-8"))
    summary = json.loads(Path(out["latest_summary"]).read_text(encoding="utf-8"))
    csv_frame = pd.read_csv(out["latest_csv"])
    assert gate["status"] == "passed"
    assert lineage["status"] == "passed"
    assert lineage["data_as_of"] == "20260506"
    assert lineage["candidates"][0]["run_id"].startswith("candidate-")
    assert summary["data_quality_gate_status"] == "passed"
    assert csv_frame.loc[0, "data_quality_level"] == "pass"
    assert csv_frame.loc[0, "data_quality_score"] == 98.0


def test_write_candidate_outputs_enforces_data_quality_gate_for_formal_latest(tmp_path):
    import pandas as pd
    import json

    (tmp_path / "data_quality_report_latest.json").write_text(
        json.dumps(
            {
                "schema_version": "candidate_data_quality_report.v1",
                "expected_latest_trade_date": "20260506",
                "stocks": [
                    {"ts_code": "000001.SZ", "quality_level": "pass", "quality_score": 98.0, "blocking_reasons": []},
                    {
                        "ts_code": "000002.SZ",
                        "quality_level": "blocked",
                        "quality_score": 0.0,
                        "blocking_reasons": ["data_delay"],
                    },
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    rows = []
    for rank, code in [(1, "000001.SZ"), (2, "000002.SZ")]:
        rows.append(
            {
                "rank": rank,
                "ts_code": code,
                "stock_name": "测试",
                "industry": "银行",
                "signal": "buy",
                "basket_role": "core",
                "basket_weight_pct": 0.5,
                "portfolio_weight_after_risk": 0.5,
                "basket_risk_flag": "ok",
                "final_score": 100.0 - rank,
                "direction_prob_up": 0.6,
                "calibrated_upside_win_rate": 0.58,
                "pred_return": 0.02,
                "calibrated_avg_return": 0.015,
                "confidence": 0.7,
                "risk_level": "low",
                "position_pct": 0.2,
                "stop_loss": 9.5,
                "take_profit": 11.0,
                "reason": "test",
                "risk_overlay_penalty": 0.0,
                "diversification_penalty": 0.0,
                "style_volatility": 0.02,
                "style_relative_strength": 0.03,
            }
        )
    out = write_candidate_outputs(
        pd.DataFrame(rows),
        tmp_path,
        validation_result={"summary": {}, "records": []},
        persist_history=False,
    )

    csv_frame = pd.read_csv(out["latest_csv"])
    summary = json.loads(Path(out["latest_summary"]).read_text(encoding="utf-8"))
    gate = json.loads(Path(out["latest_data_quality_gate"]).read_text(encoding="utf-8"))
    lineage = json.loads(Path(out["latest_lineage"]).read_text(encoding="utf-8"))

    assert csv_frame["ts_code"].tolist() == ["000001.SZ"]
    assert gate["status"] == "failed"
    assert gate["blocked_codes"] == ["000002.SZ"]
    assert summary["data_quality_gate_enforced"] is True
    assert summary["data_quality_removed_codes"] == ["000002.SZ"]
    assert lineage["candidates"][0]["ts_code"] == "000001.SZ"


def test_write_candidate_outputs_can_write_interim_latest_without_overwriting_formal_latest(tmp_path):
    import pandas as pd

    df = pd.DataFrame([
        {
            "rank": 1,
            "ts_code": "000001.SZ",
            "stock_name": "平安银行",
            "industry": "银行",
            "signal": "buy",
            "basket_role": "core",
            "basket_weight_pct": 1.0,
            "portfolio_weight_after_risk": 1.0,
            "basket_risk_flag": "ok",
            "final_score": 100.0,
            "direction_prob_up": 0.6,
            "calibrated_upside_win_rate": 0.58,
            "pred_return": 0.02,
            "calibrated_avg_return": 0.015,
            "confidence": 0.7,
            "risk_level": "low",
            "position_pct": 0.2,
            "stop_loss": 9.5,
            "take_profit": 11.0,
            "reason": "test",
            "risk_overlay_penalty": 0.0,
            "diversification_penalty": 0.0,
            "style_volatility": 0.02,
            "style_relative_strength": 0.03,
        }
    ])

    write_candidate_outputs(df, tmp_path, validation_result={"summary": {}, "records": []}, persist_history=False)
    write_candidate_outputs(
        df,
        tmp_path,
        validation_result={"summary": {}, "records": []},
        guardrail={"mode": "interim", "reasons": ["partial_generation"]},
        generation_meta={"degraded": True, "reason": "interim_partial_generation", "effective_universe_size": 1},
        persist_history=False,
        latest_tag="interim_latest",
    )

    assert (tmp_path / "candidates_top_latest.csv").exists()
    assert (tmp_path / "candidates_top_interim_latest.csv").exists()
    assert (tmp_path / "candidates_basket_summary_latest.json").exists()
    assert (tmp_path / "candidates_basket_summary_interim_latest.json").exists()


def test_candidate_run_status_writer_persists_runtime_stage(tmp_path):
    path = _write_candidate_run_status(
        tmp_path,
        {
            "status": "running",
            "stage": "batch_prediction_running",
            "stage_label": "批量预测运行中",
            "started_at": "2026-04-23T03:20:00+08:00",
            "results_ready": 45,
            "skipped_count": 1,
            "effective_universe_size": 46,
            "detail": "正在生成中间候选结果",
            "elapsed_sec": 12.5,
        },
    )

    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    assert payload["status"] == "running"
    assert payload["stage"] == "batch_prediction_running"
    assert payload["stage_label"] == "批量预测运行中"
    assert payload["results_ready"] == 45
    assert payload["skipped_count"] == 1
    assert payload["effective_universe_size"] == 46
    assert payload["latest_summary_path"].endswith("candidates_basket_summary_latest.json")
    assert payload["latest_interim_summary_path"].endswith("candidates_basket_summary_interim_latest.json")
    assert payload["updated_at"] != "-"


def test_rank_candidates_keeps_research_signal_and_execution_state_columns():
    results = [
        {
            "ts_code": "AAA",
            "signal_result": {
                "signal": "watch",
                "research_signal": "blocked_watch",
                "execution_state": "blocked",
                "score": 0,
                "reason": "market_rule_blocked(limit_up)",
            },
            "forecast_result": {
                "direction_prob_up": 0.5,
                "pred_return": 0.0,
                "confidence": 0.3,
                "calibrated_upside_win_rate": 0.5,
                "calibrated_avg_return": 0.0,
                "calibration_sample_size": 120,
                "model_agreement": 0.4,
                "prediction_dispersion": 0.1,
            },
            "risk_info": {"risk_level": "high", "stop_loss": 9.0, "take_profit": 11.0},
            "position_result": {"position_pct": 0.0},
        },
    ]
    stock_basic_map = {"AAA": {"stock_name": "AAA", "industry": "元器件", "market": "Main", "area": "SZ"}}

    ranked = rank_candidates(results, top_n=1, stock_basic_map=stock_basic_map)

    assert ranked.iloc[0]["research_signal"] == "blocked_watch"
    assert ranked.iloc[0]["execution_state"] == "blocked"


def test_apply_validation_guardrail_switches_to_defensive_mode():
    import pandas as pd

    ranked = pd.DataFrame([
        {
            "rank": 1,
            "ts_code": "000001.SZ",
            "stock_name": "平安银行",
            "industry": "银行",
            "market": "主板",
            "area": "深圳",
            "signal": "strong_buy",
            "signal_score": 100.0,
            "direction_prob_up": 0.72,
            "pred_return": 0.03,
            "confidence": 0.84,
            "calibrated_upside_win_rate": 0.66,
            "calibrated_avg_return": 0.02,
            "calibration_sample_size": 120,
            "model_agreement": 0.80,
            "prediction_dispersion": 0.04,
            "risk_level": "low",
            "stop_loss": 10.0,
            "take_profit": 12.0,
            "position_pct": 0.2,
            "reason": "model_bullish",
            "final_score": 160.0,
            "cross_section_score": 92.0,
            "industry_rank_pct": 0.9,
            "diversification_penalty": 0.0,
            "selection_score": 160.0,
            "risk_overlay_penalty": 0.0,
            "basket_weight_pct": 0.55,
            "portfolio_weight_after_risk": 0.55,
            "risk_adjusted_score": 160.0,
            "basket_role": "core",
            "basket_risk_flag": "ok",
            "basket_expected_return": 0.03,
            "basket_calibrated_return": 0.02,
            "basket_win_rate": 0.66,
            "basket_risk_pressure_score": 180.0,
            "score_percentile": 1.0,
            "basket_guardrail_mode": "normal",
            "basket_guardrail_reason": "",
        },
        {
            "rank": 2,
            "ts_code": "600036.SH",
            "stock_name": "招商银行",
            "industry": "银行",
            "market": "主板",
            "area": "上海",
            "signal": "buy",
            "signal_score": 90.0,
            "direction_prob_up": 0.68,
            "pred_return": 0.02,
            "confidence": 0.76,
            "calibrated_upside_win_rate": 0.60,
            "calibrated_avg_return": 0.015,
            "calibration_sample_size": 110,
            "model_agreement": 0.72,
            "prediction_dispersion": 0.05,
            "risk_level": "medium",
            "stop_loss": 20.0,
            "take_profit": 24.0,
            "position_pct": 0.16,
            "reason": "factor_support",
            "final_score": 150.0,
            "cross_section_score": 85.0,
            "industry_rank_pct": 0.8,
            "diversification_penalty": 8.0,
            "selection_score": 142.0,
            "risk_overlay_penalty": 12.0,
            "basket_weight_pct": 0.45,
            "portfolio_weight_after_risk": 0.45,
            "risk_adjusted_score": 130.0,
            "basket_role": "satellite",
            "basket_risk_flag": "industry_overweight",
            "basket_expected_return": 0.03,
            "basket_calibrated_return": 0.02,
            "basket_win_rate": 0.66,
            "basket_risk_pressure_score": 180.0,
            "score_percentile": 0.0,
            "basket_guardrail_mode": "normal",
            "basket_guardrail_reason": "",
        },
    ])
    validation_result = {
        "summary": {
            "rebalance_dates": 3,
            "avg_excess_return_5d": -0.01,
            "basket_win_rate_5d": 0.0,
        }
    }
    guarded, meta = apply_validation_guardrail(ranked, validation_result, top_n=5)
    assert meta["mode"] == "defensive"
    assert "negative_validation_excess" in meta["reasons"]
    assert set(guarded["basket_guardrail_mode"]) == {"defensive"}
    assert guarded["portfolio_weight_after_risk"].max() <= 0.5
    assert guarded["portfolio_weight_after_risk"].max() < ranked["portfolio_weight_after_risk"].max()


def test_summarize_candidate_basket_tracks_style_exposure():
    import pandas as pd
    from run_top_candidates import summarize_candidate_basket

    df = pd.DataFrame([
        {
            "portfolio_weight_after_risk": 0.4,
            "pred_return": 0.03,
            "calibrated_avg_return": 0.025,
            "calibrated_upside_win_rate": 0.62,
            "risk_level": "low",
            "industry": "科技",
            "risk_overlay_penalty": 3.0,
            "diversification_penalty": 1.0,
            "style_volatility": 0.05,
            "style_relative_strength": 0.06,
        },
        {
            "portfolio_weight_after_risk": 0.6,
            "pred_return": 0.02,
            "calibrated_avg_return": 0.016,
            "calibrated_upside_win_rate": 0.58,
            "risk_level": "medium",
            "industry": "医药",
            "risk_overlay_penalty": 1.0,
            "diversification_penalty": 2.0,
            "style_volatility": 0.02,
            "style_relative_strength": 0.01,
        },
    ])

    summary = summarize_candidate_basket(df)

    assert summary["style_volatility_exposure"] > 0
    assert summary["style_momentum_exposure"] > 0
    assert summary["risk_pressure_score"] > 0


def test_apply_portfolio_risk_overlay_flags_liquidity_capacity_stretch():
    import pandas as pd
    from run_top_candidates import _apply_portfolio_risk_overlay

    basket = pd.DataFrame([
        {
            "ts_code": "L1",
            "industry": "科技",
            "basket_weight_pct": 0.28,
            "selection_score": 150.0,
            "risk_level": "medium",
            "style_volatility": 0.01,
            "style_relative_strength": 0.01,
            "liquidity_score": 0.42,
            "median_amount": 1_200_000.0,
            "latest_amount": 500_000.0,
        },
        {
            "ts_code": "L2",
            "industry": "银行",
            "basket_weight_pct": 0.72,
            "selection_score": 140.0,
            "risk_level": "low",
            "style_volatility": 0.01,
            "style_relative_strength": 0.01,
            "liquidity_score": 0.84,
            "median_amount": 12_000_000.0,
            "latest_amount": 11_000_000.0,
        },
    ])

    guarded = _apply_portfolio_risk_overlay(basket)

    flagged = guarded.loc[guarded["ts_code"] == "L1"].iloc[0]
    assert "liquidity_capacity_stretched" in str(flagged["basket_risk_flag"])
    assert float(flagged["risk_overlay_penalty"]) > 0.0


def test_summarize_candidate_basket_tracks_liquidity_capacity_pressure():
    import pandas as pd
    from run_top_candidates import summarize_candidate_basket

    basket = pd.DataFrame([
        {
            "portfolio_weight_after_risk": 0.30,
            "pred_return": 0.03,
            "calibrated_avg_return": 0.025,
            "calibrated_upside_win_rate": 0.61,
            "risk_level": "medium",
            "industry": "科技",
            "risk_overlay_penalty": 6.0,
            "diversification_penalty": 1.0,
            "style_volatility": 0.03,
            "style_relative_strength": 0.02,
            "liquidity_score": 0.41,
            "basket_risk_flag": "liquidity_capacity_stretched",
        },
        {
            "portfolio_weight_after_risk": 0.70,
            "pred_return": 0.02,
            "calibrated_avg_return": 0.016,
            "calibrated_upside_win_rate": 0.57,
            "risk_level": "low",
            "industry": "银行",
            "risk_overlay_penalty": 1.0,
            "diversification_penalty": 2.0,
            "style_volatility": 0.02,
            "style_relative_strength": 0.01,
            "liquidity_score": 0.88,
            "basket_risk_flag": "ok",
        },
    ])

    summary = summarize_candidate_basket(basket)

    assert summary["weighted_liquidity_score"] < 0.8
    assert summary["liquidity_capacity_weight"] == 0.3
    assert summary["risk_pressure_score"] > 0


def test_evaluate_basket_capacity_pressure_reports_scale_stress():
    import pandas as pd

    basket = pd.DataFrame([
        {
            "portfolio_weight_after_risk": 0.26,
            "liquidity_score": 0.40,
            "latest_amount": 300_000.0,
            "median_amount": 1_200_000.0,
        },
        {
            "portfolio_weight_after_risk": 0.74,
            "liquidity_score": 0.85,
            "latest_amount": 12_000_000.0,
            "median_amount": 12_500_000.0,
        },
    ])

    pressure = evaluate_basket_capacity_pressure(basket)

    assert pressure["capacity_state"] in {"watch", "stretched"}
    assert pressure["worst_stress_score"] > 0
    assert len(pressure["scenarios"]) == 4
    assert pressure["scenarios"][-1]["scenario"] == "stress_x10"


def test_apply_validation_guardrail_can_trigger_on_capacity_stretch_alone():
    import pandas as pd

    ranked = pd.DataFrame([
        {
            "rank": 1,
            "ts_code": "000001.SZ",
            "stock_name": "平安银行",
            "industry": "银行",
            "market": "主板",
            "area": "深圳",
            "signal": "strong_buy",
            "signal_score": 100.0,
            "direction_prob_up": 0.72,
            "pred_return": 0.03,
            "confidence": 0.84,
            "calibrated_upside_win_rate": 0.66,
            "calibrated_avg_return": 0.02,
            "calibration_sample_size": 120,
            "model_agreement": 0.80,
            "prediction_dispersion": 0.04,
            "risk_level": "low",
            "stop_loss": 10.0,
            "take_profit": 12.0,
            "position_pct": 0.2,
            "reason": "model_bullish",
            "final_score": 160.0,
            "cross_section_score": 92.0,
            "industry_rank_pct": 0.9,
            "diversification_penalty": 0.0,
            "selection_score": 160.0,
            "risk_overlay_penalty": 0.0,
            "basket_weight_pct": 0.55,
            "portfolio_weight_after_risk": 0.55,
            "risk_adjusted_score": 160.0,
            "basket_role": "core",
            "basket_risk_flag": "ok",
            "liquidity_score": 0.35,
            "latest_amount": 300000.0,
            "median_amount": 1500000.0,
            "basket_guardrail_mode": "normal",
            "basket_guardrail_reason": "",
        },
        {
            "rank": 2,
            "ts_code": "600036.SH",
            "stock_name": "招商银行",
            "industry": "银行",
            "market": "主板",
            "area": "上海",
            "signal": "buy",
            "signal_score": 90.0,
            "direction_prob_up": 0.68,
            "pred_return": 0.02,
            "confidence": 0.76,
            "calibrated_upside_win_rate": 0.60,
            "calibrated_avg_return": 0.015,
            "calibration_sample_size": 110,
            "model_agreement": 0.72,
            "prediction_dispersion": 0.05,
            "risk_level": "medium",
            "stop_loss": 20.0,
            "take_profit": 24.0,
            "position_pct": 0.16,
            "reason": "factor_support",
            "final_score": 150.0,
            "cross_section_score": 85.0,
            "industry_rank_pct": 0.8,
            "diversification_penalty": 8.0,
            "selection_score": 142.0,
            "risk_overlay_penalty": 12.0,
            "basket_weight_pct": 0.45,
            "portfolio_weight_after_risk": 0.45,
            "risk_adjusted_score": 130.0,
            "basket_role": "satellite",
            "basket_risk_flag": "industry_overweight",
            "liquidity_score": 0.90,
            "latest_amount": 9000000.0,
            "median_amount": 9500000.0,
            "basket_guardrail_mode": "normal",
            "basket_guardrail_reason": "",
        },
    ])
    validation_result = {"summary": {"rebalance_dates": 0, "avg_excess_return_5d": 0.01, "basket_win_rate_5d": 0.5}}

    guarded, meta = apply_validation_guardrail(ranked, validation_result, top_n=5)

    assert meta["mode"] == "defensive"
    assert "capacity_stretched" in meta["reasons"] or "capacity_watch" in meta["reasons"]
    assert "capacity_pressure" in meta


def test_evolve_candidate_strategy_profile_prefers_top1_when_it_dominates():
    import pandas as pd

    candidate_pool = pd.DataFrame(
        [
            {"confidence": 0.82, "pred_return": 0.04, "final_score": 180.0},
            {"confidence": 0.76, "pred_return": 0.03, "final_score": 165.0},
            {"confidence": 0.64, "pred_return": 0.015, "final_score": 142.0},
            {"confidence": 0.58, "pred_return": 0.008, "final_score": 128.0},
        ]
    )
    validation_result = {
        "summary": {
            "rebalance_dates": 8,
            "avg_basket_return_5d": -0.01,
            "avg_excess_return_5d": 0.001,
            "basket_win_rate_5d": 0.25,
        },
        "variants": {
            "diversified": {"avg_excess_return_5d": 0.001},
            "raw": {"avg_excess_return_5d": 0.002},
            "top1": {"avg_excess_return_5d": 0.014},
        },
    }
    profile = evolve_candidate_strategy_profile(candidate_pool, validation_result)
    assert profile["selection_mode"] == "top1"
    assert profile["strictness"] == "tight"
    assert profile["weak_market_action"] == "top1_only"
    assert profile["min_confidence"] >= 0.7


def test_candidate_strategy_profile_round_trip_and_filters_pool(tmp_path):
    import pandas as pd

    profile = {
        "selection_mode": "raw",
        "min_confidence": 0.7,
        "min_pred_return": 0.02,
        "min_final_score": 150.0,
        "strictness": "medium",
        "weak_market_action": "top3_only",
    }
    write_candidate_strategy_profile(tmp_path, profile)
    loaded = load_candidate_strategy_profile(tmp_path)
    assert loaded["selection_mode"] == "raw"

    candidate_pool = pd.DataFrame(
        [
            {"ts_code": "A", "confidence": 0.75, "pred_return": 0.03, "final_score": 160.0},
            {"ts_code": "B", "confidence": 0.69, "pred_return": 0.025, "final_score": 158.0},
            {"ts_code": "C", "confidence": 0.8, "pred_return": 0.01, "final_score": 170.0},
        ]
    )
    filtered = _apply_candidate_strategy_profile(candidate_pool, loaded)
    assert filtered["ts_code"].tolist() == ["A"]


def test_evolve_candidate_strategy_profile_tightens_from_candidate_feedback():
    import pandas as pd

    candidate_pool = pd.DataFrame(
        [
            {"confidence": 0.82, "pred_return": 0.04, "final_score": 180.0},
            {"confidence": 0.76, "pred_return": 0.03, "final_score": 165.0},
            {"confidence": 0.64, "pred_return": 0.015, "final_score": 142.0},
        ]
    )
    validation_result = {
        "summary": {
            "rebalance_dates": 6,
            "avg_basket_return_5d": 0.004,
            "avg_excess_return_5d": 0.002,
            "basket_win_rate_5d": 0.5,
        },
        "variants": {
            "diversified": {"avg_excess_return_5d": 0.002},
            "raw": {"avg_excess_return_5d": 0.001},
            "top1": {"avg_excess_return_5d": 0.003},
        },
    }
    candidate_feedback = {
        "feedback_level": "tighten",
        "change_total": 2,
        "window_label": "5D",
        "summary_note": "recent basket observation requires tighter selection and basket risk controls",
    }

    profile = evolve_candidate_strategy_profile(candidate_pool, validation_result, candidate_feedback=candidate_feedback)

    assert profile["selection_mode"] == "top1"
    assert profile["strictness"] == "tight"
    assert profile["weak_market_action"] == "cash_preferred"
    assert profile["feedback_level"] == "tighten"
    assert profile["feedback_change_total"] == 2
    assert profile["feedback_window_label"] == "5D"


def test_evolve_candidate_strategy_profile_keeps_loose_mode_on_reinforce_feedback():
    import pandas as pd

    candidate_pool = pd.DataFrame(
        [
            {"confidence": 0.82, "pred_return": 0.04, "final_score": 180.0},
            {"confidence": 0.76, "pred_return": 0.03, "final_score": 165.0},
            {"confidence": 0.64, "pred_return": 0.015, "final_score": 142.0},
        ]
    )
    validation_result = {
        "summary": {
            "rebalance_dates": 8,
            "avg_basket_return_5d": 0.012,
            "avg_excess_return_5d": 0.008,
            "basket_win_rate_5d": 0.67,
        },
        "variants": {
            "diversified": {"avg_excess_return_5d": 0.008},
            "raw": {"avg_excess_return_5d": 0.004},
            "top1": {"avg_excess_return_5d": 0.005},
        },
    }
    candidate_feedback = {
        "feedback_level": "reinforce",
        "change_total": 0,
        "window_label": "10D",
        "summary_note": "recent basket observation is strong enough to reinforce the current selection profile",
    }

    profile = evolve_candidate_strategy_profile(candidate_pool, validation_result, candidate_feedback=candidate_feedback)

    assert profile["selection_mode"] == "diversified"
    assert profile["strictness"] == "loose"
    assert profile["weak_market_action"] == "normal"
    assert profile["feedback_level"] == "reinforce"
    assert profile["feedback_window_label"] == "10D"
