from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from openclaw.runtime import v49_handlers
from openclaw.runtime.v49_handlers import (
    HandlerFactory,
    _load_backtest_frame,
    _normalize_summary,
    _run_combo_ensemble_backtest,
    _weighted_combo_summary,
)


def test_normalize_summary_handles_negative_percent_drawdown():
    out = _normalize_summary(
        {
            "win_rate": 50.0,
            "max_drawdown": -21.96,
            "total_trades": 66,
            "sample_size": 500,
        }
    )
    assert abs(out["win_rate"] - 0.5) < 1e-9
    assert abs(out["max_drawdown"] - 0.2196) < 1e-6
    assert abs(out["signal_density"] - 0.132) < 1e-9


def test_normalize_summary_handles_fraction_drawdown():
    out = _normalize_summary({"win_rate": 0.61, "max_drawdown": 0.18, "total_trades": 90, "sample_size": 300})
    assert abs(out["win_rate"] - 0.61) < 1e-9
    assert abs(out["max_drawdown"] - 0.18) < 1e-9
    assert abs(out["signal_density"] - 0.3) < 1e-9


def test_normalize_summary_supports_total_signals_and_analyzed_stocks():
    out = _normalize_summary(
        {
            "win_rate": 44.0,
            "max_drawdown": 32.0,
            "total_signals": 88,
            "analyzed_stocks": 220,
        }
    )
    assert abs(out["win_rate"] - 0.44) < 1e-9
    assert abs(out["max_drawdown"] - 0.32) < 1e-9
    assert abs(out["signal_density"] - 0.4) < 1e-9


def test_normalize_summary_uses_runtime_sample_size_when_legacy_result_omits_it():
    out = _normalize_summary(
        {
            "win_rate": 100.0,
            "max_drawdown": 0.0,
            "total_trades": 3,
        },
        fallback_sample_size=40,
    )
    assert abs(out["win_rate"] - 1.0) < 1e-9
    assert abs(out["signal_density"] - 0.075) < 1e-9


def test_weighted_combo_summary_penalizes_worst_drawdown():
    out = _weighted_combo_summary(
        summaries={
            "v7": {"win_rate": 0.55, "max_drawdown": 0.20, "signal_density": 0.5},
            "v8": {"win_rate": 0.45, "max_drawdown": 0.30, "signal_density": 0.8},
            "v9": {"win_rate": 0.60, "max_drawdown": 0.80, "signal_density": 0.4},
            "legacy": {"win_rate": 0.50, "max_drawdown": 0.25, "signal_density": 0.6},
        },
        sample_size=100,
    )
    assert 0.0 <= out["win_rate"] <= 1.0
    assert 0.0 <= out["signal_density"] <= 2.0
    assert out["max_drawdown"] > 0.3


def test_backtest_handler_reuses_module_analyzer_and_frame_cache(monkeypatch):
    module_calls = {"n": 0}
    analyzer_inits = {"n": 0}
    frame_calls = {"n": 0}

    class FakeAnalyzer:
        def __init__(self, db_path: str):
            analyzer_inits["n"] += 1

        def backtest_v8_ultimate(self, df_bt, sample_size, holding_days, score_threshold):
            return {
                "success": True,
                "stats": {
                    "win_rate": 0.6,
                    "max_drawdown": 0.2,
                    "total_trades": 30,
                    "sample_size": 100,
                },
            }

    def fake_load_module(_path: Path):
        module_calls["n"] += 1
        return SimpleNamespace(CompleteVolumePriceAnalyzer=FakeAnalyzer)

    def fake_load_frame(db_path: Path, lookback_days: int, sample_size: int):
        frame_calls["n"] += 1
        return pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20260101"], "close_price": [10.0]})

    monkeypatch.setattr(v49_handlers, "_load_module", fake_load_module)
    monkeypatch.setattr(v49_handlers, "_load_backtest_frame", fake_load_frame)
    monkeypatch.setattr(v49_handlers, "_resolve_db_path", lambda preferred: Path("/tmp/fake.db"))

    handler = HandlerFactory(module_path=Path("/tmp/fake_module.py")).create_backtest_handler("v8")

    out1 = handler({"sample_size": 50, "holding_days": 8, "score_threshold": 50})
    out2 = handler({"sample_size": 50, "holding_days": 8, "score_threshold": 50})

    assert out1["summary"]["win_rate"] == 0.6
    assert out2["summary"]["max_drawdown"] == 0.2
    assert module_calls["n"] == 1
    assert analyzer_inits["n"] == 1
    assert frame_calls["n"] == 1


def test_load_backtest_frame_uses_recent_liquidity_pool(tmp_path):
    db = tmp_path / "sample.db"
    conn = sqlite3.connect(db)
    try:
        conn.execute("CREATE TABLE stock_basic (ts_code TEXT PRIMARY KEY, name TEXT, industry TEXT)")
        conn.execute(
            """
            CREATE TABLE daily_trading_data (
                ts_code TEXT,
                trade_date TEXT,
                close_price REAL,
                high_price REAL,
                low_price REAL,
                vol REAL,
                amount REAL,
                pct_chg REAL,
                turnover_rate REAL
            )
            """
        )
        conn.executemany(
            "INSERT INTO stock_basic(ts_code, name, industry) VALUES (?, ?, ?)",
            [("000001.SZ", "low", "bank"), ("600111.SH", "high", "metal")],
        )
        rows = []
        for i in range(70):
            day = f"202604{i + 1:02d}"
            rows.append(("000001.SZ", day, 10, 11, 9, 100, 1000, 0.1, 1.0))
            rows.append(("600111.SH", day, 20, 22, 18, 100, 9000, 0.1, 1.0))
        conn.executemany(
            """
            INSERT INTO daily_trading_data(
                ts_code, trade_date, close_price, high_price, low_price, vol, amount, pct_chg, turnover_rate
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
    finally:
        conn.close()

    out = _load_backtest_frame(db, lookback_days=120, sample_size=1)

    assert set(out["ts_code"].unique()) == {"600111.SH"}


def test_combo_backtest_handler_passes_explicit_runtime_thresholds(monkeypatch):
    seen = {}

    class FakeAnalyzer:
        def __init__(self, db_path: str):
            pass

        def backtest_combo_production(
            self,
            df_bt,
            *,
            sample_size,
            holding_days,
            combo_threshold,
            min_agree,
            thr_v5,
            thr_v8,
            thr_v9,
        ):
            seen.update(
                {
                    "sample_size": sample_size,
                    "holding_days": holding_days,
                    "combo_threshold": combo_threshold,
                    "min_agree": min_agree,
                    "thr_v5": thr_v5,
                    "thr_v8": thr_v8,
                    "thr_v9": thr_v9,
                }
            )
            return {"success": True, "stats": {"win_rate": 50.0, "max_drawdown": 0.0, "total_signals": 2, "sample_size": 100}}

    monkeypatch.setattr(v49_handlers, "_load_module", lambda _path: SimpleNamespace(CompleteVolumePriceAnalyzer=FakeAnalyzer))
    monkeypatch.setattr(v49_handlers, "_load_backtest_frame", lambda *args, **kwargs: pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20260101"], "close_price": [10.0]}))
    monkeypatch.setattr(v49_handlers, "_resolve_db_path", lambda preferred: Path("/tmp/fake.db"))

    handler = HandlerFactory(module_path=Path("/tmp/fake_module.py")).create_backtest_handler("combo")
    out = handler(
        {
            "sample_size": 80,
            "holding_days": 6,
            "score_threshold": 55,
            "min_agree": 2,
            "thr_v5": 60,
            "thr_v8": 65,
            "thr_v9": 60,
        }
    )

    assert out["summary"]["win_rate"] == 0.5
    assert seen == {
        "sample_size": 80,
        "holding_days": 6,
        "combo_threshold": 55.0,
        "min_agree": 2,
        "thr_v5": 60.0,
        "thr_v8": 65.0,
        "thr_v9": 60.0,
    }


def test_combo_scan_handler_maps_score_threshold_to_combo_threshold(monkeypatch):
    seen_env = {}

    def fake_load_module(_path: Path):
        import os

        def run_offline_all():
            seen_env["COMBO_SCORE_THRESHOLD"] = os.environ.get("COMBO_SCORE_THRESHOLD")
            seen_env["COMBO_THRESHOLD"] = os.environ.get("COMBO_THRESHOLD")
            seen_env["COMBO_THR_V5"] = os.environ.get("COMBO_THR_V5")
            seen_env["COMBO_THR_V8"] = os.environ.get("COMBO_THR_V8")
            seen_env["COMBO_THR_V9"] = os.environ.get("COMBO_THR_V9")
            return {"combo": (pd.DataFrame(), {"status": "ok"})}

        return SimpleNamespace(run_offline_all=run_offline_all)

    monkeypatch.setattr(v49_handlers, "_load_module", fake_load_module)
    monkeypatch.setattr(v49_handlers, "_resolve_db_path", lambda preferred: Path("/tmp/fake.db"))

    handler = HandlerFactory(module_path=Path("/tmp/fake_module.py")).create_scan_handler("combo")
    out = handler({"score_threshold": 65, "thr_v5": 70, "thr_v8": 45, "thr_v9": 65, "offline_stock_limit": 10})

    assert out["metrics"]["raw_rows"] == 0
    assert seen_env["COMBO_SCORE_THRESHOLD"] == "65"
    assert seen_env["COMBO_THRESHOLD"] == "65"
    assert seen_env["COMBO_THR_V5"] == "70"
    assert seen_env["COMBO_THR_V8"] == "45"
    assert seen_env["COMBO_THR_V9"] == "65"


def test_combo_ensemble_default_components_skip_v7():
    calls = {"v7": 0, "v8": 0, "v9": 0, "legacy": 0}

    class FakeAnalyzer:
        def backtest_v7_intelligent(self, *args, **kwargs):
            calls["v7"] += 1
            return {"success": True, "stats": {"win_rate": 0.5, "max_drawdown": 0.2, "total_trades": 10, "sample_size": 50}}

        def backtest_v8_ultimate(self, *args, **kwargs):
            calls["v8"] += 1
            return {"success": True, "stats": {"win_rate": 0.52, "max_drawdown": 0.22, "total_trades": 12, "sample_size": 50}}

        def backtest_v9_midterm(self, *args, **kwargs):
            calls["v9"] += 1
            return {"success": True, "stats": {"win_rate": 0.56, "max_drawdown": 0.24, "total_trades": 14, "sample_size": 50}}

        def backtest_strategy_complete(self, *args, **kwargs):
            calls["legacy"] += 1
            return {"success": True, "stats": {"win_rate": 0.5, "max_drawdown": 0.2, "total_trades": 11, "sample_size": 50}}

    out = _run_combo_ensemble_backtest(
        FakeAnalyzer(),
        pd.DataFrame({"x": [1]}),
        {"sample_size": 50, "holding_days": 8, "score_threshold": 60},
    )
    assert out["success"] is True
    assert calls["v7"] == 0
    assert calls["v8"] == 1
    assert calls["v9"] == 1
    assert calls["legacy"] == 1
