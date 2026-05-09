from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from openclaw.runtime import v49_handlers
from openclaw.runtime.v49_handlers import (
    HandlerFactory,
    _build_v8_risk_diagnostics,
    _load_backtest_frame,
    _normalize_summary,
    _run_combo_ensemble_backtest,
    _slice_index_data_for_history,
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


def test_normalize_summary_preserves_execution_constraint_evidence():
    out = _normalize_summary(
        {
            "win_rate": 60.0,
            "max_drawdown": 10.0,
            "total_signals": 6,
            "sample_size": 100,
            "tradeability_filter_enabled": True,
            "volume_constraint_enabled": True,
            "skip_untradeable": 3,
            "skip_volume": 1,
            "skip_limit": 2,
            "trading_cost": {"base_round_trip_bp": 46.0, "expected_cost_bp": 4.6},
        }
    )

    assert out["tradeability_filter_enabled"] is True
    assert out["volume_constraint_enabled"] is True
    assert out["skip_untradeable"] == 3
    assert out["trading_cost"]["base_round_trip_bp"] == 46.0


def test_normalize_summary_preserves_risk_diagnostics_payload():
    out = _normalize_summary(
        {
            "win_rate": 55.0,
            "max_drawdown": 20.0,
            "total_signals": 5,
            "sample_size": 50,
            "risk_diagnostics": {"exit_reason_counts": {"stop_loss": 2}},
        }
    )

    assert out["risk_diagnostics"] == {"exit_reason_counts": {"stop_loss": 2}}


def test_normalize_summary_preserves_risk_control_payload():
    out = _normalize_summary(
        {
            "win_rate": 55.0,
            "max_drawdown": 20.0,
            "total_signals": 5,
            "sample_size": 50,
            "risk_control": {"max_stop_loss_pct": 0.08},
        }
    )

    assert out["risk_control"] == {"max_stop_loss_pct": 0.08}


def test_build_v8_risk_diagnostics_freezes_exit_tail_and_atr_profile():
    df = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "trade_date": "20260101",
                "future_return": -6.0,
                "gross_return": -5.7,
                "exit_reason": "stop_loss",
                "holding_days_realized": 2,
                "score": 42.0,
                "pre_market_score": 50.0,
                "market_penalty": 0.8,
                "atr_stop_loss_pct": 4.0,
                "atr_stop_loss_raw_pct": 12.0,
                "risk_stop_loss_pct_cap": 8.0,
                "atr_take_profit_pct": 6.0,
            },
            {
                "ts_code": "000002.SZ",
                "trade_date": "20260102",
                "future_return": 3.0,
                "gross_return": 3.3,
                "exit_reason": "take_profit",
                "holding_days_realized": 4,
                "score": 51.0,
                "pre_market_score": 60.0,
                "market_penalty": 1.0,
                "atr_stop_loss_pct": 5.0,
                "atr_take_profit_pct": 8.0,
            },
        ]
    )

    out = _build_v8_risk_diagnostics(df)

    assert out["available"] is True
    assert out["exit_reason_counts"] == {"stop_loss": 1, "take_profit": 1}
    assert out["tail_loss_count_5pct"] == 1
    assert out["exit_reason_return_profile"]["stop_loss"]["worst_return_pct"] == -6.0
    assert out["atr_stop_loss_pct"]["avg"] == 4.5
    assert out["atr_stop_loss_raw_pct"]["avg"] == 12.0
    assert out["risk_stop_loss_pct_cap"]["avg"] == 8.0
    assert out["worst_trades"][0]["ts_code"] == "000001.SZ"


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

    class FakeEngine:
        def __init__(self, df_bt, sample_size, holding_days):
            pass

        def run_rolling(self, *, min_rows, window, step, signal_fn, stop_on_first_signal):
            hist = pd.DataFrame({"close_price": [10.0] * 80})
            g = pd.DataFrame({"name": ["A"], "industry": ["I"], "close_price": [10.0]})
            sig = signal_fn("000001.SZ", g, 0, hist)
            row = {
                "ts_code": "000001.SZ",
                "trade_date": "20260101",
                "future_return": 2.0,
                "signal_strength": sig["signal_strength"],
            }
            return pd.DataFrame([row]), 1

        def execution_constraints_summary(self):
            return {"tradeability_filter_enabled": True, "volume_constraint_enabled": True}

    class FakeEvaluatorV8:
        def evaluate_stock_v8(self, hist, ts_code, index_data):
            return {"success": True, "final_score": 62.0, "atr_stops": {"stop_loss": 9.0, "take_profit": 12.0}}

    class FakeAnalyzer:
        def __init__(self, db_path: str):
            analyzer_inits["n"] += 1
            self.evaluator_v8 = FakeEvaluatorV8()

        def _calculate_backtest_stats(self, backtest_df, analyzed, holding_days):
            return {
                "win_rate": 60.0,
                "max_drawdown": 20.0,
                "total_trades": 30,
                "sample_size": 100,
            }

    def fake_load_module(_path: Path):
        module_calls["n"] += 1
        return SimpleNamespace(CompleteVolumePriceAnalyzer=FakeAnalyzer, UnifiedBacktestEngine=FakeEngine, _ensure_price_aliases=lambda x: x)

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


def test_backtest_handler_marks_raw_unsuccessful_result_failed(monkeypatch):
    class FakeAnalyzer:
        def __init__(self, db_path: str):
            pass

        def backtest_combo_production(self, *args, **kwargs):
            return {"success": False, "error": "组合策略回测未产生有效信号", "stats": {"analyzed_stocks": 80}}

    monkeypatch.setattr(v49_handlers, "_load_module", lambda _path: SimpleNamespace(CompleteVolumePriceAnalyzer=FakeAnalyzer))
    monkeypatch.setattr(v49_handlers, "_load_backtest_frame", lambda *args, **kwargs: pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20260101"], "close_price": [10.0]}))
    monkeypatch.setattr(v49_handlers, "_resolve_db_path", lambda preferred: Path("/tmp/fake.db"))

    handler = HandlerFactory(module_path=Path("/tmp/fake_module.py")).create_backtest_handler("combo")
    out = handler({"sample_size": 80, "holding_days": 10, "score_threshold": 68})

    assert out["status"] == "failed"
    assert out["raw"]["error"] == "组合策略回测未产生有效信号"


def test_combo_backtest_handler_passes_runtime_risk_controls(monkeypatch):
    seen = {}

    class FakeAnalyzer:
        def __init__(self, db_path: str):
            pass

        def backtest_combo_production(self, df_bt, **kwargs):
            seen.update(kwargs)
            return {
                "success": True,
                "stats": {
                    "win_rate": 60.0,
                    "max_drawdown": 12.0,
                    "total_trades": 6,
                    "sample_size": 20,
                    "risk_control": {"max_stop_loss_pct": kwargs["max_stop_loss_pct"]},
                },
            }

    monkeypatch.setattr(v49_handlers, "_load_module", lambda _path: SimpleNamespace(CompleteVolumePriceAnalyzer=FakeAnalyzer))
    monkeypatch.setattr(v49_handlers, "_load_backtest_frame", lambda *args, **kwargs: pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20260101"], "close_price": [10.0]}))
    monkeypatch.setattr(v49_handlers, "_resolve_db_path", lambda preferred: Path("/tmp/fake.db"))

    handler = HandlerFactory(module_path=Path("/tmp/fake_module.py")).create_backtest_handler("combo")
    out = handler({"sample_size": 20, "holding_days": 6, "score_threshold": 65, "max_stop_loss_pct": 0.05})

    assert out["status"] == "success"
    assert seen["max_stop_loss_pct"] == 0.05
    assert out["summary"]["risk_control"] == {"max_stop_loss_pct": 0.05}


def test_v6_backtest_handler_uses_runtime_replay_path(monkeypatch):
    seen = {}

    class FakeEngine:
        def __init__(self, df_bt, sample_size, holding_days):
            seen["sample_size"] = sample_size
            seen["holding_days"] = holding_days

        def run_rolling(self, *, min_rows, window, step, signal_fn, stop_on_first_signal):
            seen["step"] = step
            hist = pd.DataFrame({"close_price": [10.0] * 80})
            g = pd.DataFrame({"name": ["A"], "industry": ["I"], "close_price": [10.0]})
            sig = signal_fn("000001.SZ", g, 0, hist)
            return pd.DataFrame([{"ts_code": "000001.SZ", "trade_date": "20260101", "future_return": 3.0, "signal_strength": sig["signal_strength"]}]), 1

        def execution_constraints_summary(self):
            return {"tradeability_filter_enabled": True, "volume_constraint_enabled": True}

    class FakeEvaluatorV6:
        def evaluate_stock_v6(self, hist, ts_code):
            return {"success": True, "final_score": 82.0, "grade": "A"}

    class FakeAnalyzer:
        def __init__(self, db_path: str):
            self.evaluator_v6 = FakeEvaluatorV6()

        def _calculate_backtest_stats(self, backtest_df, analyzed, holding_days):
            return {"win_rate": 100.0, "max_drawdown": 0.0, "total_trades": len(backtest_df), "sample_size": 20}

    fake_module = SimpleNamespace(CompleteVolumePriceAnalyzer=FakeAnalyzer, UnifiedBacktestEngine=FakeEngine, _ensure_price_aliases=lambda x: x)
    monkeypatch.setattr(v49_handlers, "_load_module", lambda _path: fake_module)
    monkeypatch.setattr(v49_handlers, "_load_backtest_frame", lambda *args, **kwargs: pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20260101"], "close_price": [10.0]}))
    monkeypatch.setattr(v49_handlers, "_resolve_db_path", lambda preferred: Path("/tmp/fake.db"))

    handler = HandlerFactory(module_path=Path("/tmp/fake_module.py")).create_backtest_handler("v6")
    out = handler({"sample_size": 20, "holding_days": 5, "score_threshold": 75, "replay_step": 12})

    assert out["status"] == "success"
    assert out["raw"]["strategy"] == "v6_runtime_replay"
    assert out["summary"]["tradeability_filter_enabled"] is True
    assert out["summary"]["risk_control"] == {"stop_loss": -0.05, "take_profit": 0.08}
    assert out["summary"]["risk_diagnostics"]["available"] is True
    assert seen["step"] == 12


def test_v6_runtime_backtest_uses_stop_take_fill_price(monkeypatch):
    seen = {}

    class FakeEngine:
        def __init__(self, df_bt, sample_size, holding_days):
            pass

        def run_rolling(self, *, min_rows, window, step, signal_fn, stop_on_first_signal):
            hist = pd.DataFrame({"trade_date": ["20260101"] * 80, "close_price": [10.0] * 80, "vol": [100.0] * 80, "pct_chg": [0.0] * 80})
            g = pd.DataFrame(
                {
                    "name": ["A"] * 4,
                    "industry": ["I"] * 4,
                    "trade_date": ["20260101", "20260102", "20260103", "20260104"],
                    "close_price": [10.0, 9.4, 9.3, 9.2],
                    "vol": [100.0] * 4,
                    "pct_chg": [0.0, -6.0, -1.0, -1.0],
                }
            )
            sig = signal_fn("000001.SZ", g, 0, hist)
            seen.update(sig)
            return pd.DataFrame([{"ts_code": "000001.SZ", "trade_date": "20260101", "future_return": -6.0, "signal_strength": sig["signal_strength"]}]), 1

        def execution_constraints_summary(self):
            return {"tradeability_filter_enabled": True, "volume_constraint_enabled": True}

    class FakeEvaluatorV6:
        def evaluate_stock_v6(self, hist, ts_code):
            return {"success": True, "final_score": 82.0, "grade": "A", "filter_passed": True}

    class FakeAnalyzer:
        def __init__(self, db_path: str):
            self.evaluator_v6 = FakeEvaluatorV6()

        def _calculate_backtest_stats(self, backtest_df, analyzed, holding_days):
            return {"win_rate": 0.0, "max_drawdown": 6.0, "total_trades": len(backtest_df), "sample_size": 20}

    fake_module = SimpleNamespace(CompleteVolumePriceAnalyzer=FakeAnalyzer, UnifiedBacktestEngine=FakeEngine, _ensure_price_aliases=lambda x: x)
    monkeypatch.setattr(v49_handlers, "_load_module", lambda _path: fake_module)
    monkeypatch.setattr(v49_handlers, "_load_backtest_frame", lambda *args, **kwargs: pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20260101"], "close_price": [10.0]}))
    monkeypatch.setattr(v49_handlers, "_resolve_db_path", lambda preferred: Path("/tmp/fake.db"))

    handler = HandlerFactory(module_path=Path("/tmp/fake_module.py")).create_backtest_handler("v6")
    out = handler({"sample_size": 20, "holding_days": 3, "score_threshold": 75, "stop_loss": -0.05})

    assert out["status"] == "success"
    assert abs(seen["__exit_price"] - 9.5) < 1e-9
    assert seen["execution_fill_model"] == "stop_take_price"
    assert seen["exit_reason"] == "stop_loss"


def test_v6_runtime_backtest_marks_advisory_candidate_filter_as_research_only(monkeypatch):
    seen = {}

    class FakeEngine:
        def __init__(self, df_bt, sample_size, holding_days):
            pass

        def run_rolling(self, *, min_rows, window, step, signal_fn, stop_on_first_signal):
            hist = pd.DataFrame({"trade_date": ["20260101"] * 80, "close_price": [10.0] * 80, "vol": [100.0] * 80, "pct_chg": [0.0] * 80})
            g = pd.DataFrame({"name": ["A"], "industry": ["I"], "trade_date": ["20260101"], "close_price": [10.0]})
            sig = signal_fn("000001.SZ", g, 0, hist)
            seen.update(sig)
            return pd.DataFrame([{"ts_code": "000001.SZ", "trade_date": "20260101", "future_return": 2.0, "signal_strength": sig["signal_strength"]}]), 1

        def execution_constraints_summary(self):
            return {"tradeability_filter_enabled": True, "volume_constraint_enabled": True}

    class FakeEvaluatorV6:
        def evaluate_stock_v6(self, hist, ts_code):
            assert self.runtime_candidate_filter_mode == "diagnostic_advisory"
            return {
                "success": True,
                "final_score": 82.0,
                "grade": "A",
                "filter_passed": False,
                "candidate_filter_relaxed": True,
            }

    class FakeAnalyzer:
        def __init__(self, db_path: str):
            self.evaluator_v6 = FakeEvaluatorV6()

        def _calculate_backtest_stats(self, backtest_df, analyzed, holding_days):
            return {"win_rate": 100.0, "max_drawdown": 0.0, "total_trades": len(backtest_df), "sample_size": 20}

    fake_module = SimpleNamespace(CompleteVolumePriceAnalyzer=FakeAnalyzer, UnifiedBacktestEngine=FakeEngine, _ensure_price_aliases=lambda x: x)
    monkeypatch.setattr(v49_handlers, "_load_module", lambda _path: fake_module)
    monkeypatch.setattr(v49_handlers, "_load_backtest_frame", lambda *args, **kwargs: pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20260101"], "close_price": [10.0]}))
    monkeypatch.setattr(v49_handlers, "_resolve_db_path", lambda preferred: Path("/tmp/fake.db"))

    handler = HandlerFactory(module_path=Path("/tmp/fake_module.py")).create_backtest_handler("v6")
    out = handler({"sample_size": 20, "holding_days": 3, "score_threshold": 75, "candidate_filter_mode": "diagnostic_advisory"})

    diag = out["raw"]["backtest_diagnostics"]["v6_runtime_diagnostics"]
    assert out["status"] == "success"
    assert seen["candidate_filter_mode"] == "diagnostic_advisory"
    assert seen["filter_passed"] is False
    assert diag["production_candidate_allowed"] is False
    assert diag["candidate_filter_mode"] == "diagnostic_advisory"
    assert diag["candidate_filter_relaxed_count"] == 1


def test_v6_backtest_handler_freezes_score_distribution_when_no_signal(monkeypatch):
    class FakeEngine:
        def __init__(self, df_bt, sample_size, holding_days):
            pass

        def run_rolling(self, *, min_rows, window, step, signal_fn, stop_on_first_signal):
            hist = pd.DataFrame({"close_price": [10.0] * 80})
            g = pd.DataFrame({"name": ["A"], "industry": ["I"], "close_price": [10.0]})
            signal_fn("000001.SZ", g, 0, hist)
            return pd.DataFrame(), 1

    class FakeEvaluatorV6:
        def evaluate_stock_v6(self, hist, ts_code):
            return {
                "success": True,
                "final_score": 42.0,
                "grade": "C",
                "base_score": 38.0,
                "synergy_bonus": 4.0,
                "risk_penalty": 0.0,
                "dimension_scores": {"资金流向": 12.0, "短期动量": 8.0},
            }

    class FakeAnalyzer:
        def __init__(self, db_path: str):
            self.evaluator_v6 = FakeEvaluatorV6()

    fake_module = SimpleNamespace(CompleteVolumePriceAnalyzer=FakeAnalyzer, UnifiedBacktestEngine=FakeEngine, _ensure_price_aliases=lambda x: x)
    monkeypatch.setattr(v49_handlers, "_load_module", lambda _path: fake_module)
    monkeypatch.setattr(v49_handlers, "_load_backtest_frame", lambda *args, **kwargs: pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20260101"], "close_price": [10.0]}))
    monkeypatch.setattr(v49_handlers, "_resolve_db_path", lambda preferred: Path("/tmp/fake.db"))

    handler = HandlerFactory(module_path=Path("/tmp/fake_module.py")).create_backtest_handler("v6")
    out = handler({"sample_size": 20, "holding_days": 5, "score_threshold": 75})

    diag = out["raw"]["backtest_diagnostics"]
    assert out["status"] == "failed"
    assert diag["type"] == "score_distribution"
    assert diag["evaluated"] == 1
    assert diag["passed_threshold"] == 0
    assert diag["max_score"] == 42.0
    assert diag["score_breakdown"]["base_score"]["avg"] == 38.0
    assert diag["score_breakdown"]["dim:资金流向"]["avg"] == 12.0
    assert diag["v6_runtime_diagnostics"]["point_in_time_context"] is True
    assert diag["v6_runtime_diagnostics"]["production_candidate_allowed"] is False
    assert diag["v6_runtime_diagnostics"]["threshold_near_samples"]["within_10"] == 0


def test_v6_backtest_handler_freezes_top_near_threshold_sample_details(monkeypatch):
    class FakeEngine:
        def __init__(self, df_bt, sample_size, holding_days):
            pass

        def run_rolling(self, *, min_rows, window, step, signal_fn, stop_on_first_signal, max_evaluations=None):
            hist = pd.DataFrame({"close_price": [10.0] * 80, "trade_date": ["20260101"] * 80})
            g = pd.DataFrame({"name": ["A"], "industry": ["I"], "close_price": [10.0], "trade_date": ["20260101"]})
            for ts_code in ("000001.SZ", "000002.SZ", "000003.SZ"):
                signal_fn(ts_code, g, 0, hist)
            return pd.DataFrame(), 3

    class FakeEvaluatorV6:
        def __init__(self):
            self.scores = iter([63.0, 58.0, 40.0])

        def evaluate_stock_v6(self, hist, ts_code):
            final_score = next(self.scores)
            return {
                "success": True,
                "final_score": final_score,
                "grade": "B",
                "base_score": final_score - 4.0,
                "synergy_bonus": 6.0,
                "synergy_combo": "强势共振",
                "risk_penalty": 2.0,
                "risk_reasons": ["中高位(-5分)", "强势非极端风险折减(5->4)"],
                "dimension_scores": {"资金流向": 20.0, "短期动量": 13.0, "龙头属性": 8.0},
                "filter_passed": False,
                "candidate_filter_relaxed": True,
                "price_position": 70.0,
                "vol_ratio": 1.8,
                "price_chg_3d": 7.0,
            }

    class FakeAnalyzer:
        def __init__(self, db_path: str):
            self.evaluator_v6 = FakeEvaluatorV6()

    fake_module = SimpleNamespace(CompleteVolumePriceAnalyzer=FakeAnalyzer, UnifiedBacktestEngine=FakeEngine, _ensure_price_aliases=lambda x: x)
    monkeypatch.setattr(v49_handlers, "_load_module", lambda _path: fake_module)
    monkeypatch.setattr(v49_handlers, "_load_backtest_frame", lambda *args, **kwargs: pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20260101"], "close_price": [10.0]}))
    monkeypatch.setattr(v49_handlers, "_resolve_db_path", lambda preferred: Path("/tmp/fake.db"))

    handler = HandlerFactory(module_path=Path("/tmp/fake_module.py")).create_backtest_handler("v6")
    out = handler({"sample_size": 20, "holding_days": 5, "score_threshold": 65, "candidate_filter_mode": "diagnostic_advisory", "max_evaluations": 3})

    samples = out["raw"]["backtest_diagnostics"]["top_near_threshold_samples"]
    assert [row["final_score"] for row in samples] == [63.0, 58.0]
    assert samples[0]["gap_to_threshold"] == 2.0
    assert samples[0]["dimension_scores"]["资金流向"] == 20.0
    assert samples[0]["synergy_combo"] == "强势共振"
    assert samples[0]["candidate_filter_relaxed"] is True


def test_v6_backtest_handler_blocks_score_pass_without_tradeable_entry(monkeypatch):
    class FakeEngine:
        def __init__(self, df_bt, sample_size, holding_days):
            pass

        def run_rolling(self, *, min_rows, window, step, signal_fn, stop_on_first_signal, max_evaluations=None):
            hist = pd.DataFrame({"close_price": [10.0] * 80, "trade_date": ["20260101"] * 80})
            g = pd.DataFrame({"name": ["A"], "industry": ["I"], "close_price": [10.0], "trade_date": ["20260101"]})
            sig = signal_fn("000001.SZ", g, 0, hist)
            assert sig is None
            return pd.DataFrame(), 1

    class FakeEvaluatorV6:
        def evaluate_stock_v6(self, hist, ts_code):
            return {
                "success": True,
                "final_score": 70.0,
                "grade": "A",
                "base_score": 82.0,
                "synergy_bonus": 30.0,
                "synergy_combo": "板块总龙头",
                "risk_penalty": 42.0,
                "risk_reasons": ["极高位(-25分)"],
                "dimension_scores": {"资金流向": 22.0, "短期动量": 18.0},
                "entry_gate": {
                    "passed": False,
                    "mode": "wait_for_pullback_reconfirmation",
                    "reason": "entry_gate_overheated_wait_for_pullback",
                    "overheat_flags": ["price_position_extreme"],
                },
            }

    class FakeAnalyzer:
        def __init__(self, db_path: str):
            self.evaluator_v6 = FakeEvaluatorV6()

    fake_module = SimpleNamespace(CompleteVolumePriceAnalyzer=FakeAnalyzer, UnifiedBacktestEngine=FakeEngine, _ensure_price_aliases=lambda x: x)
    monkeypatch.setattr(v49_handlers, "_load_module", lambda _path: fake_module)
    monkeypatch.setattr(v49_handlers, "_load_backtest_frame", lambda *args, **kwargs: pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20260101"], "close_price": [10.0]}))
    monkeypatch.setattr(v49_handlers, "_resolve_db_path", lambda preferred: Path("/tmp/fake.db"))

    handler = HandlerFactory(module_path=Path("/tmp/fake_module.py")).create_backtest_handler("v6")
    out = handler({"sample_size": 20, "holding_days": 5, "score_threshold": 65, "max_evaluations": 1})

    diag = out["raw"]["backtest_diagnostics"]
    assert out["status"] == "failed"
    assert diag["passed_threshold"] == 1
    assert diag["reason_counts"]["entry_gate_blocked"] == 1
    assert diag["entry_gate_review"]["blocked"] == 1
    assert diag["entry_gate_review"]["mode_counts"]["wait_for_pullback_reconfirmation"] == 1
    assert diag["entry_gate_review"]["overheat_flag_counts"]["price_position_extreme"] == 1
    assert diag["top_near_threshold_samples"][0]["entry_gate"]["mode"] == "wait_for_pullback_reconfirmation"
    assert diag.get("entry_gate_passed_samples", []) == []


def test_v6_backtest_handler_freezes_entry_gate_passed_samples(monkeypatch):
    class FakeEngine:
        def __init__(self, df_bt, sample_size, holding_days):
            pass

        def run_rolling(self, *, min_rows, window, step, signal_fn, stop_on_first_signal, max_evaluations=None):
            hist = pd.DataFrame({"close_price": [10.0] * 80, "trade_date": ["20260101"] * 80})
            g = pd.DataFrame({"name": ["A"], "industry": ["I"], "close_price": [10.0], "trade_date": ["20260101"]})
            signal_fn("000001.SZ", g, 0, hist)
            signal_fn("000002.SZ", g, 0, hist)
            return pd.DataFrame(), 2

    class FakeEvaluatorV6:
        def __init__(self):
            self.rows = iter(
                [
                    {
                        "success": True,
                        "final_score": 54.0,
                        "grade": "B",
                        "base_score": 62.0,
                        "synergy_bonus": 12.0,
                        "risk_penalty": 20.0,
                        "dimension_scores": {"资金流向": 18.0, "短期动量": 12.0},
                        "entry_gate": {
                            "passed": True,
                            "mode": "pullback_reconfirmed",
                            "reason": "entry_gate_pullback_reconfirmed",
                            "pullback_from_20d_high": 7.5,
                            "volume_cooling": True,
                            "secondary_confirmation": {
                                "technical_breakout_reconfirmed": False,
                                "money_flow_returned": True,
                                "sector_reheated": False,
                                "momentum_alive": True,
                                "confirmed_count": 2,
                                "technical_breakthrough_candidate_score": 2.0,
                                "technical_breakthrough_current_score": 0.0,
                                "technical_breakthrough_candidate_delta": 2.0,
                                "quality_ready": False,
                            },
                        },
                    },
                    {
                        "success": True,
                        "final_score": 63.0,
                        "grade": "A",
                        "base_score": 78.0,
                        "synergy_bonus": 30.0,
                        "risk_penalty": 45.0,
                        "dimension_scores": {"资金流向": 22.0, "短期动量": 17.0},
                        "entry_gate": {
                            "passed": False,
                            "mode": "wait_for_pullback_reconfirmation",
                            "reason": "entry_gate_overheated_wait_for_pullback",
                            "overheat_flags": ["short_term_overextended"],
                        },
                    },
                ]
            )

        def evaluate_stock_v6(self, hist, ts_code):
            return next(self.rows)

    class FakeAnalyzer:
        def __init__(self, db_path: str):
            self.evaluator_v6 = FakeEvaluatorV6()

    fake_module = SimpleNamespace(CompleteVolumePriceAnalyzer=FakeAnalyzer, UnifiedBacktestEngine=FakeEngine, _ensure_price_aliases=lambda x: x)
    monkeypatch.setattr(v49_handlers, "_load_module", lambda _path: fake_module)
    monkeypatch.setattr(v49_handlers, "_load_backtest_frame", lambda *args, **kwargs: pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20260101"], "close_price": [10.0]}))
    monkeypatch.setattr(v49_handlers, "_resolve_db_path", lambda preferred: Path("/tmp/fake.db"))

    handler = HandlerFactory(module_path=Path("/tmp/fake_module.py")).create_backtest_handler("v6")
    out = handler({"sample_size": 20, "holding_days": 5, "score_threshold": 65, "max_evaluations": 2})

    diag = out["raw"]["backtest_diagnostics"]
    assert diag["entry_gate_review"]["passed"] == 1
    assert diag["entry_gate_review"]["blocked"] == 1
    assert diag["entry_gate_review"]["mode_counts"]["pullback_reconfirmed"] == 1
    assert diag["entry_gate_review"]["reason_counts"]["entry_gate_overheated_wait_for_pullback"] == 1
    assert diag["entry_gate_passed_samples"][0]["ts_code"] == "000001.SZ"
    assert diag["entry_gate_passed_samples"][0]["entry_gate"]["mode"] == "pullback_reconfirmed"
    quality = diag["entry_gate_quality_by_mode"]["pullback_reconfirmed"]
    assert quality["count"] == 1
    assert quality["max_score"] == 54.0
    assert quality["avg_gap_to_threshold"] == 11.0
    assert quality["dimension_scores"]["资金流向"]["avg"] == 18.0
    assert quality["dimension_shortfall_to_reference"]["资金流向"]["avg"] == 4.0
    assert quality["dimension_shortfall_to_reference"]["技术突破"]["avg"] == 5.0
    assert quality["secondary_confirmation_counts"]["money_flow_returned"] == 1
    assert quality["secondary_confirmation_counts"]["momentum_alive"] == 1
    assert quality["secondary_confirmation_counts"]["confirmed_count:2"] == 1
    assert "quality_ready" not in quality["secondary_confirmation_counts"]
    assert quality["secondary_confirmation_metrics"]["technical_breakthrough_candidate_score"]["avg"] == 2.0
    assert quality["secondary_confirmation_metrics"]["technical_breakthrough_candidate_delta"]["avg"] == 2.0
    assert diag["entry_gate_passed_samples"][0]["entry_gate"]["secondary_confirmation"]["confirmed_count"] == 2


def test_v7_backtest_handler_records_runtime_replay_constraints(monkeypatch):
    seen = {}

    class FakeEngine:
        def __init__(self, df_bt, sample_size, holding_days):
            pass

        def run_rolling(self, *, min_rows, window, step, signal_fn, stop_on_first_signal):
            hist = pd.DataFrame({"close_price": [10.0] * 80, "vol": [100.0] * 80, "pct_chg": [0.0] * 80})
            g = pd.DataFrame(
                {
                    "name": ["A"] * 3,
                    "industry": ["I"] * 3,
                    "close_price": [10.0, 9.4, 9.4],
                }
            )
            sig = signal_fn("000001.SZ", g, 0, hist)
            seen.update(sig)
            return pd.DataFrame([{"ts_code": "000001.SZ", "trade_date": "20260101", "future_return": -6.0, "signal_strength": sig["signal_strength"], "exit_reason": sig["exit_reason"]}]), 1

        def execution_constraints_summary(self):
            return {"tradeability_filter_enabled": True, "volume_constraint_enabled": True}

    class FakeEvaluatorV7:
        def reset_cache(self):
            seen["cache_reset"] = True

        def evaluate_stock_v7(self, hist, ts_code, industry):
            return {
                "success": True,
                "final_score": 72.0,
                "v4_score": 61.0,
                "industry_heat": 7.0,
                "market_regime": "neutral",
                "dimension_scores": {"趋势": 8.0},
            }

    class FakeAnalyzer:
        def __init__(self, db_path: str):
            self.evaluator_v7 = FakeEvaluatorV7()

        def _calculate_backtest_stats(self, backtest_df, analyzed, holding_days):
            return {"win_rate": 0.0, "max_drawdown": 6.0, "total_signals": len(backtest_df), "sample_size": 20}

    fake_module = SimpleNamespace(CompleteVolumePriceAnalyzer=FakeAnalyzer, UnifiedBacktestEngine=FakeEngine, _ensure_price_aliases=lambda x: x)
    monkeypatch.setattr(v49_handlers, "_load_module", lambda _path: fake_module)
    monkeypatch.setattr(v49_handlers, "_load_backtest_frame", lambda *args, **kwargs: pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20260101"], "close_price": [10.0]}))
    monkeypatch.setattr(v49_handlers, "_resolve_db_path", lambda preferred: Path("/tmp/fake.db"))

    handler = HandlerFactory(module_path=Path("/tmp/fake_module.py")).create_backtest_handler("v7")
    out = handler({"sample_size": 20, "holding_days": 2, "score_threshold": 60, "stop_loss": -0.05})

    diag = out["raw"]["backtest_diagnostics"]
    assert out["status"] == "success"
    assert seen["cache_reset"] is True
    assert abs(seen["__exit_price"] - 9.5) < 1e-9
    assert seen["execution_fill_model"] == "stop_take_price"
    assert out["summary"]["risk_control"] == {"stop_loss": -0.05, "take_profit": 0.08}
    assert diag["score_breakdown"]["v4_score"]["avg"] == 61.0
    assert diag["score_breakdown"]["dim:趋势"]["avg"] == 8.0


def test_v7_backtest_handler_records_near_threshold_conversion(monkeypatch):
    seen = {}

    class FakeEngine:
        def __init__(self, df_bt, sample_size, holding_days):
            pass

        def run_rolling(self, *, min_rows, window, step, signal_fn, stop_on_first_signal):
            hist = pd.DataFrame({"close_price": [10.0] * 80, "vol": [100.0] * 80, "pct_chg": [0.0] * 80})
            g = pd.DataFrame({"name": ["A"], "industry": ["I"], "close_price": [10.0]})
            sig = signal_fn("000001.SZ", g, 0, hist)
            seen.update(sig)
            return pd.DataFrame([{"ts_code": "000001.SZ", "trade_date": "20260101", "future_return": 1.0, "signal_strength": sig["signal_strength"]}]), 1

    class FakeEvaluatorV7:
        def reset_cache(self):
            pass

        def evaluate_stock_v7(self, hist, ts_code, industry):
            return {
                "success": True,
                "final_score": 60.0,
                "v4_score": 59.2,
                "industry_heat": 0.1,
                "market_regime": "neutral",
                "dimension_scores": {"量价配合": 8.0, "MACD趋势": 8.0},
                "near_threshold_conversion": {
                    "applied": True,
                    "bonus": 0.8,
                    "original_score": 59.2,
                    "final_score": 60.0,
                },
            }

    class FakeAnalyzer:
        def __init__(self, db_path: str):
            self.evaluator_v7 = FakeEvaluatorV7()

        def _calculate_backtest_stats(self, backtest_df, analyzed, holding_days):
            return {"win_rate": 1.0, "max_drawdown": 0.0, "total_signals": len(backtest_df), "sample_size": 20}

    fake_module = SimpleNamespace(CompleteVolumePriceAnalyzer=FakeAnalyzer, UnifiedBacktestEngine=FakeEngine, _ensure_price_aliases=lambda x: x)
    monkeypatch.setattr(v49_handlers, "_load_module", lambda _path: fake_module)
    monkeypatch.setattr(v49_handlers, "_load_backtest_frame", lambda *args, **kwargs: pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20260101"], "close_price": [10.0]}))
    monkeypatch.setattr(v49_handlers, "_resolve_db_path", lambda preferred: Path("/tmp/fake.db"))

    handler = HandlerFactory(module_path=Path("/tmp/fake_module.py")).create_backtest_handler("v7")
    out = handler({"sample_size": 20, "holding_days": 2, "score_threshold": 60})

    diag = out["raw"]["backtest_diagnostics"]
    assert out["status"] == "success"
    assert seen["near_threshold_conversion"]["applied"] is True
    assert diag["reason_counts"]["near_threshold_conversion_applied"] == 1
    assert diag["score_breakdown"]["v7_near_threshold_conversion_bonus"]["avg"] == 0.8


def test_v5_backtest_handler_uses_runtime_replay_and_freezes_score_distribution(monkeypatch):
    class FakeEngine:
        def __init__(self, df_bt, sample_size, holding_days):
            pass

        def run_rolling(self, *, min_rows, window, step, signal_fn, stop_on_first_signal):
            hist = pd.DataFrame({"close_price": [10.0] * 80, "vol": [100.0] * 80, "pct_chg": [0.1] * 80})
            g = pd.DataFrame({"name": ["A"], "industry": ["I"], "close_price": [10.0]})
            signal_fn("000001.SZ", g, 0, hist)
            return pd.DataFrame(), 1

    class FakeEvaluatorV5:
        def evaluate_stock_v4(self, hist):
            return {
                "success": True,
                "final_score": 44.0,
                "base_score": 48.0,
                "synergy_bonus": 1.0,
                "risk_penalty": 5.0,
                "dim_scores": {"启动确认": 8.0, "主力行为": 6.0},
            }

    class FakeAnalyzer:
        def __init__(self, db_path: str):
            self.evaluator_v5 = FakeEvaluatorV5()

    fake_module = SimpleNamespace(CompleteVolumePriceAnalyzer=FakeAnalyzer, UnifiedBacktestEngine=FakeEngine, _ensure_price_aliases=lambda x: x)
    monkeypatch.setattr(v49_handlers, "_load_module", lambda _path: fake_module)
    monkeypatch.setattr(v49_handlers, "_load_backtest_frame", lambda *args, **kwargs: pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20260101"], "close_price": [10.0]}))
    monkeypatch.setattr(v49_handlers, "_resolve_db_path", lambda preferred: Path("/tmp/fake.db"))

    handler = HandlerFactory(module_path=Path("/tmp/fake_module.py")).create_backtest_handler("v5")
    out = handler({"sample_size": 20, "holding_days": 5, "score_threshold": 60})

    diag = out["raw"]["backtest_diagnostics"]
    assert out["status"] == "failed"
    assert out["raw"]["strategy"] == "v5"
    assert diag["strategy"] == "v5"
    assert diag["max_score"] == 44.0
    assert diag["score_breakdown"]["base_score"]["avg"] == 48.0
    assert diag["score_breakdown"]["dim:启动确认"]["avg"] == 8.0


def test_v5_runtime_backtest_uses_stop_take_fill_price(monkeypatch):
    seen = {}

    class FakeEngine:
        def __init__(self, df_bt, sample_size, holding_days):
            pass

        def run_rolling(self, *, min_rows, window, step, signal_fn, stop_on_first_signal, max_evaluations=None):
            hist = pd.DataFrame({"close_price": [10.0] * 80, "vol": [100.0] * 80, "pct_chg": [0.1] * 80})
            g = pd.DataFrame(
                {
                    "name": ["A"] * 4,
                    "industry": ["I"] * 4,
                    "close_price": [10.0, 10.0, 9.6, 9.5],
                }
            )
            sig = signal_fn("000001.SZ", g, 1, hist)
            seen.update(sig)
            return pd.DataFrame([{"ts_code": "000001.SZ", "trade_date": "20260101", "future_return": -4.0}]), 1

        def execution_constraints_summary(self):
            return {"tradeability_filter_enabled": True, "volume_constraint_enabled": True}

    class FakeEvaluatorV5:
        def evaluate_stock_v4(self, hist):
            return {"success": True, "final_score": 70.0, "base_score": 70.0, "synergy_bonus": 0.0, "risk_penalty": 0.0}

    class FakeAnalyzer:
        def __init__(self, db_path: str):
            self.evaluator_v5 = FakeEvaluatorV5()

        def _calculate_backtest_stats(self, backtest_df, analyzed, holding_days):
            return {"win_rate": 0.0, "max_drawdown": 4.0, "total_signals": len(backtest_df), "sample_size": 10}

    fake_module = SimpleNamespace(CompleteVolumePriceAnalyzer=FakeAnalyzer, UnifiedBacktestEngine=FakeEngine, _ensure_price_aliases=lambda x: x)
    monkeypatch.setattr(v49_handlers, "_load_module", lambda _path: fake_module)
    monkeypatch.setattr(v49_handlers, "_load_backtest_frame", lambda *args, **kwargs: pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20260101"], "close_price": [10.0]}))
    monkeypatch.setattr(v49_handlers, "_resolve_db_path", lambda preferred: Path("/tmp/fake.db"))

    handler = HandlerFactory(module_path=Path("/tmp/fake_module.py")).create_backtest_handler("v5")
    out = handler({"sample_size": 20, "holding_days": 3, "score_threshold": 60, "stop_loss": -0.03, "take_profit": 0.04})

    assert out["status"] == "success"
    assert seen["exit_reason"] == "stop_loss"
    assert abs(seen["__exit_price"] - 9.7) < 1e-9
    assert seen["execution_fill_model"] == "stop_take_price"
    assert out["summary"]["risk_control"] == {"stop_loss": -0.03, "take_profit": 0.04}


def test_v6_backtest_handler_classifies_mandatory_filter_failure(monkeypatch):
    class FakeEngine:
        def __init__(self, df_bt, sample_size, holding_days):
            pass

        def run_rolling(self, *, min_rows, window, step, signal_fn, stop_on_first_signal):
            hist = pd.DataFrame({"close_price": [10.0] * 80})
            g = pd.DataFrame({"name": ["A"], "industry": ["I"], "close_price": [10.0]})
            signal_fn("000001.SZ", g, 0, hist)
            return pd.DataFrame(), 1

    class FakeEvaluatorV6:
        def evaluate_stock_v6(self, hist, ts_code):
            return {
                "success": False,
                "filter_failed": True,
                "filter_reason": "价格过高(92%>=85%)",
            }

    class FakeAnalyzer:
        def __init__(self, db_path: str):
            self.evaluator_v6 = FakeEvaluatorV6()

    fake_module = SimpleNamespace(CompleteVolumePriceAnalyzer=FakeAnalyzer, UnifiedBacktestEngine=FakeEngine, _ensure_price_aliases=lambda x: x)
    monkeypatch.setattr(v49_handlers, "_load_module", lambda _path: fake_module)
    monkeypatch.setattr(v49_handlers, "_load_backtest_frame", lambda *args, **kwargs: pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20260101"], "close_price": [10.0]}))
    monkeypatch.setattr(v49_handlers, "_resolve_db_path", lambda preferred: Path("/tmp/fake.db"))

    handler = HandlerFactory(module_path=Path("/tmp/fake_module.py")).create_backtest_handler("v6")
    out = handler({"sample_size": 20, "holding_days": 5, "score_threshold": 75})

    assert out["status"] == "failed"
    assert out["raw"]["backtest_diagnostics"]["reason_counts"] == {
        "mandatory_filter:price_too_high": 1,
    }


def test_v6_backtest_handler_classifies_pit_data_unavailable_filter_failure(monkeypatch):
    class FakeEngine:
        def __init__(self, df_bt, sample_size, holding_days):
            pass

        def run_rolling(self, *, min_rows, window, step, signal_fn, stop_on_first_signal):
            hist = pd.DataFrame({"close_price": [10.0] * 80})
            g = pd.DataFrame({"name": ["A"], "industry": ["I"], "close_price": [10.0]})
            signal_fn("000001.SZ", g, 0, hist)
            return pd.DataFrame(), 1

    class FakeEvaluatorV6:
        def evaluate_stock_v6(self, hist, ts_code):
            return {
                "success": False,
                "filter_failed": True,
                "filter_reason": "板块大幅下跌(-4.0%<-3%)",
                "filter_details": {
                    "pit_data_quality": {
                        "sector_performance_available": False,
                        "money_flow_available": True,
                    }
                },
            }

    class FakeAnalyzer:
        def __init__(self, db_path: str):
            self.evaluator_v6 = FakeEvaluatorV6()

    fake_module = SimpleNamespace(CompleteVolumePriceAnalyzer=FakeAnalyzer, UnifiedBacktestEngine=FakeEngine, _ensure_price_aliases=lambda x: x)
    monkeypatch.setattr(v49_handlers, "_load_module", lambda _path: fake_module)
    monkeypatch.setattr(v49_handlers, "_load_backtest_frame", lambda *args, **kwargs: pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20260101"], "close_price": [10.0]}))
    monkeypatch.setattr(v49_handlers, "_resolve_db_path", lambda preferred: Path("/tmp/fake.db"))

    handler = HandlerFactory(module_path=Path("/tmp/fake_module.py")).create_backtest_handler("v6")
    out = handler({"sample_size": 20, "holding_days": 5, "score_threshold": 75})

    assert out["status"] == "failed"
    assert out["raw"]["backtest_diagnostics"]["reason_counts"] == {
        "mandatory_filter:pit_data_unavailable": 1,
    }


def test_v8_backtest_handler_freezes_component_score_breakdown(monkeypatch):
    class FakeEngine:
        def __init__(self, df_bt, sample_size, holding_days):
            pass

        def run_rolling(self, *, min_rows, window, step, signal_fn, stop_on_first_signal):
            hist = pd.DataFrame({"close_price": [10.0] * 80, "vol": [100.0] * 80})
            g = pd.DataFrame({"name": ["A"], "industry": ["I"], "close_price": [10.0]})
            signal_fn("000001.SZ", g, 0, hist)
            return pd.DataFrame(), 1

    class FakeEvaluatorV8:
        def evaluate_stock_v8(self, hist, ts_code, index_data):
            return {
                "success": True,
                "final_score": 33.5,
                "v7_score": 45.0,
                "advanced_score": 32.0,
                "pre_market_score": 41.75,
                "market_penalty": 0.8,
                "advanced_factors": {
                    "factors": {
                        "relative_strength": {"score": 3.0},
                        "smart_money": {"score": 5.0},
                    }
                },
                "atr_stops": {"stop_loss": 9.0, "take_profit": 12.0},
            }

    class FakeAnalyzer:
        def __init__(self, db_path: str):
            self.evaluator_v8 = FakeEvaluatorV8()

    fake_module = SimpleNamespace(CompleteVolumePriceAnalyzer=FakeAnalyzer, UnifiedBacktestEngine=FakeEngine, _ensure_price_aliases=lambda x: x)
    monkeypatch.setattr(v49_handlers, "_load_module", lambda _path: fake_module)
    monkeypatch.setattr(v49_handlers, "_load_backtest_frame", lambda *args, **kwargs: pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20260101"], "close_price": [10.0]}))
    monkeypatch.setattr(v49_handlers, "_resolve_db_path", lambda preferred: Path("/tmp/fake.db"))

    handler = HandlerFactory(module_path=Path("/tmp/fake_module.py")).create_backtest_handler("v8")
    out = handler({"sample_size": 20, "holding_days": 5, "score_threshold": 40})

    diag = out["raw"]["backtest_diagnostics"]
    assert out["status"] == "failed"
    assert diag["reason_counts"] == {"below_threshold": 1}
    assert diag["score_breakdown"]["v7_score"]["avg"] == 45.0
    assert diag["score_breakdown"]["advanced_score"]["avg"] == 32.0
    assert diag["score_breakdown"]["pre_market_score"]["avg"] == 41.75
    assert diag["score_breakdown"]["market_penalty"]["avg"] == 0.8
    assert diag["score_breakdown"]["factor:relative_strength"]["avg"] == 3.0
    assert diag["v8_suppression_diagnostics"]["suppressed_by_factor_distribution"] is True


def test_v8_index_data_slice_is_point_in_time_for_backtest_window():
    index_data = pd.DataFrame(
        {
            "trade_date": ["20260101", "20260102", "20260103", "20260104"],
            "close": [1.0, 2.0, 3.0, 4.0],
            "volume": [100.0, 100.0, 100.0, 100.0],
        }
    )
    hist = pd.DataFrame({"trade_date": ["20260101", "20260102"]})

    out = _slice_index_data_for_history(index_data, hist)

    assert out is not None
    assert out["trade_date"].tolist() == ["20260101", "20260102"]
    assert out["close"].tolist() == [1.0, 2.0]


def test_v8_runtime_backtest_caps_atr_stop_loss_pct(monkeypatch):
    seen = {}

    class FakeEngine:
        def __init__(self, df_bt, sample_size, holding_days):
            pass

        def run_rolling(self, *, min_rows, window, step, signal_fn, stop_on_first_signal):
            hist = pd.DataFrame({"trade_date": ["20260101"] * 80, "close_price": [10.0] * 80, "vol": [100.0] * 80})
            g = pd.DataFrame(
                {
                    "name": ["A"] * 8,
                    "industry": ["I"] * 8,
                    "trade_date": [f"2026010{i}" for i in range(8)],
                    "close_price": [10.0, 10.0, 9.1, 9.0, 9.0, 9.0, 9.0, 9.0],
                    "vol": [100.0] * 8,
                    "amount": [1000.0] * 8,
                    "pct_chg": [0.0] * 8,
                }
            )
            sig = signal_fn("000001.SZ", g, 1, hist)
            seen.update(sig)
            return pd.DataFrame(
                [{"ts_code": "000001.SZ", "trade_date": "20260101", "future_return": -9.0, "signal_strength": sig["signal_strength"]}]
            ), 1

        def execution_constraints_summary(self):
            return {"tradeability_filter_enabled": True, "volume_constraint_enabled": True}

    class FakeEvaluatorV8:
        def evaluate_stock_v8(self, hist, ts_code, index_data):
            return {
                "success": True,
                "final_score": 70.0,
                "pre_market_score": 70.0,
                "market_penalty": 1.0,
                "v7_score": 60.0,
                "advanced_score": 70.0,
                "advanced_factors": {"factors": {}},
                "atr_stops": {"stop_loss": 5.0, "take_profit": 14.0, "stop_loss_pct": 50.0, "take_profit_pct": 40.0},
            }

    class FakeAnalyzer:
        def __init__(self, db_path: str):
            self.evaluator_v8 = FakeEvaluatorV8()

        def _calculate_backtest_stats(self, backtest_df, analyzed, holding_days):
            return {"win_rate": 0.0, "max_drawdown": 9.0, "total_signals": len(backtest_df), "sample_size": 10}

    fake_module = SimpleNamespace(CompleteVolumePriceAnalyzer=FakeAnalyzer, UnifiedBacktestEngine=FakeEngine, _ensure_price_aliases=lambda x: x)
    monkeypatch.setattr(v49_handlers, "_load_module", lambda _path: fake_module)
    monkeypatch.setattr(v49_handlers, "_load_backtest_frame", lambda *args, **kwargs: pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20260101"], "close_price": [10.0]}))
    monkeypatch.setattr(v49_handlers, "_resolve_db_path", lambda preferred: Path("/tmp/fake.db"))

    handler = HandlerFactory(module_path=Path("/tmp/fake_module.py")).create_backtest_handler("v8")
    out = handler({"sample_size": 20, "holding_days": 5, "score_threshold": 40, "max_stop_loss_pct": 0.08})

    assert out["status"] == "success"
    assert abs(seen["atr_stop_loss"] - 9.2) < 1e-9
    assert abs(seen["__exit_price"] - 9.2) < 1e-9
    assert seen["execution_fill_model"] == "stop_take_price"
    assert seen["atr_stop_loss_pct"] == 8.0
    assert seen["atr_stop_loss_raw_pct"] == 50.0
    assert out["summary"]["risk_control"] == {"max_stop_loss_pct": 0.08}


def test_v8_runtime_backtest_can_cap_atr_take_profit_pct(monkeypatch):
    seen = {}

    class FakeEngine:
        def __init__(self, df_bt, sample_size, holding_days):
            pass

        def run_rolling(self, *, min_rows, window, step, signal_fn, stop_on_first_signal):
            hist = pd.DataFrame({"trade_date": ["20260101"] * 80, "close_price": [10.0] * 80, "vol": [100.0] * 80})
            g = pd.DataFrame(
                {
                    "name": ["A"] * 8,
                    "industry": ["I"] * 8,
                    "trade_date": [f"2026010{i}" for i in range(8)],
                    "close_price": [10.0, 10.0, 10.5, 11.2, 11.2, 11.2, 11.2, 11.2],
                    "vol": [100.0] * 8,
                    "amount": [1000.0] * 8,
                    "pct_chg": [0.0] * 8,
                }
            )
            sig = signal_fn("000001.SZ", g, 1, hist)
            seen.update(sig)
            return pd.DataFrame(
                [
                    {
                        "ts_code": "000001.SZ",
                        "trade_date": "20260101",
                        "future_return": 12.0,
                        "signal_strength": sig["signal_strength"],
                        "exit_reason": sig["exit_reason"],
                        "risk_take_profit_pct_cap": sig["risk_take_profit_pct_cap"],
                    }
                ]
            ), 1

        def execution_constraints_summary(self):
            return {"tradeability_filter_enabled": True, "volume_constraint_enabled": True}

    class FakeEvaluatorV8:
        def evaluate_stock_v8(self, hist, ts_code, index_data):
            return {
                "success": True,
                "final_score": 70.0,
                "pre_market_score": 70.0,
                "market_penalty": 1.0,
                "v7_score": 60.0,
                "advanced_score": 70.0,
                "advanced_factors": {"factors": {}},
                "atr_stops": {"stop_loss": 9.0, "take_profit": 14.0, "stop_loss_pct": 10.0, "take_profit_pct": 40.0},
            }

    class FakeAnalyzer:
        def __init__(self, db_path: str):
            self.evaluator_v8 = FakeEvaluatorV8()

        def _calculate_backtest_stats(self, backtest_df, analyzed, holding_days):
            return {"win_rate": 100.0, "max_drawdown": 0.0, "total_signals": len(backtest_df), "sample_size": 10}

    fake_module = SimpleNamespace(CompleteVolumePriceAnalyzer=FakeAnalyzer, UnifiedBacktestEngine=FakeEngine, _ensure_price_aliases=lambda x: x)
    monkeypatch.setattr(v49_handlers, "_load_module", lambda _path: fake_module)
    monkeypatch.setattr(v49_handlers, "_load_backtest_frame", lambda *args, **kwargs: pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20260101"], "close_price": [10.0]}))
    monkeypatch.setattr(v49_handlers, "_resolve_db_path", lambda preferred: Path("/tmp/fake.db"))

    handler = HandlerFactory(module_path=Path("/tmp/fake_module.py")).create_backtest_handler("v8")
    out = handler({"sample_size": 20, "holding_days": 5, "score_threshold": 40, "max_stop_loss_pct": 0.08, "max_take_profit_pct": 0.10})

    assert out["status"] == "success"
    assert abs(seen["atr_take_profit"] - 11.0) < 1e-9
    assert abs(seen["__exit_price"] - 11.0) < 1e-9
    assert seen["atr_take_profit_raw"] == 14.0
    assert seen["exit_reason"] == "take_profit"
    assert seen["execution_fill_model"] == "stop_take_price"
    assert seen["risk_take_profit_pct_cap"] == 10.0
    assert out["summary"]["risk_control"] == {"max_stop_loss_pct": 0.08, "max_take_profit_pct": 0.10}


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


def test_v4_backtest_handler_uses_runtime_replay_with_diagnostics(monkeypatch):
    seen = {}

    class FakeEngine:
        def __init__(self, df_bt, sample_size, holding_days):
            seen["sample_size"] = sample_size
            seen["holding_days"] = holding_days

        def run_rolling(self, *, min_rows, window, step, signal_fn, stop_on_first_signal):
            seen["step"] = step
            hist = pd.DataFrame({"close_price": [10.0] * 80, "vol": [100.0] * 80, "pct_chg": [0.0] * 80})
            g = pd.DataFrame({"name": ["A"], "industry": ["I"], "close_price": [10.0]})
            sig = signal_fn("000001.SZ", g, 0, hist)
            return pd.DataFrame([{"ts_code": "000001.SZ", "trade_date": "20260101", "future_return": 3.0, "signal_strength": sig["signal_strength"]}]), 1

        def execution_constraints_summary(self):
            return {"tradeability_filter_enabled": True, "volume_constraint_enabled": True}

    class FakeEvaluatorV4:
        def evaluate_stock_v4(self, hist):
            return {
                "success": True,
                "final_score": 68.0,
                "base_score": 64.0,
                "synergy_bonus": 4.0,
                "risk_penalty": 0.0,
                "dim_scores": {"潜伏价值": 12.0},
            }

    class FakeAnalyzer:
        def __init__(self, db_path: str):
            self.evaluator_v4 = FakeEvaluatorV4()

        def _calculate_backtest_stats(self, backtest_df, analyzed, holding_days):
            return {"win_rate": 51.0, "max_drawdown": 12.0, "total_signals": len(backtest_df), "sample_size": 80}

    fake_module = SimpleNamespace(CompleteVolumePriceAnalyzer=FakeAnalyzer, UnifiedBacktestEngine=FakeEngine, _ensure_price_aliases=lambda x: x)
    monkeypatch.setattr(v49_handlers, "_load_module", lambda _path: fake_module)
    monkeypatch.setattr(v49_handlers, "_load_backtest_frame", lambda *args, **kwargs: pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20260101"], "close_price": [10.0]}))
    monkeypatch.setattr(v49_handlers, "_resolve_db_path", lambda preferred: Path("/tmp/fake.db"))

    handler = HandlerFactory(module_path=Path("/tmp/fake_module.py")).create_backtest_handler("v4")
    out = handler({"sample_size": 80, "holding_days": 5, "score_threshold": 60, "replay_step": 12})

    assert out["summary"]["win_rate"] == 0.51
    assert out["raw"]["strategy"] == "v4_runtime_replay"
    assert out["raw"]["backtest_diagnostics"]["score_breakdown"]["dim:潜伏价值"]["avg"] == 12.0
    assert seen == {"sample_size": 80, "holding_days": 5, "step": 12}


def test_unimplemented_experimental_backtest_returns_failed_not_neutral_success():
    handler = HandlerFactory(module_path=Path("/tmp/fake_module.py")).create_backtest_handler("ai")

    out = handler({"run_id": "run_backtest_ai_test"})

    assert out["status"] == "failed"
    assert out["summary"] == {"win_rate": 0.0, "max_drawdown": 1.0, "signal_density": 0.0}
    assert out["raw"]["reason"] == "backtest_not_implemented"
    assert out["raw"]["promotion_blocked"] is True
    assert "explainable_signal_fact_chain" in out["raw"]["required_to_compete"]
    assert "real_runtime_backtest_handler" in out["raw"]["required_to_compete"]
    assert "required_signal_fact_fields" in out["raw"]["runtime_backtest_contract"]
    assert out["raw"]["fact_chain"]["available"] is False


def test_stable_backtest_handler_uses_real_stable_runtime_path(monkeypatch):
    seen = {}

    class FakeEngine:
        def __init__(self, df_bt, sample_size, holding_days):
            seen["sample_size"] = sample_size
            seen["holding_days"] = holding_days

        def run_rolling(self, *, min_rows, window, step, signal_fn, stop_on_first_signal):
            seen["window"] = window
            hist = pd.DataFrame({"close_price": [10.0] * 80})
            sig = signal_fn("000001.SZ", pd.DataFrame({"name": ["A"], "industry": ["I"]}), 0, hist)
            row = {
                "ts_code": "000001.SZ",
                "trade_date": "20260101",
                "future_return": 2.0,
                "signal_strength": sig["signal_strength"],
            }
            return pd.DataFrame([row]), 1

        def execution_constraints_summary(self):
            return {"tradeability_filter_enabled": True, "volume_constraint_enabled": True}

    class FakeAnalyzer:
        def __init__(self, db_path: str):
            pass

        def _calculate_backtest_stats(self, backtest_df, analyzed, holding_days):
            return {
                "win_rate": 100.0,
                "max_drawdown": 0.0,
                "total_signals": len(backtest_df),
                "analyzed_stocks": analyzed,
                "sample_size": 20,
            }

    fake_module = SimpleNamespace(CompleteVolumePriceAnalyzer=FakeAnalyzer, UnifiedBacktestEngine=FakeEngine)
    monkeypatch.setattr(v49_handlers, "_load_module", lambda _path: fake_module)
    monkeypatch.setattr(v49_handlers, "_load_backtest_frame", lambda *args, **kwargs: pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20260101"], "close_price": [10.0]}))
    monkeypatch.setattr(v49_handlers, "_resolve_db_path", lambda preferred: Path("/tmp/fake.db"))
    monkeypatch.setattr(
        v49_handlers,
        "_evaluate_stable_uptrend_history",
        lambda *args, **kwargs: {"score": 72.0, "max_dd": 0.05, "rebound": 0.12, "vol": 0.02, "trend_ok": True, "breakout": False},
    )

    handler = HandlerFactory(module_path=Path("/tmp/fake_module.py")).create_backtest_handler("stable")
    out = handler({"sample_size": 20, "holding_days": 10, "score_threshold": 60})

    assert out["summary"]["win_rate"] == 1.0
    assert out["summary"]["signal_density"] == 0.05
    assert out["raw"]["strategy"] == "stable_uptrend"
    assert out["raw"]["backtest_diagnostics"]["strategy"] == "stable"
    assert out["raw"]["backtest_diagnostics"]["reason_counts"]["pass_threshold"] == 1
    assert out["summary"]["defensive_allocator"]["contract"]["role"] == "defensive_allocator_overlay"
    assert out["summary"]["defensive_allocator"]["promotion_eligible"] is False
    assert out["raw"]["defensive_allocator"]["contract"]["not_standalone_alpha"] is True
    assert seen["sample_size"] == 20


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
