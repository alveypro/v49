import pandas as pd
import json
import time

from src.agents.feature_agent import FeatureAgent
from src.agents.forecast_agent import ForecastAgent
from src.agents.position_agent import PositionAgent
from src.agents.risk_agent import RiskAgent
from src.agents.signal_agent import SignalAgent
from src.models_engine.model_ensemble import ModelEnsemble
from src.pipeline.pipeline_manager import PipelineManager
from src.signal_engine.signal_fusion import SignalFusionEngine
from run_top_candidates import (
    _forward_return_from_history,
    _normalize_trade_date,
    rank_candidates,
    summarize_candidate_basket,
    summarize_historical_validation,
    summarize_variant_comparison,
)


def test_dynamic_blend_prefers_higher_weight_and_confident_models():
    ensemble = ModelEnsemble()
    result = ensemble.dynamic_blend(
        {
            "lightgbm": {"direction_prob": 0.78},
            "logistic": {"direction_prob": 0.56},
            "random_forest": {"direction_prob": 0.52},
        },
        performance_weights={
            "lightgbm": 0.65,
            "logistic": 0.20,
            "random_forest": 0.15,
        },
        regime="trend",
    )
    assert result["direction_prob"] > 0.65
    assert result["agreement"] > 0.5
    assert result["weights"]["lightgbm"] > result["weights"]["logistic"]


def test_prepare_training_frame_excludes_all_label_columns():
    df = pd.DataFrame({
        "date": pd.date_range("2026-01-01", periods=90, freq="D"),
        "ts_code": ["000001.SZ"] * 90,
        "open": [10 + i * 0.1 for i in range(90)],
        "high": [10.2 + i * 0.1 for i in range(90)],
        "low": [9.8 + i * 0.1 for i in range(90)],
        "close": [10 + i * 0.1 for i in range(90)],
        "volume": [1000 + i * 20 for i in range(90)],
        "turnover_rate": [1.5 + i * 0.01 for i in range(90)],
        "index_close": [3000 + i * 5 for i in range(90)],
    })
    agent = FeatureAgent({"feature_params": {"ma_windows": [5, 10, 20], "ema_windows": [5, 10, 20]}})
    featured = agent.build_features(df)
    featured["label_return_5"] = featured["close"].shift(-5) / featured["close"] - 1
    frame, feature_cols, target_col = agent.prepare_training_frame(featured)

    assert not frame.empty
    assert target_col == "label_excess_direction_5"
    assert "label_direction_5" not in feature_cols
    assert "label_return_5" not in feature_cols
    assert "label_excess_return_5" not in feature_cols
    assert "ma_gap_5_20" in feature_cols
    assert "model_agreement" not in feature_cols
    assert "label_excess_direction_5" in frame.columns


def test_prepare_training_frame_prefers_excess_direction_target():
    df = pd.DataFrame({
        "date": pd.date_range("2026-01-01", periods=90, freq="D"),
        "ts_code": ["000001.SZ"] * 90,
        "open": [10 + i * 0.1 for i in range(90)],
        "high": [10.2 + i * 0.1 for i in range(90)],
        "low": [9.8 + i * 0.1 for i in range(90)],
        "close": [10 + i * 0.1 for i in range(90)],
        "volume": [1000 + i * 20 for i in range(90)],
        "turnover_rate": [1.5 + i * 0.01 for i in range(90)],
        "index_close": [3000 + i * 4 for i in range(90)],
    })
    agent = FeatureAgent({"settings": {"training": {}}, "feature_params": {"ma_windows": [5, 10, 20], "ema_windows": [5, 10, 20]}})
    featured = agent.build_features(df)
    frame, feature_cols, target_col = agent.prepare_training_frame(featured)

    assert not frame.empty
    assert "label_excess_direction_5" in frame.columns
    assert target_col == "label_excess_direction_5"
    assert "label_excess_direction_5" not in feature_cols


def test_feature_agent_uses_default_windows_when_feature_params_missing():
    df = pd.DataFrame({
        "date": pd.date_range("2026-01-01", periods=90, freq="D"),
        "ts_code": ["000001.SZ"] * 90,
        "open": [10 + i * 0.1 for i in range(90)],
        "high": [10.2 + i * 0.1 for i in range(90)],
        "low": [9.8 + i * 0.1 for i in range(90)],
        "close": [10 + i * 0.1 for i in range(90)],
        "volume": [1000 + i * 20 for i in range(90)],
        "turnover_rate": [1.5 + i * 0.01 for i in range(90)],
        "index_close": [3000 + i * 4 for i in range(90)],
    })
    agent = FeatureAgent({"settings": {"training": {}}, "feature_params": None})

    featured = agent.build_features(df)
    frame, feature_cols, target_col = agent.prepare_training_frame(featured)

    assert not frame.empty
    assert "ma_5" in frame.columns
    assert "ema_5" in frame.columns
    assert target_col == "label_excess_direction_5"
    assert feature_cols


def test_select_training_symbols_spreads_across_universe():
    symbols = [f"{i:06d}.SZ" for i in range(1, 11)]
    selected = PipelineManager._select_training_symbols(symbols, 4)

    assert selected[0] == "000001.SZ"
    assert selected[-1] == "000010.SZ"
    assert len(selected) == 4
    assert len(set(selected)) == 4


def test_run_batch_prediction_trains_on_pooled_multi_symbol_frame():
    pm = object.__new__(PipelineManager)
    pm.config = {"settings": {"training": {"batch_training_symbols": 3}}}

    base_frame = pd.DataFrame({
        "feature_a": [1.0, 2.0, 3.0],
        "feature_b": [0.1, 0.2, 0.3],
        "label_direction_5": [0, 1, 1],
    })

    class StubDataAgent:
        def prepare_dataset(self, ts_code: str) -> pd.DataFrame:
            offset = int(ts_code[0])
            df = base_frame.copy()
            df["feature_a"] = df["feature_a"] + offset
            return df

    class StubFeatureAgent:
        def build_features(self, df: pd.DataFrame) -> pd.DataFrame:
            return df

        def prepare_training_frame(self, df: pd.DataFrame):
            return df.copy(), ["feature_a", "feature_b"], "label_direction_5"

    class StubForecastAgent:
        def __init__(self) -> None:
            self.models = {}
            self.trained_frames: list[pd.DataFrame] = []

        def train_models(self, df, feature_cols, target_col):
            self.trained_frames.append(df.copy())
            self.models = {"logistic": object()}
            return {"trained_models": ["logistic"]}

    class StubRegimeAgent:
        def detect_market_regime(self, df):
            return {"environment_score": 0.6}

    class StubRiskAgent:
        def evaluate_trade_risk(self, df, forecast_result, regime_info):
            return {"allow_trade": True, "risk_level": "low"}

    class StubSignalAgent:
        def generate_signal(self, df, forecast_result, regime_info, risk_info):
            return {"signal": "buy", "score": 60}

    class StubPositionAgent:
        def calculate_position_size(self, signal_result, risk_info, account_info):
            return {"position_pct": 0.2}

    class StubReporter:
        def generate_signal_report(self, results):
            return None

    pm.data_agent = StubDataAgent()
    pm.feature_agent = StubFeatureAgent()
    pm.forecast_agent = StubForecastAgent()
    pm.regime_agent = StubRegimeAgent()
    pm.risk_agent = StubRiskAgent()
    pm.signal_agent = StubSignalAgent()
    pm.position_agent = StubPositionAgent()
    pm.reporter = StubReporter()

    def stub_predict(df, feature_cols=None, regime_info=None):
        return {"direction_prob": 0.62, "confidence": 0.7, "pred_return": 0.03}

    pm.forecast_agent.predict = stub_predict

    batch_result = pm.run_batch_prediction(["100001.SZ", "200001.SZ", "300001.SZ", "400001.SZ"])
    results = batch_result["results"]

    assert len(results) == 4
    assert batch_result["skipped"] == []
    assert len(pm.forecast_agent.trained_frames) == 1
    trained = pm.forecast_agent.trained_frames[0]
    assert len(trained) == 9
    assert sorted(trained["feature_a"].unique().tolist()) == [2.0, 3.0, 4.0, 5.0, 6.0, 7.0]


def test_apply_champion_profile_restricts_active_models():
    pm = object.__new__(PipelineManager)
    pm.config = {
        "signal_rules": {
            "strong_buy_score": 70,
            "buy_score": 55,
            "watch_score": 40,
            "sell_score": 25,
            "buy_position_multiplier": 0.45,
            "strong_buy_position_multiplier": 1.0,
        },
        "risk_rules": {
            "range_confidence_min": 0.05,
            "environment_weak_position_scale": 0.60,
            "environment_risk_off_position_scale": 0.35,
        },
    }

    class StubVersionManager:
        @staticmethod
        def load_registry():
            return {
                "champion_version": "evo_test",
                "champion_payload": {
                    "model_evolution": {
                        "selected_models": ["lightgbm"],
                        "model_weights": {"lightgbm": 0.8, "logistic": 0.2},
                        "tuned_params": {"lightgbm": {"num_leaves": 31}},
                    },
                    "threshold_profile": {
                        "profile": "offensive",
                        "signal_rules": {
                            "strong_buy_score": 68,
                            "buy_score": 53.5,
                            "watch_score": 39,
                            "sell_score": 24.5,
                            "buy_position_multiplier": 0.48,
                            "strong_buy_position_multiplier": 1.05,
                        },
                        "risk_rules": {
                            "range_confidence_min": 0.04,
                            "environment_weak_position_scale": 0.65,
                            "environment_risk_off_position_scale": 0.37,
                        },
                    },
                },
            }

    class StubTrainer:
        def __init__(self) -> None:
            self.params = {"enabled_models": ["logistic", "lightgbm"], "lightgbm": {"learning_rate": 0.05}}

    class StubForecastAgent:
        def __init__(self) -> None:
            self.trainer = StubTrainer()
            self.models = {"logistic": object(), "lightgbm": object()}
            self.eval_results = {"logistic": {"accuracy": 0.55}, "lightgbm": {"accuracy": 0.61}}
            self.model_weights = {"logistic": 0.4, "lightgbm": 0.6}

        def apply_external_model_weights(self, external_weights, *, blend_ratio=0.65):
            total = sum(float(v or 0.0) for v in external_weights.values())
            self.model_weights = {
                name: float(value or 0.0) / total for name, value in external_weights.items() if total > 0
            }
            return self.model_weights

    pm.version_manager = StubVersionManager()
    pm.forecast_agent = StubForecastAgent()
    pm.signal_agent = type("SignalAgentStub", (), {"fusion": type("FusionStub", (), {
        "strong_buy_score": 70.0,
        "buy_score": 55.0,
        "watch_score": 40.0,
        "sell_score": 25.0,
    })()})()
    pm.position_agent = type("PositionStub", (), {
        "buy_position_multiplier": 0.45,
        "strong_buy_position_multiplier": 1.0,
    })()
    pm.risk_agent = type("RiskStub", (), {
        "range_confidence_min": 0.05,
        "env_weak_position_scale": 0.60,
        "env_risk_off_position_scale": 0.35,
    })()

    champion = pm._apply_champion_profile()

    assert champion["champion_selected_models"] == ["lightgbm"]
    assert champion["champion_selected_models_applied"] is True
    assert champion["active_enabled_models"] == ["lightgbm"]
    assert list(pm.forecast_agent.models.keys()) == ["lightgbm"]
    assert list(pm.forecast_agent.eval_results.keys()) == ["lightgbm"]
    assert pm.forecast_agent.trainer.params["enabled_models"] == ["lightgbm"]
    assert pm.forecast_agent.trainer.params["lightgbm"]["num_leaves"] == 31
    assert champion["champion_thresholds_applied"] is True
    assert champion["champion_threshold_profile"]["profile"] == "offensive"
    assert pm.signal_agent.fusion.strong_buy_score == 68.0
    assert pm.position_agent.buy_position_multiplier == 0.48
    assert pm.risk_agent.range_confidence_min == 0.04
    assert pm.config["signal_rules"]["buy_score"] == 53.5


def test_ensure_models_for_universe_uses_pooled_frame():
    pm = object.__new__(PipelineManager)

    class StubForecastAgent:
        def __init__(self) -> None:
            self.models = {}
            self.calls = []

        def train_models(self, df, feature_cols, target_col):
            self.calls.append((df.copy(), list(feature_cols), target_col))
            self.models = {"logistic": object()}

    pooled = pd.DataFrame({
        "feature_a": [1.0, 2.0, 3.0],
        "label_direction_5": [0, 1, 1],
    })
    pm.forecast_agent = StubForecastAgent()
    pm._build_pooled_training_frame = lambda ts_codes: (pooled, ["feature_a"], "label_direction_5")

    pm._ensure_models_for_universe(["000001.SZ", "600036.SH"])

    assert len(pm.forecast_agent.calls) == 1
    trained_df, feature_cols, target_col = pm.forecast_agent.calls[0]
    assert len(trained_df) == 3
    assert feature_cols == ["feature_a"]
    assert target_col == "label_direction_5"


def test_run_batch_prediction_stops_after_runtime_budget():
    pm = object.__new__(PipelineManager)
    pm.config = {"settings": {"training": {"batch_training_symbols": 3}, "runtime": {"batch_prediction_max_runtime_sec": 0.01}}}

    base_frame = pd.DataFrame({
        "feature_a": [1.0, 2.0, 3.0],
        "feature_b": [0.1, 0.2, 0.3],
        "label_direction_5": [0, 1, 1],
    })

    class StubDataAgent:
        def prepare_dataset(self, ts_code: str) -> pd.DataFrame:
            return base_frame.copy()

    class StubFeatureAgent:
        def build_features(self, df: pd.DataFrame) -> pd.DataFrame:
            return df

        def prepare_training_frame(self, df: pd.DataFrame):
            return df.copy(), ["feature_a", "feature_b"], "label_direction_5"

    class StubForecastAgent:
        def __init__(self) -> None:
            self.models = {}

        def train_models(self, df, feature_cols, target_col):
            self.models = {"logistic": object()}
            return {"trained_models": ["logistic"]}

    class StubRegimeAgent:
        def detect_market_regime(self, df):
            return {"environment_score": 0.6}

    class StubRiskAgent:
        def evaluate_trade_risk(self, df, forecast_result, regime_info):
            return {"allow_trade": True, "risk_level": "low"}

    class StubSignalAgent:
        def generate_signal(self, df, forecast_result, regime_info, risk_info):
            return {"signal": "buy", "score": 60}

    class StubPositionAgent:
        def calculate_position_size(self, signal_result, risk_info, account_info):
            return {"position_pct": 0.2}

    class StubReporter:
        def generate_signal_report(self, results):
            return None

    pm.data_agent = StubDataAgent()
    pm.feature_agent = StubFeatureAgent()
    pm.forecast_agent = StubForecastAgent()
    pm.regime_agent = StubRegimeAgent()
    pm.risk_agent = StubRiskAgent()
    pm.signal_agent = StubSignalAgent()
    pm.position_agent = StubPositionAgent()
    pm.reporter = StubReporter()

    def slow_predict(df, feature_cols=None, regime_info=None):
        time.sleep(0.02)
        return {"direction_prob": 0.62, "confidence": 0.7, "pred_return": 0.03}

    pm.forecast_agent.predict = slow_predict

    batch_result = pm.run_batch_prediction(["100001.SZ", "200001.SZ", "300001.SZ"])

    assert batch_result["degraded"] is True
    assert "timeout" in batch_result["degradation_reason"]
    assert len(batch_result["results"]) >= 1


def test_add_cross_sectional_targets_generates_relative_labels():
    pooled = pd.DataFrame({
        "date": ["2026-03-20", "2026-03-20", "2026-03-21", "2026-03-21"],
        "ts_code": ["AAA", "BBB", "AAA", "BBB"],
        "label_excess_return_5": [0.03, 0.01, -0.01, 0.02],
    })

    out = PipelineManager._add_cross_sectional_targets(pooled)

    assert "label_cross_sectional_excess_return_5" in out.columns
    assert "label_cross_sectional_excess_direction_5" in out.columns
    assert float(out.loc[0, "label_cross_sectional_excess_return_5"]) > 0
    assert int(out.loc[1, "label_cross_sectional_excess_direction_5"]) == 0
    assert int(out.loc[3, "label_cross_sectional_excess_direction_5"]) == 1
    assert "label_cross_sectional_top_quantile_5" not in out.columns or out["label_cross_sectional_top_quantile_5"].dropna().empty


def test_add_cross_sectional_targets_generates_top_quantile_labels_for_wide_universe():
    pooled = pd.DataFrame({
        "date": ["2026-03-20"] * 5 + ["2026-03-21"] * 5,
        "ts_code": [f"C{i}" for i in range(10)],
        "label_excess_return_5": [0.05, 0.03, 0.01, -0.01, -0.03, 0.06, 0.04, 0.02, 0.00, -0.02],
    })

    out = PipelineManager._add_cross_sectional_targets(pooled)

    assert "label_cross_sectional_top_quantile_5" in out.columns
    assert int(out.loc[0, "label_cross_sectional_top_quantile_5"]) == 1
    assert int(out.loc[4, "label_cross_sectional_top_quantile_5"]) == 0


def test_resolve_pooled_target_col_falls_back_when_preferred_has_single_class():
    df = pd.DataFrame({
        "label_cross_sectional_top_quantile_5": [1, 1, 1, 1],
        "label_cross_sectional_excess_direction_5": [1, 0, 1, 0],
        "label_direction_5": [1, 0, 1, 0],
    })

    target = PipelineManager._resolve_pooled_target_col(
        df,
        preferred="label_cross_sectional_top_quantile_5",
        fallback="label_direction_5",
    )

    assert target == "label_cross_sectional_excess_direction_5"


def test_rank_candidates_uses_cross_section_and_industry_leadership():
    results = [
        {
            "ts_code": "AAA",
            "signal_result": {"signal": "strong_buy", "score": 72, "reason": "alpha"},
            "forecast_result": {
                "direction_prob_up": 0.72,
                "pred_return": 0.048,
                "confidence": 0.76,
                "calibrated_upside_win_rate": 0.74,
                "calibrated_avg_return": 0.042,
                "calibration_sample_size": 96,
                "model_agreement": 0.82,
                "prediction_dispersion": 0.03,
            },
            "regime_info": {"regime": "trend", "environment_score": 0.82, "market_trend": "bullish"},
            "risk_info": {"risk_level": "low", "stop_loss": 9.2, "take_profit": 11.1},
            "position_result": {"position_pct": 0.2},
        },
        {
            "ts_code": "BBB",
            "signal_result": {"signal": "strong_buy", "score": 74, "reason": "beta"},
            "forecast_result": {
                "direction_prob_up": 0.68,
                "pred_return": 0.034,
                "confidence": 0.71,
                "calibrated_upside_win_rate": 0.67,
                "calibrated_avg_return": 0.028,
                "calibration_sample_size": 96,
                "model_agreement": 0.74,
                "prediction_dispersion": 0.08,
            },
            "regime_info": {"regime": "trend_volatile", "environment_score": 0.62, "market_trend": "bullish"},
            "risk_info": {"risk_level": "low", "stop_loss": 8.8, "take_profit": 10.4},
            "position_result": {"position_pct": 0.2},
        },
        {
            "ts_code": "CCC",
            "signal_result": {"signal": "buy", "score": 69, "reason": "gamma"},
            "forecast_result": {
                "direction_prob_up": 0.64,
                "pred_return": 0.031,
                "confidence": 0.69,
                "calibrated_upside_win_rate": 0.61,
                "calibrated_avg_return": 0.019,
                "calibration_sample_size": 96,
                "model_agreement": 0.7,
                "prediction_dispersion": 0.09,
            },
            "regime_info": {"regime": "range", "environment_score": 0.48, "market_trend": "bearish"},
            "risk_info": {"risk_level": "medium", "stop_loss": 12.1, "take_profit": 13.8},
            "position_result": {"position_pct": 0.15},
        },
    ]
    stock_basic_map = {
        "AAA": {"stock_name": "A", "industry": "Oil", "market": "Main", "area": "SZ"},
        "BBB": {"stock_name": "B", "industry": "Oil", "market": "Main", "area": "SH"},
        "CCC": {"stock_name": "C", "industry": "Tech", "market": "Main", "area": "SH"},
    }

    ranked = rank_candidates(results, top_n=3, stock_basic_map=stock_basic_map)

    assert ranked.iloc[0]["ts_code"] == "AAA"
    assert "cross_section_score" in ranked.columns
    assert "industry_rank_pct" in ranked.columns
    assert float(ranked.loc[ranked["ts_code"] == "AAA", "industry_rank_pct"].iloc[0]) > float(
        ranked.loc[ranked["ts_code"] == "BBB", "industry_rank_pct"].iloc[0]
    )
    assert float(ranked.loc[ranked["ts_code"] == "AAA", "cross_section_score"].iloc[0]) > float(
        ranked.loc[ranked["ts_code"] == "CCC", "cross_section_score"].iloc[0]
    )
    assert "robustness_score" in ranked.columns
    assert float(ranked.loc[ranked["ts_code"] == "AAA", "robustness_score"].iloc[0]) > float(
        ranked.loc[ranked["ts_code"] == "CCC", "robustness_score"].iloc[0]
    )
    assert float(ranked.loc[ranked["ts_code"] == "AAA", "regime_bonus"].iloc[0]) > float(
        ranked.loc[ranked["ts_code"] == "CCC", "regime_bonus"].iloc[0]
    )


def test_signal_fusion_becomes_more_defensive_in_volatile_regime():
    fusion = SignalFusionEngine({"strong_buy_score": 70, "buy_score": 55, "watch_score": 40, "sell_score": 25})
    forecast = {"direction_prob": 0.66, "confidence": 0.72, "model_agreement": 0.75, "pred_return": 0.035}
    factor = {"total": 18}
    risk = {"allow_trade": True, "risk_level": "low"}

    trend = fusion.fuse(forecast, factor, {"regime": "trend", "environment_score": 0.82, "market_trend": "bullish"}, risk)
    volatile = fusion.fuse(
        forecast,
        factor,
        {"regime": "trend_volatile", "environment_score": 0.82, "market_trend": "bullish"},
        risk,
    )

    assert trend["score"] > volatile["score"]
    assert "volatile_regime" in volatile["reason"]


def test_signal_fusion_supports_configurable_regime_overrides():
    fusion = SignalFusionEngine({
        "strong_buy_score": 70,
        "buy_score": 55,
        "watch_score": 40,
        "sell_score": 25,
        "regime_overrides": {
            "trend": {"score_delta": 5, "strong_buy_score_delta": -4, "buy_score_delta": -3},
        },
    })
    forecast = {"direction_prob": 0.64, "confidence": 0.7, "model_agreement": 0.72, "pred_return": 0.03}
    factor = {"total": 14}
    risk = {"allow_trade": True, "risk_level": "low"}

    result = fusion.fuse(forecast, factor, {"regime": "trend", "environment_score": 0.8, "market_trend": "bullish"}, risk)

    assert result["signal"] in {"buy", "strong_buy"}
    assert result["score"] > 55


def test_position_agent_applies_regime_position_override():
    agent = PositionAgent({
        "settings": {"risk": {"max_position_pct": 0.2}},
        "signal_rules": {
            "buy_position_multiplier": 0.5,
            "strong_buy_position_multiplier": 1.0,
            "regime_overrides": {
                "trend_volatile": {"buy_position_multiplier": 0.25, "max_position_pct_scale": 0.8},
            },
        },
    })

    normal = agent.calculate_position_size(
        {"signal": "buy", "score": 70, "regime": "trend"},
        {"max_position_pct": 0.2, "position_scale": 1.0},
        {"cash": 1_000_000},
    )
    volatile = agent.calculate_position_size(
        {"signal": "buy", "score": 70, "regime": "trend_volatile"},
        {"max_position_pct": 0.2, "position_scale": 1.0},
        {"cash": 1_000_000},
    )

    assert volatile["position_pct"] < normal["position_pct"]


def test_build_runtime_agents_applies_latest_regime_profile(tmp_path):
    profile_path = tmp_path / "regime_profiles.json"
    profile_path.write_text(json.dumps({
        "trend": {
            "run_id": "run_trend_001",
            "params": {
                "buy_score": 41,
                "buy_position_multiplier": 0.22,
                "stop_loss_pct": 0.03,
            },
        }
    }), encoding="utf-8")

    pm = object.__new__(PipelineManager)
    pm.config = {
        "settings": {
            "risk": {"max_position_pct": 0.2, "stop_loss_pct": 0.05},
            "runtime": {"regime_profile_path": str(profile_path)},
        },
        "signal_rules": {"buy_score": 38, "buy_position_multiplier": 0.45, "strong_buy_position_multiplier": 1.0},
        "risk_rules": {},
        "market_rules": {},
    }
    pm._regime_profiles_cache = None
    pm.risk_agent = RiskAgent(pm.config)
    pm.signal_agent = SignalAgent(pm.config)
    pm.position_agent = PositionAgent(pm.config)

    risk_agent, signal_agent, position_agent, runtime_profile = pm._build_runtime_agents({"regime": "trend_volatile"})

    assert runtime_profile["run_id"] == "run_trend_001"
    assert signal_agent.fusion.buy_score == 41
    assert position_agent.buy_position_multiplier == 0.22
    assert risk_agent.config["settings"]["risk"]["stop_loss_pct"] == 0.03


def test_calibration_bucket_lookup_prefers_matching_range():
    records = pd.DataFrame({
        "direction_prob_up": [0.51, 0.54, 0.58, 0.62, 0.67, 0.71, 0.76, 0.82],
        "realized_return": [0.002, 0.004, 0.006, 0.012, 0.018, 0.021, 0.03, 0.036],
        "realized_up": [0, 1, 1, 1, 1, 1, 1, 1],
    })

    buckets = ForecastAgent._bucket_calibration_records(records, max_buckets=4)
    matched = ForecastAgent._lookup_calibration_bucket(0.74, buckets)

    assert len(buckets) >= 2
    assert matched["sample_size"] > 0
    assert matched["avg_return"] > 0
    assert matched["up_rate"] >= 0.5


def test_rank_candidates_adds_soft_diversification_penalty():
    results = []
    stock_basic_map = {}
    payloads = [
        ("AAA", "Oil", 86, 0.76, 0.050, 0.78, 0.045),
        ("BBB", "Oil", 85, 0.75, 0.049, 0.77, 0.044),
        ("CCC", "Tech", 82, 0.71, 0.041, 0.73, 0.034),
        ("DDD", "Tech", 80, 0.69, 0.038, 0.71, 0.030),
    ]
    for code, industry, score, prob, pred_ret, cal_prob, cal_ret in payloads:
        results.append({
            "ts_code": code,
            "signal_result": {"signal": "strong_buy", "score": score, "reason": "test"},
            "forecast_result": {
                "direction_prob_up": prob,
                "pred_return": pred_ret,
                "confidence": 0.75,
                "calibrated_upside_win_rate": cal_prob,
                "calibrated_avg_return": cal_ret,
                "calibration_sample_size": 120,
                "model_agreement": 0.8,
                "prediction_dispersion": 0.03,
            },
            "risk_info": {"risk_level": "low", "stop_loss": 9.0, "take_profit": 11.0},
            "position_result": {"position_pct": 0.2},
        })
        stock_basic_map[code] = {"stock_name": code, "industry": industry, "market": "Main", "area": "SZ"}

    ranked = rank_candidates(results, top_n=3, stock_basic_map=stock_basic_map)

    assert len(ranked) == 3
    assert ranked.iloc[0]["ts_code"] == "AAA"
    assert "diversification_penalty" in ranked.columns
    assert "selection_score" in ranked.columns
    oil_rows = ranked[ranked["industry"] == "Oil"]
    tech_rows = ranked[ranked["industry"] == "Tech"]
    assert len(oil_rows) <= 2
    assert len(tech_rows) >= 1
    assert ranked["diversification_penalty"].notna().all()
    if len(oil_rows) == 2:
        assert float(oil_rows["diversification_penalty"].max()) > 0.0
    assert "basket_weight_pct" in ranked.columns
    assert "basket_role" in ranked.columns
    assert "portfolio_weight_after_risk" in ranked.columns
    assert "basket_risk_flag" in ranked.columns
    assert "risk_overlay_penalty" in ranked.columns
    assert abs(float(ranked["basket_weight_pct"].sum()) - 1.0) < 1e-6
    assert abs(float(ranked["portfolio_weight_after_risk"].sum()) - 1.0) < 1e-6
    assert ranked.iloc[0]["basket_role"] == "core"
    assert ranked.iloc[0]["portfolio_weight_after_risk"] <= 0.35


def test_summarize_candidate_basket_returns_portfolio_outlook():
    df = pd.DataFrame({
        "portfolio_weight_after_risk": [0.35, 0.25, 0.20, 0.20],
        "pred_return": [0.03, 0.02, 0.015, 0.01],
        "calibrated_avg_return": [0.025, 0.018, 0.012, 0.008],
        "calibrated_upside_win_rate": [0.64, 0.60, 0.57, 0.55],
        "risk_level": ["low", "low", "medium", "high"],
        "industry": ["Oil", "Tech", "Tech", "Oil"],
        "risk_overlay_penalty": [8.0, 0.0, 0.0, 2.0],
        "diversification_penalty": [6.0, 0.0, 0.0, 3.0],
    })

    summary = summarize_candidate_basket(df)

    assert summary["candidate_count"] == 4
    assert summary["expected_basket_return"] > 0
    assert summary["calibrated_basket_return"] > 0
    assert 0.0 < summary["basket_win_rate"] < 1.0
    assert summary["top_industry"] in {"Oil", "Tech"}
    assert abs(summary["max_single_weight"] - 0.35) < 1e-6
    assert abs(summary["high_risk_weight"] - 0.20) < 1e-6
    assert summary["risk_pressure_score"] > 0


def test_summarize_historical_validation_returns_excess_metrics():
    summary = summarize_historical_validation([
        {"trade_date": "2026-03-10", "basket_return_5d": 0.023, "universe_return_5d": 0.011, "top1_return_5d": 0.031},
        {"trade_date": "2026-03-11", "basket_return_5d": -0.004, "universe_return_5d": -0.009, "top1_return_5d": 0.006},
    ])

    assert summary["rebalance_dates"] == 2
    assert abs(summary["avg_basket_return_5d"] - 0.0095) < 1e-6
    assert abs(summary["avg_universe_return_5d"] - 0.001) < 1e-6
    assert abs(summary["avg_excess_return_5d"] - 0.0085) < 1e-6
    assert summary["basket_win_rate_5d"] == 0.5


def test_summarize_variant_comparison_returns_ab_metrics():
    summary = summarize_variant_comparison([
        {
            "trade_date": "2026-03-10",
            "diversified_return_5d": 0.02,
            "raw_return_5d": 0.01,
            "top1_return_5d": -0.01,
            "universe_return_5d": 0.005,
        },
        {
            "trade_date": "2026-03-11",
            "diversified_return_5d": -0.01,
            "raw_return_5d": -0.02,
            "top1_return_5d": 0.03,
            "universe_return_5d": -0.005,
        },
    ])

    assert summary["diversified"]["avg_return_5d"] == 0.005
    assert summary["diversified"]["avg_excess_return_5d"] == 0.005
    assert summary["raw"]["win_rate_5d"] == 0.5
    assert summary["top1"]["avg_return_5d"] == 0.01


def test_trade_date_normalization_supports_sqlite_and_history_formats():
    assert _normalize_trade_date("20260310") == "2026-03-10"
    assert _normalize_trade_date("2026-03-10") == "2026-03-10"

    history = pd.DataFrame({
        "date": ["2026-03-10", "2026-03-11", "2026-03-12", "2026-03-13", "2026-03-14", "2026-03-15"],
        "close": [10.0, 10.1, 10.3, 10.2, 10.4, 10.6],
    })
    forward = _forward_return_from_history(history, "20260310", horizon=5)
    assert abs(float(forward) - 0.06) < 1e-6


def test_rank_candidates_rebalances_when_industry_is_too_concentrated():
    results = []
    stock_basic_map = {}
    payloads = [
        ("AAA", "Oil", 88, 0.79, 0.051, 0.79, 0.042),
        ("BBB", "Oil", 87, 0.78, 0.050, 0.78, 0.041),
        ("CCC", "Oil", 86, 0.77, 0.049, 0.77, 0.040),
        ("DDD", "Tech", 79, 0.70, 0.038, 0.72, 0.031),
        ("EEE", "Software", 78, 0.69, 0.037, 0.71, 0.030),
        ("FFF", "Chemical", 77, 0.68, 0.036, 0.70, 0.029),
    ]
    for code, industry, score, prob, pred_ret, cal_prob, cal_ret in payloads:
        results.append({
            "ts_code": code,
            "signal_result": {"signal": "strong_buy", "score": score, "reason": "rebalance"},
            "forecast_result": {
                "direction_prob_up": prob,
                "pred_return": pred_ret,
                "confidence": 0.76,
                "calibrated_upside_win_rate": cal_prob,
                "calibrated_avg_return": cal_ret,
                "calibration_sample_size": 140,
                "model_agreement": 0.82,
                "prediction_dispersion": 0.03,
            },
            "risk_info": {"risk_level": "low", "stop_loss": 9.0, "take_profit": 11.0},
            "position_result": {"position_pct": 0.2},
        })
        stock_basic_map[code] = {"stock_name": code, "industry": industry, "market": "Main", "area": "SZ"}

    ranked = rank_candidates(results, top_n=4, stock_basic_map=stock_basic_map)

    assert len(ranked) == 4
    assert ranked["industry"].value_counts().get("Oil", 0) <= 2
    assert float(ranked["basket_risk_pressure_score"].iloc[0]) <= 75.0
