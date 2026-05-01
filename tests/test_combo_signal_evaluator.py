from __future__ import annotations

import math

import pandas as pd

from openclaw.runtime.combo_signal_evaluator import (
    evaluate_combo_score_components,
    evaluate_combo_signal,
    finalize_combo_scan_score,
    resolve_combo_signal_config,
)


class FakeV5:
    def __init__(self, score=70, success=True):
        self.score = score
        self.success = success

    def evaluate_stock_v4(self, current_data):
        return {"success": self.success, "final_score": self.score}


class FakeV8:
    def __init__(self, score=80, success=True):
        self.score = score
        self.success = success

    def evaluate_stock_v8(self, current_data, *, ts_code, index_data, industry):
        return {"success": self.success, "final_score": self.score}


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"trade_date": "20260101", "close": 10.0, "industry": "半导体"},
            {"trade_date": "20260102", "close": 10.5, "industry": "半导体"},
        ]
    )


def test_resolve_combo_signal_config_applies_production_env_and_health_weights():
    config = resolve_combo_signal_config(
        combo_params={"combo_threshold": 68, "min_agree": 2},
        combo_threshold=60,
        min_agree=1,
        market_env="bear",
        production_only=True,
        health_multipliers={"v5": 0.5, "v8": 1.0, "v9": 2.0},
    )

    assert config["market_env"] == "bear"
    assert config["combo_threshold"] == 68.0
    assert config["min_agree"] == 2
    assert abs(sum(config["weights"].values()) - 1.0) < 1e-9
    assert config["weights"]["v9"] > config["weights"]["v5"]


def test_evaluate_combo_signal_returns_weighted_consensus():
    result = evaluate_combo_signal(
        ts_code="000001.SZ",
        current_data=_frame(),
        v5_evaluator=FakeV5(70),
        v8_evaluator=FakeV8(80),
        v9_score_fn=lambda hist: {"score": 90},
        index_data=None,
        thresholds={"v5": 60, "v8": 65, "v9": 60},
        weights={"v5": 0.2, "v8": 0.3, "v9": 0.5},
        combo_threshold=75,
        min_agree=2,
        market_env="oscillation",
    )

    assert result is not None
    assert result["agree_count"] == 3
    assert result["required_agree"] == 2
    assert abs(result["signal_strength"] - 83.0) < 1e-9
    assert result["market_env"] == "oscillation"


def test_evaluate_combo_signal_blocks_when_agree_gate_fails():
    result = evaluate_combo_signal(
        ts_code="000001.SZ",
        current_data=_frame(),
        v5_evaluator=FakeV5(50),
        v8_evaluator=FakeV8(80),
        v9_score_fn=lambda hist: {"score": 55},
        index_data=None,
        thresholds={"v5": 60, "v8": 65, "v9": 60},
        weights={"v5": 0.3, "v8": 0.3, "v9": 0.4},
        combo_threshold=60,
        min_agree=2,
        market_env="oscillation",
    )

    assert result is None


def test_evaluate_combo_signal_reweights_active_scores_when_one_evaluator_fails():
    result = evaluate_combo_signal(
        ts_code="000001.SZ",
        current_data=_frame(),
        v5_evaluator=FakeV5(70),
        v8_evaluator=FakeV8(success=False),
        v9_score_fn=lambda hist: {"score": 90},
        index_data=None,
        thresholds={"v5": 60, "v8": 65, "v9": 60},
        weights={"v5": 0.25, "v8": 0.50, "v9": 0.25},
        combo_threshold=79,
        min_agree=2,
        market_env="bull",
    )

    assert result is not None
    assert math.isnan(result["v8_score"])
    assert abs(result["signal_strength"] - 80.0) < 1e-9


def test_evaluate_combo_score_components_supports_scan_v4_v7_compatibility():
    result = evaluate_combo_score_components(
        scores={"v4": 64, "v5": 70, "v7": 68, "v8": None, "v9": 90},
        thresholds={"v4": 60, "v5": 60, "v7": 65, "v8": 65, "v9": 60},
        weights={"v4": 0.1, "v5": 0.2, "v7": 0.3, "v8": 0.2, "v9": 0.2},
        combo_threshold=68,
        min_agree=3,
        market_env="oscillation",
    )

    assert result is not None
    assert result["agree_count"] == 4
    assert result["required_agree"] == 3
    assert abs(result["signal_strength"] - 73.5) < 1e-9
    assert result["v7_score"] == 68
    assert result["v8_score"] != result["v8_score"]
    assert abs(result["v4_contrib"] - 8.0) < 1e-9


def test_finalize_combo_scan_score_applies_penalty_market_factor_and_bonus():
    consensus = evaluate_combo_score_components(
        scores={"v4": None, "v5": 70, "v7": None, "v8": 80, "v9": 90},
        thresholds={"v5": 60, "v8": 85, "v9": 60},
        weights={"v5": 0.2, "v8": 0.3, "v9": 0.5},
        combo_threshold=70,
        min_agree=2,
        market_env="bear",
    )

    result = finalize_combo_scan_score(
        consensus_result=consensus,
        scores={"v4": None, "v5": 70, "v7": None, "v8": 80, "v9": 90},
        thresholds={"v5": 60, "v8": 85, "v9": 60},
        weights={"v5": 0.2, "v8": 0.3, "v9": 0.5},
        combo_threshold=70,
        disagree_std_weight=0.35,
        disagree_count_weight=1.0,
        market_adjust_strength=0.5,
        market_env="bear",
        external_bonus=2.0,
    )

    assert result["agree_count"] == 2
    assert abs(result["weighted_score"] - 83.0) < 1e-9
    assert abs(result["adj_factor"] - 0.975) < 1e-9
    assert result["penalty"] > 1.0
    assert abs(result["final_score"] - ((83.0 * 0.975) + 2.0 - result["penalty"])) < 1e-9
    assert result["contrib"]["v9贡献"] == 45.0
