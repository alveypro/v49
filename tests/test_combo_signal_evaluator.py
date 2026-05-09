from __future__ import annotations

import math

import pandas as pd

from openclaw.runtime.combo_signal_evaluator import (
    classify_combo_consensus_breakpoint,
    evaluate_combo_component_scores,
    evaluate_combo_score_components,
    evaluate_combo_signal,
    finalize_combo_scan_score,
    freeze_combo_component_diagnostics,
    prewarm_v8_base_context,
    record_combo_component_diagnostics,
    record_combo_gate_diagnostics,
    resolve_combo_signal_config,
    slice_index_data_for_combo_history,
)


class FakeV5:
    def __init__(self, score=70, success=True, dim_scores=None, synergy_bonus=6):
        self.score = score
        self.success = success
        self.dim_scores = dim_scores or {"启动确认": 18, "主力行为": 12}
        self.synergy_bonus = synergy_bonus

    def evaluate_stock_v4(self, current_data):
        return {
            "success": self.success,
            "final_score": self.score,
            "base_score": self.score - 4,
            "synergy_bonus": self.synergy_bonus,
            "synergy_combo": "量价启动",
            "risk_penalty": 2,
            "risk_reasons": ["放量过快"],
            "dim_scores": self.dim_scores,
        }


class FakeV8:
    def __init__(self, score=80, success=True, runtime_diagnostics=None):
        self.score = score
        self.success = success
        self.calls = 0
        self.runtime_diagnostics = runtime_diagnostics

    def evaluate_stock_v8(self, current_data, *, ts_code, index_data, industry):
        self.calls += 1
        result = {"success": self.success, "final_score": self.score}
        if self.runtime_diagnostics is not None:
            result["runtime_diagnostics"] = self.runtime_diagnostics
        return result


class FakeMarketAnalyzer:
    def __init__(self):
        self.regime_calls = 0
        self.sentiment_calls = 0

    def identify_market_regime(self):
        self.regime_calls += 1
        return "震荡市"

    def calculate_market_sentiment(self):
        self.sentiment_calls += 1
        return -0.25


class FakeIndustryAnalyzer:
    def __init__(self):
        self.hot_calls = 0

    def get_hot_industries(self, top_n=8):
        self.hot_calls += 1
        return ["银行"][:top_n]


class FakeV7Base:
    def __init__(self):
        self.current_regime = None
        self.current_sentiment = 0
        self.hot_industries = []
        self.market_analyzer = FakeMarketAnalyzer()
        self.industry_analyzer = FakeIndustryAnalyzer()


class FakeV8WithBase:
    def __init__(self):
        self.v7_evaluator = FakeV7Base()


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
    assert config["base_weights"] == {"v5": 0.20, "v8": 0.20, "v9": 0.60}
    assert config["health_multipliers"] == {"v5": 0.5, "v8": 1.0, "v9": 2.0}
    assert abs(sum(config["weights"].values()) - 1.0) < 1e-9
    assert config["weights"]["v9"] > config["weights"]["v5"]


def test_prewarm_v8_base_context_sets_v7_runtime_context():
    diagnostics = {}
    v8 = FakeV8WithBase()

    result = prewarm_v8_base_context(v8, diagnostics)
    second = prewarm_v8_base_context(v8, diagnostics)

    assert result["status"] == "warmed"
    assert second["status"] == "already_warm"
    assert v8.v7_evaluator.current_regime == "震荡市"
    assert v8.v7_evaluator.current_sentiment == -0.25
    assert v8.v7_evaluator.hot_industries == ["银行"]
    assert v8.v7_evaluator.market_analyzer.regime_calls == 1
    assert diagnostics["v7_context_prewarm"]["status"] == "already_warm"


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


def test_evaluate_combo_component_scores_exposes_scores_for_diagnostics():
    scores = evaluate_combo_component_scores(
        ts_code="000001.SZ",
        current_data=_frame(),
        v5_evaluator=FakeV5(70),
        v8_evaluator=FakeV8(success=False),
        v9_score_fn=lambda hist: {"score": 90},
        index_data=None,
    )

    assert scores == {"v5": 70.0, "v8": None, "v9": 90.0}


def test_evaluate_combo_component_scores_records_component_timing():
    diagnostics = {}

    evaluate_combo_component_scores(
        ts_code="000001.SZ",
        current_data=_frame(),
        v5_evaluator=FakeV5(70),
        v8_evaluator=FakeV8(80),
        v9_score_fn=lambda hist: {"score": 90},
        index_data=None,
        timing_sink=diagnostics,
    )

    out = freeze_combo_component_diagnostics(diagnostics)

    assert set(out["component_timing_ms"]) == {"v5", "v8", "v9"}
    assert out["component_timing_ms"]["v5"]["count"] == 1
    assert out["component_timing_totals_ms"]["count"] == 3


def test_evaluate_combo_component_scores_records_v5_score_breakdown():
    diagnostics = {}

    evaluate_combo_component_scores(
        ts_code="000001.SZ",
        current_data=_frame(),
        v5_evaluator=FakeV5(70),
        v8_evaluator=FakeV8(80),
        v9_score_fn=lambda hist: {"score": 90},
        index_data=None,
        timing_sink=diagnostics,
    )

    out = freeze_combo_component_diagnostics(diagnostics)

    assert out["v5_score_breakdown"]["final_score"]["avg"] == 70.0
    assert out["v5_score_breakdown"]["base_score"]["avg"] == 66.0
    assert out["v5_score_breakdown"]["dim:启动确认"]["avg"] == 18.0
    assert out["v5_synergy_combo_counts"]["量价启动"] == 1
    assert out["v5_risk_reason_counts"]["放量过快"] == 1


def test_evaluate_combo_component_scores_can_filter_non_candidate_v5_without_losing_diagnostics():
    diagnostics = {}

    scores = evaluate_combo_component_scores(
        ts_code="000001.SZ",
        current_data=_frame(),
        v5_evaluator=FakeV5(25, dim_scores={"启动确认": 4, "量价配合": 3, "主力行为": 3}, synergy_bonus=0),
        v8_evaluator=FakeV8(80),
        v9_score_fn=lambda hist: {"score": 90},
        index_data=None,
        timing_sink=diagnostics,
        v5_candidate_aligned=True,
        v5_threshold=60,
    )

    out = freeze_combo_component_diagnostics(diagnostics)

    assert scores["v5"] is None
    assert out["v5_score_breakdown"]["final_score"]["avg"] == 25.0
    assert out["v5_candidate_filter"]["filtered_out"] == 1
    assert out["v5_candidate_filter"]["reason_counts"]["launch_confirmation_below_candidate_floor"] == 1


def test_evaluate_combo_component_scores_can_reuse_component_cache():
    diagnostics = {}
    cache = {}
    v8 = FakeV8(80)

    for _ in range(2):
        scores = evaluate_combo_component_scores(
            ts_code="000001.SZ",
            current_data=_frame(),
            v5_evaluator=FakeV5(70),
            v8_evaluator=v8,
            v9_score_fn=lambda hist: {"score": 90},
            index_data=None,
            timing_sink=diagnostics,
            score_cache=cache,
        )

    out = freeze_combo_component_diagnostics(diagnostics)

    assert scores == {"v5": 70.0, "v8": 80.0, "v9": 90.0}
    assert v8.calls == 1
    assert out["component_score_cache"]["v8"]["hit"] == 1
    assert out["component_score_cache"]["v8"]["miss"] == 1
    assert out["component_timing_ms"]["v8"]["count"] == 1


def test_evaluate_combo_component_scores_records_v8_stage_timing():
    diagnostics = {}

    evaluate_combo_component_scores(
        ts_code="000001.SZ",
        current_data=_frame(),
        v5_evaluator=FakeV5(70),
        v8_evaluator=FakeV8(80, runtime_diagnostics={"stage_timing_ms": {"base_evaluator": 12.0}}),
        v9_score_fn=lambda hist: {"score": 90},
        index_data=None,
        timing_sink=diagnostics,
    )

    out = freeze_combo_component_diagnostics(diagnostics)

    assert out["v8_stage_timing_ms"]["base_evaluator"]["count"] == 1
    assert out["v8_stage_timing_ms"]["base_evaluator"]["avg"] == 12.0


def test_evaluate_combo_component_scores_records_nested_v7_stage_timing():
    diagnostics = {}

    evaluate_combo_component_scores(
        ts_code="000001.SZ",
        current_data=_frame(),
        v5_evaluator=FakeV5(70),
        v8_evaluator=FakeV8(
            80,
            runtime_diagnostics={
                "stage_timing_ms": {"base_evaluator": 20.0},
                "v7_stage_timing_ms": {"v4_score": 16.0},
            },
        ),
        v9_score_fn=lambda hist: {"score": 90},
        index_data=None,
        timing_sink=diagnostics,
    )

    out = freeze_combo_component_diagnostics(diagnostics)

    assert out["v8_stage_timing_ms"]["base_evaluator"]["avg"] == 20.0
    assert out["v7_stage_timing_ms"]["v4_score"]["avg"] == 16.0


def test_slice_index_data_for_combo_history_is_point_in_time():
    index_data = pd.DataFrame(
        {
            "trade_date": ["20260101", "20260102", "20260103"],
            "close": [1.0, 2.0, 3.0],
        }
    )
    current = pd.DataFrame({"trade_date": ["20260101", "20260102"], "close": [10.0, 11.0]})

    out = slice_index_data_for_combo_history(index_data, current)

    assert out is not None
    assert out["trade_date"].tolist() == ["20260101", "20260102"]


def test_record_combo_component_diagnostics_freezes_score_distribution():
    diagnostics = {}
    record_combo_component_diagnostics(
        diagnostics,
        scores={"v5": 62.0, "v8": 58.0, "v9": 71.0},
        thresholds={"v5": 60.0, "v8": 65.0, "v9": 60.0},
        combo_threshold=60.0,
    )
    record_combo_component_diagnostics(
        diagnostics,
        scores={"v5": 55.0, "v8": 64.0, "v9": None},
        thresholds={"v5": 60.0, "v8": 65.0, "v9": 60.0},
        combo_threshold=60.0,
    )

    out = freeze_combo_component_diagnostics(diagnostics)

    assert out["component_score_stats"]["v8"]["count"] == 2
    assert out["component_score_stats"]["v8"]["max"] == 64.0
    assert out["component_near_threshold"]["v8"]["within_10"] == 1
    assert out["component_near_threshold"]["v8"]["within_5"] == 1


def test_record_combo_gate_diagnostics_freezes_pair_and_consensus_gap():
    diagnostics = {}
    gate = record_combo_gate_diagnostics(
        diagnostics,
        scores={"v5": 62.0, "v8": 66.0, "v9": 40.0},
        thresholds={"v5": 60.0, "v8": 65.0, "v9": 60.0},
        weights={"v5": 0.75, "v8": 0.05, "v9": 0.20},
        combo_threshold=60.0,
        min_agree=2,
    )

    out = freeze_combo_component_diagnostics(diagnostics)

    assert gate["agree_count"] == 2
    assert out["pair_agreement"]["v5+v8"] == 1
    assert out["weighted_consensus_candidates"]["count"] == 1
    assert out["weighted_consensus_candidates"]["below_combo_threshold"] == 1
    assert out["weighted_consensus_candidates"]["avg_gap"] > 0
    assert gate["breakpoint"]["type"] == "weighted_consensus_gap"
    assert out["consensus_breakpoint_summary"]["weighted_consensus_gap"]["below_combo_threshold"] == 1


def test_classify_combo_consensus_breakpoint_explains_agreement_shortfall_without_lowering_gate():
    out = classify_combo_consensus_breakpoint(
        scores={"v5": 59.0, "v8": 66.0, "v9": 58.0},
        thresholds={"v5": 60.0, "v8": 65.0, "v9": 60.0},
        weights={"v5": 0.3, "v8": 0.4, "v9": 0.3},
        combo_threshold=60.0,
        min_agree=2,
    )

    assert out["type"] == "component_agreement_shortfall"
    assert out["required_agree"] == 2
    assert out["agree_count"] == 1
    assert out["passing_components"] == ["v8"]
    assert out["failed_components"] == ["v5", "v9"]


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
