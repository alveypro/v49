import pandas as pd

from strategies.evaluators.comprehensive_stock_evaluator_v6_ultimate import (
    ComprehensiveStockEvaluatorV6Ultimate,
)


class FakeProvider:
    def get_stock_sector(self, ts_code):
        return {"industry": "电子", "concept": ["芯片"], "area": "未知"}

    def get_sector_performance(self, industry, days=3):
        return {"change_3d": 3.0, "avg_change": 1.0, "rank": 1, "total_industries": 10, "money_flow": 0}

    def get_money_flow(self, ts_code, days=3):
        return {
            "net_mf_amount": 12000,
            "consecutive_inflow_days": 3,
            "buy_lg_amount": 12000,
            "sell_lg_amount": 0,
            "buy_elg_amount": 3000,
            "sell_elg_amount": 0,
            "today_net": 5000,
        }

    def get_north_money_flow(self, ts_code, days=3):
        return {"is_connect_stock": False, "north_net_3d": 0}

    def get_market_change(self, days=3):
        return 0.5


class FakeLeader:
    def calculate_leader_score(self, ts_code, industry, recent_change_3d):
        return {
            "sector_rank": 1,
            "total_stocks": 20,
            "limit_up_count_20d": 1,
        }


class MissingPITProvider(FakeProvider):
    def get_sector_performance(self, industry, days=3):
        return {
            "change_3d": 0,
            "avg_change": 0,
            "rank": 50,
            "total_industries": 100,
            "money_flow": 0,
            "data_available": False,
            "proxy_source": "missing_market_frame_industry_pct_chg",
        }

    def get_money_flow(self, ts_code, days=3):
        out = super().get_money_flow(ts_code, days=days)
        out.update({"net_mf_amount": 0, "consecutive_inflow_days": 0, "data_available": False, "proxy_source": "missing_stock_hist_vol_pct_chg"})
        return out


def _hist():
    rows = []
    for i in range(70):
        rows.append(
            {
                "close_price": 10.0 + i * 0.02,
                "vol": 1000 + i * 10,
                "pct_chg": 1.0 if i >= 67 else 0.2,
                "name": "芯片A",
            }
        )
    return pd.DataFrame(rows)


def test_v6_evaluator_records_internal_stage_timing():
    evaluator = ComprehensiveStockEvaluatorV6Ultimate.__new__(ComprehensiveStockEvaluatorV6Ultimate)
    evaluator.data_provider = FakeProvider()
    evaluator.leader_analyzer = FakeLeader()
    evaluator.runtime_candidate_filter_mode = "diagnostic_advisory"

    out = evaluator.evaluate_stock_v6(_hist(), "000001.SZ")

    assert out["success"] is True
    timing = out["runtime_diagnostics"]["stage_timing_ms"]
    assert timing["indicators"] >= 0.0
    assert timing["mandatory_conditions"] >= 0.0
    assert timing["score_money_flow"] >= 0.0
    assert timing["score_sector_heat"] >= 0.0
    assert timing["score_leader"] >= 0.0
    assert timing["risk"] >= 0.0
    assert timing["total"] >= timing["indicators"]


def test_v6_runtime_money_flow_uses_observable_proxy_cap():
    evaluator = ComprehensiveStockEvaluatorV6Ultimate.__new__(ComprehensiveStockEvaluatorV6Ultimate)
    evaluator.data_provider = FakeProvider()

    score = evaluator._score_money_flow_strict("000001.SZ")

    assert score >= 20
    assert score <= 30


def test_v6_mandatory_filter_marks_missing_pit_data_as_warning_not_hard_failure():
    evaluator = ComprehensiveStockEvaluatorV6Ultimate.__new__(ComprehensiveStockEvaluatorV6Ultimate)
    evaluator.data_provider = MissingPITProvider()
    evaluator.leader_analyzer = FakeLeader()

    out = evaluator._check_mandatory_conditions(
        "000001.SZ",
        "电子",
        {"price_position": 0.55, "vol_ratio": 1.0},
        pct_chg=[0.1, 0.2, 0.3, 0.4, 0.5],
        volume=[1000.0] * 12,
    )

    assert out["passed"] is True
    assert out["pit_data_quality"]["sector_performance_available"] is False
    assert out["pit_data_quality"]["money_flow_available"] is False
    assert any("板块PIT数据不可用" in item for item in out["warnings"])
    assert any("资金PIT数据不可用" in item for item in out["warnings"])


def test_v6_calibrated_synergy_and_risk_keep_extreme_risk_penalized():
    evaluator = ComprehensiveStockEvaluatorV6Ultimate.__new__(ComprehensiveStockEvaluatorV6Ultimate)
    evaluator.data_provider = FakeProvider()

    dim_scores = {
        "资金流向": 20.0,
        "板块热度": 15.0,
        "短期动量": 13.0,
        "龙头属性": 8.0,
        "相对强度": 5.0,
        "技术突破": 3.0,
        "安全边际": 1.0,
    }
    synergy = evaluator._calculate_synergy_v6_strict(
        dim_scores,
        {"price_position": 0.55},
        pct_chg=[0.5, 1.0, 2.5, 4.0, 6.0],
        volume=[1200, 1100, 1000, 900, 1500],
    )
    assert synergy["bonus"] >= 20
    assert "强势共振" in synergy["combo_type"]

    controlled = evaluator._calculate_risk_v6_strict(
        {"price_position": 0.70, "volatility": 0.04, "vol_ratio": 1.6},
        close=[10, 10.2, 10.5, 10.8, 11.2],
        pct_chg=[1.0, 1.5, 2.0, 3.0, 4.0],
        volume=[1000, 1050, 1100, 1200, 1500],
        ts_code="000001.SZ",
    )
    extreme = evaluator._calculate_risk_v6_strict(
        {"price_position": 0.96, "volatility": 0.11, "vol_ratio": 5.5},
        close=[10, 11, 12, 13, 14],
        pct_chg=[10.0, 10.0, 10.0, 4.0, 3.0],
        volume=[1000, 2000, 3000, 4000, 6000],
        ts_code="000001.SZ",
    )

    assert controlled["penalty"] < extreme["penalty"]
    assert extreme["penalty"] >= 35


def test_v6_entry_gate_blocks_overheated_chase_and_allows_pullback_reconfirmation():
    evaluator = ComprehensiveStockEvaluatorV6Ultimate.__new__(ComprehensiveStockEvaluatorV6Ultimate)

    overheated = evaluator._evaluate_tradeable_entry_v6(
        {"price_position": 0.98, "volatility": 0.11, "vol_ratio": 2.2},
        close=[10.0 + i for i in range(25)],
        pct_chg=[0.5] * 20 + [10.0, 10.0, 4.0, 3.0, 2.0],
        volume=[1000.0] * 25,
    )
    assert overheated["passed"] is False
    assert overheated["mode"] == "wait_for_pullback_reconfirmation"
    assert "price_position_extreme" in overheated["overheat_flags"]

    pullback = evaluator._evaluate_tradeable_entry_v6(
        {"price_position": 0.72, "volatility": 0.04, "vol_ratio": 1.2},
        close=[10.0 + i * 0.5 for i in range(20)] + [18.5, 18.0, 17.7, 18.0, 18.3],
        pct_chg=[0.2] * 20 + [-2.0, -1.2, -0.8, 0.9, 1.1],
        volume=[1000.0] * 20 + [1100.0, 1050.0, 980.0, 1030.0, 1080.0],
    )
    assert pullback["passed"] is True
    assert pullback["mode"] == "pullback_reconfirmed"

    quality = evaluator._evaluate_tradeable_entry_v6(
        {"price_position": 0.72, "volatility": 0.04, "vol_ratio": 1.7},
        close=[10.0 + i * 0.5 for i in range(20)] + [18.5, 18.0, 17.7, 18.0, 18.3],
        pct_chg=[0.2] * 20 + [-2.0, -1.2, -0.8, 0.9, 1.1],
        volume=[1000.0] * 20 + [1100.0, 1050.0, 980.0, 1030.0, 1080.0],
        dim_scores={"资金流向": 12.0, "板块热度": 10.0, "短期动量": 8.0, "龙头属性": 6.0, "相对强度": 4.0, "技术突破": 1.0},
    )
    assert quality["mode"] == "pullback_reconfirmed"
    secondary = quality["secondary_confirmation"]
    assert secondary["technical_breakout_reconfirmed"] is True
    assert secondary["volume_breakout"] is True
    assert secondary["technical_breakthrough_current_score"] == 1.0
    assert secondary["technical_breakthrough_candidate_score"] == 3.0
    assert secondary["technical_breakthrough_candidate_delta"] == 2.0
    assert secondary["money_flow_returned"] is True
    assert secondary["sector_reheated"] is True
    assert secondary["quality_ready"] is True

    platform = evaluator._evaluate_tradeable_entry_v6(
        {"price_position": 0.55, "volatility": 0.03, "vol_ratio": 1.1},
        close=[12.0, 13.0, 14.0, 15.0, 16.0, 20.0] + [17.0] * 14 + [17.8, 17.4, 17.1, 17.2, 17.9],
        pct_chg=[0.1] * 20 + [-2.0, -1.5, -1.0, 0.6, 4.1],
        volume=[1000.0] * 20 + [980.0, 960.0, 940.0, 930.0, 970.0],
        dim_scores={"资金流向": 12.0, "板块热度": 10.0, "短期动量": 8.0, "龙头属性": 6.0, "相对强度": 4.0, "技术突破": 0.0},
    )
    assert platform["mode"] == "pullback_reconfirmed"
    assert platform["secondary_confirmation"]["volume_breakout"] is False
    assert platform["secondary_confirmation"]["pullback_platform_breakout"] is True
    assert platform["secondary_confirmation"]["technical_breakout_reconfirmed"] is True
    assert platform["secondary_confirmation"]["technical_breakthrough_current_score"] == 0.0
    assert platform["secondary_confirmation"]["ma5_reclaim"] or platform["secondary_confirmation"]["ma10_reclaim"]
    assert platform["secondary_confirmation"]["technical_breakthrough_candidate_score"] == 3.0
    assert platform["secondary_confirmation"]["technical_breakthrough_candidate_delta"] == 3.0


def test_v6_pullback_quality_lifts_technical_breakthrough_score():
    evaluator = ComprehensiveStockEvaluatorV6Ultimate.__new__(ComprehensiveStockEvaluatorV6Ultimate)
    dim_scores = {"技术突破": 0.0, "资金流向": 12.0}
    entry_gate = {
        "mode": "pullback_reconfirmed",
        "secondary_confirmation": {
            "quality_ready": True,
            "technical_breakthrough_candidate_score": 3.0,
        },
    }

    evaluator._apply_pullback_technical_breakthrough_score_lift_v6(dim_scores, entry_gate)

    assert dim_scores["技术突破"] == 3.0
    assert entry_gate["technical_breakthrough_score_lift_applied"] is True


def test_v6_direct_strength_secondary_confirmation_lifts_technical_breakthrough_score():
    evaluator = ComprehensiveStockEvaluatorV6Ultimate.__new__(ComprehensiveStockEvaluatorV6Ultimate)
    dim_scores = {"技术突破": 0.5, "资金流向": 15.0}
    entry_gate = {
        "mode": "direct_strength_entry",
        "secondary_confirmation": {
            "quality_ready": False,
            "technical_breakout_reconfirmed": True,
            "confirmed_count": 5,
            "technical_breakthrough_candidate_score": 3.0,
            "money_flow_returned": True,
            "momentum_alive": True,
            "leader_supported": True,
        },
    }

    evaluator._apply_pullback_technical_breakthrough_score_lift_v6(dim_scores, entry_gate)

    assert dim_scores["技术突破"] == 3.0
    assert entry_gate["technical_breakthrough_score_lift_applied"] is True
    assert entry_gate["technical_breakthrough_score_lift_reason"] == "direct_strength_secondary_confirmation"


def test_v6_pullback_score_lift_requires_quality_ready():
    evaluator = ComprehensiveStockEvaluatorV6Ultimate.__new__(ComprehensiveStockEvaluatorV6Ultimate)
    dim_scores = {"技术突破": 1.0}
    entry_gate = {
        "mode": "pullback_reconfirmed",
        "secondary_confirmation": {
            "quality_ready": False,
            "technical_breakthrough_candidate_score": 3.0,
        },
    }

    evaluator._apply_pullback_technical_breakthrough_score_lift_v6(dim_scores, entry_gate)

    assert dim_scores["技术突破"] == 1.0
    assert entry_gate["technical_breakthrough_score_lift_applied"] is False
