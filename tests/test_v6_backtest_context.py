from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from openclaw.runtime.v6_backtest_context import (
    PointInTimeV6DataProvider,
    PointInTimeV6LeaderAnalyzer,
    build_v6_runtime_diagnostics,
    v6_point_in_time_context,
)


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "trade_date": "20260101", "pct_chg": 1.0, "vol": 100.0, "industry": "电子", "name": "芯片A"},
            {"ts_code": "000001.SZ", "trade_date": "20260102", "pct_chg": 2.0, "vol": 160.0, "industry": "电子", "name": "芯片A"},
            {"ts_code": "000001.SZ", "trade_date": "20260103", "pct_chg": 3.0, "vol": 220.0, "industry": "电子", "name": "芯片A"},
            {"ts_code": "000001.SZ", "trade_date": "20260104", "pct_chg": -9.0, "vol": 500.0, "industry": "电子", "name": "芯片A"},
            {"ts_code": "000002.SZ", "trade_date": "20260101", "pct_chg": 0.5, "vol": 100.0, "industry": "电子", "name": "芯片B"},
            {"ts_code": "000002.SZ", "trade_date": "20260102", "pct_chg": 0.5, "vol": 100.0, "industry": "电子", "name": "芯片B"},
            {"ts_code": "000002.SZ", "trade_date": "20260103", "pct_chg": 0.5, "vol": 100.0, "industry": "电子", "name": "芯片B"},
            {"ts_code": "000003.SZ", "trade_date": "20260101", "pct_chg": 4.0, "vol": 100.0, "industry": "医药", "name": "医药C"},
            {"ts_code": "000003.SZ", "trade_date": "20260102", "pct_chg": 4.0, "vol": 100.0, "industry": "医药", "name": "医药C"},
            {"ts_code": "000003.SZ", "trade_date": "20260103", "pct_chg": 4.0, "vol": 100.0, "industry": "医药", "name": "医药C"},
        ]
    )


def test_v6_point_in_time_provider_does_not_read_future_rows():
    frame = _frame()
    hist = frame[frame["ts_code"] == "000001.SZ"].iloc[:3].copy()

    provider = PointInTimeV6DataProvider(market_frame=frame, stock_hist=hist, as_of_date="20260103")

    money = provider.get_money_flow("000001.SZ", days=2)
    assert money["net_mf_amount"] > 0
    assert money["consecutive_inflow_days"] == 2
    assert "-9.0" not in str(money)


def test_v6_leader_analyzer_uses_historical_sector_ranking():
    frame = _frame()
    hist = frame[frame["ts_code"] == "000001.SZ"].iloc[:3].copy()
    provider = PointInTimeV6DataProvider(market_frame=frame, stock_hist=hist, as_of_date="20260103")
    leader = PointInTimeV6LeaderAnalyzer(provider)

    out = leader.calculate_leader_score("000001.SZ", "电子", 6.0)

    assert out["sector_rank"] == 1
    assert out["total_stocks"] == 2
    assert out["is_sector_leader"] is True


def test_v6_context_restores_evaluator_dependencies():
    old_provider = object()
    old_leader = object()
    evaluator = SimpleNamespace(data_provider=old_provider, leader_analyzer=old_leader)
    frame = _frame()
    hist = frame[frame["ts_code"] == "000001.SZ"].iloc[:3].copy()

    with v6_point_in_time_context(evaluator, market_frame=frame, stock_hist=hist, as_of_date="20260103"):
        assert evaluator.data_provider is not old_provider
        assert evaluator.leader_analyzer is not old_leader

    assert evaluator.data_provider is old_provider
    assert evaluator.leader_analyzer is old_leader


def test_build_v6_runtime_diagnostics_keeps_strategy_out_of_production_candidate():
    out = build_v6_runtime_diagnostics(
        {
            "threshold": 75.0,
            "evaluated": 3,
            "near_threshold": {"within_2": 1, "within_5": 2, "within_10": 3},
            "score_breakdown": {
                "base_score": {"avg": 60.0},
                "risk_penalty": {"avg": 6.5},
                "dim:短期动量": {"avg": 8.0},
            },
        },
        replay_step=12,
    )

    assert out["point_in_time_context"] is True
    assert out["production_candidate_allowed"] is False
    assert out["short_cycle_noise_review"]["coarse_step"] is True
    assert out["threshold_near_samples"]["within_5"] == 2
    assert out["factor_distribution"]["dim:短期动量"]["avg"] == 8.0
