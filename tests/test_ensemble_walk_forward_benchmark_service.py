from __future__ import annotations

from openclaw.services.ensemble_walk_forward_benchmark_service import build_ensemble_walk_forward_shadow_benchmark


def test_ensemble_walk_forward_benchmark_blocks_without_valid_windows():
    review = build_ensemble_walk_forward_shadow_benchmark(
        [
            {
                "execution_cost_replay": {
                    "research_only": True,
                    "not_for_production": True,
                    "blocking_reasons": ["missing_shadow_weights"],
                    "trade_replay": [],
                },
                "formal_pool_benchmark": {"available": True, "avg_return_pct": 1.0},
            }
        ]
    )

    assert review["research_only"] is True
    assert review["not_for_production"] is True
    assert review["passed"] is False
    assert "insufficient_walk_forward_windows:1/5" in review["blocking_reasons"]
    assert "window_0:execution_blocked:missing_shadow_weights" in review["blocking_reasons"]
    assert "window_0:missing_after_cost_trade_replay" in review["blocking_reasons"]
    assert review["after_cost_excess_return"] is None


def test_ensemble_walk_forward_benchmark_computes_metrics_only_after_minimum_valid_windows():
    rows = []
    for idx in range(5):
        rows.append(
            {
                "market_regime_label": "risk_on" if idx % 2 == 0 else "neutral",
                "shadow_portfolio": {
                    "industry_exposure": {"电子": 0.2 + idx * 0.01},
                    "shadow_weights": [
                        {"weight": 0.1, "source_strategy": "v4", "industry": "电子"},
                        {"weight": 0.1, "source_strategy": "v8", "industry": "医药"},
                    ],
                },
                "execution_cost_replay": {
                    "research_only": True,
                    "not_for_production": True,
                    "blocking_reasons": [],
                    "net_return": 2.0 + idx,
                    "turnover": 0.2,
                    "trade_replay": [
                        {
                            "traded": True,
                            "capacity_usage": 0.04,
                        }
                    ],
                },
                "formal_pool_benchmark": {
                    "available": True,
                    "avg_return_pct": 1.0,
                },
            }
        )

    review = build_ensemble_walk_forward_shadow_benchmark(rows)

    assert review["passed"] is True
    assert review["valid_window_count"] == 5
    assert review["after_cost_excess_return"] == 3.0
    assert review["hit_rate"] == 1.0
    assert review["turnover"] == 0.2
    assert review["capacity_utilization"] == 0.04
    assert review["industry_concentration"] == 0.24
    assert review["regime_split"]["risk_on"]["window_count"] == 3
    assert review["risk_contribution"]["source_strategy_weight_share"] == {"v4": 0.5, "v8": 0.5}
    assert review["blocking_reasons"] == []
