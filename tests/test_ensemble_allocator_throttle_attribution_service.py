from __future__ import annotations

import json
from pathlib import Path

from openclaw.services.ensemble_allocator_throttle_attribution_service import (
    build_ensemble_allocator_throttle_attribution,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _payload(*, excess: float, hit_rate: float, drawdown: float, turnover: float, net_return: float) -> dict:
    return {
        "candidate": "hard_event_alpha_candidate",
        "benchmark": {
            "passed": True,
            "after_cost_excess_return": excess,
            "hit_rate": hit_rate,
            "max_drawdown": drawdown,
            "turnover": turnover,
            "regime_split": {
                "risk_off": {"avg_after_cost_excess_return": excess, "hit_rate": hit_rate, "window_count": 1}
            },
        },
        "windows": [
            {
                "as_of_date": "20260304",
                "market_regime_label": "risk_off",
                "execution_cost_replay": {"net_return": net_return},
                "formal_pool_benchmark": {"avg_return_pct": 1.0},
                "shadow_portfolio": {"turnover_estimate": turnover, "cash_weight": 1.0 - turnover},
            }
        ],
    }


def test_allocator_throttle_attribution_separates_risk_control_from_alpha(tmp_path: Path):
    throttled = _write_json(tmp_path / "throttled.json", _payload(excess=0.5, hit_rate=0.7, drawdown=-10, turnover=0.6, net_return=1.5))
    unthrottled = _write_json(tmp_path / "unthrottled.json", _payload(excess=-0.4, hit_rate=0.4, drawdown=-21, turnover=0.9, net_return=0.6))

    payload = build_ensemble_allocator_throttle_attribution(
        throttled_benchmark_artifact_path=str(throttled),
        unthrottled_benchmark_artifact_path=str(unthrottled),
        output_dir=str(tmp_path / "out"),
    )

    assert payload["formal_candidate_allowed"] is False
    assert payload["summary"]["unthrottled_after_cost_excess_return"] == -0.4
    assert payload["summary"]["allocator_throttle_excess_delta"] == 0.9
    assert payload["conclusion"] == "positive_observation_result_is_allocator_risk_control_dependent_not_standalone_alpha"
    assert "unthrottled_alpha_after_cost_excess_not_positive:-0.4" in payload["warning_reasons"]
    assert Path(payload["artifacts"]["json"]).exists()


def test_allocator_throttle_attribution_blocks_missing_overlap(tmp_path: Path):
    throttled = _write_json(tmp_path / "throttled.json", {"benchmark": {"passed": True}, "windows": []})
    unthrottled = _write_json(tmp_path / "unthrottled.json", {"benchmark": {"passed": True}, "windows": []})

    payload = build_ensemble_allocator_throttle_attribution(
        throttled_benchmark_artifact_path=str(throttled),
        unthrottled_benchmark_artifact_path=str(unthrottled),
    )

    assert "missing_overlapping_attribution_windows" in payload["blocking_reasons"]
