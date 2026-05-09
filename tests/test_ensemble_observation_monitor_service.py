from __future__ import annotations

import json
from pathlib import Path

from openclaw.services.ensemble_observation_monitor_service import build_ensemble_observation_monitor


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _observation_record() -> dict:
    return {
        "record_version": "ensemble_observation_pool_record.v1",
        "record_id": "dec_apply",
        "status": "applied",
        "strategy": "ensemble_core",
        "candidate": "hard_event_alpha_candidate",
        "from_pool": "research_only",
        "to_pool": "observation",
        "source_promotion_decision_id": "dec_ready",
        "formal_pool_eligible": False,
        "formal_ranking_allowed": False,
    }


def _benchmark_payload() -> dict:
    windows = []
    for idx, regime in enumerate(["risk_on", "neutral", "risk_on"]):
        windows.append(
            {
                "as_of_date": f"2026030{idx + 1}",
                "market_regime_label": regime,
                "execution_cost_replay": {
                    "research_only": True,
                    "not_for_production": True,
                    "trade_replay": [{"traded": True, "capacity_usage": 0.02}],
                },
                "formal_pool_benchmark": {"available": True, "avg_return_pct": -0.2},
                "shadow_portfolio": {
                    "allocator_controls": {
                        "market_regime_label": regime,
                        "target_gross_exposure": 0.45 if regime == "neutral" else 0.75,
                        "pre_throttle_invested_weight": 0.9,
                    },
                    "industry_exposure": {"银行": 0.2},
                },
            }
        )
    return {
        "candidate": "hard_event_alpha_candidate",
        "research_only": True,
        "benchmark": {
            "research_only": True,
            "passed": True,
            "blocking_reasons": [],
            "after_cost_excess_return": 1.0,
            "hit_rate": 0.8,
            "turnover": 0.5,
            "industry_concentration": 0.2,
            "capacity_utilization": 0.03,
            "max_drawdown": -4.0,
            "regime_split": {
                "neutral": {"window_count": 2, "avg_after_cost_excess_return": 0.4, "hit_rate": 0.5},
                "risk_on": {"window_count": 3, "avg_after_cost_excess_return": 1.2, "hit_rate": 1.0},
            },
            "risk_contribution": {
                "source_strategy_weight_share": {"v4": 0.4, "v8": 0.3},
                "industry_weight_share": {"银行": 0.2, "元器件": 0.1},
            },
        },
        "windows": windows,
    }


def test_ensemble_observation_monitor_passes_observation_but_blocks_formal(tmp_path: Path):
    benchmark = _write_json(tmp_path / "benchmark.json", _benchmark_payload())
    audit = _write_json(
        tmp_path / "stage_audit.json",
        {"passed": True, "top_strategies": ["v4", "v5", "v9"], "observation_pool": [{"strategy": "ensemble_core"}]},
    )

    payload = build_ensemble_observation_monitor(
        observation_records=[_observation_record()],
        shadow_benchmark_artifact_path=str(benchmark),
        stage_audit_artifact_path=str(audit),
        output_dir=str(tmp_path / "monitor"),
    )

    assert payload["observation_monitor_passed"] is True
    assert payload["current_pool"] == "observation"
    assert payload["formal_candidate_allowed"] is False
    assert payload["formal_ranking_allowed"] is False
    assert "allocator_throttle_present_do_not_treat_as_alpha_improvement" in payload["warning_reasons"]
    assert "missing_risk_off_observation_window" in payload["warning_reasons"]
    assert Path(payload["artifacts"]["json"]).exists()


def test_ensemble_observation_monitor_blocks_metric_drift_and_formal_top(tmp_path: Path):
    source = _benchmark_payload()
    source["benchmark"]["turnover"] = 0.9
    source["benchmark"]["hit_rate"] = 0.4
    benchmark = _write_json(tmp_path / "benchmark.json", source)
    audit = _write_json(
        tmp_path / "stage_audit.json",
        {"passed": True, "top_strategies": ["ensemble_core"], "observation_pool": [{"strategy": "ensemble_core"}]},
    )

    payload = build_ensemble_observation_monitor(
        observation_records=[_observation_record()],
        shadow_benchmark_artifact_path=str(benchmark),
        stage_audit_artifact_path=str(audit),
    )

    assert payload["observation_monitor_passed"] is False
    assert "hit_rate_threshold_failed:0.4:>=:0.6" in payload["blocking_reasons"]
    assert "turnover_threshold_failed:0.9:<=:0.75" in payload["blocking_reasons"]
    assert "ensemble_core_unexpectedly_in_formal_top" in payload["blocking_reasons"]


def test_ensemble_observation_monitor_blocks_failed_risk_off_regime(tmp_path: Path):
    source = _benchmark_payload()
    source["benchmark"]["regime_split"]["risk_off"] = {
        "window_count": 2,
        "avg_after_cost_excess_return": -1.6,
        "hit_rate": 0.0,
    }
    benchmark = _write_json(tmp_path / "benchmark.json", source)

    payload = build_ensemble_observation_monitor(
        observation_records=[_observation_record()],
        shadow_benchmark_artifact_path=str(benchmark),
    )

    assert payload["observation_monitor_passed"] is False
    assert payload["regime_reviews"]["risk_off"]["passed"] is False
    assert "regime_monitor_failed:risk_off" in payload["blocking_reasons"]
