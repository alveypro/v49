from __future__ import annotations

import json
from pathlib import Path

from openclaw.services.ensemble_risk_off_alpha_repair_review_service import build_ensemble_risk_off_alpha_repair_review


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_risk_off_alpha_repair_review_blocks_when_unthrottled_alpha_is_weak(tmp_path: Path):
    monitor = _write_json(
        tmp_path / "monitor.json",
        {"candidate": "hard_event_alpha_candidate", "observation_monitor_passed": True, "formal_candidate_allowed": False},
    )
    attribution = _write_json(
        tmp_path / "attribution.json",
        {
            "candidate": "hard_event_alpha_candidate",
            "formal_candidate_allowed": False,
            "summary": {
                "unthrottled_after_cost_excess_return": 0.05,
                "unthrottled_hit_rate": 0.5,
                "unthrottled_max_drawdown": -17.0,
                "unthrottled_turnover": 0.9,
                "allocator_throttle_excess_delta": 0.8,
            },
            "regime_attribution": {
                "risk_off": {"unthrottled_avg_after_cost_excess_return": -0.1, "unthrottled_hit_rate": 0.5},
                "neutral": {"unthrottled_avg_after_cost_excess_return": -0.4, "unthrottled_hit_rate": 0.25},
                "risk_on": {"unthrottled_avg_after_cost_excess_return": 0.6, "unthrottled_hit_rate": 0.75},
            },
        },
    )

    payload = build_ensemble_risk_off_alpha_repair_review(
        monitor_artifact_path=str(monitor),
        throttle_attribution_artifact_path=str(attribution),
        output_dir=str(tmp_path / "out"),
    )

    assert payload["risk_off_alpha_repair_passed"] is False
    assert payload["observation_monitor_watch_allowed"] is False
    assert payload["formal_candidate_allowed"] is False
    assert "unthrottled_alpha_hit_rate_below_floor:0.5/0.6" in payload["blocking_reasons"]
    assert "unthrottled_regime_excess_not_positive:risk_off:-0.1" in payload["blocking_reasons"]
    assert Path(payload["artifacts"]["json"]).exists()


def test_risk_off_alpha_repair_review_passes_only_when_unthrottled_regimes_are_stable(tmp_path: Path):
    monitor = _write_json(
        tmp_path / "monitor.json",
        {"candidate": "hard_event_alpha_candidate", "observation_monitor_passed": True, "formal_candidate_allowed": False},
    )
    attribution = _write_json(
        tmp_path / "attribution.json",
        {
            "candidate": "hard_event_alpha_candidate",
            "formal_candidate_allowed": False,
            "summary": {
                "unthrottled_after_cost_excess_return": 0.4,
                "unthrottled_hit_rate": 0.7,
                "unthrottled_max_drawdown": -10.0,
                "unthrottled_turnover": 0.7,
            },
            "regime_attribution": {
                "risk_off": {"unthrottled_avg_after_cost_excess_return": 0.1, "unthrottled_hit_rate": 0.5},
                "neutral": {"unthrottled_avg_after_cost_excess_return": 0.2, "unthrottled_hit_rate": 0.5},
                "risk_on": {"unthrottled_avg_after_cost_excess_return": 0.6, "unthrottled_hit_rate": 0.75},
            },
        },
    )

    payload = build_ensemble_risk_off_alpha_repair_review(
        monitor_artifact_path=str(monitor),
        throttle_attribution_artifact_path=str(attribution),
    )

    assert payload["risk_off_alpha_repair_passed"] is True
    assert payload["observation_monitor_watch_allowed"] is True
    assert payload["formal_candidate_allowed"] is False
