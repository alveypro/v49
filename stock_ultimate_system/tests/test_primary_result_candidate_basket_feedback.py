import json
from pathlib import Path

from src.primary_result_candidate_basket_feedback import build_primary_result_candidate_basket_feedback


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _observation(*, status: str = "failed", basket_return: float = -0.02, excess_return: float = -0.03, max_drawdown: float = -0.11) -> dict:
    return {
        "observation_version": "primary_result_candidate_basket_observation.v1",
        "status": status,
        "basket_id": "basket-001",
        "observation_window": {"started_at": "2026-04-15", "ended_at": "2026-04-20", "status": "closed"},
        "metrics": {
            "basket_return": basket_return,
            "benchmark_return": 0.01,
            "excess_return": excess_return,
            "max_drawdown": max_drawdown,
            "item_total": 3,
        },
        "completion_criteria": {
            "min_success_return": 0.0,
            "max_drawdown_floor": -0.08,
            "passed": status == "completed",
        },
    }


def test_candidate_basket_feedback_flags_failed_observation_for_review(tmp_path):
    observation_path = _write_json(tmp_path / "observation.json", _observation())
    summary_path = _write_json(
        tmp_path / "summary.json",
        {"entry_total": 4, "success_rate": 0.25, "average_excess_return": -0.02, "worst_max_drawdown": -0.11},
    )

    exit_code, payload = build_primary_result_candidate_basket_feedback(
        observation_path=observation_path,
        performance_summary_path=summary_path,
        output_path=tmp_path / "feedback.json",
    )

    assert exit_code == 0
    assert payload["feedback_level"] == "tighten"
    assert payload["attribution_required"] is True
    assert payload["change_total"] >= 2
    change_ids = {change["change_id"] for change in payload["recommended_changes"]}
    assert "review_selection_factors" in change_ids
    assert "tighten_risk_overlay" in change_ids
    assert payload["window_label"] == "5D"


def test_candidate_basket_feedback_reinforces_strong_success_without_changes(tmp_path):
    observation_path = _write_json(
        tmp_path / "observation.json",
        _observation(status="completed", basket_return=0.05, excess_return=0.04, max_drawdown=-0.02),
    )
    summary_path = _write_json(
        tmp_path / "summary.json",
        {"entry_total": 5, "success_rate": 0.8, "average_excess_return": 0.015, "worst_max_drawdown": -0.05},
    )

    exit_code, payload = build_primary_result_candidate_basket_feedback(
        observation_path=observation_path,
        performance_summary_path=summary_path,
    )

    assert exit_code == 0
    assert payload["feedback_level"] == "reinforce"
    assert payload["attribution_required"] is False
    assert payload["change_total"] == 0


def test_candidate_basket_feedback_rejects_pytest_derived_latest_sources(tmp_path):
    observation_path = _write_json(tmp_path / "observation.json", _observation())
    summary_path = _write_json(tmp_path / "summary.json", {"entry_total": 1})

    try:
        build_primary_result_candidate_basket_feedback(
            observation_path=Path("/private/var/folders/x/pytest-of-mac/pytest-12/test_case/observation.json"),
            performance_summary_path=summary_path,
            enforce_production_source_guard=True,
        )
    except ValueError as exc:
        assert "source_observation_path" in str(exc)
    else:
        raise AssertionError("expected ValueError for pytest-derived observation path")
