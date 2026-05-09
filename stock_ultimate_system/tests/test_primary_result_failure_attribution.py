import json
import subprocess
import sys
from pathlib import Path

import pytest

from src.primary_result_failure_attribution import (
    build_primary_result_failure_attribution,
    build_primary_result_failure_attribution_from_paths,
)


def _observation(
    *,
    status: str = "failed",
    observed_return: float = -0.03,
    benchmark_return: float = 0.01,
    excess_return: float = -0.04,
    max_drawdown: float = -0.09,
    criteria_passed: bool = False,
    risk_level: str = "medium",
    signal_level: str = "high",
) -> dict[str, object]:
    return {
        "observation_version": "primary_result_observation.v1",
        "generated_at": "2026-04-20T08:00:00Z",
        "observation_status": status,
        "requested_observation_status": "completed",
        "observation_reason": "local observation window completed",
        "result_id": "primary:000001.SZ",
        "ts_code": "000001.SZ",
        "stock_name": "平安银行",
        "checks": [],
        "observation_window": {
            "started_at": "2026-04-15T09:30:00Z",
            "ended_at": "2026-04-20T15:00:00Z",
            "status": "closed",
        },
        "observation_metrics": {
            "observed_return": observed_return,
            "benchmark_return": benchmark_return,
            "excess_return": excess_return,
            "max_drawdown": max_drawdown,
        },
        "completion_criteria": {
            "min_success_return": 0.0,
            "max_drawdown_floor": -0.08,
            "passed": criteria_passed,
        },
        "primary_result_payload": {
            "risk_level": risk_level,
            "signal_level": signal_level,
        },
    }


def test_failure_attribution_classifies_risk_and_benchmark_underperformance():
    payload = build_primary_result_failure_attribution(_observation())

    categories = {item["category"] for item in payload["contributing_categories"]}
    assert payload["attribution_version"] == "primary_result_failure_attribution.v1"
    assert payload["outcome"] == "failed"
    assert payload["attribution_required"] is True
    assert payload["primary_failure_category"] == "risk_control_failure"
    assert "risk_control_failure" in categories
    assert "benchmark_underperformance" in categories
    assert "negative_absolute_return" in categories
    assert any("drawdown" in action for action in payload["recommended_actions"])


def test_failure_attribution_separates_market_drag_from_selection_failure():
    payload = build_primary_result_failure_attribution(
        _observation(observed_return=-0.01, benchmark_return=-0.04, excess_return=0.03, max_drawdown=-0.02)
    )

    categories = {item["category"] for item in payload["contributing_categories"]}
    assert payload["primary_failure_category"] == "negative_absolute_return"
    assert "market_drag" in categories
    assert "benchmark_underperformance" not in categories


def test_failure_attribution_marks_weak_success_when_excess_return_is_negative():
    payload = build_primary_result_failure_attribution(
        _observation(
            status="completed",
            observed_return=0.02,
            benchmark_return=0.05,
            excess_return=-0.03,
            max_drawdown=-0.02,
            criteria_passed=True,
        )
    )

    categories = {item["category"] for item in payload["contributing_categories"]}
    assert payload["outcome"] == "success"
    assert payload["attribution_required"] is True
    assert payload["primary_failure_category"] == "benchmark_underperformance"
    assert "weak_success" in categories


def test_failure_attribution_not_required_for_strong_success():
    payload = build_primary_result_failure_attribution(
        _observation(
            status="completed",
            observed_return=0.08,
            benchmark_return=0.03,
            excess_return=0.05,
            max_drawdown=-0.02,
            criteria_passed=True,
        )
    )

    assert payload["outcome"] == "success"
    assert payload["attribution_required"] is False
    assert payload["primary_failure_category"] is None
    assert payload["contributing_categories"] == []


def test_failure_attribution_rejects_open_observation():
    with pytest.raises(ValueError, match="closed observation"):
        build_primary_result_failure_attribution(_observation(status="observing"))


def test_failure_attribution_from_paths_links_ledger_entry_and_hash(tmp_path):
    observation_path = tmp_path / "observation.json"
    observation_path.write_text(json.dumps(_observation(), ensure_ascii=False), encoding="utf-8")
    ledger_path = tmp_path / "ledger.jsonl"
    ledger_path.write_text(
        json.dumps(
            {
                "entry_id": "entry-1",
                "result_id": "primary:000001.SZ",
                "window_ended_at": "2026-04-20T15:00:00Z",
                "outcome": "failed",
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    payload = build_primary_result_failure_attribution_from_paths(
        observation_path=observation_path,
        ledger_jsonl_path=ledger_path,
    )

    assert payload["ledger_entry"]["entry_id"] == "entry-1"
    assert payload["source_observation_path"] == str(observation_path)
    assert payload["source_observation_hash"]


def test_run_primary_result_failure_attribution_cli(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "run_primary_result_failure_attribution.py"
    observation_path = tmp_path / "observation.json"
    output_path = tmp_path / "attribution.json"
    observation_path.write_text(json.dumps(_observation(), ensure_ascii=False), encoding="utf-8")

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--observation-json",
            str(observation_path),
            "--output",
            str(output_path),
            "--json",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["status"] == "passed"
    assert payload["primary_failure_category"] == "risk_control_failure"
    assert json.loads(output_path.read_text(encoding="utf-8"))["attribution_required"] is True
