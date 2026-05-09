import json
import subprocess
import sys
from pathlib import Path

import pytest

from src.primary_result_learning_feedback import (
    build_primary_result_learning_feedback,
    build_primary_result_learning_feedback_from_path,
)


def _attribution(*, required: bool = True, categories: list[str] | None = None) -> dict[str, object]:
    resolved_categories = categories or ["risk_control_failure", "benchmark_underperformance", "negative_absolute_return"]
    return {
        "attribution_version": "primary_result_failure_attribution.v1",
        "generated_at": "2026-04-20T08:00:00Z",
        "status": "passed",
        "result_id": "primary:000001.SZ",
        "ts_code": "000001.SZ",
        "stock_name": "平安银行",
        "observation_status": "failed" if required else "completed",
        "outcome": "failed" if required else "success",
        "attribution_required": required,
        "primary_failure_category": resolved_categories[0] if required and resolved_categories else None,
        "contributing_categories": [
            {"category": category, "severity": "high", "evidence": {}}
            for category in (resolved_categories if required else [])
        ],
        "recommended_actions": [],
        "metrics": {},
        "source_context": {},
    }


def test_learning_feedback_maps_failure_categories_to_governed_changes():
    payload = build_primary_result_learning_feedback(_attribution())

    change_ids = {change["change_id"] for change in payload["recommended_changes"]}
    assert payload["feedback_version"] == "primary_result_learning_feedback.v1"
    assert payload["change_total"] == 3
    assert payload["max_severity"] == "high"
    assert payload["review_priority"] == "high"
    assert "risk_control_failure_requires_risk_model_review" in payload["priority_reasons"]
    assert payload["requires_baseline_revalidation"] is True
    assert payload["do_not_auto_apply"] is True
    assert "tighten_drawdown_controls" in change_ids
    assert "review_selection_factors" in change_ids
    assert "review_entry_timing" in change_ids
    assert all(change["do_not_auto_apply"] is True for change in payload["recommended_changes"])


def test_learning_feedback_handles_data_quality_without_baseline_revalidation():
    payload = build_primary_result_learning_feedback(_attribution(categories=["data_quality_failure"]))

    assert payload["max_severity"] == "critical"
    assert payload["review_priority"] == "critical"
    assert payload["priority_reasons"] == ["data_quality_failure_requires_dataset_repair"]
    assert payload["requires_baseline_revalidation"] is False
    assert payload["recommended_changes"][0]["affected_module"] == "observation_metrics"


def test_learning_feedback_strong_success_has_no_changes_but_keeps_guardrail():
    payload = build_primary_result_learning_feedback(_attribution(required=False))

    assert payload["change_total"] == 0
    assert payload["max_severity"] == "none"
    assert payload["review_priority"] == "none"
    assert payload["priority_reasons"] == []
    assert payload["requires_baseline_revalidation"] is False
    assert payload["do_not_auto_apply"] is True


def test_learning_feedback_rejects_invalid_attribution_version():
    bad = _attribution()
    bad["attribution_version"] = "wrong"

    with pytest.raises(ValueError, match="version"):
        build_primary_result_learning_feedback(bad)


def test_learning_feedback_from_path_links_source_hash(tmp_path):
    attribution_path = tmp_path / "attribution.json"
    attribution_path.write_text(json.dumps(_attribution(), ensure_ascii=False), encoding="utf-8")

    payload = build_primary_result_learning_feedback_from_path(attribution_path=attribution_path)

    assert payload["source_attribution_path"] == str(attribution_path)
    assert payload["source_attribution_hash"]


def test_run_primary_result_learning_feedback_cli(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "run_primary_result_learning_feedback.py"
    attribution_path = tmp_path / "attribution.json"
    output_path = tmp_path / "feedback.json"
    attribution_path.write_text(json.dumps(_attribution(), ensure_ascii=False), encoding="utf-8")

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--attribution-json",
            str(attribution_path),
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
    assert payload["requires_baseline_revalidation"] is True
    assert payload["review_priority"] == "high"
    assert json.loads(output_path.read_text(encoding="utf-8"))["do_not_auto_apply"] is True
