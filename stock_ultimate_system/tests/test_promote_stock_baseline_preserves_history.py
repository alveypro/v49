import json

import pytest

from baseline_test_helpers import write_release_artifacts, write_release_decision
from src.stock_baseline_registry import StockBaselineRegistry


def test_promote_stock_baseline_preserves_history(tmp_path):
    artifacts_dir = tmp_path / "artifacts" / "baselines"
    registry = StockBaselineRegistry(
        baselines_dir=artifacts_dir,
        policy_path=tmp_path / "baseline_policy.md",
    )
    (tmp_path / "baseline_policy.md").write_text("# baseline policy\n", encoding="utf-8")

    release_one = write_release_artifacts(tmp_path / "release1")
    release_two = write_release_artifacts(tmp_path / "release2", run_id="stock-release-test002")
    release_decision_one = write_release_decision(tmp_path / "release1")
    release_decision_two = write_release_decision(tmp_path / "release2")

    first_snapshot = registry.promote(
        baseline_id="stock-baseline-001",
        benchmark_report_path=release_one["report"],
        benchmark_diff_path=release_one["diff"],
        release_gates_path=release_one["gates"],
        evidence_bundle_path=release_one["bundle"],
        manifest_path=release_one["manifest"],
        release_decision_path=release_decision_one,
    )
    with pytest.raises(FileExistsError):
        registry.promote(
            baseline_id="stock-baseline-001",
            benchmark_report_path=release_two["report"],
            benchmark_diff_path=release_two["diff"],
            release_gates_path=release_two["gates"],
            evidence_bundle_path=release_two["bundle"],
            manifest_path=release_two["manifest"],
            release_decision_path=release_decision_two,
        )
    second_snapshot = registry.promote(
        baseline_id="stock-baseline-002",
        benchmark_report_path=release_two["report"],
        benchmark_diff_path=release_two["diff"],
        release_gates_path=release_two["gates"],
        evidence_bundle_path=release_two["bundle"],
        manifest_path=release_two["manifest"],
        release_decision_path=release_decision_two,
    )

    first_snapshot_path = artifacts_dir / "history" / "stock-baseline-001.json"
    second_snapshot_path = artifacts_dir / "history" / "stock-baseline-002.json"
    assert first_snapshot_path.exists()
    assert second_snapshot_path.exists()
    assert json.loads(first_snapshot_path.read_text(encoding="utf-8"))["run_id"] == first_snapshot["run_id"]
    assert json.loads(second_snapshot_path.read_text(encoding="utf-8"))["run_id"] == second_snapshot["run_id"]
    history = registry.list_history()
    assert [item["baseline_id"] for item in history] == ["stock-baseline-001", "stock-baseline-002"]
