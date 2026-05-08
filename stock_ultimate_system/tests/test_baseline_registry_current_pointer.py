from baseline_test_helpers import write_release_artifacts, write_release_decision
from src.stock_baseline_registry import StockBaselineRegistry


def test_baseline_registry_current_pointer_switches_and_supports_rollback(tmp_path):
    policy_path = tmp_path / "baseline_policy.md"
    policy_path.write_text("# baseline policy\n", encoding="utf-8")
    registry = StockBaselineRegistry(
        baselines_dir=tmp_path / "artifacts" / "baselines",
        policy_path=policy_path,
    )

    release_one = write_release_artifacts(tmp_path / "release1")
    release_two = write_release_artifacts(tmp_path / "release2", run_id="stock-release-test002")
    release_decision_one = write_release_decision(tmp_path / "release1")
    release_decision_two = write_release_decision(tmp_path / "release2")

    registry.promote(
        baseline_id="stock-baseline-001",
        benchmark_report_path=release_one["report"],
        benchmark_diff_path=release_one["diff"],
        release_gates_path=release_one["gates"],
        evidence_bundle_path=release_one["bundle"],
        manifest_path=release_one["manifest"],
        release_decision_path=release_decision_one,
    )
    registry.promote(
        baseline_id="stock-baseline-002",
        benchmark_report_path=release_two["report"],
        benchmark_diff_path=release_two["diff"],
        release_gates_path=release_two["gates"],
        evidence_bundle_path=release_two["bundle"],
        manifest_path=release_two["manifest"],
        release_decision_path=release_decision_two,
    )

    current_pointer = registry.get_current_pointer()
    assert current_pointer["baseline_id"] == "stock-baseline-002"
    assert current_pointer["run_id"] == "stock-release-test002"

    rolled_back_snapshot = registry.rollback("stock-baseline-001")
    current_pointer_after_rollback = registry.get_current_pointer()
    assert rolled_back_snapshot["baseline_id"] == "stock-baseline-001"
    assert current_pointer_after_rollback["baseline_id"] == "stock-baseline-001"
    assert current_pointer_after_rollback["rollback_of_baseline_id"] == "stock-baseline-001"
    current_snapshot = registry.get_current_snapshot()
    assert current_snapshot is not None
    assert current_snapshot["run_id"] == "stock-release-test001"
