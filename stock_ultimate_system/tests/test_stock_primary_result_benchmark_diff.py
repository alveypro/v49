from src.stock_primary_result_benchmark_diff import (
    BENCHMARK_DIFF_TEMPLATE_FIELDS,
    build_stock_primary_result_benchmark_diff,
)
from src.stock_primary_result_benchmark_report import build_stock_primary_result_benchmark_report


def test_stock_primary_result_benchmark_diff_shape_is_stable():
    base_report = build_stock_primary_result_benchmark_report().as_dict()
    target_report = dict(base_report)
    target_report["observation_total"] = base_report["observation_total"] + 1

    diff = build_stock_primary_result_benchmark_diff(base_report, target_report)

    assert tuple(diff.as_dict().keys()) == BENCHMARK_DIFF_TEMPLATE_FIELDS
    assert diff.change_total == 1
    assert diff.has_blocking_regression is False


def test_stock_primary_result_benchmark_diff_classifies_blocking_and_enhancement():
    base_report = build_stock_primary_result_benchmark_report().as_dict()
    target_report = dict(base_report)
    target_report["has_blocking_regression"] = True
    target_report["blocking_total"] = base_report["blocking_total"] + 1
    target_report["core_sample_total"] = base_report["core_sample_total"] + 1

    diff = build_stock_primary_result_benchmark_diff(base_report, target_report)

    assert diff.has_blocking_regression is True
    assert {item["field"] for item in diff.blocking_regressions} == {
        "blocking_total",
        "has_blocking_regression",
    }
    assert {item["field"] for item in diff.enhancements} == {"core_sample_total"}


def test_stock_primary_result_benchmark_diff_classifies_observation_change():
    base_report = build_stock_primary_result_benchmark_report().as_dict()
    target_report = dict(base_report)
    target_report["runtime_observability_version"] = "v2"

    diff = build_stock_primary_result_benchmark_diff(base_report, target_report)

    assert diff.has_blocking_regression is False
    assert len(diff.observation_changes) == 1
    assert diff.observation_changes[0]["field"] == "runtime_observability_version"
    assert diff.observation_changes[0]["classification"] == "observation_change"
