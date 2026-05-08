import json

from src.dashboard_support import STOCK_PRIMARY_RESULT_RENDER_CONTRACT_VERSION
from src.stock_primary_result_benchmark_report import (
    BENCHMARK_REGISTRY,
    BENCHMARK_REPORT_TEMPLATE_FIELDS,
    BENCHMARK_VERSION,
    BENCHMARK_REGISTRY_VERSION,
    RUNTIME_OBSERVABILITY_VERSION,
    build_stock_primary_result_benchmark_report,
    render_stock_primary_result_benchmark_report_json,
    render_stock_primary_result_benchmark_report_markdown,
    write_stock_primary_result_benchmark_report_artifacts,
)


def test_stock_primary_result_benchmark_report_shape_is_stable():
    report = build_stock_primary_result_benchmark_report()
    report_dict = report.as_dict()

    assert tuple(report_dict.keys()) == BENCHMARK_REPORT_TEMPLATE_FIELDS
    assert report_dict["benchmark_version"] == BENCHMARK_VERSION
    assert report_dict["registry_version"] == BENCHMARK_REGISTRY_VERSION
    assert report_dict["render_contract_version"] == STOCK_PRIMARY_RESULT_RENDER_CONTRACT_VERSION
    assert report_dict["runtime_observability_version"] == RUNTIME_OBSERVABILITY_VERSION


def test_stock_primary_result_benchmark_report_matches_registry_counts():
    report = build_stock_primary_result_benchmark_report()

    assert report.sample_total == len(BENCHMARK_REGISTRY)
    assert report.core_sample_total == sum(
        1 for entry in BENCHMARK_REGISTRY.values() if entry["sample_tier"] == "core"
    )
    assert report.extended_sample_total == sum(
        1 for entry in BENCHMARK_REGISTRY.values() if entry["sample_tier"] == "extended"
    )
    assert report.blocking_total == sum(
        1 for entry in BENCHMARK_REGISTRY.values() if entry["gate_level"] == "blocking"
    )
    assert report.observation_total == sum(
        1 for entry in BENCHMARK_REGISTRY.values() if entry["gate_level"] == "observation"
    )
    assert report.has_blocking_regression is False


def test_stock_primary_result_benchmark_report_can_mark_blocking_regression():
    report = build_stock_primary_result_benchmark_report(blocking_regression_ids=("B001_NORMAL",))

    assert report.has_blocking_regression is True


def test_stock_primary_result_benchmark_report_serialization_is_stable(tmp_path):
    report = build_stock_primary_result_benchmark_report()
    json_text = render_stock_primary_result_benchmark_report_json(report)
    markdown_text = render_stock_primary_result_benchmark_report_markdown(report)
    json_path, markdown_path = write_stock_primary_result_benchmark_report_artifacts(tmp_path, report)

    payload = json.loads(json_text)
    assert tuple(payload.keys()) == BENCHMARK_REPORT_TEMPLATE_FIELDS
    assert payload["benchmark_version"] == BENCHMARK_VERSION
    assert payload["registry_version"] == BENCHMARK_REGISTRY_VERSION
    assert payload["sample_total"] == len(BENCHMARK_REGISTRY)
    assert payload["blocking_total"] >= 1
    assert payload["observation_total"] >= 1

    assert "# Stock Primary Result Benchmark Report" in markdown_text
    assert "- benchmark_version: `v1`" in markdown_text
    assert "- registry_version: `v1`" in markdown_text
    assert "- render_contract_version: `v1`" in markdown_text
    assert "- runtime_observability_version: `v1`" in markdown_text

    assert json_path.name == "stock_primary_result_benchmark_report.json"
    assert markdown_path.name == "stock_primary_result_benchmark_report.md"
    assert json.loads(json_path.read_text(encoding="utf-8"))["benchmark_version"] == BENCHMARK_VERSION
    assert "# Stock Primary Result Benchmark Report" in markdown_path.read_text(encoding="utf-8")
