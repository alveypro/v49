from src.stock_primary_result_benchmark_report import BENCHMARK_REGISTRY
from src.stock_primary_result import (
    STOCK_PRIMARY_BOUNDARY_NOTE_COUNT,
    STOCK_PRIMARY_MAX_EXPLANATION_ITEMS,
    build_stock_primary_result_view_model,
    render_stock_primary_result,
)


def _benchmark_samples() -> dict[str, dict[str, object]]:
    return {
        sample_id: entry["sample"]
        for sample_id, entry in BENCHMARK_REGISTRY.items()
    }


def test_stock_primary_result_benchmark_registry_is_stable():
    assert set(BENCHMARK_REGISTRY.keys()) == {
        "B001_NORMAL",
        "B002_EMPTY",
        "B003_DEGRADED",
        "B004_DISABLED_INVALID",
        "B101_NOISY_INPUT",
        "B102_GOVERNANCE_POLLUTED",
        "B103_MAIN_SITE_POLLUTED",
    }
    assert {entry["gate_level"] for entry in BENCHMARK_REGISTRY.values()} == {"blocking", "observation"}


def test_stock_primary_result_benchmarks_hold_under_canonical_single_track():
    for sample_id, sample in _benchmark_samples().items():
        vm = build_stock_primary_result_view_model(sample)
        html_text = render_stock_primary_result(vm)
        assert sample_id in BENCHMARK_REGISTRY
        assert vm.conclusion.primary_result_label
        assert len(vm.explanation.history_items) <= STOCK_PRIMARY_MAX_EXPLANATION_ITEMS
        assert len(
            (
                vm.boundary.scope_note,
                vm.boundary.reference_note,
                vm.boundary.governance_boundary_note,
            )
        ) == STOCK_PRIMARY_BOUNDARY_NOTE_COUNT
        assert "当前推进状态" not in html_text
        assert "当前主要阻断" not in html_text
        assert "当前治理备注" not in html_text
        assert "Airivo 是统一母平台" not in html_text
        assert "面向未来" not in html_text
