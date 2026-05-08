from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path


BENCHMARK_DIFF_TEMPLATE_FIELDS = (
    "base_benchmark_version",
    "target_benchmark_version",
    "change_total",
    "has_blocking_regression",
    "blocking_regressions",
    "observation_changes",
    "enhancements",
)

BLOCKING_DIFF_FIELDS = {"has_blocking_regression", "blocking_total"}
ENHANCEMENT_DIFF_FIELDS = {"sample_total", "core_sample_total", "extended_sample_total"}
OBSERVATION_DIFF_FIELDS = {
    "benchmark_version",
    "registry_version",
    "observation_total",
    "render_contract_version",
    "runtime_observability_version",
}


@dataclass(frozen=True)
class StockPrimaryResultBenchmarkDiff:
    base_benchmark_version: str
    target_benchmark_version: str
    change_total: int
    has_blocking_regression: bool
    blocking_regressions: tuple[dict[str, object], ...]
    observation_changes: tuple[dict[str, object], ...]
    enhancements: tuple[dict[str, object], ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "base_benchmark_version": self.base_benchmark_version,
            "target_benchmark_version": self.target_benchmark_version,
            "change_total": self.change_total,
            "has_blocking_regression": self.has_blocking_regression,
            "blocking_regressions": list(self.blocking_regressions),
            "observation_changes": list(self.observation_changes),
            "enhancements": list(self.enhancements),
        }


def _load_report(source: str | Path | dict[str, object]) -> dict[str, object]:
    if isinstance(source, dict):
        return dict(source)
    path = Path(source)
    return json.loads(path.read_text(encoding="utf-8"))


def _classify_change(field: str, before: object, after: object) -> str:
    if field == "has_blocking_regression" and before is False and after is True:
        return "blocking_regression"
    if field == "blocking_total" and isinstance(before, int) and isinstance(after, int) and after > before:
        return "blocking_regression"
    if field in ENHANCEMENT_DIFF_FIELDS and isinstance(before, int) and isinstance(after, int) and after > before:
        return "enhancement"
    if field in BLOCKING_DIFF_FIELDS:
        return "blocking_regression"
    if field in OBSERVATION_DIFF_FIELDS or field in ENHANCEMENT_DIFF_FIELDS:
        return "observation_change"
    return "observation_change"


def build_stock_primary_result_benchmark_diff(
    base_report: str | Path | dict[str, object],
    target_report: str | Path | dict[str, object],
) -> StockPrimaryResultBenchmarkDiff:
    base_payload = _load_report(base_report)
    target_payload = _load_report(target_report)

    blocking_regressions: list[dict[str, object]] = []
    observation_changes: list[dict[str, object]] = []
    enhancements: list[dict[str, object]] = []

    for field in (
        "benchmark_version",
        "registry_version",
        "sample_total",
        "core_sample_total",
        "extended_sample_total",
        "blocking_total",
        "observation_total",
        "render_contract_version",
        "runtime_observability_version",
        "has_blocking_regression",
    ):
        before = base_payload.get(field)
        after = target_payload.get(field)
        if before == after:
            continue
        change = {
            "field": field,
            "before": before,
            "after": after,
            "classification": _classify_change(field, before, after),
        }
        if change["classification"] == "blocking_regression":
            blocking_regressions.append(change)
        elif change["classification"] == "enhancement":
            enhancements.append(change)
        else:
            observation_changes.append(change)

    return StockPrimaryResultBenchmarkDiff(
        base_benchmark_version=str(base_payload.get("benchmark_version", "")),
        target_benchmark_version=str(target_payload.get("benchmark_version", "")),
        change_total=len(blocking_regressions) + len(observation_changes) + len(enhancements),
        has_blocking_regression=bool(blocking_regressions),
        blocking_regressions=tuple(blocking_regressions),
        observation_changes=tuple(observation_changes),
        enhancements=tuple(enhancements),
    )


def render_stock_primary_result_benchmark_diff_json(diff: StockPrimaryResultBenchmarkDiff) -> str:
    return json.dumps(diff.as_dict(), ensure_ascii=False, indent=2)
