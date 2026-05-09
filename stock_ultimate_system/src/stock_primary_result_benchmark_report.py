from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path

from src.dashboard_support import STOCK_PRIMARY_RESULT_RENDER_CONTRACT_VERSION


BENCHMARK_VERSION = "v1"
BENCHMARK_REGISTRY_VERSION = "v1"
RUNTIME_OBSERVABILITY_VERSION = "v1"
BENCHMARK_REPORT_TEMPLATE_FIELDS = (
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
)

BENCHMARK_REGISTRY: dict[str, dict[str, object]] = {
    "B001_NORMAL": {
        "category": "正常态",
        "sample_tier": "core",
        "gate_level": "blocking",
        "constraints": ("结论层清晰", "解释层受控", "边界层轻量", "无污染项"),
        "blocking_regressions": ("主结论失稳", "顺序失稳"),
        "sample": {
            "ts_code": "000001.SZ",
            "stock_name": "平安银行",
            "result_lifecycle_stage": "L2",
            "result_type": "candidate",
            "candidate_status": "shortlisted",
            "risk_level": "low",
            "data_sync_note": "制度字段已对齐。",
            "source_timestamps": {"buylist_latest.json": "2026-04-12 09:11:19"},
            "history_summary": "候选记录 shortlisted",
        },
    },
    "B002_EMPTY": {
        "category": "空态",
        "sample_tier": "core",
        "gate_level": "blocking",
        "constraints": ("占位词受控", "边界层不膨胀"),
        "blocking_regressions": ("出现自由文本空态",),
        "sample": {},
    },
    "B003_DEGRADED": {
        "category": "降级态",
        "sample_tier": "core",
        "gate_level": "blocking",
        "constraints": ("统一落到降级说明",),
        "blocking_regressions": ("降级文案漂移",),
        "sample": {
            "result_lifecycle_stage": "L2",
            "data_sync_note": "降级显示：历史文件缺失。",
            "history_source_file": "buylist_latest.json",
            "history_source_timestamp": "2026-04-12 09:11:19",
            "history_generation_mode": "degraded",
        },
    },
    "B004_DISABLED_INVALID": {
        "category": "禁用/失效态",
        "sample_tier": "core",
        "gate_level": "blocking",
        "constraints": ("禁用解释稳定", "失效解释稳定"),
        "blocking_regressions": ("失效术语漂移",),
        "sample": {
            "result_lifecycle_stage": "L2",
            "disabled_reason": "当前进入冻结窗口。",
            "terminal_outcome": "expired",
        },
    },
    "B101_NOISY_INPUT": {
        "category": "噪声字段",
        "sample_tier": "extended",
        "gate_level": "observation",
        "constraints": ("噪声字段不污染主结果",),
        "blocking_regressions": tuple(),
        "sample": {
            "ts_code": "000001.SZ",
            "stock_name": "平安银行",
            "result_lifecycle_stage": "L2",
            "result_type": "candidate",
            "candidate_status": "shortlisted",
            "risk_level": "low",
            "data_sync_note": "制度字段已对齐。",
            "legacy_debug_dump": "unused",
            "legacy_banner_copy": "ignored",
        },
    },
    "B102_GOVERNANCE_POLLUTED": {
        "category": "治理污染",
        "sample_tier": "extended",
        "gate_level": "observation",
        "constraints": ("治理摘要词汇不进入/stock",),
        "blocking_regressions": tuple(),
        "sample": {
            "result_lifecycle_stage": "L2",
            "current_progress_status": "不可推进",
            "current_primary_blocker": "治理阻断",
            "current_governance_note": "治理备注",
        },
    },
    "B103_MAIN_SITE_POLLUTED": {
        "category": "主站叙事污染",
        "sample_tier": "extended",
        "gate_level": "observation",
        "constraints": ("主站叙事不进入/stock",),
        "blocking_regressions": tuple(),
        "sample": {
            "result_lifecycle_stage": "L2",
            "platform_story": "Airivo 是统一母平台",
            "brand_slogan": "面向未来",
        },
    },
}


@dataclass(frozen=True)
class StockPrimaryResultBenchmarkReport:
    benchmark_version: str
    registry_version: str
    sample_total: int
    core_sample_total: int
    extended_sample_total: int
    blocking_total: int
    observation_total: int
    render_contract_version: str
    runtime_observability_version: str
    has_blocking_regression: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "benchmark_version": self.benchmark_version,
            "registry_version": self.registry_version,
            "sample_total": self.sample_total,
            "core_sample_total": self.core_sample_total,
            "extended_sample_total": self.extended_sample_total,
            "blocking_total": self.blocking_total,
            "observation_total": self.observation_total,
            "render_contract_version": self.render_contract_version,
            "runtime_observability_version": self.runtime_observability_version,
            "has_blocking_regression": self.has_blocking_regression,
        }


def render_stock_primary_result_benchmark_report_json(
    report: StockPrimaryResultBenchmarkReport,
) -> str:
    return json.dumps(report.as_dict(), ensure_ascii=False, indent=2)


def render_stock_primary_result_benchmark_report_markdown(
    report: StockPrimaryResultBenchmarkReport,
) -> str:
    return "\n".join(
        [
            "# Stock Primary Result Benchmark Report",
            "",
            f"- benchmark_version: `{report.benchmark_version}`",
            f"- registry_version: `{report.registry_version}`",
            f"- render_contract_version: `{report.render_contract_version}`",
            f"- runtime_observability_version: `{report.runtime_observability_version}`",
            f"- sample_total: `{report.sample_total}`",
            f"- core_sample_total: `{report.core_sample_total}`",
            f"- extended_sample_total: `{report.extended_sample_total}`",
            f"- blocking_total: `{report.blocking_total}`",
            f"- observation_total: `{report.observation_total}`",
            f"- has_blocking_regression: `{str(report.has_blocking_regression).lower()}`",
        ]
    )


def write_stock_primary_result_benchmark_report_artifacts(
    output_dir: str | Path,
    report: StockPrimaryResultBenchmarkReport,
) -> tuple[Path, Path]:
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    json_path = destination / "stock_primary_result_benchmark_report.json"
    markdown_path = destination / "stock_primary_result_benchmark_report.md"
    json_path.write_text(
        render_stock_primary_result_benchmark_report_json(report) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(
        render_stock_primary_result_benchmark_report_markdown(report) + "\n",
        encoding="utf-8",
    )
    return json_path, markdown_path


def build_stock_primary_result_benchmark_report(
    registry: dict[str, dict[str, object]] | None = None,
    *,
    blocking_regression_ids: tuple[str, ...] = (),
) -> StockPrimaryResultBenchmarkReport:
    active_registry = registry or BENCHMARK_REGISTRY
    sample_total = len(active_registry)
    core_sample_total = sum(1 for item in active_registry.values() if item.get("sample_tier") == "core")
    blocking_total = sum(1 for item in active_registry.values() if item.get("gate_level") == "blocking")
    extended_sample_total = sample_total - core_sample_total
    observation_total = sample_total - blocking_total
    return StockPrimaryResultBenchmarkReport(
        benchmark_version=BENCHMARK_VERSION,
        registry_version=BENCHMARK_REGISTRY_VERSION,
        sample_total=sample_total,
        core_sample_total=core_sample_total,
        extended_sample_total=extended_sample_total,
        blocking_total=blocking_total,
        observation_total=observation_total,
        render_contract_version=STOCK_PRIMARY_RESULT_RENDER_CONTRACT_VERSION,
        runtime_observability_version=RUNTIME_OBSERVABILITY_VERSION,
        has_blocking_regression=bool(blocking_regression_ids),
    )
