from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

from src.artifact_registry import sha256_file
from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_BENCHMARK_PLAN_VERSION = "primary_result_benchmark_plan.v1"
PRIMARY_RESULT_BENCHMARK_PLAN_POINTER_VERSION = "primary_result_benchmark_plan_pointer.v1"
SUPPORTED_REVIEW_QUEUE_VERSION = "primary_result_feedback_review_queue.v1"

MODULE_TEST_MAP: dict[str, tuple[str, ...]] = {
    "risk_control": (
        "tests/test_primary_result_rollback_terminal.py",
        "tests/test_primary_result_observation_metrics.py",
        "tests/test_primary_result_failure_attribution.py",
    ),
    "candidate_selection": (
        "tests/test_stock_primary_result_benchmarks.py",
        "tests/test_stock_primary_result_benchmark_report.py",
        "tests/test_stock_primary_result_benchmark_diff.py",
    ),
    "execution_timing": (
        "tests/test_primary_result_execution.py",
        "tests/test_primary_result_observation_metrics.py",
    ),
    "market_regime_filter": (
        "tests/test_model_capability_upgrade.py",
    ),
    "audit_gate": (
        "tests/test_primary_result_audit.py",
    ),
    "learning_dataset": (
        "tests/test_primary_result_learning_feedback.py",
    ),
    "observation_metrics": (
        "tests/test_primary_result_observation_metrics.py",
    ),
    "review_workflow": (
        "tests/test_primary_result_feedback_review_queue.py",
    ),
}

CORE_BENCHMARK_TESTS = (
    "tests/test_stock_primary_result_benchmarks.py",
    "tests/test_stock_primary_result_benchmark_report.py",
    "tests/test_stock_primary_result_benchmark_diff.py",
    "tests/test_run_stock_release_pipeline_fast.py",
    "tests/test_run_stock_release_pipeline_functional.py",
    "tests/test_run_stock_release_pipeline_integration.py",
    "tests/test_run_stock_release_pipeline_e2e.py",
)

EXPECTED_EVIDENCE_ARTIFACTS = (
    "stock_primary_result_benchmark_report.json",
    "stock_primary_result_benchmark_diff.json",
    "release_gates.json",
    "release_evidence_bundle.json",
    "release_pipeline_manifest.json",
    "baseline_promotion_decision",
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _safe_id_part(value: object) -> str:
    text = _normalize_text(value).lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-") or "unknown"


def _normalize_plan_id(value: str) -> str:
    plan_id = _normalize_text(value)
    if not plan_id:
        raise ValueError("plan_id must not be empty")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
    if any(ch not in allowed for ch in plan_id):
        raise ValueError("plan_id must contain only letters, numbers, '-' or '_'")
    return plan_id


def _dedupe(values: list[str] | tuple[str, ...]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _default_plan_id(review_item: dict[str, object]) -> str:
    return _normalize_plan_id(
        "primary-benchmark-plan-"
        f"{_safe_id_part(review_item.get('review_id'))}-"
        f"{_safe_id_part(review_item.get('updated_at') or review_item.get('created_at'))}"
    )


def _affected_modules(review_item: dict[str, object]) -> list[str]:
    changes = review_item.get("recommended_changes")
    if not isinstance(changes, list):
        return []
    modules = [
        _normalize_text(change.get("affected_module"))
        for change in changes
        if isinstance(change, dict) and _normalize_text(change.get("affected_module"))
    ]
    return _dedupe(modules)


def _required_tests(affected_modules: list[str]) -> list[str]:
    tests: list[str] = list(CORE_BENCHMARK_TESTS)
    for module in affected_modules:
        tests.extend(MODULE_TEST_MAP.get(module, ()))
    return _dedupe(tests)


def _execution_batch(review_priority: str) -> str:
    normalized = _normalize_text(review_priority).lower()
    if normalized in {"critical", "high"}:
        return "batch_01_expedite"
    if normalized == "medium":
        return "batch_02_standard"
    return "batch_03_backlog"


class PrimaryResultBenchmarkPlanRegistry:
    def __init__(self, *, plans_dir: str | Path = "artifacts/primary_result_benchmark_plans") -> None:
        self.plans_dir = resolve_project_path(plans_dir)
        self.history_dir = self.plans_dir / "history"
        self.current_path = self.plans_dir / "current.json"
        self.ensure_layout()

    def ensure_layout(self) -> None:
        self.history_dir.mkdir(parents=True, exist_ok=True)
        if not self.current_path.exists():
            _write_json(
                self.current_path,
                {
                    "pointer_version": PRIMARY_RESULT_BENCHMARK_PLAN_POINTER_VERSION,
                    "plan_id": None,
                    "plan_path": None,
                    "review_id": None,
                    "updated_at": None,
                },
            )

    def get_current_pointer(self) -> dict[str, object]:
        return _read_json(self.current_path)

    def get_plan(self, plan_id: str) -> dict[str, object]:
        resolved_plan_id = _normalize_plan_id(plan_id)
        path = self.history_dir / f"{resolved_plan_id}.json"
        if not path.exists():
            raise FileNotFoundError(f"primary result benchmark plan not found: {path}")
        payload = _read_json(path)
        payload["_plan_path"] = str(path)
        return payload

    def create_plan(
        self,
        *,
        review_item_path: str | Path,
        plan_id: str | None = None,
        planned_at: str | None = None,
    ) -> dict[str, object]:
        resolved_review_item_path = resolve_project_path(review_item_path)
        review_item = self._validate_review_item(resolved_review_item_path)
        affected_modules = _affected_modules(review_item)
        resolved_plan_id = _normalize_plan_id(plan_id or _default_plan_id(review_item))
        plan_path = self.history_dir / f"{resolved_plan_id}.json"
        if plan_path.exists():
            raise FileExistsError(f"primary result benchmark plan already exists: {plan_path}")

        plan = {
            "plan_version": PRIMARY_RESULT_BENCHMARK_PLAN_VERSION,
            "plan_id": resolved_plan_id,
            "planned_at": planned_at or _utc_now_iso(),
            "status": "planned",
            "review_id": review_item.get("review_id"),
            "result_id": review_item.get("result_id"),
            "ts_code": review_item.get("ts_code"),
            "stock_name": review_item.get("stock_name"),
            "primary_failure_category": review_item.get("primary_failure_category"),
            "review_priority": review_item.get("review_priority"),
            "priority_reasons": list(review_item.get("priority_reasons", []) or []),
            "affected_modules": affected_modules,
            "recommended_changes": list(review_item.get("recommended_changes", []) or []),
            "required_tests": _required_tests(affected_modules),
            "expected_evidence_artifacts": list(EXPECTED_EVIDENCE_ARTIFACTS),
            "release_gates_required": True,
            "baseline_policy_required": True,
            "requires_baseline_revalidation": True,
            "do_not_auto_apply": True,
            "execution_priority": (
                "expedite"
                if str(review_item.get("review_priority") or "").strip().lower() in {"critical", "high"}
                else "normal"
            ),
            "execution_batch": _execution_batch(str(review_item.get("review_priority") or "")),
            "execution_boundary": (
                "benchmark plan is evidence planning only; it does not apply strategy, risk, execution, or baseline changes"
            ),
            "source_review_item_path": str(resolved_review_item_path),
            "source_review_item_hash": sha256_file(resolved_review_item_path),
        }
        _write_json(plan_path, plan)
        _write_json(
            self.current_path,
            {
                "pointer_version": PRIMARY_RESULT_BENCHMARK_PLAN_POINTER_VERSION,
                "plan_id": resolved_plan_id,
                "plan_path": str(plan_path),
                "review_id": review_item.get("review_id"),
                "updated_at": plan["planned_at"],
            },
        )
        return plan

    def _validate_review_item(self, review_item_path: Path) -> dict[str, object]:
        if not review_item_path.exists():
            raise FileNotFoundError(f"feedback review item missing: {review_item_path}")
        review_item = _read_json(review_item_path)
        if review_item.get("queue_version") != SUPPORTED_REVIEW_QUEUE_VERSION:
            raise ValueError("feedback review item version is invalid")
        if review_item.get("status") != "needs_benchmark":
            raise ValueError("benchmark plan requires review item status needs_benchmark")
        if review_item.get("requires_baseline_revalidation") is not True:
            raise ValueError("benchmark plan requires baseline revalidation")
        if review_item.get("do_not_auto_apply") is not True:
            raise ValueError("feedback review item must keep do_not_auto_apply=true")
        if str(review_item.get("review_priority") or "").strip().lower() not in {"critical", "high", "medium", "low", "none"}:
            raise ValueError("feedback review item review_priority is invalid")
        changes = review_item.get("recommended_changes")
        if not isinstance(changes, list) or not changes:
            raise ValueError("feedback review item must include recommended_changes")
        for change in changes:
            if not isinstance(change, dict):
                raise ValueError("feedback review item recommended_changes must contain objects")
            if change.get("do_not_auto_apply") is not True:
                raise ValueError("feedback review item change must keep do_not_auto_apply=true")
        return review_item
