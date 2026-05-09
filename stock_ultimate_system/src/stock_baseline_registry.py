from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from src.utils.project_paths import resolve_project_path


SNAPSHOT_VERSION = "v1"
CURRENT_POINTER_VERSION = "v1"
REQUIRED_BENCHMARK_REPORT_FIELDS = {
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
}
REQUIRED_MANIFEST_FIELDS = {
    "run_id",
    "benchmark_report_hash",
    "benchmark_diff_hash",
    "release_gates_hash",
}
REQUIRED_RELEASE_DECISION_FIELDS = {
    "decision_version",
    "decision",
    "baseline_promotion_allowed",
    "source_checklist_hash",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _normalize_baseline_id(value: str) -> str:
    baseline_id = str(value).strip()
    if not baseline_id:
        raise ValueError("baseline_id must not be empty")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
    if any(ch not in allowed for ch in baseline_id):
        raise ValueError("baseline_id must contain only letters, numbers, '-' or '_'")
    return baseline_id


@dataclass(frozen=True)
class StockBaselineSnapshot:
    baseline_id: str
    promoted_at: str
    run_id: str
    source_report_path: str
    source_diff_path: str
    source_gates_path: str
    source_evidence_bundle_path: str
    manifest_path: str
    release_decision_path: str
    release_decision_hash: str
    report_hash: str
    policy_path: str
    snapshot_version: str = SNAPSHOT_VERSION

    def as_dict(self) -> dict[str, object]:
        return {
            "snapshot_version": self.snapshot_version,
            "baseline_id": self.baseline_id,
            "promoted_at": self.promoted_at,
            "run_id": self.run_id,
            "source_report_path": self.source_report_path,
            "source_diff_path": self.source_diff_path,
            "source_gates_path": self.source_gates_path,
            "source_evidence_bundle_path": self.source_evidence_bundle_path,
            "manifest_path": self.manifest_path,
            "release_decision_path": self.release_decision_path,
            "release_decision_hash": self.release_decision_hash,
            "report_hash": self.report_hash,
            "policy_path": self.policy_path,
        }


class StockBaselineRegistry:
    def __init__(
        self,
        *,
        baselines_dir: str | Path = "artifacts/baselines",
        policy_path: str | Path = "STOCK_PRIMARY_RESULT_BASELINE_POLICY.md",
    ) -> None:
        self.baselines_dir = resolve_project_path(baselines_dir)
        self.history_dir = self.baselines_dir / "history"
        self.current_path = self.baselines_dir / "current.json"
        self.policy_path = resolve_project_path(policy_path)
        self.ensure_layout()

    def ensure_layout(self) -> None:
        self.baselines_dir.mkdir(parents=True, exist_ok=True)
        self.history_dir.mkdir(parents=True, exist_ok=True)
        if not self.current_path.exists():
            _write_json(
                self.current_path,
                {
                    "pointer_version": CURRENT_POINTER_VERSION,
                    "baseline_id": None,
                    "snapshot_path": None,
                    "run_id": None,
                    "updated_at": None,
                    "rollback_of_baseline_id": None,
                },
            )

    def get_current_pointer(self) -> dict[str, object]:
        return _read_json(self.current_path)

    def get_current_snapshot(self) -> dict[str, object] | None:
        pointer = self.get_current_pointer()
        snapshot_path = pointer.get("snapshot_path")
        if not snapshot_path:
            return None
        return _read_json(Path(str(snapshot_path)))

    def list_history(self) -> list[dict[str, object]]:
        entries: list[dict[str, object]] = []
        for path in sorted(self.history_dir.glob("*.json")):
            payload = _read_json(path)
            payload["_snapshot_path"] = str(path)
            entries.append(payload)
        entries.sort(key=lambda item: (str(item.get("promoted_at", "")), str(item.get("baseline_id", ""))))
        return entries

    def promote(
        self,
        *,
        benchmark_report_path: str | Path,
        benchmark_diff_path: str | Path,
        release_gates_path: str | Path,
        evidence_bundle_path: str | Path,
        manifest_path: str | Path,
        release_decision_path: str | Path,
        baseline_id: str | None = None,
        promoted_at: str | None = None,
    ) -> dict[str, object]:
        report_path = resolve_project_path(benchmark_report_path)
        diff_path = resolve_project_path(benchmark_diff_path)
        gates_path = resolve_project_path(release_gates_path)
        bundle_path = resolve_project_path(evidence_bundle_path)
        manifest_file = resolve_project_path(manifest_path)
        decision_path = resolve_project_path(release_decision_path)

        self._validate_promotion_inputs(
            report_path=report_path,
            diff_path=diff_path,
            gates_path=gates_path,
            bundle_path=bundle_path,
            manifest_path=manifest_file,
            decision_path=decision_path,
        )

        manifest_payload = _read_json(manifest_file)
        run_id = str(manifest_payload.get("run_id", "") or "").strip()
        if not run_id:
            raise ValueError("manifest missing run_id")

        resolved_baseline_id = _normalize_baseline_id(baseline_id or f"stock-baseline-{run_id}")
        snapshot_path = self.history_dir / f"{resolved_baseline_id}.json"
        if snapshot_path.exists():
            raise FileExistsError(f"baseline snapshot already exists: {snapshot_path}")

        snapshot = StockBaselineSnapshot(
            baseline_id=resolved_baseline_id,
            promoted_at=promoted_at or _utc_now_iso(),
            run_id=run_id,
            source_report_path=str(report_path),
            source_diff_path=str(diff_path),
            source_gates_path=str(gates_path),
            source_evidence_bundle_path=str(bundle_path),
            manifest_path=str(manifest_file),
            release_decision_path=str(decision_path),
            release_decision_hash=_sha256_file(decision_path),
            report_hash=_sha256_file(report_path),
            policy_path=str(self.policy_path),
        )
        _write_json(snapshot_path, snapshot.as_dict())
        self._write_current_pointer(
            baseline_id=resolved_baseline_id,
            snapshot_path=snapshot_path,
            run_id=run_id,
            rollback_of_baseline_id=None,
        )
        return snapshot.as_dict()

    def rollback(self, baseline_id: str, *, rolled_back_at: str | None = None) -> dict[str, object]:
        resolved_baseline_id = _normalize_baseline_id(baseline_id)
        snapshot_path = self.history_dir / f"{resolved_baseline_id}.json"
        if not snapshot_path.exists():
            raise FileNotFoundError(f"baseline snapshot not found: {snapshot_path}")
        snapshot = _read_json(snapshot_path)
        self._write_current_pointer(
            baseline_id=resolved_baseline_id,
            snapshot_path=snapshot_path,
            run_id=str(snapshot.get("run_id", "") or ""),
            rollback_of_baseline_id=resolved_baseline_id,
            updated_at=rolled_back_at or _utc_now_iso(),
        )
        return snapshot

    def _write_current_pointer(
        self,
        *,
        baseline_id: str,
        snapshot_path: Path,
        run_id: str,
        rollback_of_baseline_id: str | None,
        updated_at: str | None = None,
    ) -> None:
        _write_json(
            self.current_path,
            {
                "pointer_version": CURRENT_POINTER_VERSION,
                "baseline_id": baseline_id,
                "snapshot_path": str(snapshot_path),
                "run_id": run_id,
                "updated_at": updated_at or _utc_now_iso(),
                "rollback_of_baseline_id": rollback_of_baseline_id,
            },
        )

    def _validate_promotion_inputs(
        self,
        *,
        report_path: Path,
        diff_path: Path,
        gates_path: Path,
        bundle_path: Path,
        manifest_path: Path,
        decision_path: Path,
    ) -> None:
        for path in (report_path, diff_path, gates_path, bundle_path, manifest_path, decision_path):
            if not path.exists():
                raise FileNotFoundError(f"required artifact missing: {path}")
        if not self.policy_path.exists():
            raise FileNotFoundError(f"baseline policy missing: {self.policy_path}")

        report_payload = _read_json(report_path)
        diff_payload = _read_json(diff_path)
        gates_payload = _read_json(gates_path)
        bundle_payload = _read_json(bundle_path)
        manifest_payload = _read_json(manifest_path)
        decision_payload = _read_json(decision_path)

        missing_report_fields = sorted(REQUIRED_BENCHMARK_REPORT_FIELDS - set(report_payload.keys()))
        if missing_report_fields:
            raise ValueError(f"benchmark report missing required fields: {', '.join(missing_report_fields)}")
        missing_manifest_fields = sorted(REQUIRED_MANIFEST_FIELDS - set(manifest_payload.keys()))
        if missing_manifest_fields:
            raise ValueError(f"manifest missing required fields: {', '.join(missing_manifest_fields)}")
        missing_decision_fields = sorted(REQUIRED_RELEASE_DECISION_FIELDS - set(decision_payload.keys()))
        if missing_decision_fields:
            raise ValueError(f"release decision missing required fields: {', '.join(missing_decision_fields)}")
        if decision_payload.get("decision_version") != "primary_result_release_decision.v1":
            raise ValueError("baseline promotion blocked: release decision version is invalid")
        if str(decision_payload.get("decision", "")).strip().lower() != "approved":
            raise ValueError("baseline promotion blocked: release decision is not approved")
        if decision_payload.get("baseline_promotion_allowed") is not True:
            raise ValueError("baseline promotion blocked: release decision does not allow baseline promotion")
        if decision_payload.get("do_not_auto_apply") is not True:
            raise ValueError("baseline promotion blocked: release decision must keep do_not_auto_apply=true")
        if bool(report_payload.get("has_blocking_regression")):
            raise ValueError("baseline policy violation: benchmark report has blocking regression")
        if bool(diff_payload.get("has_blocking_regression")):
            raise ValueError("baseline promotion blocked: benchmark diff has blocking regression")
        if str(gates_payload.get("status", "")).strip().lower() != "passed":
            raise ValueError("baseline promotion blocked: release gates did not pass")
        if int(gates_payload.get("failed_total", 0) or 0) > 0:
            raise ValueError("baseline promotion blocked: release gates contain failures")

        blocking_summary = bundle_payload.get("blocking_status_summary", {})
        if not isinstance(blocking_summary, dict):
            raise ValueError("release evidence bundle missing blocking_status_summary")
        if bool(blocking_summary.get("has_blocking_regression")):
            raise ValueError("baseline promotion blocked: evidence bundle reports blocking regression")
        if bool(blocking_summary.get("release_gate_failed")):
            raise ValueError("baseline promotion blocked: evidence bundle reports failed release gate")

        manifest_run_id = str(manifest_payload.get("run_id", "") or "").strip()
        bundle_run_id = str(bundle_payload.get("run_id", "") or "").strip()
        if not manifest_run_id:
            raise ValueError("manifest missing run_id")
        if bundle_run_id != manifest_run_id:
            raise ValueError("evidence bundle run_id does not match manifest run_id")
        if str(manifest_payload.get("benchmark_report_hash", "") or "") != _sha256_file(report_path):
            raise ValueError("manifest benchmark_report_hash does not match benchmark report")
        if str(manifest_payload.get("benchmark_diff_hash", "") or "") != _sha256_file(diff_path):
            raise ValueError("manifest benchmark_diff_hash does not match benchmark diff")
        if str(manifest_payload.get("release_gates_hash", "") or "") != _sha256_file(gates_path):
            raise ValueError("manifest release_gates_hash does not match release gates")

        bundle_diff = bundle_payload.get("benchmark_diff", {})
        if isinstance(bundle_diff, dict) and bool(bundle_diff.get("has_blocking_regression")):
            raise ValueError("baseline promotion blocked: evidence bundle diff reports blocking regression")
        bundle_gate_result = bundle_payload.get("release_gate_result", {})
        if isinstance(bundle_gate_result, dict):
            if str(bundle_gate_result.get("status", "")).strip().lower() not in {"", "passed"}:
                raise ValueError("baseline promotion blocked: evidence bundle gate result did not pass")
            if int(bundle_gate_result.get("failed_total", 0) or 0) > 0:
                raise ValueError("baseline promotion blocked: evidence bundle gate result contains failures")
