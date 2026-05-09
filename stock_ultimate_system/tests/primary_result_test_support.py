from __future__ import annotations

from pathlib import Path

from src.current_result_pointer import CurrentResultPointerStore
from src.result_registry import ResultRegistry
from src.run_registry import RunRegistry


def seed_current_primary_pointer(
    tmp_path: Path,
    *,
    ts_code: str,
    stock_name: str,
    lifecycle_stage: str = "L2",
    result_id: str | None = None,
    run_id: str = "run-001",
    lifecycle_id: str = "lifecycle-001",
    artifact_ids: tuple[str, ...] = ("artifact:a",),
    as_of_date: str = "2026-04-28",
) -> None:
    artifacts_dir = tmp_path / "artifacts"
    resolved_result_id = result_id or f"primary:{ts_code}"

    RunRegistry(runs_dir=artifacts_dir / "run_registry").register(
        run_id=run_id,
        run_type="daily_research",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
    )
    ResultRegistry(results_dir=artifacts_dir / "result_registry").register(
        record_id=f"result-record-{ts_code.replace('.', '-').lower()}-{lifecycle_stage.lower()}",
        result_id=resolved_result_id,
        run_id=run_id,
        ts_code=ts_code,
        stock_name=stock_name,
        lifecycle_stage=lifecycle_stage,
        artifact_ids=list(artifact_ids),
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=artifacts_dir / "current_result_pointer").point_to(
        pointer_snapshot_id=f"pointer-{ts_code.replace('.', '-').lower()}-{lifecycle_stage.lower()}",
        result_id=resolved_result_id,
        run_id=run_id,
        lifecycle_id=lifecycle_id,
        artifact_ids=list(artifact_ids),
        as_of_date=as_of_date,
    )
