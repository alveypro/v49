import json
from datetime import datetime, timezone
from pathlib import Path

from scripts.repair_primary_result_chain import repair_primary_result_chain
from src.current_result_pointer import CurrentResultPointerStore
from src.result_registry import ResultRegistry
from src.run_registry import RunRegistry


def _write_primary_result_artifacts_without_pointer(tmp_path: Path) -> Path:
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True)
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n"
        "000001.SZ,平安银行,银行,strong_buy,low,155\n",
        encoding="utf-8",
    )
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"000001.SZ"}]}', encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t12_rollback_drill_latest.json").write_text('{"triggered": false}', encoding="utf-8")
    return exp_dir


def test_repair_primary_result_chain_bootstraps_empty_pointer_and_passes_guard(tmp_path):
    exp_dir = _write_primary_result_artifacts_without_pointer(tmp_path)

    exit_code, payload = repair_primary_result_chain(
        exp_dir=exp_dir,
        artifacts_dir=tmp_path / "artifacts",
        candidate_index=0,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        bootstrap_run_id="bootstrap_run_000001_sz",
        bootstrap_lifecycle_id="bootstrap-lifecycle-000001-sz",
        lifecycle_run_id="primary_run_000001_sz",
        lifecycle_id="primary-lifecycle-000001-sz",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )

    assert exit_code == 0
    assert payload["status"] == "passed"
    assert payload["guard_payload"]["ok"] is True
    assert payload["lifecycle_payload"]["status"] == "passed"
    assert [step["step"] for step in payload["bootstrap_steps"]] == ["audit", "execution", "rollback", "observation"]

    artifacts_dir = tmp_path / "artifacts"
    current_pointer = json.loads((artifacts_dir / "current_result_pointer" / "current.json").read_text(encoding="utf-8"))
    current_result_record = json.loads((artifacts_dir / "result_registry" / "current.json").read_text(encoding="utf-8"))
    current_run_record = json.loads((artifacts_dir / "run_registry" / "current.json").read_text(encoding="utf-8"))
    lifecycle_current = json.loads((artifacts_dir / "primary_result_lifecycle" / "current.json").read_text(encoding="utf-8"))

    assert current_pointer["result_id"] == "primary:000001.SZ"
    assert current_pointer["run_id"] == "primary_run_000001_sz"
    assert current_result_record["result_id"] == "primary:000001.SZ"
    assert current_run_record["run_id"] == "primary_run_000001_sz"
    assert lifecycle_current["lifecycle_id"] == "primary-lifecycle-000001-sz"
    assert (artifacts_dir / "stock_entry_guard_latest.json").exists()


def test_repair_primary_result_chain_seeds_from_latest_candidate_when_old_pointer_exists(tmp_path):
    exp_dir = _write_primary_result_artifacts_without_pointer(tmp_path)
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n"
        "300395.SZ,菲利华,玻璃,strong_buy,medium,148.26\n",
        encoding="utf-8",
    )
    (exp_dir / "buylist_latest.json").write_text(
        json.dumps({"items": [{"ts_code": "300395.SZ"}]}, ensure_ascii=False),
        encoding="utf-8",
    )
    artifacts_dir = tmp_path / "artifacts"
    RunRegistry(runs_dir=artifacts_dir / "run_registry").register(
        run_id="old-run",
        run_type="primary_result_lifecycle",
        producer="test",
        config_hash="cfg",
        data_snapshot_id="data",
        code_revision="rev",
        make_current=True,
    )
    ResultRegistry(results_dir=artifacts_dir / "result_registry").register(
        record_id="old-result",
        result_id="primary:300750.SZ",
        run_id="old-run",
        ts_code="300750.SZ",
        stock_name="宁德时代",
        lifecycle_stage="L4",
        artifact_ids=["old-run:audit"],
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=artifacts_dir / "current_result_pointer").point_to(
        pointer_snapshot_id="old-pointer",
        result_id="primary:300750.SZ",
        run_id="old-run",
        lifecycle_id="old-lifecycle",
        artifact_ids=["old-run:audit"],
        as_of_date="2026-04-29",
    )

    exit_code, payload = repair_primary_result_chain(
        exp_dir=exp_dir,
        artifacts_dir=artifacts_dir,
        candidate_index=0,
        max_source_age_hours=100000,
        observation_window_start="2026-05-08T01:30:00Z",
        bootstrap_run_id="bootstrap_run_300395_sz",
        bootstrap_lifecycle_id="bootstrap-lifecycle-300395-sz",
        lifecycle_run_id="primary_run_300395_sz",
        lifecycle_id="primary-lifecycle-300395-sz",
        now=datetime(2026, 5, 7, 17, 30, 20, tzinfo=timezone.utc),
    )

    assert exit_code == 0
    assert payload["seed_payload"]["ts_code"] == "300395.SZ"
    current_pointer = json.loads((artifacts_dir / "current_result_pointer" / "current.json").read_text(encoding="utf-8"))
    assert current_pointer["result_id"] == "primary:300395.SZ"
    assert current_pointer["lifecycle_id"] == "primary-lifecycle-300395-sz"
