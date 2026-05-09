import json
import subprocess
import sys
from pathlib import Path

from src.current_result_pointer import CurrentResultPointerStore
from src.primary_result_candidate_handoff_gate import build_primary_result_candidate_handoff_gate
from src.result_registry import ResultRegistry
from src.run_registry import RunRegistry


def _write_candidate(exp_dir: Path, ts_code: str = "000001.SZ") -> None:
    exp_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n"
        f"{ts_code},测试股,测试,strong_buy,medium,155\n",
        encoding="utf-8",
    )
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")


def _write_lifecycle_pointer(lifecycles_dir: Path, ts_code: str = "000001.SZ") -> None:
    lifecycles_dir.mkdir(parents=True, exist_ok=True)
    (lifecycles_dir / "current.json").write_text(
        json.dumps(
            {
                "pointer_version": "primary_result_lifecycle_current_pointer.v1",
                "lifecycle_id": "lifecycle-001",
                "snapshot_path": str(lifecycles_dir / "history" / "lifecycle-001.json"),
                "result_id": f"primary:{ts_code}",
                "ts_code": ts_code,
                "updated_at": "2026-04-17T00:00:00Z",
                "rollback_of_lifecycle_id": None,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _write_current_primary_pointer(tmp_path: Path, ts_code: str) -> None:
    artifacts_dir = tmp_path / "artifacts"
    RunRegistry(runs_dir=artifacts_dir / "run_registry").register(
        run_id="run-001",
        run_type="daily_research",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
    )
    ResultRegistry(results_dir=artifacts_dir / "result_registry").register(
        record_id="result-record-001",
        result_id=f"primary:{ts_code}",
        run_id="run-001",
        ts_code=ts_code,
        stock_name="测试股",
        lifecycle_stage="L2",
        artifact_ids=["artifact:a"],
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=artifacts_dir / "current_result_pointer").point_to(
        pointer_snapshot_id="pointer-001",
        result_id=f"primary:{ts_code}",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=["artifact:a"],
        as_of_date="2026-04-28",
    )


def test_candidate_handoff_gate_passes_when_lifecycle_matches_current_candidate(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    lifecycles = tmp_path / "artifacts" / "primary_result_lifecycle"
    _write_candidate(exp_dir, "000001.SZ")
    _write_current_primary_pointer(tmp_path, "000001.SZ")
    _write_lifecycle_pointer(lifecycles, "000001.SZ")

    exit_code, payload = build_primary_result_candidate_handoff_gate(
        exp_dir=exp_dir,
        lifecycles_dir=lifecycles,
        output_path=tmp_path / "handoff.json",
    )

    assert exit_code == 0
    assert payload["gate_version"] == "primary_result_candidate_handoff_gate.v1"
    assert payload["decision"] == "aligned"
    assert payload["status"] == "passed"
    assert (tmp_path / "handoff.json").exists()


def test_candidate_handoff_gate_requires_handoff_when_candidate_changes(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    lifecycles = tmp_path / "artifacts" / "primary_result_lifecycle"
    _write_candidate(exp_dir, "000002.SZ")
    _write_current_primary_pointer(tmp_path, "000002.SZ")
    _write_lifecycle_pointer(lifecycles, "000001.SZ")

    exit_code, payload = build_primary_result_candidate_handoff_gate(exp_dir=exp_dir, lifecycles_dir=lifecycles)

    assert exit_code == 1
    assert payload["decision"] == "handoff_required"
    assert "lifecycle current pointer must match the current top candidate before closure" in payload["blocking_reasons"]
    assert "run primary result lifecycle for current candidate 000002.SZ" in payload["next_actions"]


def test_candidate_handoff_gate_detects_stale_artifacts(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    lifecycles = tmp_path / "artifacts" / "primary_result_lifecycle"
    _write_candidate(exp_dir, "000002.SZ")
    _write_current_primary_pointer(tmp_path, "000002.SZ")
    _write_lifecycle_pointer(lifecycles, "000002.SZ")
    (exp_dir / "primary_result_observation_latest.json").write_text(
        json.dumps({"result_id": "primary:000001.SZ", "ts_code": "000001.SZ"}, ensure_ascii=False),
        encoding="utf-8",
    )

    exit_code, payload = build_primary_result_candidate_handoff_gate(exp_dir=exp_dir, lifecycles_dir=lifecycles)

    assert exit_code == 1
    assert payload["decision"] == "handoff_required"
    assert payload["stale_artifacts"][0]["step"] == "observation"


def test_candidate_handoff_gate_cli_outputs_json(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script = project_root / "scripts" / "build_primary_result_candidate_handoff_gate.py"
    exp_dir = tmp_path / "data" / "experiments"
    lifecycles = tmp_path / "artifacts" / "primary_result_lifecycle"
    _write_candidate(exp_dir, "000001.SZ")
    _write_current_primary_pointer(tmp_path, "000001.SZ")
    _write_lifecycle_pointer(lifecycles, "000001.SZ")

    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--exp-dir",
            str(exp_dir),
            "--lifecycles-dir",
            str(lifecycles),
            "--output",
            str(tmp_path / "handoff.json"),
            "--json",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["decision"] == "aligned"
    assert json.loads((tmp_path / "handoff.json").read_text(encoding="utf-8"))["status"] == "passed"


def test_candidate_handoff_gate_blocks_pytest_derived_lifecycle_snapshot_path(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    lifecycles = tmp_path / "artifacts" / "primary_result_lifecycle"
    _write_candidate(exp_dir, "000001.SZ")
    _write_current_primary_pointer(tmp_path, "000001.SZ")
    lifecycles.mkdir(parents=True, exist_ok=True)
    (lifecycles / "current.json").write_text(
        json.dumps(
            {
                "pointer_version": "primary_result_lifecycle_current_pointer.v1",
                "lifecycle_id": "lifecycle-001",
                "snapshot_path": "/private/var/folders/x/pytest-of-mac/pytest-9/test_case/history/lifecycle-001.json",
                "result_id": "primary:000001.SZ",
                "ts_code": "000001.SZ",
                "updated_at": "2026-04-17T00:00:00Z",
                "rollback_of_lifecycle_id": None,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    exit_code, payload = build_primary_result_candidate_handoff_gate(
        exp_dir=exp_dir,
        lifecycles_dir=lifecycles,
        enforce_production_source_guard=True,
    )

    assert exit_code == 1
    assert any(check["name"] == "lifecycle_snapshot_path_is_not_temp" and check["passed"] is False for check in payload["checks"])


def test_candidate_handoff_gate_blocks_when_current_result_pointer_missing(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    lifecycles = tmp_path / "artifacts" / "primary_result_lifecycle"
    _write_candidate(exp_dir, "000001.SZ")
    _write_lifecycle_pointer(lifecycles, "000001.SZ")

    exit_code, payload = build_primary_result_candidate_handoff_gate(exp_dir=exp_dir, lifecycles_dir=lifecycles)

    assert exit_code == 1
    assert payload["fact_source_role"] == "gate_only"
    assert any(
        check["name"] == "current_candidate_is_pointer_governed" and check["passed"] is False
        for check in payload["checks"]
    )
