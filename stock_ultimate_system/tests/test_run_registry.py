from src.run_registry import RUN_REGISTRY_ENTRY_VERSION, RunRegistry


def test_run_registry_registers_history_and_current_pointer(tmp_path):
    registry = RunRegistry(runs_dir=tmp_path / "artifacts" / "run_registry")

    entry = registry.register(
        run_id="run-001",
        run_type="daily_research",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        status="created",
        created_at="2026-04-28T09:00:00+00:00",
        make_current=True,
    )

    assert entry["schema_version"] == RUN_REGISTRY_ENTRY_VERSION
    assert registry.get_current_pointer()["run_id"] == "run-001"
    assert registry.get_run("run-001")["producer"] == "test"


def test_run_registry_rejects_missing_code_revision(tmp_path):
    registry = RunRegistry(runs_dir=tmp_path / "artifacts" / "run_registry")

    try:
        registry.register(
            run_id="run-001",
            run_type="daily_research",
            producer="test",
            config_hash="cfg-001",
            data_snapshot_id="data-001",
            code_revision="",
        )
    except ValueError as exc:
        assert "code_revision is required" in str(exc)
    else:
        raise AssertionError("expected ValueError for missing code_revision")
