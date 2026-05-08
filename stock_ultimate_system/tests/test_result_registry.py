from src.result_registry import RESULT_REGISTRY_ENTRY_VERSION, ResultRegistry


def test_result_registry_registers_history_and_current_pointer(tmp_path):
    registry = ResultRegistry(results_dir=tmp_path / "artifacts" / "result_registry")

    entry = registry.register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        stock_name="平安银行",
        lifecycle_stage="L2",
        artifact_ids=["artifact:a", "artifact:b"],
        registered_at="2026-04-28T09:00:00+00:00",
        make_current=True,
    )

    assert entry["schema_version"] == RESULT_REGISTRY_ENTRY_VERSION
    assert entry["result_id"] == "primary:000001.SZ"
    assert registry.get_current_pointer()["record_id"] == "result-record-001"
    records = registry.list_records(result_id="primary:000001.SZ")
    assert len(records) == 1
    assert records[0]["artifact_ids"] == ["artifact:a", "artifact:b"]
    assert registry.get_latest_record_for_result("primary:000001.SZ")["record_id"] == "result-record-001"


def test_result_registry_rejects_missing_lifecycle_stage(tmp_path):
    registry = ResultRegistry(results_dir=tmp_path / "artifacts" / "result_registry")

    try:
        registry.register(
            result_id="primary:000001.SZ",
            run_id="run-001",
            ts_code="000001.SZ",
            lifecycle_stage="",
            artifact_ids=["artifact:a"],
        )
    except ValueError as exc:
        assert "lifecycle_stage is required" in str(exc)
    else:
        raise AssertionError("expected ValueError for missing lifecycle_stage")
