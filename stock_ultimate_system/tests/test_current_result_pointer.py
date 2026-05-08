from src.current_result_pointer import CurrentResultPointerStore


def test_current_result_pointer_store_writes_snapshot_and_current_pointer(tmp_path):
    store = CurrentResultPointerStore(pointer_dir=tmp_path / "artifacts" / "current_result_pointer")

    snapshot = store.point_to(
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=["artifact:a", "artifact:b"],
        as_of_date="2026-04-28",
        source_scope="stock",
        updated_at="2026-04-28T09:00:00+00:00",
        pointer_snapshot_id="pointer-001",
    )

    assert snapshot["pointer_snapshot_id"] == "pointer-001"
    assert snapshot["artifact_ids"] == ["artifact:a", "artifact:b"]
    current = store.get_current_pointer()
    assert current["result_id"] == "primary:000001.SZ"
    assert current["run_id"] == "run-001"
    assert current["snapshot_path"].endswith("pointer-001.json")
    loaded = store.get_snapshot("pointer-001")
    assert loaded["lifecycle_id"] == "lifecycle-001"


def test_current_result_pointer_store_rejects_empty_artifact_ids(tmp_path):
    store = CurrentResultPointerStore(pointer_dir=tmp_path / "artifacts" / "current_result_pointer")

    try:
        store.point_to(
            result_id="primary:000001.SZ",
            run_id="run-001",
            lifecycle_id="lifecycle-001",
            artifact_ids=[],
            as_of_date="2026-04-28",
        )
    except ValueError as exc:
        assert "artifact_ids must not be empty" in str(exc)
    else:
        raise AssertionError("expected ValueError for empty artifact_ids")
