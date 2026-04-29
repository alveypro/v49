from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


_P = Path(__file__).resolve().parents[1] / "tools" / "async_scan_smoke.py"
_SPEC = importlib.util.spec_from_file_location("async_scan_smoke_for_test", str(_P))
assert _SPEC is not None and _SPEC.loader is not None
async_scan_smoke = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(async_scan_smoke)


def test_normalize_strategies_rejects_unknown():
    with pytest.raises(ValueError):
        async_scan_smoke._normalize_strategies(["v5", "bad"])


def test_assert_success_requires_running_transition(tmp_path):
    result_csv = tmp_path / "out.csv"
    result_csv.write_text("ts_code,score\n000001.SZ,88\n", encoding="utf-8")
    summary = async_scan_smoke._assert_success(
        strategy="v5",
        run_id="v5_demo",
        state={"status": "success", "row_count": 1, "result_csv": str(result_csv), "ended_at": 123.0},
        observed_statuses=["running", "success"],
    )
    assert summary["row_count"] == 1
    assert Path(summary["result_csv"]).exists()


def test_assert_success_rejects_zero_rows(tmp_path):
    result_csv = tmp_path / "out.csv"
    result_csv.write_text("ts_code,score\n", encoding="utf-8")
    with pytest.raises(RuntimeError):
        async_scan_smoke._assert_success(
            strategy="v9",
            run_id="v9_demo",
            state={"status": "success", "row_count": 0, "result_csv": str(result_csv)},
            observed_statuses=["running", "success"],
        )
