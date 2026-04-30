from __future__ import annotations

import os
import threading

import pandas as pd

from openclaw.runtime import async_task_state as task_state


def test_recover_async_scan_task_prefers_result_artifacts_over_running_pid(tmp_path, monkeypatch):
    run_id = "v9_0430_demo"
    result_dir = str(tmp_path)
    result_csv = tmp_path / f"v9_{run_id}_20260430_120000.csv"
    meta_json = tmp_path / f"v9_{run_id}_20260430_120000.meta.json"

    pd.DataFrame([
        {"股票代码": "000001.SZ", "综合评分": 88},
        {"股票代码": "600000.SH", "综合评分": 79},
    ]).to_csv(result_csv, index=False)

    task_state.write_async_scan_state(
        run_id,
        {
            "run_id": run_id,
            "strategy": "v9",
            "status": "running",
            "progress": 1,
            "pid": os.getpid(),
            "row_count": 0,
            "result_csv": str(result_csv),
        },
        result_dir,
    )
    meta_json.write_text(
        (
            "{\n"
            f'  "run_id": "{run_id}",\n'
            '  "strategy": "v9",\n'
            '  "status": "success",\n'
            '  "row_count": 2,\n'
            f'  "result_csv": "{result_csv}"\n'
            "}\n"
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(task_state, "is_pid_alive", lambda pid: True)
    recovered = task_state.recover_async_scan_task(
        run_id,
        result_dir=result_dir,
        async_scan_tasks={},
        async_scan_lock=threading.Lock(),
        now_ts=1714478400.0,
    )

    assert recovered is not None
    assert recovered["status"] == "success"
    assert recovered["stage"] == "done"
    assert recovered["progress"] == 100
    assert recovered["row_count"] == 2
    assert recovered["result_csv"] == str(result_csv)
    assert "恢复" in recovered["message"]
