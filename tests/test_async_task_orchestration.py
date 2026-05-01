from __future__ import annotations

import json

import pandas as pd

from openclaw.runtime import async_task_orchestration
from openclaw.runtime.async_task_orchestration import (
    launch_async_scan_process,
    start_async_backtest_job,
    start_async_scan_task,
    run_async_backtest_worker_main,
    run_async_scan_job,
    run_async_scan_worker_main,
)
from openclaw.runtime.v49_handlers import execute_offline_scan_strategy


def test_launch_async_scan_process_sets_worker_env(monkeypatch, tmp_path):
    calls = {}

    class FakePopen:
        def __init__(self, cmd, cwd, env, stdout, stderr, stdin, start_new_session):
            calls["cmd"] = cmd
            calls["cwd"] = cwd
            calls["env"] = env
            calls["start_new_session"] = start_new_session
            self.pid = 4321

    monkeypatch.setattr(async_task_orchestration.subprocess, "Popen", FakePopen)
    monkeypatch.setattr(async_task_orchestration.subprocess, "DEVNULL", object())
    app_root = str(tmp_path)

    out = launch_async_scan_process(
        app_root=app_root,
        run_id="run_scan_v9_demo",
        strategy="v9",
        params={"limit": 3},
        score_col="score",
        async_scan_log_paths=lambda run_id: (str(tmp_path / f"{run_id}.out"), str(tmp_path / f"{run_id}.err")),
        build_async_scan_env=lambda strategy, params: {"OFFLINE_ONLY": strategy, "OFFLINE_STOCK_LIMIT": params["limit"]},
        python_executable="/usr/bin/python3",
    )

    assert out["pid"] == 4321
    assert calls["cmd"] == ["/usr/bin/python3", str(tmp_path / "v49_app.py")]
    assert calls["cwd"] == app_root
    assert calls["start_new_session"] is True
    assert calls["env"]["OPENCLAW_ASYNC_SCAN_WORKER"] == "1"
    assert calls["env"]["OPENCLAW_ASYNC_SCAN_RUN_ID"] == "run_scan_v9_demo"
    assert calls["env"]["OPENCLAW_ASYNC_SCAN_STRATEGY"] == "v9"
    assert calls["env"]["OPENCLAW_ASYNC_SCAN_SCORE_COL"] == "score"
    assert calls["env"]["OFFLINE_ONLY"] == "v9"
    assert calls["env"]["OFFLINE_STOCK_LIMIT"] == "3"


def test_run_async_scan_job_writes_result_and_updates_state(tmp_path):
    states = {}
    updates = []
    current_run_ids = []
    recorded = []

    def update(run_id: str, **kwargs):
        updates.append((run_id, kwargs))
        states.setdefault(run_id, {}).update(kwargs)

    def run_scan(strategy, params, env_overrides):
        assert strategy == "v9"
        assert env_overrides == {"OFFLINE_ONLY": "v9"}
        return pd.DataFrame([{"ts_code": "000001.SZ", "score": 88}]), {"source": "unit"}

    run_async_scan_job(
        run_id="run_scan_v9_demo",
        strategy="v9",
        params={"limit": 1},
        score_col="score",
        result_dir=str(tmp_path),
        cancelled_error="cancelled",
        get_async_scan_task=lambda run_id: {},
        update_async_scan_task=update,
        build_async_scan_env=lambda strategy, params: {"OFFLINE_ONLY": strategy},
        run_scan=run_scan,
        now_text=lambda: "2026-05-01 08:30:00",
        now_ts=lambda: 1777595400.0,
        set_current_run_id=lambda run_id: current_run_ids.append(run_id),
        record_signal_chain=lambda **kwargs: recorded.append(kwargs),
    )

    assert states["run_scan_v9_demo"]["status"] == "success"
    assert states["run_scan_v9_demo"]["row_count"] == 1
    assert current_run_ids == ["run_scan_v9_demo", ""]
    result_csv = tmp_path / states["run_scan_v9_demo"]["result_csv"].split("/")[-1]
    meta_json = tmp_path / states["run_scan_v9_demo"]["meta_json"].split("/")[-1]
    assert result_csv.exists()
    assert meta_json.exists()
    meta = json.loads(meta_json.read_text(encoding="utf-8"))
    assert meta["run_id"] == "run_scan_v9_demo"
    assert meta["meta"] == {"source": "unit"}
    assert updates[0][1]["status"] == "running"
    assert recorded[0]["run_id"] == "run_scan_v9_demo"
    assert recorded[0]["row_count"] == 1


def test_start_async_scan_task_uses_blueprint_run_id_factory():
    tasks = {}

    ok, msg, run_id = start_async_scan_task(
        strategy="v9",
        params={"limit": 3},
        score_col="score",
        async_scan_tasks=tasks,
        async_scan_lock=type("Lock", (), {"__enter__": lambda self: self, "__exit__": lambda self, *args: None})(),
        cleanup_async_scan_tasks=lambda: None,
        scan_params_fingerprint=lambda params: repr(sorted((params or {}).items())),
        now_ts=lambda: 1777595400.0,
        now_text=lambda: "2026-05-01 08:30:00",
        async_scan_state_path=lambda rid: f"{rid}.state.json",
        persist_async_scan_task=lambda rid: None,
        launch_async_scan_process=lambda rid, strategy, params, score_col: {"pid": 1, "stdout_log": "out", "stderr_log": "err"},
        merge_async_scan_task=lambda rid, base=None, **updates: tasks[rid].update(updates) or tasks[rid],
        run_id_factory=lambda strategy: f"run_scan_{strategy}_20260501_083000_demo",
    )

    assert ok, msg
    assert run_id == "run_scan_v9_20260501_083000_demo"
    assert tasks[run_id]["run_id"] == run_id
    assert tasks[run_id]["state_json"] == f"{run_id}.state.json"


def test_run_async_scan_worker_main_delegates_from_env(monkeypatch):
    updates = []
    called = {}
    current_run_ids = []

    monkeypatch.setenv("OPENCLAW_ASYNC_SCAN_RUN_ID", "run_scan_v9_demo")
    monkeypatch.setenv("OPENCLAW_ASYNC_SCAN_STRATEGY", "v9")
    monkeypatch.setenv("OPENCLAW_ASYNC_SCAN_SCORE_COL", "score")

    def run_job(run_id, strategy, params, score_col):
        called.update({"run_id": run_id, "strategy": strategy, "params": params, "score_col": score_col})

    rc = run_async_scan_worker_main(
        load_async_scan_state=lambda run_id: {"params": {"limit": 2}},
        update_async_scan_task=lambda run_id, **kwargs: updates.append((run_id, kwargs)),
        run_async_scan_job=run_job,
        now_text=lambda: "2026-05-01 08:30:00",
        now_ts=lambda: 1777595400.0,
        process_id=lambda: 1234,
        set_current_run_id=lambda run_id: current_run_ids.append(run_id),
    )

    assert rc == 0
    assert called == {"run_id": "run_scan_v9_demo", "strategy": "v9", "params": {"limit": 2}, "score_col": "score"}
    assert updates[0][0] == "run_scan_v9_demo"
    assert updates[0][1]["pid"] == 1234
    assert current_run_ids == ["run_scan_v9_demo", ""]


def test_run_async_scan_worker_main_rejects_missing_env(monkeypatch):
    monkeypatch.delenv("OPENCLAW_ASYNC_SCAN_RUN_ID", raising=False)
    monkeypatch.delenv("OPENCLAW_ASYNC_SCAN_STRATEGY", raising=False)
    messages = []

    rc = run_async_scan_worker_main(
        load_async_scan_state=lambda run_id: {},
        update_async_scan_task=lambda run_id, **kwargs: None,
        run_async_scan_job=lambda run_id, strategy, params, score_col: None,
        now_text=lambda: "",
        now_ts=lambda: 0.0,
        stderr_write=messages.append,
    )

    assert rc == 2
    assert messages == ["missing async scan worker env\n"]


def test_execute_offline_scan_strategy_dispatches_registered_handler():
    calls = []

    class Env:
        def __enter__(self):
            calls.append("enter")

        def __exit__(self, exc_type, exc, tb):
            calls.append("exit")

    out_df, meta = execute_offline_scan_strategy(
        strategy="v9",
        env_overrides={"OFFLINE_ONLY": "v9"},
        analyzer_factory=lambda: "analyzer",
        scan_handlers={"v9": lambda analyzer: (pd.DataFrame([{"ts_code": "000001.SZ"}]), {"analyzer": analyzer})},
        v7_scan_handler=lambda: (pd.DataFrame(), {}),
        temp_environ=lambda env: Env(),
    )

    assert calls == ["enter", "exit"]
    assert out_df is not None and len(out_df) == 1
    assert meta == {"analyzer": "analyzer"}


def test_start_async_backtest_job_uses_blueprint_run_id_factory():
    jobs = {}

    ok, msg, run_id = start_async_backtest_job(
        job_kind="single",
        payload={"strategy": "v9"},
        async_backtest_jobs=jobs,
        async_backtest_lock=type("Lock", (), {"__enter__": lambda self: self, "__exit__": lambda self, *args: None})(),
        now_ts=lambda: 1777595400.0,
        now_text=lambda: "2026-05-01 08:30:00",
        merge_async_backtest_job=lambda rid, base=None, **updates: jobs.setdefault(rid, dict(base or {})) | updates,
        launch_async_backtest_process=lambda rid, job_kind, payload: {"pid": 1, "stdout_log": "out", "stderr_log": "err"},
        run_id_factory=lambda kind: f"run_backtest_{kind}_20260501_083000_demo",
    )

    assert ok, msg
    assert run_id == "run_backtest_single_20260501_083000_demo"


def test_run_async_backtest_worker_main_records_success(monkeypatch):
    updates = []
    recorded = []
    monkeypatch.setenv("OPENCLAW_ASYNC_BACKTEST_RUN_ID", "run_backtest_single_demo")
    monkeypatch.setenv("OPENCLAW_ASYNC_BACKTEST_JOB_KIND", "single")

    rc = run_async_backtest_worker_main(
        load_async_backtest_state=lambda run_id: {"payload": {"strategy": "v9", "sample_size": 10}},
        merge_async_backtest_job=lambda run_id, base=None, **kwargs: updates.append((run_id, kwargs)) or {**dict(base or {}), **kwargs},
        run_single_backtest_worker=lambda payload: {"success": True, "result": {"stats": {"win_rate": 60}}},
        run_comparison_backtest_worker=lambda payload: {"success": False},
        now_text=lambda: "2026-05-01 08:30:00",
        now_ts=lambda: 1777595400.0,
        process_id=lambda: 1234,
        record_backtest_chain=lambda **kwargs: recorded.append(kwargs),
    )

    assert rc == 0
    assert updates[0][1]["status"] == "running"
    assert updates[1][1]["status"] == "success"
    assert recorded[0]["run_id"] == "run_backtest_single_demo"
    assert recorded[0]["payload"] == {"strategy": "v9", "sample_size": 10}


def test_run_async_backtest_worker_main_records_structured_failure(monkeypatch):
    updates = []
    recorded = []
    monkeypatch.setenv("OPENCLAW_ASYNC_BACKTEST_RUN_ID", "run_backtest_single_failed")
    monkeypatch.setenv("OPENCLAW_ASYNC_BACKTEST_JOB_KIND", "single")

    rc = run_async_backtest_worker_main(
        load_async_backtest_state=lambda run_id: {"payload": {"strategy": "v9", "sample_size": 10}},
        merge_async_backtest_job=lambda run_id, base=None, **kwargs: updates.append((run_id, kwargs)) or {**dict(base or {}), **kwargs},
        run_single_backtest_worker=lambda payload: {"success": False, "error": "无法获取历史数据"},
        run_comparison_backtest_worker=lambda payload: {"success": False},
        now_text=lambda: "2026-05-01 08:30:00",
        now_ts=lambda: 1777595400.0,
        process_id=lambda: 1234,
        record_backtest_chain=lambda **kwargs: recorded.append(kwargs),
    )

    assert rc == 1
    assert updates[1][1]["status"] == "failed"
    assert updates[1][1]["error"] == "无法获取历史数据"
    assert recorded[0]["run_id"] == "run_backtest_single_failed"
    assert recorded[0]["result"] == {"success": False, "error": "无法获取历史数据"}


def test_run_async_backtest_worker_main_records_crash_failure(monkeypatch):
    recorded = []
    monkeypatch.setenv("OPENCLAW_ASYNC_BACKTEST_RUN_ID", "run_backtest_single_crashed")
    monkeypatch.setenv("OPENCLAW_ASYNC_BACKTEST_JOB_KIND", "single")

    def boom(payload):
        raise RuntimeError("boom")

    rc = run_async_backtest_worker_main(
        load_async_backtest_state=lambda run_id: {"payload": {"strategy": "v9"}},
        merge_async_backtest_job=lambda run_id, base=None, **kwargs: {**dict(base or {}), **kwargs},
        run_single_backtest_worker=boom,
        run_comparison_backtest_worker=lambda payload: {"success": False},
        now_text=lambda: "2026-05-01 08:30:00",
        now_ts=lambda: 1777595400.0,
        process_id=lambda: 1234,
        record_backtest_chain=lambda **kwargs: recorded.append(kwargs),
    )

    assert rc == 1
    assert recorded[0]["run_id"] == "run_backtest_single_crashed"
    assert recorded[0]["result"]["success"] is False
    assert recorded[0]["result"]["error"] == "后台回测 worker crashed"
