from __future__ import annotations

from contextlib import nullcontext

import pandas as pd

from openclaw.runtime import async_task_ui


class _FakeStreamlit:
    def __init__(self) -> None:
        self.session_state = {"v9_async_task_id": "v9_0430_demo"}
        self.captions: list[str] = []
        self.successes: list[str] = []
        self.downloads: list[str] = []
        self.dataframes: list[pd.DataFrame] = []

    def warning(self, *args, **kwargs):
        return None

    def markdown(self, *args, **kwargs):
        return None

    def columns(self, n):
        return [nullcontext() for _ in range(n)]

    def metric(self, *args, **kwargs):
        return None

    def progress(self, *args, **kwargs):
        return None

    def button(self, *args, **kwargs):
        return False

    def caption(self, text, *args, **kwargs):
        self.captions.append(str(text))

    def info(self, *args, **kwargs):
        return None

    def success(self, text, *args, **kwargs):
        self.successes.append(str(text))

    def download_button(self, label, *args, **kwargs):
        self.downloads.append(str(label))
        return None

    def dataframe(self, df, *args, **kwargs):
        self.dataframes.append(df.copy())
        return None


def test_render_async_scan_status_prefers_recovered_disk_state_over_stale_running(monkeypatch):
    fake_st = _FakeStreamlit()
    monkeypatch.setattr(async_task_ui, "st", fake_st)

    result_df = pd.DataFrame([{"股票代码": "000001.SZ", "综合评分": 91}])
    stale_task = {
        "run_id": "v9_0430_demo",
        "status": "running",
        "progress": 1,
        "row_count": 0,
        "pid": 12345,
        "message": "任务已启动",
        "result_csv": "",
        "meta_json": "",
        "params": {},
        "strategy": "v9",
        "score_col": "综合评分",
    }
    recovered_task = {
        **stale_task,
        "status": "success",
        "stage": "done",
        "progress": 100,
        "row_count": 1,
        "message": "任务结果已从磁盘恢复",
        "result_csv": "/tmp/v9.csv",
        "meta_json": "/tmp/v9.meta.json",
    }

    out = async_task_ui.render_async_scan_status(
        task_key="v9_async_task_id",
        title="v9.0中线均衡版",
        score_col="综合评分",
        get_async_scan_task=lambda run_id: dict(stale_task),
        recover_async_scan_task=lambda run_id: dict(recovered_task),
        is_pid_alive=lambda pid: True,
        update_async_scan_task=lambda *args, **kwargs: None,
        now_ts=lambda: 1714478400.0,
        read_async_scan_df=lambda task: result_df.copy() if str(task.get("result_csv", "")) else None,
        standardize_result_df=lambda df, score_col: df,
        df_to_csv_bytes=lambda df: b"csv",
        set_stock_pool_candidate=lambda *args, **kwargs: None,
    )

    assert out is not None
    assert len(out) == 1
    assert fake_st.successes == ["后台扫描完成，返回 1 条"]
    assert fake_st.downloads == ["下载本次扫描结果（CSV）"]
    assert fake_st.dataframes
