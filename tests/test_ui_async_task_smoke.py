from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


_P = Path(__file__).resolve().parents[1] / "tools" / "ui_async_task_smoke.py"
_SPEC = importlib.util.spec_from_file_location("ui_async_task_smoke_for_test", str(_P))
assert _SPEC is not None and _SPEC.loader is not None
ui_async_task_smoke = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(ui_async_task_smoke)


class _FakeMetric:
    def __init__(self, label: str, value: str) -> None:
        self.label = label
        self.value = value


class _FakeText:
    def __init__(self, value: str) -> None:
        self.value = value


class _FakeAppTest:
    def __init__(self) -> None:
        self.metric = [_FakeMetric("状态", "success"), _FakeMetric("结果数", "17 条")]
        self.markdown = [_FakeText("### 后台扫描任务（v5.0启动确认）")]
        self.text = []
        self.caption = []
        self.success = [_FakeText("后台扫描完成，返回 17 条")]
        self.info = []
        self.warning = []
        self.error = []


def test_assert_task_panel_visible_accepts_success_panel():
    summary = ui_async_task_smoke._assert_task_panel_visible(_FakeAppTest(), "v5")
    assert summary["status"] == "success"
    assert summary["result_count"] == 17


def test_assert_task_panel_visible_rejects_non_positive_results():
    at = _FakeAppTest()
    at.metric[1] = _FakeMetric("结果数", "0 条")
    with pytest.raises(RuntimeError):
        ui_async_task_smoke._assert_task_panel_visible(at, "v5")
