#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List

from streamlit.testing.v1 import AppTest


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from openclaw.runtime.async_task_state import latest_async_scan_run_id  # noqa: E402


ASYNC_SCAN_RESULT_DIR = ROOT_DIR / "logs" / "openclaw" / "async_scan"
TASK_KEYS = {
    "v5": "v5_async_task_id",
    "v8": "v8_async_task_id",
    "v9": "v9_async_task_id",
}
STRATEGY_LABELS = {
    "v5": "v5.0 趋势版（生产 / 启动确认 / 5-10日）",
    "v8": "v8.0 进阶版（生产 / ATR风控 / 5-15日）",
    "v9": "v9.0 中线均衡版（生产 / 2-6周）",
}
TASK_TITLES = {
    "v5": "后台扫描任务（v5.0启动确认）",
    "v8": "后台扫描任务（v8.0进阶版）",
    "v9": "后台扫描任务（v9.0中线均衡版）",
}


def _normalize_strategies(raw: Iterable[str]) -> List[str]:
    out: List[str] = []
    for item in raw:
        s = str(item or "").strip().lower()
        if not s:
            continue
        if s not in TASK_KEYS:
            raise ValueError(f"unsupported strategy for ui smoke: {s}")
        out.append(s)
    if not out:
        raise ValueError("no strategies selected")
    return out


def _latest_success_run_id(strategy: str) -> str:
    run_id = latest_async_scan_run_id(strategy, str(ASYNC_SCAN_RESULT_DIR), statuses={"success"})
    if not run_id:
        raise RuntimeError(f"no successful async scan run found for {strategy}")
    return run_id


@lru_cache(maxsize=1)
def _app_path() -> str:
    return str(ROOT_DIR / "v49_app.py")


def _collect_metric_map(at: AppTest) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for metric in at.metric:
        out[str(metric.label)] = str(metric.value)
    return out


def _find_strategy_radio(at: AppTest) -> int:
    for idx, radio in enumerate(at.radio):
        options = [str(opt) for opt in radio.options]
        if any("v9.0 中线均衡版" in opt for opt in options):
            return idx
    raise RuntimeError("production strategy radio not found in AppTest page")


def _metric_int(metric_text: str) -> int:
    match = re.search(r"(\d+)", str(metric_text))
    if not match:
        raise ValueError(f"metric has no integer payload: {metric_text}")
    return int(match.group(1))


def _collect_page_texts(at: AppTest) -> List[str]:
    texts: List[str] = []
    for bucket in (at.markdown, at.text, at.caption, at.success, at.info, at.warning, at.error):
        for item in bucket:
            value = getattr(item, "value", "")
            if value is not None:
                texts.append(str(value))
    return texts


def _assert_task_panel_visible(at: AppTest, strategy: str) -> Dict[str, Any]:
    metrics = _collect_metric_map(at)
    page_text = "\n".join(_collect_page_texts(at))
    title = TASK_TITLES[strategy]
    if title not in page_text:
        raise RuntimeError(f"{strategy} task panel title not visible in main entry page")
    status_text = metrics.get("状态", "")
    result_text = metrics.get("结果数", "")
    if status_text != "success":
        raise RuntimeError(f"{strategy} ui status mismatch: expected success got {status_text!r}")
    result_count = _metric_int(result_text)
    if result_count <= 0:
        raise RuntimeError(f"{strategy} ui result count invalid: {result_text!r}")
    if "后台扫描完成，返回" not in page_text:
        raise RuntimeError(f"{strategy} success banner not visible in main entry page")
    return {
        "strategy": strategy,
        "status": status_text,
        "result_count": result_count,
        "task_title": title,
    }


def _run_strategy_page_smoke(strategy: str, run_id: str) -> Dict[str, Any]:
    at = AppTest.from_file(_app_path(), default_timeout=120)
    at.session_state["desired_main_tab"] = "今日决策"
    at.session_state["desired_production_tab"] = "今日决策"
    at.session_state["airivo_root_route"] = "生产后台"
    at.session_state["airivo_production_route"] = "今日决策"
    at.session_state[TASK_KEYS[strategy]] = run_id
    at.run()

    radio_idx = _find_strategy_radio(at)
    at.radio[radio_idx].set_value(STRATEGY_LABELS[strategy]).run()
    summary = _assert_task_panel_visible(at, strategy)
    summary["run_id"] = run_id
    return summary


def run_ui_async_task_smoke(*, strategies: Iterable[str]) -> List[Dict[str, Any]]:
    selected = _normalize_strategies(strategies)
    results: List[Dict[str, Any]] = []
    for strategy in selected:
        run_id = _latest_success_run_id(strategy)
        print(f"[ui-async-task-smoke] inspect strategy={strategy} run_id={run_id}")
        summary = _run_strategy_page_smoke(strategy, run_id)
        print(
            "[ui-async-task-smoke] success strategy={strategy} run_id={run_id} status={status} rows={rows}".format(
                strategy=strategy,
                run_id=summary["run_id"],
                status=summary["status"],
                rows=summary["result_count"],
            )
        )
        results.append(summary)
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify async task panel rendering on the Streamlit main entry page.")
    parser.add_argument("--strategies", default="v5,v8,v9", help="Comma-separated strategies to inspect.")
    parser.add_argument("--json", action="store_true", help="Print summary as JSON.")
    args = parser.parse_args()

    results = run_ui_async_task_smoke(strategies=[item.strip() for item in args.strategies.split(",")])
    if args.json:
        print(json.dumps(results, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
