#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Tuple


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


DEFAULT_SCAN_PROFILES: Dict[str, Dict[str, Any]] = {
    "v5": {
        "score_threshold": 70,
        "top_percent": 1,
        "select_mode": "分位数筛选(Top%)",
        "cap_min": 100.0,
        "cap_max": 15000.0,
        "enable_consistency": True,
        "min_align": 2,
    },
    "v8": {
        "score_threshold": [45.0, 90.0],
        "top_percent": 1,
        "select_mode": "分位数筛选(Top%)",
        "scan_all": False,
        "cap_min": 100.0,
        "cap_max": 15000.0,
        "enable_consistency": True,
        "min_align": 2,
    },
    "v9": {
        "score_threshold": 65,
        "top_percent": 1,
        "select_mode": "分位数筛选(Top%)",
        "scan_all": False,
        "cap_min": 100.0,
        "cap_max": 15000.0,
        "enable_consistency": True,
        "min_align": 2,
        "holding_days": 8,
        "lookback_days": 120,
        "min_turnover": 5.0,
        "candidate_count": 800,
    },
}

SCORE_COLS = {"v5": "综合评分", "v8": "综合评分", "v9": "综合评分"}
TERMINAL_STATUSES = {"success", "failed", "cancelled"}


@lru_cache(maxsize=1)
def _load_app():
    import v49_app as app_module  # noqa: WPS433

    return app_module


def _stderr_tail(state: Dict[str, Any], limit: int = 20) -> str:
    path = str(state.get("stderr_log", "") or "")
    if not path:
        return ""
    fp = Path(path)
    if not fp.exists():
        return ""
    try:
        lines = fp.read_text(encoding="utf-8", errors="ignore").splitlines()
        return "\n".join(lines[-limit:])
    except Exception:
        return ""


def _normalize_strategies(raw: Iterable[str]) -> List[str]:
    out: List[str] = []
    for item in raw:
        s = str(item or "").strip().lower()
        if not s:
            continue
        if s not in DEFAULT_SCAN_PROFILES:
            raise ValueError(f"unsupported strategy for async smoke: {s}")
        out.append(s)
    if not out:
        raise ValueError("no strategies selected")
    return out


def _wait_for_terminal(run_id: str, timeout_sec: int, poll_sec: float) -> Tuple[Dict[str, Any], List[str]]:
    app = _load_app()
    deadline = time.time() + max(1, int(timeout_sec))
    observed: List[str] = []
    last_state: Dict[str, Any] = {}
    while time.time() < deadline:
        state = app._recover_async_scan_task(run_id) or app._load_async_scan_state(run_id) or {}
        if state:
            last_state = dict(state)
            status = str(state.get("status", "") or "")
            if status and (not observed or observed[-1] != status):
                observed.append(status)
            if status in TERMINAL_STATUSES:
                return last_state, observed
        time.sleep(max(0.2, float(poll_sec)))
    raise TimeoutError(f"async scan timed out after {timeout_sec}s: run_id={run_id} last_status={last_state.get('status')}")


def _assert_success(*, strategy: str, run_id: str, state: Dict[str, Any], observed_statuses: List[str]) -> Dict[str, Any]:
    status = str(state.get("status", "") or "")
    row_count = int(state.get("row_count", 0) or 0)
    result_csv = str(state.get("result_csv", "") or "")
    if status != "success":
        tail = _stderr_tail(state)
        raise RuntimeError(
            f"{strategy} did not finish successfully: run_id={run_id} status={status} observed={observed_statuses}\n{tail}"
        )
    if not any(s in {"queued", "running"} for s in observed_statuses):
        raise RuntimeError(f"{strategy} never entered queued/running: run_id={run_id} observed={observed_statuses}")
    if row_count <= 0:
        raise RuntimeError(f"{strategy} returned no rows: run_id={run_id} row_count={row_count}")
    if not result_csv or not Path(result_csv).exists():
        raise RuntimeError(f"{strategy} missing result_csv artifact: run_id={run_id} result_csv={result_csv}")
    return {
        "strategy": strategy,
        "run_id": run_id,
        "observed_statuses": observed_statuses,
        "final_status": status,
        "row_count": row_count,
        "result_csv": result_csv,
        "ended_at": state.get("ended_at"),
    }


def run_async_scan_smoke(*, strategies: Iterable[str], timeout_sec: int, poll_sec: float) -> List[Dict[str, Any]]:
    app = _load_app()
    selected = _normalize_strategies(strategies)
    results: List[Dict[str, Any]] = []
    for strategy in selected:
        params = dict(DEFAULT_SCAN_PROFILES[strategy])
        score_col = SCORE_COLS[strategy]
        ok, msg, run_id = app._start_async_scan_task(strategy, params, score_col=score_col)
        if not ok:
            raise RuntimeError(f"{strategy} async smoke submit failed: {msg}")
        print(f"[async-scan-smoke] submitted strategy={strategy} run_id={run_id}")
        state, observed_statuses = _wait_for_terminal(run_id, timeout_sec=timeout_sec, poll_sec=poll_sec)
        summary = _assert_success(
            strategy=strategy,
            run_id=run_id,
            state=state,
            observed_statuses=observed_statuses,
        )
        print(
            "[async-scan-smoke] success strategy={strategy} run_id={run_id} rows={rows} observed={observed}".format(
                strategy=strategy,
                run_id=run_id,
                rows=summary["row_count"],
                observed="->".join(observed_statuses),
            )
        )
        results.append(summary)
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Run production-style async scan smoke checks for v5/v8/v9.")
    parser.add_argument("--strategies", default="v5,v8,v9", help="Comma-separated strategies to test.")
    parser.add_argument("--timeout-sec", type=int, default=180, help="Per-strategy timeout in seconds.")
    parser.add_argument("--poll-sec", type=float, default=1.0, help="State polling interval in seconds.")
    parser.add_argument("--json", action="store_true", help="Print final summary as JSON.")
    args = parser.parse_args()

    strategies = [item.strip() for item in str(args.strategies).split(",")]
    results = run_async_scan_smoke(
        strategies=strategies,
        timeout_sec=int(args.timeout_sec),
        poll_sec=float(args.poll_sec),
    )
    if args.json:
        print(json.dumps(results, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
