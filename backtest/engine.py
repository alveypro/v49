from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict

from openclaw.adapters import V49Adapter
from risk.trading_cost import estimate_round_trip_cost


@dataclass(frozen=True)
class RollingWindow:
    date_from: str
    date_to: str
    role: str  # train/test


class BacktestEngine:
    def __init__(self, adapter: V49Adapter):
        self.adapter = adapter

    def run(self, strategy: str, date_from: str, date_to: str, params: Dict[str, Any]) -> Dict[str, Any]:
        params = dict(params or {})
        mode = str(params.get("mode", "rolling")).lower()
        if mode == "single":
            return self._run_single(strategy, date_from, date_to, params)
        return self._run_rolling(strategy, date_from, date_to, params)

    def _run_single(self, strategy: str, date_from: str, date_to: str, params: Dict[str, Any]) -> Dict[str, Any]:
        raw = self.adapter.run_backtest(strategy=strategy, date_from=date_from, date_to=date_to, params=params)
        summary = ((raw.get("result") or {}).get("summary") or {})
        enriched = self._enrich_summary(summary, params)
        if raw.get("result"):
            raw["result"]["summary"] = enriched
        return raw

    def _run_rolling(self, strategy: str, date_from: str, date_to: str, params: Dict[str, Any]) -> Dict[str, Any]:
        train_days = int(params.get("train_window_days", 180))
        test_days = int(params.get("test_window_days", 60))
        step_days = int(params.get("step_days", test_days))
        windows = _build_windows(date_from, date_to, train_days, test_days, step_days)
        if not windows:
            return self._run_single(strategy, date_from, date_to, params)

        train_rows = []
        test_rows = []
        failed = []

        for idx, w in enumerate(windows):
            run_params = dict(params)
            run_params["window_index"] = idx
            bt = self.adapter.run_backtest(strategy=strategy, date_from=w.date_from, date_to=w.date_to, params=run_params)
            if bt.get("status") != "success":
                failed.append({"window": idx, "role": w.role, "error": bt.get("error", "unknown")})
                continue
            summary = self._enrich_summary(((bt.get("result") or {}).get("summary") or {}), params)
            row = {
                "window": idx,
                "role": w.role,
                "date_from": w.date_from,
                "date_to": w.date_to,
                "summary": summary,
            }
            if w.role == "train":
                train_rows.append(row)
            else:
                test_rows.append(row)

        test_agg = _aggregate([r["summary"] for r in test_rows])
        train_agg = _aggregate([r["summary"] for r in train_rows])
        result = {
            "summary": test_agg,
            "rolling": {
                "mode": "rolling",
                "train_test_separated": True,
                "windows_total": len(windows),
                "train_windows": len(train_rows),
                "test_windows": len(test_rows),
                "failed_windows": failed,
                "train_summary": train_agg,
                "test_summary": test_agg,
            },
            "window_results": {"train": train_rows, "test": test_rows},
        }
        status = "success" if test_rows else "failed"
        error = ""
        if not test_rows:
            error = (
                f"rolling backtest produced 0 successful test windows "
                f"(failed_windows={len(failed)})"
            )
        return {
            "run_id": f"rolling_{strategy}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "status": status,
            "strategy": strategy,
            "result": result,
            **({"error": error} if error else {}),
        }

    def _enrich_summary(self, summary: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(summary or {})
        signal_density = float(out.get("signal_density", 0.0) or 0.0)
        holding_days = int(params.get("holding_days", 5))
        cost = estimate_round_trip_cost(
            holding_days=holding_days,
            signal_density=signal_density,
            commission_bp=float(params.get("commission_bp", 8.0)),
            slippage_bp=float(params.get("slippage_bp", 10.0)),
            stamp_duty_bp=float(params.get("stamp_duty_bp", 10.0)),
        )
        out["trading_cost"] = cost
        return out


def _build_windows(date_from: str, date_to: str, train_days: int, test_days: int, step_days: int) -> list[RollingWindow]:
    if train_days <= 0 or test_days <= 0 or step_days <= 0:
        return []
    start = datetime.strptime(date_from, "%Y-%m-%d")
    end = datetime.strptime(date_to, "%Y-%m-%d")
    cursor = start
    windows: list[RollingWindow] = []
    idx = 0
    while True:
        train_start = cursor
        train_end = train_start + timedelta(days=train_days - 1)
        test_start = train_end + timedelta(days=1)
        test_end = test_start + timedelta(days=test_days - 1)
        if test_end > end:
            break
        windows.append(
            RollingWindow(
                date_from=train_start.strftime("%Y-%m-%d"),
                date_to=train_end.strftime("%Y-%m-%d"),
                role="train",
            )
        )
        windows.append(
            RollingWindow(
                date_from=test_start.strftime("%Y-%m-%d"),
                date_to=test_end.strftime("%Y-%m-%d"),
                role="test",
            )
        )
        idx += 1
        cursor = start + timedelta(days=idx * step_days)
    return windows


def _aggregate(summaries: list[Dict[str, Any]]) -> Dict[str, Any]:
    if not summaries:
        return {"win_rate": 0.0, "max_drawdown": 1.0, "signal_density": 0.0}

    def _avg(k: str) -> float:
        vals = [float((x or {}).get(k, 0.0) or 0.0) for x in summaries]
        return sum(vals) / len(vals)

    return {
        "win_rate": _avg("win_rate"),
        "max_drawdown": _avg("max_drawdown"),
        "signal_density": _avg("signal_density"),
        "samples": len(summaries),
    }
