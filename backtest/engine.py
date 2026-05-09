from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict

from openclaw.adapters import V49Adapter
from openclaw.services.backtest_credibility_service import build_backtest_credibility_audit
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
        global_budget = _optional_positive_int(params.get("max_evaluations_global"))
        remaining_budget = int(global_budget or 0)

        for idx, w in enumerate(windows):
            if global_budget is not None and remaining_budget <= 0:
                failed.append({"window": idx, "role": w.role, "error": "global evaluation budget exhausted"})
                break
            run_params = dict(params)
            run_params["window_index"] = idx
            if global_budget is not None:
                per_window_limit = _optional_positive_int(run_params.get("max_evaluations"))
                run_params["max_evaluations"] = min(per_window_limit or remaining_budget, remaining_budget)
            bt = self.adapter.run_backtest(strategy=strategy, date_from=w.date_from, date_to=w.date_to, params=run_params)
            diagnostics = _extract_backtest_diagnostics(bt)
            if global_budget is not None:
                remaining_budget = max(0, remaining_budget - _diagnostic_evaluated_count(diagnostics))
            if bt.get("status") != "success":
                failed_row = {"window": idx, "role": w.role, "error": bt.get("error", "unknown")}
                if diagnostics:
                    failed_row["backtest_diagnostics"] = diagnostics
                failed.append(failed_row)
                continue
            summary = self._enrich_summary(((bt.get("result") or {}).get("summary") or {}), params)
            row = {
                "window": idx,
                "role": w.role,
                "date_from": w.date_from,
                "date_to": w.date_to,
                "summary": summary,
            }
            diagnostics = _extract_backtest_diagnostics(bt)
            if diagnostics:
                row["backtest_diagnostics"] = diagnostics
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
        if global_budget is not None:
            result["rolling"]["evaluation_budget"] = {
                "global_max_evaluations": int(global_budget),
                "remaining": int(remaining_budget),
            }
        status = "success" if test_rows else "failed"
        error = ""
        if not test_rows:
            error = (
                f"rolling backtest produced 0 successful test windows "
                f"(failed_windows={len(failed)})"
            )
        out = {
            "run_id": f"rolling_{strategy}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "status": status,
            "strategy": strategy,
            "result": result,
            **({"error": error} if error else {}),
        }
        out["backtest_credibility"] = build_backtest_credibility_audit(
            result=out,
            params=params,
            param_runs=int(params.get("param_runs", 1) or 1),
            failed_runs=[],
        )
        return out

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

    out = {
        "win_rate": _avg("win_rate"),
        "max_drawdown": _avg("max_drawdown"),
        "signal_density": _avg("signal_density"),
        "samples": len(summaries),
    }
    if all(bool((x or {}).get("tradeability_filter_enabled")) for x in summaries):
        out["tradeability_filter_enabled"] = True
    if all(bool((x or {}).get("volume_constraint_enabled")) for x in summaries):
        out["volume_constraint_enabled"] = True
    costs = [(x or {}).get("trading_cost") for x in summaries if isinstance((x or {}).get("trading_cost"), dict)]
    if costs:
        out["trading_cost"] = costs[-1]
    controls = [(x or {}).get("risk_control") for x in summaries if isinstance((x or {}).get("risk_control"), dict)]
    if controls:
        out["risk_control"] = controls[-1]
    risk_diagnostics = [
        (x or {}).get("risk_diagnostics")
        for x in summaries
        if isinstance((x or {}).get("risk_diagnostics"), dict)
    ]
    if risk_diagnostics:
        out["risk_diagnostics"] = _aggregate_risk_diagnostics(risk_diagnostics)
    defensive_allocators = [
        (x or {}).get("defensive_allocator")
        for x in summaries
        if isinstance((x or {}).get("defensive_allocator"), dict) and (x or {}).get("defensive_allocator")
    ]
    if defensive_allocators:
        out["defensive_allocator"] = _aggregate_defensive_allocator_reviews(defensive_allocators)
    return out


def _extract_backtest_diagnostics(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
    raw = result.get("raw") if isinstance(result.get("raw"), dict) else {}
    if not raw:
        raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    diagnostics = raw.get("backtest_diagnostics") if isinstance(raw.get("backtest_diagnostics"), dict) else {}
    return dict(diagnostics) if diagnostics else {}


def _optional_positive_int(value: Any) -> int | None:
    try:
        parsed = int(value or 0)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _diagnostic_evaluated_count(diagnostics: Dict[str, Any]) -> int:
    try:
        return max(0, int((diagnostics or {}).get("evaluated", 0) or 0))
    except (TypeError, ValueError):
        return 0


def _aggregate_risk_diagnostics(items: list[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "window_count": len(items),
        "windows": items,
        "exit_reason_counts": {},
        "tail_loss_count_5pct": 0,
        "tail_loss_count_8pct": 0,
        "worst_return_pct": 0.0,
    }
    worst_trades = []
    for item in items:
        out["tail_loss_count_5pct"] += int(item.get("tail_loss_count_5pct", 0) or 0)
        out["tail_loss_count_8pct"] += int(item.get("tail_loss_count_8pct", 0) or 0)
        out["worst_return_pct"] = min(float(out["worst_return_pct"]), float(item.get("worst_return_pct", 0.0) or 0.0))
        for reason, count in (item.get("exit_reason_counts") or {}).items():
            out["exit_reason_counts"][str(reason)] = int(out["exit_reason_counts"].get(str(reason), 0) or 0) + int(count or 0)
        if isinstance(item.get("worst_trades"), list):
            worst_trades.extend(item.get("worst_trades") or [])
    if worst_trades:
        deduped: Dict[str, Dict[str, Any]] = {}
        for row in worst_trades:
            key = "|".join(
                str((row or {}).get(part, ""))
                for part in ("ts_code", "trade_date", "future_return", "exit_reason")
            )
            deduped.setdefault(key, row)
        out["worst_trades"] = sorted(
            deduped.values(),
            key=lambda row: float((row or {}).get("future_return", 0.0) or 0.0),
        )[:5]
    return out


def _aggregate_defensive_allocator_reviews(items: list[Dict[str, Any]]) -> Dict[str, Any]:
    blocking = sorted(
        {
            str(reason)
            for item in items
            for reason in (item.get("blocking_reasons") or [])
            if str(reason or "")
        }
    )
    available_items = [item for item in items if item.get("available") is True]
    out: Dict[str, Any] = {
        "available": bool(available_items),
        "window_count": len(items),
        "promotion_eligible": False,
        "allocator_candidate_eligible": bool(available_items) and all(
            item.get("allocator_candidate_eligible") is True for item in available_items
        ),
        "success_metric_passed": bool(available_items) and all(
            item.get("success_metric_passed") is True for item in available_items
        ),
        "blocking_reasons": blocking,
        "contract": dict((items[-1].get("contract") or {})) if isinstance(items[-1].get("contract"), dict) else {},
        "windows": items,
    }
    if available_items:
        out["avg_drawdown_reduction"] = _avg_float(available_items, "drawdown_reduction")
        out["avg_excess_return_pct"] = _avg_float(available_items, "excess_return_pct")
        out["avg_benchmark_drawdown_reduction"] = _avg_float(available_items, "benchmark_drawdown_reduction")
        out["avg_benchmark_excess_return_pct"] = _avg_float(available_items, "benchmark_excess_return_pct")
        out["avg_overlay_max_drawdown"] = _avg_float(available_items, "overlay_max_drawdown")
        out["avg_full_exposure_max_drawdown"] = _avg_float(available_items, "full_exposure_max_drawdown")
    return out


def _avg_float(items: list[Dict[str, Any]], key: str) -> float:
    if not items:
        return 0.0
    vals = [float((item or {}).get(key, 0.0) or 0.0) for item in items]
    return sum(vals) / float(len(vals))
