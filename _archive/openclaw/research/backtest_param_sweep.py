from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from itertools import product
from multiprocessing import get_context
from pathlib import Path
from statistics import pstdev
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
import csv
import json
import sqlite3
import uuid

from backtest.engine import BacktestEngine
from openclaw.adapters import V49Adapter
from openclaw.runtime.v49_handlers import HandlerFactory
from openclaw.services.backtest_credibility_service import build_backtest_credibility_audit
from openclaw.services.data_version_service import build_code_version, build_data_version, build_param_version
from openclaw.services.lineage_service import apply_professional_migrations, insert_signal_run
from openclaw.services.strategy_backtest_diagnostic_service import build_strategy_backtest_diagnostics


JsonDict = Dict[str, Any]


def _float_with_default(value: Any, default: float) -> float:
    if value is None:
        return float(default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


@dataclass(frozen=True)
class SweepConfig:
    strategy: str
    module_path: Path
    output_dir: Path
    date_from: str
    date_to: str
    mode: str
    train_window_days: int
    test_window_days: int
    step_days: int
    score_thresholds: Sequence[int]
    sample_sizes: Sequence[int]
    holding_days: Sequence[int]
    max_stop_loss_pcts: Sequence[float] = (0.08,)
    max_take_profit_pcts: Sequence[Optional[float]] = (None,)
    stop_losses: Sequence[Optional[float]] = (None,)
    take_profits: Sequence[Optional[float]] = (None,)
    db_path: Optional[str] = None
    max_runs: Optional[int] = None
    per_run_timeout_sec: Optional[int] = None
    runtime_params: Dict[str, Any] = field(default_factory=dict)


def parse_int_list(raw: str, fallback: Sequence[int]) -> List[int]:
    text = (raw or "").strip()
    if not text:
        return list(fallback)
    out: List[int] = []
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        out.append(int(part))
    return out or list(fallback)


def parse_float_list(raw: str, fallback: Sequence[float]) -> List[float]:
    text = (raw or "").strip()
    if not text:
        return list(fallback)
    out: List[float] = []
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        out.append(float(part))
    return out or list(fallback)


def default_date_window(days: int = 365) -> Tuple[str, str]:
    end = datetime.now().date()
    start = end - timedelta(days=max(30, int(days)))
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def default_threshold_grid(base: int) -> List[int]:
    vals = [max(1, base - 10), max(1, base - 5), max(1, base), min(100, base + 5), min(100, base + 10)]
    return sorted(set(vals))


def default_sample_grid(base: int) -> List[int]:
    vals = [max(50, int(base * 0.5)), max(50, int(base * 0.75)), max(50, int(base))]
    return sorted(set(vals))


def default_holding_grid(base: int) -> List[int]:
    vals = [max(2, base - 2), max(2, base), max(2, base + 2)]
    return sorted(set(vals))


def compute_objective(
    summary: JsonDict,
    rolling: Optional[JsonDict],
    test_rows: Sequence[JsonDict],
    status: str,
) -> float:
    if status != "success":
        return -9999.0

    win_rate = float(summary.get("win_rate", 0.0) or 0.0)
    max_drawdown = _float_with_default(summary.get("max_drawdown"), 0.30)
    signal_density = float(summary.get("signal_density", 0.0) or 0.0)
    cost_pct = float(((summary.get("trading_cost") or {}).get("expected_cost_pct", 0.0)) or 0.0)

    win_stability = 0.0
    if len(test_rows) >= 2:
        wr = [float(((x.get("summary") or {}).get("win_rate", 0.0) or 0.0)) for x in test_rows]
        win_stability = float(pstdev(wr))

    pass_ratio = 1.0
    if rolling:
        total = int(rolling.get("windows_total", 0) or 0)
        failed = len(rolling.get("failed_windows", []) or [])
        if total > 0:
            pass_ratio = max(0.0, min(1.0, (total - failed) / float(total)))

    # Higher is better.
    return (
        win_rate * 100.0
        - max_drawdown * 60.0
        + min(signal_density * 100.0, 12.0)
        - cost_pct * 10.0
        - win_stability * 12.0
        + pass_ratio * 5.0
    )


def run_param_sweep(cfg: SweepConfig) -> JsonDict:
    run_id = f"sweep_{cfg.strategy}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    engine: Optional[BacktestEngine] = None
    if not (cfg.per_run_timeout_sec and cfg.per_run_timeout_sec > 0):
        adapter = V49Adapter(module_path=cfg.module_path)
        factory = HandlerFactory(adapter.module_path)
        adapter.register_backtest_handler(cfg.strategy, factory.create_backtest_handler(cfg.strategy))
        engine = BacktestEngine(adapter)

    take_profit_grid = list(cfg.max_take_profit_pcts) or [None]
    stop_loss_grid = list(cfg.stop_losses) or [None]
    take_profit_pct_grid = list(cfg.take_profits) or [None]
    combos = list(
        product(
            cfg.score_thresholds,
            cfg.sample_sizes,
            cfg.holding_days,
            cfg.max_stop_loss_pcts,
            take_profit_grid,
            stop_loss_grid,
            take_profit_pct_grid,
        )
    )
    if cfg.max_runs is not None and cfg.max_runs > 0:
        combos = combos[: cfg.max_runs]

    rows: List[JsonDict] = []
    errors: List[JsonDict] = []
    stages: List[JsonDict] = [{"stage": "backtest_sweep", "status": "running", "total_runs": len(combos)}]

    for idx, (
        score_th,
        sample_size,
        hold_days,
        max_stop_loss_pct,
        max_take_profit_pct,
        stop_loss,
        take_profit,
    ) in enumerate(combos, start=1):
        params: JsonDict = {
            "score_threshold": int(score_th),
            "sample_size": int(sample_size),
            "holding_days": int(hold_days),
            "max_stop_loss_pct": float(max_stop_loss_pct),
            "mode": cfg.mode,
            "train_window_days": int(cfg.train_window_days),
            "test_window_days": int(cfg.test_window_days),
            "step_days": int(cfg.step_days),
        }
        params.update(dict(cfg.runtime_params or {}))
        if max_take_profit_pct is not None:
            params["max_take_profit_pct"] = float(max_take_profit_pct)
        if stop_loss is not None:
            params["stop_loss"] = float(stop_loss)
        if take_profit is not None:
            params["take_profit"] = float(take_profit)
        if cfg.db_path:
            params["db_path"] = cfg.db_path

        out = _run_engine_with_timeout(
            engine=engine,
            cfg=cfg,
            params=params,
        )
        status = str(out.get("status", "failed"))
        result = out.get("result") or {}
        summary = result.get("summary") or {}
        rolling = result.get("rolling") or {}
        failure_diagnostics = [
            dict(item.get("backtest_diagnostics") or {})
            for item in (rolling.get("failed_windows", []) or [])
            if isinstance(item, dict) and isinstance(item.get("backtest_diagnostics"), dict)
        ]
        window_results = result.get("window_results") if isinstance(result.get("window_results"), dict) else {}
        train_rows = (window_results.get("train") or []) if isinstance(window_results, dict) else []
        test_rows = (window_results.get("test") or []) if isinstance(window_results, dict) else []
        run_diagnostics = _collect_run_backtest_diagnostics(
            train_rows=train_rows,
            test_rows=test_rows,
            failure_diagnostics=failure_diagnostics,
            result=result,
        )
        objective = compute_objective(summary=summary, rolling=rolling, test_rows=test_rows, status=status)

        row = {
            "idx": idx,
            "status": status,
            "strategy": cfg.strategy,
            "score_threshold": int(score_th),
            "sample_size": int(sample_size),
            "holding_days": int(hold_days),
            "max_stop_loss_pct": float(max_stop_loss_pct),
            "max_take_profit_pct": (float(max_take_profit_pct) if max_take_profit_pct is not None else None),
            "stop_loss": (float(stop_loss) if stop_loss is not None else None),
            "take_profit": (float(take_profit) if take_profit is not None else None),
            "win_rate": float(summary.get("win_rate", 0.0) or 0.0),
            "max_drawdown": _float_with_default(summary.get("max_drawdown"), 0.30),
            "signal_density": float(summary.get("signal_density", 0.0) or 0.0),
            "objective": float(objective),
            "error": str(out.get("error", "")),
            "run_id": out.get("run_id", ""),
            "rolling_test_windows": int(rolling.get("test_windows", 0) or 0),
            "rolling_failed_windows": len(rolling.get("failed_windows", []) or []),
            "tradeability_filter_enabled": bool(summary.get("tradeability_filter_enabled") is True),
            "volume_constraint_enabled": bool(summary.get("volume_constraint_enabled") is True),
            "trading_cost": summary.get("trading_cost") if isinstance(summary.get("trading_cost"), dict) else {},
            "risk_diagnostics": summary.get("risk_diagnostics") if isinstance(summary.get("risk_diagnostics"), dict) else {},
            "risk_control": summary.get("risk_control") if isinstance(summary.get("risk_control"), dict) else {},
            "defensive_allocator": summary.get("defensive_allocator") if isinstance(summary.get("defensive_allocator"), dict) else {},
            "failure_diagnostics": failure_diagnostics,
            "run_diagnostics": run_diagnostics,
        }
        rows.append(row)
        if status != "success":
            error_payload = {"idx": idx, "params": params, "error": row["error"] or "unknown"}
            if failure_diagnostics:
                error_payload["failure_diagnostics"] = failure_diagnostics
            errors.append(error_payload)

    rows_sorted = sorted(rows, key=lambda x: float(x.get("objective", -9999.0)), reverse=True)
    best = rows_sorted[0] if rows_sorted else None
    stages[0]["status"] = "completed"
    stages[0]["failed_runs"] = len([x for x in rows if x.get("status") != "success"])

    artifacts = _write_artifacts(run_id=run_id, cfg=cfg, rows=rows_sorted, errors=errors, best=best)
    status = "success" if best and str(best.get("status")) == "success" else "failed"
    aggregate_result = _build_sweep_aggregate_result(
        strategy=cfg.strategy,
        status=status,
        best=best,
        rows=rows,
        errors=errors,
        artifacts=artifacts,
    )
    audit = build_backtest_credibility_audit(
        result=aggregate_result,
        params={
            "strategy": cfg.strategy,
            "mode": cfg.mode,
            "date_from": cfg.date_from,
            "date_to": cfg.date_to,
            "score_thresholds": list(cfg.score_thresholds),
            "sample_sizes": list(cfg.sample_sizes),
            "holding_days": list(cfg.holding_days),
            "max_stop_loss_pcts": list(cfg.max_stop_loss_pcts),
            "max_take_profit_pcts": list(cfg.max_take_profit_pcts),
            "stop_losses": list(cfg.stop_losses),
            "take_profits": list(cfg.take_profits),
            "runtime_params": dict(cfg.runtime_params or {}),
        },
        param_runs=len(rows),
        failed_runs=errors,
        artifact_path=str(artifacts.get("json", "")),
    )
    aggregate_result["backtest_credibility"] = audit
    diagnostics = build_strategy_backtest_diagnostics(
        strategy=cfg.strategy,
        rows=rows,
        errors=errors,
        backtest_credibility=audit,
    )
    aggregate_result["strategy_backtest_diagnostics"] = diagnostics
    _write_diagnostics_artifacts(artifacts=artifacts, diagnostics=diagnostics, backtest_credibility=audit)
    _persist_sweep_backtest_chain(
        cfg=cfg,
        run_id=run_id,
        status=status,
        result=aggregate_result,
        artifact_path=str(artifacts.get("json", "")),
    )

    return {
        "run_id": run_id,
        "status": status,
        "stages": stages,
        "artifacts": artifacts,
        "errors": errors[:20],
        "best": best,
        "tried": len(rows),
        "backtest_credibility": audit,
        "strategy_backtest_diagnostics": diagnostics,
    }


def _build_sweep_aggregate_result(
    *,
    strategy: str,
    status: str,
    best: Optional[JsonDict],
    rows: Sequence[JsonDict],
    errors: Sequence[JsonDict],
    artifacts: JsonDict,
) -> JsonDict:
    best = best or {}
    failed_windows = [
        {"idx": int(row.get("idx", 0) or 0), "error": str(row.get("error", "") or "failed")}
        for row in rows
        if str(row.get("status")) != "success"
    ]
    test_windows = int(best.get("rolling_test_windows", 0) or 0)
    summary = {
        "win_rate": float(best.get("win_rate", 0.0) or 0.0),
        "max_drawdown": _float_with_default(best.get("max_drawdown"), 1.0),
        "signal_density": float(best.get("signal_density", 0.0) or 0.0),
        "samples": test_windows,
        "parameter_objective_best": float(best.get("objective", 0.0) or 0.0),
        "tradeability_filter_enabled": bool(best.get("tradeability_filter_enabled") is True),
        "volume_constraint_enabled": bool(best.get("volume_constraint_enabled") is True),
    }
    if isinstance(best.get("trading_cost"), dict):
        summary["trading_cost"] = best.get("trading_cost")
    if isinstance(best.get("risk_diagnostics"), dict):
        summary["risk_diagnostics"] = best.get("risk_diagnostics")
    if isinstance(best.get("risk_control"), dict):
        summary["risk_control"] = best.get("risk_control")
    return {
        "run_id": str(best.get("run_id") or ""),
        "status": status,
        "strategy": strategy,
        "result": {
            "summary": summary,
            "rolling": {
                "mode": "sweep_aggregate",
                "train_test_separated": True,
                "windows_total": test_windows,
                "train_windows": test_windows,
                "test_windows": test_windows,
                "failed_windows": failed_windows,
            },
            "artifacts": artifacts,
            "errors": list(errors),
        },
    }


def _collect_run_backtest_diagnostics(
    *,
    train_rows: Sequence[JsonDict],
    test_rows: Sequence[JsonDict],
    failure_diagnostics: Sequence[JsonDict],
    result: JsonDict | None = None,
) -> List[JsonDict]:
    diagnostics: List[JsonDict] = []
    raw = (result or {}).get("raw") if isinstance((result or {}).get("raw"), dict) else {}
    if isinstance(raw.get("backtest_diagnostics"), dict):
        diagnostics.append(dict(raw.get("backtest_diagnostics") or {}))
    for source in list(train_rows or []) + list(test_rows or []):
        if isinstance(source, dict) and isinstance(source.get("backtest_diagnostics"), dict):
            diagnostics.append(dict(source.get("backtest_diagnostics") or {}))
    for item in failure_diagnostics or []:
        if isinstance(item, dict):
            diagnostics.append(dict(item))
    return diagnostics


def _persist_sweep_backtest_chain(
    *,
    cfg: SweepConfig,
    run_id: str,
    status: str,
    result: JsonDict,
    artifact_path: str,
) -> None:
    if not cfg.db_path:
        return
    conn = sqlite3.connect(str(cfg.db_path), timeout=30)
    try:
        apply_professional_migrations(conn)
        params = {
            "strategy": cfg.strategy,
            "mode": cfg.mode,
            "date_from": cfg.date_from,
            "date_to": cfg.date_to,
            "score_thresholds": list(cfg.score_thresholds),
            "sample_sizes": list(cfg.sample_sizes),
            "holding_days": list(cfg.holding_days),
            "max_stop_loss_pcts": list(cfg.max_stop_loss_pcts),
            "max_take_profit_pcts": list(cfg.max_take_profit_pcts),
            "stop_losses": list(cfg.stop_losses),
            "take_profits": list(cfg.take_profits),
            "runtime_params": dict(cfg.runtime_params or {}),
        }
        insert_signal_run(
            conn,
            run_id=run_id,
            run_type="backtest",
            strategy=str(cfg.strategy),
            trade_date=str(cfg.date_to),
            data_version=build_data_version(conn),
            code_version=build_code_version(root=cfg.module_path.resolve().parent),
            param_version=build_param_version(params),
            status=str(status or "failed"),
            artifact_path=str(artifact_path or ""),
            summary=result,
        )
    finally:
        conn.close()


def _run_engine_with_timeout(
    engine: Optional[BacktestEngine],
    cfg: SweepConfig,
    params: JsonDict,
) -> JsonDict:
    timeout = int(cfg.per_run_timeout_sec or 0)
    if timeout <= 0:
        if engine is None:
            raise RuntimeError("engine is required when per_run_timeout_sec is disabled")
        return engine.run(cfg.strategy, cfg.date_from, cfg.date_to, params)

    ctx = get_context("spawn")
    q = ctx.Queue(maxsize=1)
    p = ctx.Process(
        target=_run_single_combo_process,
        args=(
            str(cfg.module_path),
            cfg.strategy,
            cfg.date_from,
            cfg.date_to,
            dict(params),
            q,
        ),
    )
    p.start()
    p.join(timeout=float(timeout))
    if p.is_alive():
        p.terminate()
        p.join(timeout=5.0)
        return _timeout_failed_result(cfg.strategy, timeout)
    if p.exitcode not in (0, None):
        return {
            "run_id": f"subproc_fail_{cfg.strategy}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "status": "failed",
            "strategy": cfg.strategy,
            "error": f"subprocess_exit_{p.exitcode}",
            "result": {"summary": {"win_rate": 0.0, "max_drawdown": 1.0, "signal_density": 0.0}},
        }
    if q.empty():
        return {
            "run_id": f"subproc_empty_{cfg.strategy}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "status": "failed",
            "strategy": cfg.strategy,
            "error": "subprocess returned no payload",
            "result": {"summary": {"win_rate": 0.0, "max_drawdown": 1.0, "signal_density": 0.0}},
        }
    return q.get()


def _timeout_failed_result(strategy: str, timeout_sec: int) -> JsonDict:
    return {
        "run_id": f"timeout_{strategy}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        "status": "failed",
        "strategy": strategy,
        "error": f"per-run timeout after {timeout_sec}s",
        "result": {"summary": {"win_rate": 0.0, "max_drawdown": 1.0, "signal_density": 0.0}},
    }


def _run_single_combo_process(
    module_path: str,
    strategy: str,
    date_from: str,
    date_to: str,
    params: JsonDict,
    queue: Any,
) -> None:
    try:
        adapter = V49Adapter(module_path=Path(module_path))
        factory = HandlerFactory(adapter.module_path)
        adapter.register_backtest_handler(strategy, factory.create_backtest_handler(strategy))
        engine = BacktestEngine(adapter)
        out = engine.run(strategy, date_from, date_to, params)
    except Exception as exc:
        out = {
            "run_id": f"subproc_exception_{strategy}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "status": "failed",
            "strategy": strategy,
            "error": str(exc),
            "result": {"summary": {"win_rate": 0.0, "max_drawdown": 1.0, "signal_density": 0.0}},
        }
    try:
        queue.put(out)
    except Exception:
        pass


def _write_artifacts(
    run_id: str,
    cfg: SweepConfig,
    rows: Sequence[JsonDict],
    errors: Sequence[JsonDict],
    best: Optional[JsonDict],
) -> JsonDict:
    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = cfg.output_dir / f"backtest_sweep_{cfg.strategy}_{ts}.csv"
    json_path = cfg.output_dir / f"backtest_sweep_{cfg.strategy}_{ts}.json"
    md_path = cfg.output_dir / f"backtest_sweep_{cfg.strategy}_{ts}.md"

    fields = [
        "idx",
        "status",
        "strategy",
        "score_threshold",
        "sample_size",
        "holding_days",
        "max_stop_loss_pct",
        "max_take_profit_pct",
        "stop_loss",
        "take_profit",
        "win_rate",
        "max_drawdown",
        "signal_density",
        "objective",
        "rolling_test_windows",
        "rolling_failed_windows",
        "run_id",
        "error",
    ]
    with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fields})

    payload = {
        "run_id": run_id,
        "strategy": cfg.strategy,
        "date_from": cfg.date_from,
        "date_to": cfg.date_to,
        "mode": cfg.mode,
        "rows": list(rows),
        "errors": list(errors),
        "best": best,
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    md_lines = [
        f"# Backtest Sweep {cfg.strategy}",
        "",
        f"- run_id: `{run_id}`",
        f"- period: `{cfg.date_from}` -> `{cfg.date_to}`",
        f"- mode: `{cfg.mode}`",
        f"- tried: `{len(rows)}`",
        "",
        "## Best Params",
    ]
    if best:
        md_lines.extend(
            [
                f"- score_threshold: `{best.get('score_threshold')}`",
                f"- sample_size: `{best.get('sample_size')}`",
                f"- holding_days: `{best.get('holding_days')}`",
                f"- max_stop_loss_pct: `{best.get('max_stop_loss_pct')}`",
                f"- max_take_profit_pct: `{best.get('max_take_profit_pct')}`",
                f"- stop_loss: `{best.get('stop_loss')}`",
                f"- take_profit: `{best.get('take_profit')}`",
                f"- win_rate: `{best.get('win_rate')}`",
                f"- max_drawdown: `{best.get('max_drawdown')}`",
                f"- signal_density: `{best.get('signal_density')}`",
                f"- objective: `{best.get('objective')}`",
            ]
        )
    else:
        md_lines.append("- none")

    if errors:
        md_lines.extend(["", "## Errors", ""])
        for e in list(errors)[:10]:
            md_lines.append(f"- idx={e.get('idx')} error={e.get('error')}")

    md_path.write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    return {"csv": str(csv_path), "json": str(json_path), "markdown": str(md_path)}


def _write_diagnostics_artifacts(*, artifacts: JsonDict, diagnostics: JsonDict, backtest_credibility: JsonDict | None = None) -> None:
    json_path_raw = artifacts.get("json")
    md_path_raw = artifacts.get("markdown")
    if json_path_raw:
        json_path = Path(str(json_path_raw))
        if json_path.exists():
            payload = json.loads(json_path.read_text(encoding="utf-8"))
            if isinstance(backtest_credibility, dict):
                payload["backtest_credibility"] = backtest_credibility
            payload["strategy_backtest_diagnostics"] = diagnostics
            json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    if md_path_raw:
        md_path = Path(str(md_path_raw))
        if md_path.exists():
            lines = md_path.read_text(encoding="utf-8").rstrip().splitlines()
            lines.extend(
                [
                    "",
                    "## Strategy Backtest Diagnostics",
                    "",
                    f"- credible_evidence_present: `{diagnostics.get('credible_evidence_present')}`",
                    f"- eligible_for_formal_ranking: `{diagnostics.get('eligible_for_formal_ranking')}`",
                    f"- quality_floor_passed: `{diagnostics.get('quality_floor_passed')}`",
                    f"- successful_param_runs: `{diagnostics.get('successful_param_runs')}`",
                    f"- failed_param_runs: `{diagnostics.get('failed_param_runs')}`",
                    "",
                    "### Failure Classes",
                    "",
                ]
            )
            failures = diagnostics.get("failure_classes") or []
            if failures:
                lines.extend([f"- `{x}`" for x in failures])
            else:
                lines.append("- none")
            lines.extend(["", "### Next Actions", ""])
            actions = diagnostics.get("next_actions") or []
            if actions:
                lines.extend([f"- `{x}`" for x in actions])
            else:
                lines.append("- none")
            window_diag = diagnostics.get("window_diagnostics") if isinstance(diagnostics.get("window_diagnostics"), dict) else {}
            if window_diag:
                lines.extend(
                    [
                        "",
                        "### Window Diagnostics",
                        "",
                        f"- available: `{window_diag.get('available')}`",
                        f"- evaluated: `{window_diag.get('evaluated', 0)}`",
                        f"- passed_threshold: `{window_diag.get('passed_threshold', 0)}`",
                        f"- pass_rate: `{window_diag.get('pass_rate', 0.0)}`",
                        f"- max_score: `{window_diag.get('max_score', 0.0)}`",
                    ]
                )
            md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
