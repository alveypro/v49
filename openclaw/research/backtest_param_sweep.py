from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from itertools import product
from multiprocessing import get_context
from pathlib import Path
from statistics import pstdev
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
import csv
import json
import uuid

from backtest.engine import BacktestEngine
from openclaw.adapters import V49Adapter
from openclaw.runtime.v49_handlers import HandlerFactory


JsonDict = Dict[str, Any]


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
    db_path: Optional[str] = None
    max_runs: Optional[int] = None
    per_run_timeout_sec: Optional[int] = None


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
    max_drawdown = float(summary.get("max_drawdown", 0.30) or 0.30)
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

    combos = list(product(cfg.score_thresholds, cfg.sample_sizes, cfg.holding_days))
    if cfg.max_runs is not None and cfg.max_runs > 0:
        combos = combos[: cfg.max_runs]

    rows: List[JsonDict] = []
    errors: List[JsonDict] = []
    stages: List[JsonDict] = [{"stage": "backtest_sweep", "status": "running", "total_runs": len(combos)}]

    for idx, (score_th, sample_size, hold_days) in enumerate(combos, start=1):
        params: JsonDict = {
            "score_threshold": int(score_th),
            "sample_size": int(sample_size),
            "holding_days": int(hold_days),
            "mode": cfg.mode,
            "train_window_days": int(cfg.train_window_days),
            "test_window_days": int(cfg.test_window_days),
            "step_days": int(cfg.step_days),
        }
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
        test_rows = ((result.get("window_results") or {}).get("test") or []) if isinstance(result, dict) else []
        objective = compute_objective(summary=summary, rolling=rolling, test_rows=test_rows, status=status)

        row = {
            "idx": idx,
            "status": status,
            "strategy": cfg.strategy,
            "score_threshold": int(score_th),
            "sample_size": int(sample_size),
            "holding_days": int(hold_days),
            "win_rate": float(summary.get("win_rate", 0.0) or 0.0),
            "max_drawdown": float(summary.get("max_drawdown", 0.30) or 0.30),
            "signal_density": float(summary.get("signal_density", 0.0) or 0.0),
            "objective": float(objective),
            "error": str(out.get("error", "")),
            "run_id": out.get("run_id", ""),
            "rolling_test_windows": int(rolling.get("test_windows", 0) or 0),
            "rolling_failed_windows": len(rolling.get("failed_windows", []) or []),
        }
        rows.append(row)
        if status != "success":
            errors.append({"idx": idx, "params": params, "error": row["error"] or "unknown"})

    rows_sorted = sorted(rows, key=lambda x: float(x.get("objective", -9999.0)), reverse=True)
    best = rows_sorted[0] if rows_sorted else None
    stages[0]["status"] = "completed"
    stages[0]["failed_runs"] = len([x for x in rows if x.get("status") != "success"])

    artifacts = _write_artifacts(run_id=run_id, cfg=cfg, rows=rows_sorted, errors=errors, best=best)
    status = "success" if best and str(best.get("status")) == "success" else "failed"

    return {
        "run_id": run_id,
        "status": status,
        "stages": stages,
        "artifacts": artifacts,
        "errors": errors[:20],
        "best": best,
        "tried": len(rows),
    }


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
