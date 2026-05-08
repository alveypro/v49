from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from src.utils.project_paths import resolve_project_path
from src.utils.research_pool import resolve_research_pool_with_meta


def _load_settings(config_dir: Path) -> dict[str, Any]:
    with (config_dir / "settings.yaml").open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _run_step(cmd: list[str], cwd: Path) -> dict[str, Any]:
    started_at = datetime.now().isoformat()
    proc = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True, check=False)
    return {
        "cmd": cmd,
        "started_at": started_at,
        "ended_at": datetime.now().isoformat(),
        "returncode": proc.returncode,
        "stdout_tail": (proc.stdout or "")[-4000:],
        "stderr_tail": (proc.stderr or "")[-4000:],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run expanded nightly research batch")
    parser.add_argument("--config-dir", default="config", help="Config directory")
    parser.add_argument("--candidate-universe-size", type=int, default=800, help="Universe size for candidate scan")
    parser.add_argument("--candidate-top-n", type=int, default=20, help="Top N candidates to keep")
    parser.add_argument(
        "--daily-profiles",
        nargs="*",
        default=["short", "medium"],
        choices=["short", "medium", "long"],
        help="Profiles for daily research",
    )
    parser.add_argument(
        "--backtest-profile",
        default="medium",
        choices=["short", "medium", "long"],
        help="Grid backtest profile for nightly run",
    )
    parser.add_argument("--research-pool-size", type=int, default=None, help="Override research pool size for batch backtest/research")
    parser.add_argument(
        "--search-mode",
        default="focused",
        choices=["focused", "broad"],
        help="Focused search avoids broad noisy sweeps by default",
    )
    parser.add_argument(
        "--experiment",
        default=None,
        choices=["direction_a_medium", "direction_b_medium", "direction_c_medium"],
        help="Optional formal experiment preset for nightly grid backtest",
    )
    parser.add_argument(
        "--skip-candidates",
        action="store_true",
        help="Skip candidate generation step and only run later research stages",
    )
    parser.add_argument(
        "--skip-daily-research",
        action="store_true",
        help="Skip daily research step and only run remaining stages",
    )
    parser.add_argument(
        "--skip-grid-backtest",
        action="store_true",
        help="Skip grid backtest step and only run earlier stages",
    )
    parser.add_argument(
        "--out",
        default="data/experiments/research_batch_latest.json",
        help="Summary JSON output path",
    )
    args = parser.parse_args()

    project_root = resolve_project_path(".")
    config_dir = resolve_project_path(args.config_dir)
    settings = _load_settings(config_dir)
    stock_pool, research_pool_meta = resolve_research_pool_with_meta(config_dir, size_override=args.research_pool_size)
    if not stock_pool:
        raise RuntimeError("No stock_pool found in settings.yaml")

    summary: dict[str, Any] = {
        "started_at": datetime.now().isoformat(),
        "config_dir": str(config_dir),
        "candidate_universe_size": args.candidate_universe_size,
        "candidate_top_n": args.candidate_top_n,
        "daily_profiles": list(args.daily_profiles),
        "backtest_profile": args.backtest_profile,
        "search_mode": args.search_mode,
        "experiment": args.experiment,
        "research_pool_size": len(stock_pool),
        "research_pool_meta": research_pool_meta,
        "stock_pool": stock_pool,
        "skip_candidates": bool(args.skip_candidates),
        "skip_daily_research": bool(args.skip_daily_research),
        "skip_grid_backtest": bool(args.skip_grid_backtest),
        "steps": {},
    }

    all_steps = {
        "candidates": [
            sys.executable,
            str(project_root / "run_top_candidates.py"),
            "--config-dir",
            str(config_dir),
            "--universe-size",
            str(int(args.candidate_universe_size)),
            "--top-n",
            str(int(args.candidate_top_n)),
        ],
        "daily_research": [
            sys.executable,
            str(project_root / "run_daily_research.py"),
            "--config-dir",
            str(config_dir),
            "--profiles",
            *args.daily_profiles,
            "--out",
            "data/experiments/daily_research_latest.md",
        ],
        "grid_backtest": [
            sys.executable,
            str(project_root / "run_grid_backtest.py"),
            "--config-dir",
            str(config_dir),
            "--profile",
            args.backtest_profile,
            "--search-mode",
            args.search_mode,
            "--stocks",
            *stock_pool,
        ],
    }
    if args.experiment:
        all_steps["grid_backtest"].extend(["--experiment", args.experiment])
    steps: dict[str, list[str]] = {}
    if not args.skip_candidates:
        steps["candidates"] = all_steps["candidates"]
    if not args.skip_daily_research:
        steps["daily_research"] = all_steps["daily_research"]
    if not args.skip_grid_backtest:
        steps["grid_backtest"] = all_steps["grid_backtest"]
    if not steps:
        raise RuntimeError("All research batch steps were skipped")

    failed = False
    for key, cmd in steps.items():
        result = _run_step(cmd, project_root)
        summary["steps"][key] = result
        if result["returncode"] != 0:
            failed = True
            break

    summary["status"] = "failed" if failed else "completed"
    summary["ended_at"] = datetime.now().isoformat()

    out_path = resolve_project_path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": summary["status"], "out": str(out_path)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
