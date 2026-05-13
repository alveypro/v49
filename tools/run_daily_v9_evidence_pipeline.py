#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from openclaw.paths import db_path as default_db_path  # noqa: E402


PIPELINE_VERSION = "daily_v9_evidence_pipeline.v1"


def _now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _run(cmd: list[str], *, env: dict[str, str], timeout: int) -> dict[str, Any]:
    started = time.time()
    try:
        completed = subprocess.run(
            cmd,
            cwd=str(ROOT),
            env=env,
            text=True,
            capture_output=True,
            timeout=timeout,
        )
        return {
            "cmd": cmd,
            "returncode": completed.returncode,
            "elapsed_sec": round(time.time() - started, 3),
            "stdout_tail": completed.stdout[-4000:],
            "stderr_tail": completed.stderr[-4000:],
            "passed": completed.returncode == 0,
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "cmd": cmd,
            "returncode": None,
            "elapsed_sec": round(time.time() - started, 3),
            "stdout_tail": (exc.stdout or "")[-4000:] if isinstance(exc.stdout, str) else "",
            "stderr_tail": (exc.stderr or "")[-4000:] if isinstance(exc.stderr, str) else "",
            "passed": False,
            "error": f"timeout_after_{timeout}s",
        }


def _db_health(db_path: Path, *, max_lag_days: int) -> dict[str, Any]:
    required = ("daily_trading_data", "stock_basic", "signal_runs", "signal_items")
    if not db_path.exists():
        return {"passed": False, "blocking_reasons": ["db_missing"], "db_path": str(db_path)}
    conn = sqlite3.connect(str(db_path), timeout=30)
    blocking: list[str] = []
    tables: dict[str, Any] = {}
    try:
        for table in required:
            try:
                tables[table] = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            except Exception as exc:
                tables[table] = {"error": str(exc)}
                blocking.append(f"missing_or_unreadable_table:{table}")
        window: dict[str, Any] = {}
        try:
            row = conn.execute(
                """
                SELECT MIN(trade_date), MAX(trade_date), COUNT(DISTINCT trade_date), COUNT(DISTINCT ts_code)
                FROM daily_trading_data
                """
            ).fetchone()
            window = {
                "min_trade_date": str(row[0] or ""),
                "max_trade_date": str(row[1] or ""),
                "distinct_trade_dates": int(row[2] or 0),
                "distinct_symbols": int(row[3] or 0),
            }
            if window["distinct_trade_dates"] < 250:
                blocking.append("daily_window_too_short")
            if window["distinct_symbols"] < 3000:
                blocking.append("symbol_coverage_too_low")
            lag = _calendar_lag_days(str(window["max_trade_date"] or ""))
            window["calendar_lag_days"] = lag
            if lag is not None and lag > int(max_lag_days):
                blocking.append(f"daily_data_stale:{lag}>{max_lag_days}")
        except Exception as exc:
            window = {"error": str(exc)}
            blocking.append("daily_window_unreadable")
    finally:
        conn.close()
    return {
        "passed": not blocking,
        "blocking_reasons": blocking,
        "db_path": str(db_path),
        "size_bytes": db_path.stat().st_size,
        "tables": tables,
        "daily_window": window,
    }


def _calendar_lag_days(compact_date: str) -> int | None:
    raw = str(compact_date or "").replace("-", "")
    if len(raw) != 8 or not raw.isdigit():
        return None
    try:
        then = time.strptime(raw, "%Y%m%d")
        today = time.localtime()
        then_sec = time.mktime((then.tm_year, then.tm_mon, then.tm_mday, 0, 0, 0, 0, 0, -1))
        today_sec = time.mktime((today.tm_year, today.tm_mon, today.tm_mday, 0, 0, 0, 0, 0, -1))
        return max(0, int((today_sec - then_sec) // 86400))
    except Exception:
        return None


def _load_top5_manifest(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"passed": False, "blocking_reasons": [f"manifest_unreadable:{exc}"], "path": str(path)}
    gate = payload.get("top5_canary_gate") if isinstance(payload.get("top5_canary_gate"), dict) else {}
    blocking = []
    if gate.get("passed") is not True:
        blocking.append("top5_canary_gate_not_passed")
    if not str(payload.get("csv") or "").strip():
        blocking.append("manifest_csv_missing")
    return {
        "passed": not blocking,
        "blocking_reasons": blocking,
        "path": str(path),
        "written_at": payload.get("written_at"),
        "trade_date_compact": payload.get("trade_date_compact"),
        "competition_run_id": payload.get("competition_run_id"),
        "top5_canary_gate": gate,
        "csv": payload.get("csv"),
        "markdown": payload.get("markdown"),
    }


def _http_health(url: str, *, timeout: int) -> dict[str, Any]:
    if not url:
        return {"passed": True, "skipped": True}
    try:
        req = Request(url, headers={"User-Agent": "airivo-daily-evidence-pipeline"})
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read(512).decode("utf-8", errors="replace")
            return {"passed": 200 <= int(resp.status) < 500, "status": int(resp.status), "body_sample": body}
    except Exception as exc:
        return {"passed": False, "blocking_reasons": [str(exc)], "url": url}


def main() -> int:
    parser = argparse.ArgumentParser(description="Run daily v9 evidence -> Top5 audit -> manifest verification pipeline.")
    parser.add_argument("--db-path", default=os.getenv("PERMANENT_DB_PATH", ""))
    parser.add_argument("--date-to", default="")
    parser.add_argument("--output-dir", default="logs/openclaw/daily_evidence_pipeline")
    parser.add_argument("--python", default=os.getenv("TOP5_AUDIT_PYTHON", ""))
    parser.add_argument("--max-db-lag-days", type=int, default=5)
    parser.add_argument("--site-health-url", default=os.getenv("AIRIVO_SITE_HEALTH_URL", ""))
    parser.add_argument("--record-planned-observations", action="store_true", default=os.getenv("AIRIVO_RECORD_PLANNED_OBSERVATIONS", "") == "1")
    parser.add_argument("--require-execution-closure", action="store_true", default=os.getenv("AIRIVO_REQUIRE_EXECUTION_CLOSURE", "") == "1")
    parser.add_argument("--require-no-stale-open", action="store_true", default=os.getenv("AIRIVO_REQUIRE_NO_STALE_OPEN", "") == "1")
    parser.add_argument("--skip-evidence", action="store_true")
    parser.add_argument("--skip-top5-chain", action="store_true")
    args = parser.parse_args()

    py = Path(args.python).expanduser() if args.python else ROOT / ".venv/bin/python"
    if not py.exists():
        py = Path(sys.executable)
    dbp = Path(args.db_path).expanduser() if args.db_path else default_db_path()
    output_dir = ROOT / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    env["PERMANENT_DB_PATH"] = str(dbp)
    env["OPENCLAW_DB_PATH"] = str(dbp)
    env["AIRIVO_DB_PATH"] = str(dbp)

    checks: list[dict[str, Any]] = []
    health = _db_health(dbp, max_lag_days=args.max_db_lag_days)
    checks.append({"name": "data_health", **health})

    if health.get("passed") is True and not args.skip_evidence:
        cmd = [
            str(py),
            "tools/archive/research/all_strategy_evidence_run.py",
            "--strategies",
            "v9,stable",
            "--offline-stock-limit",
            "300",
            "--scan-limit",
            "30",
            "--sweep-max-runs",
            "2",
            "--per-run-timeout-sec",
            "240",
            "--no-research-only",
        ]
        if args.date_to:
            cmd.extend(["--date-to", args.date_to])
        checks.append({"name": "v9_stable_evidence", **_run(cmd, env=env, timeout=1800)})

    if not args.skip_top5_chain:
        checks.append({"name": "top5_audit_chain", **_run(["bash", "tools/run_top5_competition_audit_then_gate.sh"], env=env, timeout=1800)})

    manifest_path = ROOT / "exports/top5_trader_brief_latest_manifest.json"
    manifest_check = _load_top5_manifest(manifest_path)
    checks.append({"name": "top5_manifest", **manifest_check})
    if args.record_planned_observations and manifest_check.get("passed") is True:
        checks.append(
            {
                "name": "record_planned_observations",
                **_run(
                    [
                        str(py),
                        "tools/record_top5_execution_observation.py",
                        "--manifest",
                        str(manifest_path),
                        "--operator",
                        "daily_v9_evidence_pipeline",
                        "--status",
                        "planned",
                    ],
                    env=env,
                    timeout=120,
                ),
            }
        )
    observation_cmd = [
        str(py),
        "tools/check_top5_execution_observation_completeness.py",
        "--ledger",
        "logs/openclaw/top5_execution_observations.jsonl",
        "--open-output-csv",
        "exports/top5_execution_open_observations.csv",
    ]
    if args.require_execution_closure:
        observation_cmd.extend(["--fail-on-open", "--fail-on-quality"])
    checks.append({"name": "execution_observation_completeness", **_run(observation_cmd, env=env, timeout=120)})
    checks.append(
        {
            "name": "execution_evidence_summary",
            **_run(
                [
                    str(py),
                    "tools/build_top5_execution_evidence_summary.py",
                    "--ledger",
                    "logs/openclaw/top5_execution_observations.jsonl",
                    "--output-json",
                    "exports/top5_execution_evidence_summary.json",
                    "--output-md",
                    "exports/top5_execution_evidence_summary.md",
                    "--min-closure-rate",
                    "0.95",
                ],
                env=env,
                timeout=120,
            ),
        }
    )
    ops_sla_cmd = [
        str(py),
        "tools/check_top5_execution_ops_sla.py",
        "--ledger",
        "logs/openclaw/top5_execution_observations.jsonl",
        "--output-json",
        "exports/top5_execution_ops_sla.json",
        "--output-md",
        "exports/top5_execution_ops_sla.md",
    ]
    if args.require_no_stale_open:
        ops_sla_cmd.append("--fail-on-stale")
    checks.append({"name": "execution_ops_sla", **_run(ops_sla_cmd, env=env, timeout=120)})
    checks.append(
        {
            "name": "v9_canary_promotion_readiness",
            **_run(
                [
                    str(py),
                    "tools/evaluate_v9_canary_promotion_readiness.py",
                    "--ledger",
                    "logs/openclaw/top5_execution_observations.jsonl",
                    "--import-dir",
                    "logs/openclaw/top5_execution_imports",
                    "--output-json",
                    "exports/v9_canary_promotion_readiness.json",
                    "--output-md",
                    "exports/v9_canary_promotion_readiness.md",
                ],
                env=env,
                timeout=120,
            ),
        }
    )
    checks.append(
        {
            "name": "top5_execution_court_record",
            **_run(
                [
                    str(py),
                    "tools/build_top5_execution_court_record.py",
                    "--root",
                    ".",
                    "--output-json",
                    "exports/top5_execution_court_record.json",
                    "--output-md",
                    "exports/top5_execution_court_record.md",
                ],
                env=env,
                timeout=120,
            ),
        }
    )
    checks.append({"name": "page_health", **_http_health(args.site_health_url, timeout=15)})

    blocking = [c for c in checks if c.get("passed") is not True]
    payload = {
        "artifact_version": PIPELINE_VERSION,
        "created_at": _now_text(),
        "passed": not blocking,
        "blocking_checks": [str(c.get("name") or "") for c in blocking],
        "db_path": str(dbp),
        "checks": checks,
        "policy": {
            "strategy_scope": "v9_canary_only_for_top5;stable_shadow;v8_combo_shadow;v5_baseline;v6_v7_not_production",
            "failure_display": "if pipeline fails, UI must present current Top5 as not executable",
            "execution_evidence": "planned observations are recorded automatically; closure can be promoted from audit-only to hard gate with AIRIVO_REQUIRE_EXECUTION_CLOSURE=1",
            "execution_ops_sla": "stale open observations are audited; hard gate can be enabled with AIRIVO_REQUIRE_NO_STALE_OPEN=1",
        },
    }
    stamp = f"{time.strftime('%Y%m%d_%H%M%S')}_{time.time_ns() % 1_000_000_000:09d}"
    path = output_dir / f"daily_v9_evidence_pipeline_{stamp}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"artifact": str(path), "passed": payload["passed"], "blocking_checks": payload["blocking_checks"]}, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if payload["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
