#!/usr/bin/env python3
"""Safety and stability audit for v49 + OpenClaw stack."""

from __future__ import annotations

import argparse
import glob
import json
import os
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str
    severity: str = "info"


def _run(cmd: List[str], timeout: int = 120) -> Tuple[int, str]:
    p = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True, timeout=timeout)
    out = (p.stdout or "") + ("\n" + p.stderr if p.stderr else "")
    return p.returncode, out.strip()


def _resolve_db_path() -> Optional[Path]:
    candidates = [
        os.getenv("PERMANENT_DB_PATH", "").strip(),
        str(ROOT / "permanent_stock_database.db"),
        str(ROOT / "permanent_stock_database.backup.db"),
        "/Users/mac/QLIB/permanent_stock_database.db",
    ]
    for p in candidates:
        if p and Path(p).exists():
            return Path(p).resolve()
    return None


def _check_db(db_path: Optional[Path]) -> CheckResult:
    if not db_path:
        return CheckResult("db_path", False, "database not found", "critical")
    try:
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {r[0] for r in cur.fetchall()}
        required = {"stock_basic", "daily_trading_data"}
        miss = sorted(required - tables)
        if miss:
            conn.close()
            return CheckResult("db_tables", False, f"missing tables: {miss}", "critical")
        cur.execute("SELECT MAX(trade_date) FROM daily_trading_data")
        latest = cur.fetchone()[0]
        cur.execute(
            """
            WITH last AS (
              SELECT ts_code, MAX(trade_date) AS last_date
              FROM daily_trading_data
              GROUP BY ts_code
            )
            SELECT
              COUNT(*) AS total_symbols,
              SUM(CASE WHEN last_date = ? THEN 1 ELSE 0 END) AS fresh_symbols,
              SUM(CASE WHEN last_date < ? THEN 1 ELSE 0 END) AS stale_symbols
            FROM last
            """,
            (latest, latest),
        )
        row = cur.fetchone() or (0, 0, 0)
        total_symbols = int(row[0] or 0)
        fresh_symbols = int(row[1] or 0)
        stale_symbols = int(row[2] or 0)
        conn.close()
        if not latest:
            return CheckResult("db_freshness", False, "daily_trading_data has no trade_date", "high")
        if total_symbols <= 0:
            return CheckResult("db_freshness", False, "daily_trading_data has no symbols", "high")

        stale_ratio = stale_symbols / max(1, total_symbols)
        detail = (
            f"latest trade_date={latest}, symbols={total_symbols}, "
            f"fresh={fresh_symbols}, stale={stale_symbols} ({stale_ratio:.1%})"
        )
        if stale_symbols >= 100 or stale_ratio >= 0.05:
            return CheckResult("db_freshness", False, detail, "high")
        if stale_symbols > 0:
            return CheckResult("db_freshness", False, detail, "medium")
        return CheckResult("db_freshness", True, detail)
    except Exception as e:
        return CheckResult("db_check", False, str(e), "critical")


def _check_compile() -> CheckResult:
    targets = [
        "终极量价暴涨系统_v49.0_长期稳健版.py",
        "openclaw/run_daily.py",
        "openclaw/runtime/v49_handlers.py",
        "openclaw/adapters/v49_adapter.py",
        "openclaw/assistant/stock_qa.py",
    ]
    cmd = [sys.executable, "-m", "py_compile", *targets]
    code, out = _run(cmd, timeout=180)
    if code != 0:
        return CheckResult("py_compile", False, out[-1200:], "critical")
    return CheckResult("py_compile", True, "key modules compile successfully")


def _check_openclaw_demo() -> CheckResult:
    cmd = [
        sys.executable,
        "openclaw/run_daily.py",
        "--use-demo",
        "--strategy",
        "v6",
        "--output-dir",
        "logs/openclaw/audit_demo",
    ]
    code, out = _run(cmd, timeout=180)
    if code != 0:
        return CheckResult("openclaw_demo", False, out[-1200:], "critical")
    return CheckResult("openclaw_demo", True, "demo orchestration success")


def _check_openclaw_real(skip_real: bool) -> CheckResult:
    if skip_real:
        return CheckResult("openclaw_real", True, "skipped by --skip-real")
    cmd = [
        sys.executable,
        "openclaw/run_daily.py",
        "--strategy",
        "v6",
        "--offline-stock-limit",
        "50",
        "--sample-size",
        "60",
        "--score-threshold",
        "70",
        "--output-dir",
        "logs/openclaw/audit_real",
    ]
    try:
        code, out = _run(cmd, timeout=420)
    except subprocess.TimeoutExpired:
        return CheckResult("openclaw_real", False, "timed out (>420s)", "high")
    if code != 0:
        return CheckResult("openclaw_real", False, out[-1200:], "high")
    return CheckResult("openclaw_real", True, "real orchestration completed")


def _read_latest_summary(path_glob: str) -> Dict[str, Any]:
    files = sorted(glob.glob(path_glob), reverse=True)
    if not files:
        return {}
    try:
        with open(files[0], "r", encoding="utf-8") as f:
            data = json.load(f)
        data["_source_file"] = files[0]
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _check_sensitive_tracked() -> CheckResult:
    code, out = _run(["git", "ls-files"])
    if code != 0:
        return CheckResult("tracked_sensitive", False, "failed to list tracked files", "medium")
    flagged = []
    for line in out.splitlines():
        full = ROOT / line
        if not full.exists():
            # Ignore historical tracked entries already deleted in worktree.
            continue
        low = line.lower()
        if any(k in low for k in ["token", "password", "secret", ".env"]):
            flagged.append(line)
    if not flagged:
        return CheckResult("tracked_sensitive", True, "no sensitive-like tracked filenames")
    return CheckResult(
        "tracked_sensitive",
        False,
        f"found {len(flagged)} potentially sensitive tracked files",
        "high",
    )


def _check_risky_scripts() -> CheckResult:
    target = ROOT / "start_v49_full.sh"
    if not target.exists():
        return CheckResult("risky_scripts", True, "start_v49_full.sh not present")
    txt = target.read_text(encoding="utf-8", errors="ignore")
    risky = ("killall -9" in txt) or ("pkill -9" in txt)
    if "kill -9" in txt and "FORCE_KILL" not in txt:
        risky = True
    if risky:
        return CheckResult("risky_scripts", False, "start_v49_full.sh contains force-kill commands", "high")
    return CheckResult("risky_scripts", True, "no force-kill commands in start_v49_full.sh")


def _check_single_source_integrity() -> CheckResult:
    root_main = ROOT / "终极量价暴涨系统_v49.0_长期稳健版.py"
    mirror_main = ROOT / "v49" / "终极量价暴涨系统_v49.0_长期稳健版.py"
    adapter_file = ROOT / "openclaw" / "adapters" / "v49_adapter.py"
    run_daily_file = ROOT / "openclaw" / "run_daily.py"

    if not root_main.exists():
        return CheckResult("single_source_integrity", False, "root main file missing", "critical")
    if not mirror_main.exists():
        return CheckResult("single_source_integrity", False, "v49 mirror file missing", "high")
    if not adapter_file.exists() or not run_daily_file.exists():
        return CheckResult("single_source_integrity", False, "openclaw adapter files missing", "high")

    mirror_text = mirror_main.read_text(encoding="utf-8", errors="ignore")
    if "Compatibility launcher" not in mirror_text or "runpy.run_path" not in mirror_text:
        return CheckResult(
            "single_source_integrity",
            False,
            "v49 mirror is not compatibility launcher; potential dual-source drift",
            "high",
        )

    adapter_text = adapter_file.read_text(encoding="utf-8", errors="ignore")
    if "DEFAULT_MODULE_PATH" not in adapter_text:
        return CheckResult("single_source_integrity", False, "adapter missing DEFAULT_MODULE_PATH guard", "medium")

    run_daily_text = run_daily_file.read_text(encoding="utf-8", errors="ignore")
    if "DEFAULT_V49_MODULE_PATH" not in run_daily_text:
        return CheckResult("single_source_integrity", False, "run_daily missing default root module path", "medium")

    return CheckResult("single_source_integrity", True, "single source routing is enforced")


def _check_hardcoded_legacy_paths() -> CheckResult:
    code, out = _run(
        [
            "rg",
            "-n",
            "/Users/mac/QLIB/终极量价暴涨系统_v49.0_长期稳健版.py",
            "--glob",
            "*.py",
            "--glob",
            "*.sh",
            "--glob",
            "*.md",
        ]
    )
    if code not in (0, 1):
        return CheckResult("legacy_path_refs", False, "failed to scan legacy path refs", "medium")

    lines = [x for x in out.splitlines() if x.strip()] if out else []
    # Allow docs/history to contain old path, but flag for hygiene if too many.
    if len(lines) <= 5:
        return CheckResult("legacy_path_refs", True, f"legacy refs count={len(lines)}")
    return CheckResult(
        "legacy_path_refs",
        False,
        f"legacy refs count={len(lines)} (consider gradual cleanup)",
        "medium",
    )


def _check_hardcoded_tokens() -> CheckResult:
    main_file = ROOT / "终极量价暴涨系统_v49.0_长期稳健版.py"
    if not main_file.exists():
        return CheckResult("hardcoded_tokens", False, "main file missing", "high")
    txt = main_file.read_text(encoding="utf-8", errors="ignore")
    # detect non-empty default token assignment
    marker = 'DEFAULT_TUSHARE_TOKEN = "'
    idx = txt.find(marker)
    if idx < 0:
        return CheckResult("hardcoded_tokens", True, "token marker not found")
    tail = txt[idx + len(marker):]
    val = tail.split('"', 1)[0]
    if val.strip():
        return CheckResult("hardcoded_tokens", False, "non-empty DEFAULT_TUSHARE_TOKEN found", "critical")
    return CheckResult("hardcoded_tokens", True, "no hardcoded default token")


def _to_risk_contract(checks: List[CheckResult], summaries: List[Dict[str, Any]]) -> Dict[str, Any]:
    triggered: List[str] = []
    evidence: Dict[str, Any] = {"checks": []}

    has_critical_fail = False
    has_high_fail = False
    for c in checks:
        evidence["checks"].append(
            {"name": c.name, "ok": c.ok, "detail": c.detail, "severity": c.severity}
        )
        if not c.ok:
            if c.severity in {"high", "critical"}:
                triggered.append(c.name)
            if c.severity == "critical":
                has_critical_fail = True
            if c.severity == "high":
                has_high_fail = True

    for sm in summaries:
        if not sm:
            continue
        risk = (sm.get("risk") or {}).get("risk_level")
        if risk:
            evidence.setdefault("openclaw_risks", []).append(
                {"risk_level": risk, "source": sm.get("_source_file", "")}
            )
        if str(risk).lower() == "red":
            triggered.append("openclaw_runtime_red")
            has_high_fail = True

    if has_critical_fail:
        level = "red"
    elif has_high_fail:
        level = "orange"
    elif triggered:
        level = "yellow"
    else:
        level = "green"

    actions = {
        "green": ["keep current mode"],
        "yellow": ["review warnings before next run"],
        "orange": ["reduce aggressiveness", "disable auto-publish", "manual review required"],
        "red": ["stop automation", "manual review required", "fix critical checks before rerun"],
    }[level]

    return {
        "risk_level": level,
        "triggered_rules": sorted(set(triggered)),
        "recommended_actions": actions,
        "evidence": evidence,
        "generated_at": datetime.now().isoformat(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit v49 + OpenClaw safety/stability")
    parser.add_argument("--skip-real", action="store_true", help="skip real openclaw run")
    parser.add_argument("--output-dir", default="logs/openclaw", help="report output directory")
    args = parser.parse_args()

    checks: List[CheckResult] = []
    db_path = _resolve_db_path()
    checks.append(CheckResult("db_path", bool(db_path), f"db_path={db_path or 'N/A'}", "critical" if not db_path else "info"))
    checks.append(_check_db(db_path))
    checks.append(_check_compile())
    checks.append(_check_openclaw_demo())
    checks.append(_check_openclaw_real(args.skip_real))
    checks.append(_check_sensitive_tracked())
    checks.append(_check_risky_scripts())
    checks.append(_check_single_source_integrity())
    checks.append(_check_hardcoded_legacy_paths())
    checks.append(_check_hardcoded_tokens())

    demo_summary = _read_latest_summary("logs/openclaw/audit_demo/run_summary_*.json")
    summaries: List[Dict[str, Any]] = [demo_summary]
    if not args.skip_real:
        summaries.append(_read_latest_summary("logs/openclaw/audit_real/run_summary_*.json"))
    contract = _to_risk_contract(checks, summaries)

    out_dir = ROOT / args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = out_dir / f"safety_audit_{ts}.json"
    out_md = out_dir / f"safety_audit_{ts}.md"

    out_json.write_text(json.dumps(contract, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# v49 + OpenClaw Safety Audit",
        "",
        f"- generated_at: {contract['generated_at']}",
        f"- risk_level: {contract['risk_level']}",
        f"- triggered_rules: {', '.join(contract['triggered_rules']) or '(none)'}",
        "",
        "## Recommended Actions",
    ]
    for a in contract["recommended_actions"]:
        lines.append(f"- {a}")
    lines.extend(["", "## Checks"])
    for c in checks:
        mark = "PASS" if c.ok else "FAIL"
        lines.append(f"- [{mark}] {c.name}: {c.detail} (severity={c.severity})")
    out_md.write_text("\n".join(lines), encoding="utf-8")

    print(json.dumps({"risk_level": contract["risk_level"], "json": str(out_json), "md": str(out_md)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
