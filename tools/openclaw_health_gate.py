#!/usr/bin/env python3
"""全链路健康检查 — 凌晨 01:50 由 launchd 执行，覆盖所有关键子系统。

检查项:
  1. DB 可达 & 数据新鲜度
  2. 缓存目录完整性
  3. 关键 Python 模块可导入
  4. launchd 任务状态
  5. 磁盘空间
  6. 最近 run_summary 是否成功
  7. 端口服务存活 (5101/8501)
  8. Git 哈希一致性 (local vs remote)

输出: JSON 报告 → logs/doctor/health_gate_YYYYMMDD.json
退出码: 0=全绿, 1=有警告, 2=有红灯
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    from openclaw.paths import project_root, db_path, cache_dir, log_dir, doctor_log_dir
except ImportError:
    project_root = lambda: ROOT
    db_path = lambda p=None: ROOT / "permanent_stock_database.db"
    cache_dir = lambda: ROOT / "cache_v9"
    log_dir = lambda: ROOT / "logs" / "openclaw"
    doctor_log_dir = lambda: ROOT / "logs" / "doctor"


class Check:
    def __init__(self, name: str, name_cn: str):
        self.name = name
        self.name_cn = name_cn
        self.status = "green"
        self.detail = ""

    def warn(self, msg: str):
        self.status = "orange"
        self.detail = msg

    def fail(self, msg: str):
        self.status = "red"
        self.detail = msg

    def ok(self, msg: str = ""):
        self.status = "green"
        self.detail = msg

    def to_dict(self) -> Dict[str, str]:
        return {"name": self.name, "name_cn": self.name_cn, "status": self.status, "detail": self.detail}


def check_db() -> Check:
    c = Check("db_health", "数据库健康")
    try:
        p = db_path()
        conn = sqlite3.connect(str(p), timeout=10)
        cur = conn.cursor()
        cur.execute("SELECT MAX(trade_date) FROM daily_trading_data")
        row = cur.fetchone()
        conn.close()
        if not row or not row[0]:
            c.warn("数据表为空")
            return c
        last = str(row[0])
        if "-" in last:
            d = datetime.strptime(last[:10], "%Y-%m-%d").date()
        else:
            d = datetime.strptime(last[:8], "%Y%m%d").date()
        stale = (datetime.now().date() - d).days
        weekday = datetime.now().weekday()
        threshold = 4 if weekday in (5, 6, 0) else 2
        if stale > threshold:
            c.warn(f"数据滞后 {stale} 天 (最新={last})")
        else:
            c.ok(f"最新数据={last}, 滞后={stale}天")
    except Exception as e:
        c.fail(f"DB不可达: {e}")
    return c


def check_cache() -> Check:
    c = Check("cache_integrity", "缓存完整性")
    cd = Path(cache_dir())
    if not cd.exists():
        c.warn(f"缓存目录不存在: {cd}")
        return c
    meta_files = list(cd.glob("*.meta.json"))
    csv_files = list(cd.glob("*.csv"))
    orphans = []
    for m in meta_files:
        expected_csv = m.with_suffix("").with_suffix(".csv")
        if not expected_csv.exists():
            orphans.append(m.name)
    if orphans:
        c.warn(f"{len(orphans)} 个元数据无对应CSV: {orphans[:3]}")
    else:
        c.ok(f"缓存={len(csv_files)}个CSV, {len(meta_files)}个元数据")
    return c


def check_imports() -> Check:
    c = Check("module_imports", "关键模块导入")
    failures = []
    modules = [
        "data.dao",
        "openclaw.paths",
        "openclaw.runtime.v49_handlers",
        "strategies.registry",
        "risk.engine",
    ]
    for mod in modules:
        try:
            __import__(mod)
        except Exception as e:
            failures.append(f"{mod}: {type(e).__name__}")
    if failures:
        c.fail("; ".join(failures))
    else:
        c.ok(f"{len(modules)} 模块全部可导入")
    return c


def check_launchd() -> Check:
    c = Check("launchd", "定时任务状态")
    labels = [
        "com.airivo.auto-evolve",
        "com.airivo.syncdb",
        "com.airivo.openclaw.data-update",
        "com.airivo.openclaw.data-optimize",
        "com.airivo.v49.streamlit",
        "com.airivo.openclaw.stock-agent",
    ]
    uid = os.getuid()
    missing = []
    for label in labels:
        try:
            r = subprocess.run(
                ["launchctl", "print", f"gui/{uid}/{label}"],
                capture_output=True, text=True, timeout=5,
            )
            if r.returncode != 0:
                missing.append(label.split(".")[-1])
        except Exception:
            missing.append(label.split(".")[-1])
    if missing:
        c.warn(f"未加载: {', '.join(missing)}")
    else:
        c.ok(f"{len(labels)} 个任务全部活跃")
    return c


def check_disk() -> Check:
    c = Check("disk_space", "磁盘空间")
    total, used, free = shutil.disk_usage(str(project_root()))
    free_gb = free / (1024 ** 3)
    pct_used = used / total * 100
    if free_gb < 2:
        c.fail(f"磁盘剩余仅 {free_gb:.1f}GB")
    elif free_gb < 10:
        c.warn(f"磁盘剩余 {free_gb:.1f}GB")
    else:
        c.ok(f"可用 {free_gb:.1f}GB ({pct_used:.0f}% 已用)")
    return c


def check_recent_run() -> Check:
    c = Check("last_run_summary", "最近运行摘要")
    ld = log_dir()
    summaries = sorted(ld.glob("run_summary_*.json"), reverse=True)
    if not summaries:
        c.warn("未找到 run_summary")
        return c
    latest = summaries[0]
    try:
        data = json.loads(latest.read_text(encoding="utf-8"))
        risk = data.get("risk", {}).get("risk_level", "unknown")
        strat = data.get("strategy", "?")
        scan_status = data.get("scan", {}).get("status", "?")
        bt_status = data.get("backtest", {}).get("status", "?")
        age_h = (datetime.now() - datetime.fromtimestamp(latest.stat().st_mtime)).total_seconds() / 3600
        info = f"策略={strat} 扫描={scan_status} 回测={bt_status} 风险={risk} ({age_h:.0f}h前)"
        if risk == "red":
            c.fail(info)
        elif risk == "orange" or age_h > 48:
            c.warn(info)
        else:
            c.ok(info)
    except Exception as e:
        c.warn(f"解析失败: {e}")
    return c


def check_port(port: int, label: str, label_cn: str) -> Check:
    c = Check(f"port_{port}", f"{label_cn}({port})")
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=3):
            c.ok(f"{label} 在线")
    except Exception:
        c.warn(f"{label} 未响应")
    return c


def check_git_hash() -> Check:
    c = Check("git_hash", "Git哈希一致性")
    try:
        local = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True, text=True, cwd=str(project_root()), timeout=5,
        )
        if local.returncode != 0:
            c.warn("非Git仓库或git不可用")
            return c
        local_hash = local.stdout.strip()[:12]

        remote = subprocess.run(
            ["git", "ls-remote", "origin", "HEAD"],
            capture_output=True, text=True, cwd=str(project_root()), timeout=15,
        )
        if remote.returncode != 0 or not remote.stdout.strip():
            c.warn(f"无法获取远端哈希 (local={local_hash})")
            return c
        remote_hash = remote.stdout.strip().split()[0][:12]

        if local_hash == remote_hash:
            c.ok(f"一致 {local_hash}")
        else:
            c.warn(f"不一致 local={local_hash} remote={remote_hash}")
    except Exception as e:
        c.warn(f"Git检查失败: {e}")
    return c


def run_all() -> Dict[str, Any]:
    checks: List[Check] = [
        check_db(),
        check_cache(),
        check_imports(),
        check_launchd(),
        check_disk(),
        check_recent_run(),
        check_port(5101, "stock-agent", "股票智能体"),
        check_port(8501, "streamlit-v49", "策略看板"),
        check_git_hash(),
    ]

    results = [c.to_dict() for c in checks]
    reds = sum(1 for c in checks if c.status == "red")
    oranges = sum(1 for c in checks if c.status == "orange")
    greens = sum(1 for c in checks if c.status == "green")

    overall = "red" if reds else ("orange" if oranges else "green")

    report = {
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "overall": overall,
        "summary_cn": f"🟢{greens} 🟡{oranges} 🔴{reds}",
        "checks": results,
    }

    out_dir = doctor_log_dir()
    out_path = out_dir / f"health_gate_{datetime.now().strftime('%Y%m%d')}.json"
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    _print_report(report)

    if reds:
        return report, 2
    if oranges:
        return report, 1
    return report, 0


def _print_report(report: Dict[str, Any]) -> None:
    icons = {"green": "🟢", "orange": "🟡", "red": "🔴"}
    print(f"\n{'=' * 60}")
    print(f"  OpenClaw 全链路健康检查  {report['ts']}")
    print(f"  总体: {icons.get(report['overall'], '?')} {report['overall'].upper()}")
    print(f"{'=' * 60}")
    for c in report["checks"]:
        icon = icons.get(c["status"], "?")
        print(f"  {icon} {c['name_cn']:<16s}  {c['detail']}")
    print(f"{'=' * 60}\n")


def _try_send_alert(report: Dict[str, Any]) -> None:
    """If overall is orange/red, attempt to send notification."""
    if report.get("overall") == "green":
        return
    try:
        from notification_service import send_notification
        title = f"[健康告警] OpenClaw {report['overall'].upper()}"
        body = report.get("summary_cn", "") + "\n"
        for c in report.get("checks", []):
            if c["status"] != "green":
                body += f"  - {c['name_cn']}: {c['detail']}\n"
        send_notification(title=title, body=body)
    except Exception:
        pass


if __name__ == "__main__":
    report, code = run_all()
    _try_send_alert(report)
    raise SystemExit(code)
