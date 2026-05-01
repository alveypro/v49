#!/usr/bin/env python3
"""全链路健康检查 — 由真实部署调度器执行，覆盖所有关键子系统。

检查项:
  0. Python 运行时版本 (>=3.11) 与解释器路径
  1. DB 可达 & 数据新鲜度
  2. 缓存目录完整性
  3. 关键 Python 模块可导入
  4. 定时任务/服务管理器状态
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
import platform
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

try:
    from strategies.center_config import load_center_config
except Exception:
    load_center_config = None  # type: ignore[assignment]


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


def check_python_runtime() -> Check:
    c = Check("python_runtime", "Python运行时")
    try:
        current = sys.executable
        ver = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        try:
            from openclaw.paths import venv_python
            preferred = str(venv_python())
        except Exception:
            preferred = ""
        if sys.version_info < (3, 11):
            c.fail(f"当前解释器过低: {ver} ({current})")
            return c
        if preferred and Path(preferred).exists() and Path(current).resolve() != Path(preferred).resolve():
            c.warn(f"当前={current}({ver})，建议使用={preferred}")
            return c
        c.ok(f"解释器={current} 版本={ver}")
    except Exception as e:
        c.fail(f"运行时检查失败: {e}")
    return c


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
        "strategies.center_config",
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


def _csv_env(name: str) -> List[str]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


def _service_manager() -> str:
    forced = os.getenv("OPENCLAW_HEALTH_SERVICE_MANAGER", "").strip().lower()
    if forced in {"systemd", "launchd", "none"}:
        return forced
    if platform.system().lower() == "darwin" and shutil.which("launchctl"):
        return "launchd"
    if shutil.which("systemctl"):
        return "systemd"
    return "none"


def check_timed_services() -> Check:
    c = Check("timed_services", "定时任务状态")
    manager = _service_manager()
    if manager == "systemd":
        units = _csv_env("OPENCLAW_HEALTH_SYSTEMD_UNITS") or [
            "openclaw-data-updater.timer",
            "openclaw-daily-pipeline.timer",
            "openclaw-auto-evolve-opt.timer",
            "openclaw-tracking-guard.timer",
        ]
        missing = []
        inactive = []
        for unit in units:
            try:
                r = subprocess.run(
                    ["systemctl", "is-enabled", unit],
                    capture_output=True, text=True, timeout=5,
                )
                if r.returncode != 0:
                    missing.append(unit)
                    continue
                r = subprocess.run(
                    ["systemctl", "is-active", unit],
                    capture_output=True, text=True, timeout=5,
                )
                if r.returncode != 0:
                    inactive.append(unit)
            except Exception:
                inactive.append(unit)
        if missing or inactive:
            parts = []
            if missing:
                parts.append(f"未启用: {', '.join(missing)}")
            if inactive:
                parts.append(f"未活跃: {', '.join(inactive)}")
            c.warn("; ".join(parts))
        else:
            c.ok(f"systemd timers active: {', '.join(units)}")
        return c

    if manager == "launchd":
        return check_launchd()

    c.warn("未识别 systemd/launchd，无法验证定时任务")
    return c


def check_launchd() -> Check:
    c = Check("timed_services", "定时任务状态")
    labels = _csv_env("OPENCLAW_HEALTH_LAUNCHD_LABELS") or [
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
        c.ok(f"launchd labels active: {', '.join(labels)}")
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
    ld = _run_summary_dir()
    summaries = sorted(ld.glob("run_summary_*.json"), reverse=True)
    if not summaries:
        c.warn("未找到 run_summary")
        return c
    latest = summaries[0]
    try:
        center_cfg = _load_strategy_center_config()
        data = json.loads(latest.read_text(encoding="utf-8"))
        risk = str(data.get("risk", {}).get("risk_level", "unknown")).lower()
        strat = data.get("strategy", "?")
        scan_status = data.get("scan", {}).get("status", "?")
        bt_status = data.get("backtest", {}).get("status", "?")
        bt_summary = (((data.get("backtest") or {}).get("result") or {}).get("summary") or {})
        market_stats = ((data.get("risk") or {}).get("evidence") or {}).get("market_stats") or bt_summary
        limits = _risk_limits_for_strategy(strat, center_cfg)
        breaches = _metric_breaches(market_stats, limits)
        age_h = (datetime.now() - datetime.fromtimestamp(latest.stat().st_mtime)).total_seconds() / 3600
        stale_warn_h = int(((center_cfg.get("health_gate") or {}).get("stale_run_hours_warn", 48)) or 48)
        stale_fail_h = int(((center_cfg.get("health_gate") or {}).get("stale_run_hours_fail", 96)) or 96)
        info = (
            f"策略={strat} 扫描={scan_status} 回测={bt_status} 风险={risk} "
            f"({age_h:.0f}h前) 指标={_brief_metric_line(market_stats)}"
        )
        if risk == "red" or age_h > stale_fail_h:
            c.fail(info)
        elif breaches:
            c.warn(f"{info} 阈值观察={','.join(breaches)}")
        elif risk == "orange" or age_h > stale_warn_h:
            c.warn(info)
        else:
            c.ok(info)
    except Exception as e:
        c.warn(f"解析失败: {e}")
    return c


def _run_summary_dir() -> Path:
    env = os.getenv("OPENCLAW_RUN_SUMMARY_DIR", "").strip()
    candidates = []
    if env:
        candidates.append(Path(env))
    candidates.append(log_dir())
    candidates.append(Path("/opt/openclaw/app/logs/openclaw"))
    candidates.append(Path("/opt/openclaw/logs/openclaw"))
    for candidate in candidates:
        if candidate.exists() and list(candidate.glob("run_summary_*.json")):
            return candidate
    return candidates[0]


def _load_strategy_center_config() -> Dict[str, Any]:
    if load_center_config is None:
        return {}
    try:
        cfg_path = ROOT / "openclaw" / "config" / "strategy_center.yaml"
        return load_center_config(cfg_path)
    except Exception:
        return {}


def _risk_limits_for_strategy(strategy: str, center_cfg: Dict[str, Any]) -> Dict[str, float]:
    # Fallback limits aligned with run_daily defaults.
    base = {"win_rate_min": 0.45, "max_drawdown_max": 0.12, "signal_density_min": 0.02}
    risk_overrides = center_cfg.get("risk_overrides") if isinstance(center_cfg, dict) else {}
    if not isinstance(risk_overrides, dict):
        return base
    this = risk_overrides.get(str(strategy), {})
    if not isinstance(this, dict):
        return base
    for k in ("win_rate_min", "max_drawdown_max", "signal_density_min"):
        if k in this:
            try:
                base[k] = float(this[k])
            except Exception:
                pass
    return base


def _metric_breaches(market_stats: Dict[str, Any], limits: Dict[str, float]) -> List[str]:
    out: List[str] = []
    try:
        win_rate = float(market_stats.get("win_rate", 0.0) or 0.0)
        max_dd_raw = market_stats.get("max_drawdown", 1.0)
        max_dd = 1.0 if max_dd_raw is None else float(max_dd_raw)
        density = float(market_stats.get("signal_density", 0.0) or 0.0)
    except Exception:
        return ["market_stats_unreadable"]

    if win_rate < float(limits.get("win_rate_min", 0.45)):
        out.append("win_rate")
    if max_dd > float(limits.get("max_drawdown_max", 0.12)):
        out.append("max_drawdown")
    if density < float(limits.get("signal_density_min", 0.02)):
        out.append("signal_density")
    return out


def _brief_metric_line(market_stats: Dict[str, Any]) -> str:
    try:
        wr = float(market_stats.get("win_rate", 0.0) or 0.0)
        max_dd_raw = market_stats.get("max_drawdown", 1.0)
        dd = 1.0 if max_dd_raw is None else float(max_dd_raw)
        sd = float(market_stats.get("signal_density", 0.0) or 0.0)
        return f"wr={wr:.3f},dd={dd:.3f},sd={sd:.3f}"
    except Exception:
        return "wr=?,dd=?,sd=?"


def check_port(port: int, label: str, label_cn: str) -> Check:
    c = Check(f"port_{port}", f"{label_cn}({port})")
    http_probe = {
        5101: "http://127.0.0.1:5101/health",
        8501: "http://127.0.0.1:8501/_stcore/health",
    }.get(int(port))
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=3):
            c.ok(f"{label} 在线")
            return c
    except PermissionError:
        # Some sandboxed Python runtimes deny localhost sockets even when the
        # service is healthy. Fall back to an HTTP probe before warning.
        pass
    except Exception:
        pass

    if http_probe:
        try:
            r = subprocess.run(
                ["curl", "-sS", "--max-time", "3", http_probe],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if r.returncode == 0 and r.stdout.strip():
                c.ok(f"{label} 在线")
                return c
        except Exception:
            pass

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
        check_python_runtime(),
        check_db(),
        check_cache(),
        check_imports(),
        check_timed_services(),
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
