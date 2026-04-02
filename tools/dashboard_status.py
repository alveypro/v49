#!/usr/bin/env python3
"""中文化系统状态看板 — 终端输出 + 可选 Markdown 文件。

可被 Streamlit UI 调用, 也可独立运行:
  python tools/dashboard_status.py
  python tools/dashboard_status.py --md   # 输出 Markdown 到 logs/openclaw/dashboard.md
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

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
    from strategies.registry import STRATEGIES, production_strategies, experimental_strategies, primary_strategies
except ImportError:
    STRATEGIES = {}
    production_strategies = lambda: ["v9", "v8", "v5", "combo"]
    primary_strategies = lambda: ["v5"]
    experimental_strategies = lambda: ["v4", "v6", "v7", "stable", "ai"]

try:
    from strategies.center_config import (
        apply_risk_overrides,
        load_center_config,
        resolve_runtime_params,
        resolve_strategy_weight,
    )
except Exception:
    apply_risk_overrides = None  # type: ignore[assignment]
    load_center_config = None  # type: ignore[assignment]
    resolve_runtime_params = None  # type: ignore[assignment]
    resolve_strategy_weight = None  # type: ignore[assignment]


def _icon(status: str) -> str:
    return {"green": "🟢", "orange": "🟡", "yellow": "🟡", "red": "🔴"}.get(status, "⚪")


def _latest_health_report() -> Optional[Dict[str, Any]]:
    dd = doctor_log_dir()
    files = sorted(dd.glob("health_gate_*.json"), reverse=True)
    if not files:
        return None
    try:
        return json.loads(files[0].read_text(encoding="utf-8"))
    except Exception:
        return None


def _latest_regression_report() -> Optional[Dict[str, Any]]:
    ld = log_dir()
    files = sorted(ld.glob("regression_gate_*.json"), reverse=True)
    if not files:
        return None
    try:
        return json.loads(files[0].read_text(encoding="utf-8"))
    except Exception:
        return None


def _latest_run_summary() -> Optional[Dict[str, Any]]:
    ld = log_dir()
    files = sorted(ld.glob("run_summary_*.json"), reverse=True)
    if not files:
        return None
    try:
        return json.loads(files[0].read_text(encoding="utf-8"))
    except Exception:
        return None


def _db_stats() -> Dict[str, Any]:
    try:
        p = db_path()
        conn = sqlite3.connect(str(p), timeout=10)
        cur = conn.cursor()
        cur.execute("SELECT MAX(trade_date) FROM daily_trading_data")
        row = cur.fetchone()
        last = str(row[0]) if row and row[0] else "无"
        cur.execute("SELECT COUNT(DISTINCT ts_code) FROM daily_trading_data")
        stock_cnt = cur.fetchone()[0] or 0
        cur.execute("SELECT COUNT(*) FROM daily_trading_data")
        row_cnt = cur.fetchone()[0] or 0
        conn.close()
        size_mb = p.stat().st_size / (1024 * 1024)
        return {"最新日期": last, "股票数": stock_cnt, "记录数": f"{row_cnt:,}", "体积": f"{size_mb:.0f}MB"}
    except Exception as e:
        return {"错误": str(e)}


def _git_info() -> Dict[str, str]:
    try:
        h = subprocess.run(["git", "rev-parse", "--short=12", "HEAD"],
                           capture_output=True, text=True, cwd=str(project_root()), timeout=5)
        b = subprocess.run(["git", "branch", "--show-current"],
                           capture_output=True, text=True, cwd=str(project_root()), timeout=5)
        d = subprocess.run(["git", "log", "-1", "--format=%ci"],
                           capture_output=True, text=True, cwd=str(project_root()), timeout=5)
        return {
            "分支": b.stdout.strip() or "?",
            "哈希": h.stdout.strip() or "?",
            "提交时间": d.stdout.strip()[:19] or "?",
        }
    except Exception:
        return {"状态": "Git不可用"}


def _cache_stats() -> Dict[str, Any]:
    cd = Path(cache_dir())
    if not cd.exists():
        return {"状态": "目录不存在"}
    csvs = list(cd.glob("*.csv"))
    total_mb = sum(f.stat().st_size for f in csvs) / (1024 * 1024)
    return {"CSV文件数": len(csvs), "总计": f"{total_mb:.1f}MB"}


def _load_strategy_center() -> Dict[str, Any]:
    if load_center_config is None:
        return {}
    try:
        return load_center_config(ROOT / "openclaw" / "config" / "strategy_center.yaml")
    except Exception:
        return {}


def _effective_strategy_view(strategy: str, center_cfg: Dict[str, Any]) -> Dict[str, Any]:
    role = ""
    p = STRATEGIES.get(strategy)
    if p:
        role = str(getattr(p, "role", "") or "")

    runtime = {
        "score_threshold": getattr(p, "default_score_threshold", 0),
        "sample_size": getattr(p, "default_sample_size", 0),
        "holding_days": getattr(p, "default_holding_days", 0),
        "source": {"score_threshold": "registry_default", "sample_size": "registry_default", "holding_days": "registry_default"},
    }
    if resolve_runtime_params is not None:
        try:
            runtime = resolve_runtime_params(
                strategy=strategy,
                requested_score_threshold=None,
                requested_sample_size=None,
                requested_holding_days=None,
                center_config=center_cfg,
                project_root=ROOT,
            )
        except Exception:
            pass

    base_risk = {"win_rate_min": 0.45, "max_drawdown_max": 0.12, "signal_density_min": 0.02}
    risk = base_risk
    if apply_risk_overrides is not None:
        try:
            risk = apply_risk_overrides(strategy, base_risk, center_cfg)
        except Exception:
            risk = base_risk

    weight = 1.0
    if resolve_strategy_weight is not None:
        try:
            weight = resolve_strategy_weight(strategy, center_config=center_cfg, default=1.0)
        except Exception:
            weight = 1.0

    src = runtime.get("source", {}) if isinstance(runtime, dict) else {}
    src_tag = str(src.get("score_threshold", "unknown"))
    return {
        "strategy": strategy,
        "role": role or "-",
        "score_threshold": int(runtime.get("score_threshold", 0) or 0),
        "sample_size": int(runtime.get("sample_size", 0) or 0),
        "holding_days": int(runtime.get("holding_days", 0) or 0),
        "source": src_tag,
        "weight": float(weight),
        "risk_text": f"wr>={float(risk['win_rate_min']):.2f} dd<={float(risk['max_drawdown_max']):.2f} sd>={float(risk['signal_density_min']):.2f}",
    }


def build_dashboard() -> str:
    lines: List[str] = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines.append(f"# OpenClaw 系统状态看板")
    lines.append(f"*生成时间: {now}*\n")
    center_cfg = _load_strategy_center()

    # ─── 策略中心 ───
    lines.append("## 〇、策略中心")
    prod = production_strategies()
    primary = set(primary_strategies())
    exp = experimental_strategies()
    lines.append("")
    lines.append("**生产候选策略**")
    lines.append("")
    lines.append("| 策略 | 角色 | 阈值 | 样本 | 持仓天数 | 参数来源 | 权重 | 风控线 |")
    lines.append("|------|------|------|------|----------|----------|------|--------|")
    for k in prod:
        row = _effective_strategy_view(k, center_cfg)
        lines.append(
            f"| **{row['strategy']}{' (主生产)' if row['strategy'] in primary else ''}** | {row['role']} | {row['score_threshold']} | {row['sample_size']} | "
            f"{row['holding_days']} | {row['source']} | {row['weight']:.2f} | {row['risk_text']} |"
        )
    lines.append("")
    lines.append("**实验策略** (手动开关, 不影响主流程)")
    lines.append("")
    lines.append("| 策略 | 角色 | 状态 |")
    lines.append("|------|------|------|")
    for k in exp:
        p = STRATEGIES.get(k)
        if p:
            lines.append(f"| {k} | {p.role} | 🧪 实验 |")
    lines.append("")

    # ─── 健康检查 ───
    hr = _latest_health_report()
    lines.append("## 一、全链路健康检查")
    if hr:
        lines.append(f"- 总体: {_icon(hr['overall'])} **{hr['overall'].upper()}** ({hr.get('summary_cn', '')})")
        lines.append(f"- 检查时间: {hr.get('ts', '?')}")
        lines.append("")
        lines.append("| 检查项 | 状态 | 详情 |")
        lines.append("|--------|------|------|")
        for c in hr.get("checks", []):
            lines.append(f"| {c['name_cn']} | {_icon(c['status'])} | {c['detail']} |")
    else:
        lines.append("- ⚪ 尚无健康检查报告（请运行 `python tools/openclaw_health_gate.py`）")
    lines.append("")

    # ─── 回归验证 ───
    rr = _latest_regression_report()
    lines.append("## 二、默认组合回归验证")
    if rr:
        icon = "✅" if rr.get("all_passed") else "❌"
        lines.append(f"- 结果: {icon} {'全部通过' if rr.get('all_passed') else '存在失败'}")
        lines.append(f"- 策略: {', '.join(rr.get('strategies', []))}")
        lines.append(f"- 完成轮次: {rr.get('completed_rounds', '?')}/{rr.get('total_rounds', '?')}")
        lines.append(f"- 时间: {rr.get('ts', '?')[:19]}")
    else:
        lines.append("- ⚪ 尚无回归报告（请运行 `python tools/regression_combo_gate.py`）")
    lines.append("")

    # ─── 最近策略运行 ───
    rs = _latest_run_summary()
    lines.append("## 三、最近策略运行")
    if rs:
        strat = rs.get("strategy", "?")
        risk = rs.get("risk", {})
        scan = rs.get("scan", {})
        bt = rs.get("backtest", {})
        lines.append(f"- 策略: **{strat}**")
        lines.append(f"- 扫描: {scan.get('status', '?')} (选出 {len(rs.get('opportunities', []))} 只)")
        lines.append(f"- 回测: {bt.get('status', '?')}")
        risk_level = risk.get("risk_level", "?")
        lines.append(f"- 风险: {_icon(str(risk_level).lower())} {risk_level}")
        rules = risk.get("triggered_rules", [])
        if rules:
            lines.append(f"- 触发规则: {', '.join(rules)}")
    else:
        lines.append("- ⚪ 尚无运行记录")
    lines.append("")

    # ─── 数据库 ───
    db = _db_stats()
    lines.append("## 四、数据库状态")
    for k, v in db.items():
        lines.append(f"- {k}: {v}")
    lines.append("")

    # ─── 缓存 ───
    cs = _cache_stats()
    lines.append("## 五、扫描缓存")
    for k, v in cs.items():
        lines.append(f"- {k}: {v}")
    lines.append("")

    # ─── Git ───
    gi = _git_info()
    lines.append("## 六、版本信息")
    for k, v in gi.items():
        lines.append(f"- {k}: `{v}`")
    lines.append("")

    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="中文化系统状态看板")
    parser.add_argument("--md", action="store_true", help="输出 Markdown 文件")
    args = parser.parse_args()

    md = build_dashboard()
    print(md)

    if args.md:
        out = log_dir() / "dashboard.md"
        out.write_text(md, encoding="utf-8")
        print(f"\n已保存至: {out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
