import argparse
import json
from datetime import datetime
from pathlib import Path

from src.utils.project_paths import resolve_project_path


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _alert(name: str, level: str, ok: bool, detail: str, action: str) -> dict:
    return {
        "name": name,
        "level": level,
        "ok": bool(ok),
        "detail": detail,
        "action": action,
    }


def _build_markdown(payload: dict) -> str:
    summary = payload.get("summary", {})
    trend = payload.get("trend", {})
    adaptive = payload.get("adaptive_thresholds", {})
    lines = [
        "# T12 Onsite Alert Center",
        "",
        f"Generated at: {payload.get('generated_at', '')}",
        f"Overall status: `{summary.get('overall_status', 'unknown')}`",
        f"Critical count: {summary.get('critical_count', 0)}",
        f"Warn count: {summary.get('warn_count', 0)}",
        "",
        "## Trend",
        f"- Recent runs: {trend.get('recent_runs', 0)}",
        f"- Pass/Warn/Fail: {trend.get('pass_runs', 0)}/{trend.get('warn_runs', 0)}/{trend.get('fail_runs', 0)}",
        f"- Recent fail streak: {trend.get('recent_fail_streak', 0)}",
        f"- Risk heat: `{trend.get('risk_heat', 'normal')}`",
        "",
        "## Adaptive Thresholds",
        f"- Current warn/fail/fail_streak: {adaptive.get('current_warn_hot_threshold', 0)}/"
        f"{adaptive.get('current_fail_hot_threshold', 0)}/{adaptive.get('current_fail_streak_hot_threshold', 0)}",
        f"- Suggested warn/fail/fail_streak: {adaptive.get('suggested_warn_hot_threshold', 0)}/"
        f"{adaptive.get('suggested_fail_hot_threshold', 0)}/{adaptive.get('suggested_fail_streak_hot_threshold', 0)}",
        f"- Recommendation: {adaptive.get('recommendation', 'keep_current')}",
        f"- Reason: {adaptive.get('reason', 'n/a')}",
        "",
        "## Alerts",
    ]
    for it in payload.get("alerts", []):
        status = "OK" if it.get("ok") else it.get("level", "warn").upper()
        lines.append(f"- [{status}] `{it.get('name', '')}`: {it.get('detail', '')}")
        if not it.get("ok"):
            lines.append(f"  - Action: {it.get('action', '')}")
    lines.append("")
    return "\n".join(lines)


def _build_review_markdown(payload: dict) -> str:
    lines = [
        "# T12 One-Click Review Checklist",
        "",
        f"Generated at: {payload.get('generated_at', '')}",
        f"Overall status: `{payload.get('overall_status', 'unknown')}`",
        "",
        "## Actions",
    ]
    actions = payload.get("actions", [])
    if not actions:
        lines.append("- No blocking action. Keep routine open/close checks.")
    else:
        for idx, item in enumerate(actions, start=1):
            lines.append(
                f"{idx}. [{item.get('priority', 'P2')}] `{item.get('name', '')}` - {item.get('action', '')} "
                f"(detail: {item.get('detail', '')})"
            )
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate onsite T12 alert center artifacts")
    parser.add_argument("--output-dir", default="data/experiments")
    parser.add_argument("--history-limit", type=int, default=30)
    parser.add_argument("--recent-window", type=int, default=7)
    parser.add_argument("--warn-hot-threshold", type=int, default=2)
    parser.add_argument("--fail-hot-threshold", type=int, default=2)
    parser.add_argument("--fail-streak-hot-threshold", type=int, default=1)
    args = parser.parse_args()

    out_dir = resolve_project_path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    audit = _read_json(out_dir / "governance_audit_latest.json")
    checklist = _read_json(out_dir / "t1_execution_checklist_latest.json")
    buylist = _read_json(out_dir / "buylist_latest.json")

    audit_status = str(((audit.get("summary", {}) or {}).get("overall_status", "unknown"))).strip().lower()
    checklist_status = str(((checklist.get("summary", {}) or {}).get("overall_status", "unknown"))).strip().lower()

    target_count = int((buylist or {}).get("target_count", 0) or 0)
    buyable_count = int((buylist or {}).get("buyable_count", 0) or 0)
    turnover_ratio = float((buylist or {}).get("turnover_ratio", 0.0) or 0.0)
    max_turnover_ratio = float((buylist or {}).get("max_turnover_ratio", 0.8) or 0.8)
    constraints = (buylist or {}).get("constraints", {}) or {}
    actual_high_risk_count = int(constraints.get("actual_high_risk_count", 0) or 0)
    max_high_risk_count = int(constraints.get("max_high_risk_count", 1) or 1)

    alerts = [
        _alert(
            "governance_audit_status",
            "critical",
            ok=audit_status in {"pass", "warn"},
            detail=f"overall_status={audit_status}",
            action="若为 fail，暂停晋级并复核治理报告中的 critical 项。",
        ),
        _alert(
            "t1_checklist_status",
            "warn",
            ok=checklist_status in {"pass", "warn"},
            detail=f"overall_status={checklist_status}",
            action="若为 fail，按 T+1 执行清单逐项补齐后再开盘执行。",
        ),
        _alert(
            "buylist_target_fill",
            "warn",
            ok=(target_count > 0 and buyable_count >= target_count),
            detail=f"buyable_count={buyable_count}, target_count={target_count}",
            action="若不足目标数量，降低仓位并进入防守模式。",
        ),
        _alert(
            "turnover_ratio_control",
            "warn",
            ok=turnover_ratio <= max_turnover_ratio,
            detail=f"turnover_ratio={round(turnover_ratio, 4)}, max_turnover_ratio={round(max_turnover_ratio, 4)}",
            action="换手超限时优先保留原持仓，减少当日替换数量。",
        ),
        _alert(
            "high_risk_exposure",
            "warn",
            ok=actual_high_risk_count <= max_high_risk_count,
            detail=f"actual_high_risk_count={actual_high_risk_count}, max_high_risk_count={max_high_risk_count}",
            action="高风险暴露超限时降杠杆并替换为 medium/low 风险标的。",
        ),
    ]

    critical_count = sum(1 for x in alerts if (not x["ok"]) and x["level"] == "critical")
    warn_count = sum(1 for x in alerts if (not x["ok"]) and x["level"] == "warn")
    overall = "pass" if critical_count == 0 and warn_count == 0 else ("warn" if critical_count == 0 else "fail")

    history_json = out_dir / "t12_alert_center_history.json"
    history = _read_json(history_json).get("items", [])
    if not isinstance(history, list):
        history = []

    recent_window = max(int(args.recent_window), 3)
    recent = history[-(recent_window - 1) :] + [
        {
            "generated_at": datetime.now().isoformat(),
            "overall_status": overall,
            "critical_count": critical_count,
            "warn_count": warn_count,
        }
    ]
    pass_runs = sum(1 for x in recent if str(x.get("overall_status", "")).lower() == "pass")
    warn_runs = sum(1 for x in recent if str(x.get("overall_status", "")).lower() == "warn")
    fail_runs = sum(1 for x in recent if str(x.get("overall_status", "")).lower() == "fail")
    recent_fail_streak = 0
    for x in reversed(recent):
        if str(x.get("overall_status", "")).lower() == "fail":
            recent_fail_streak += 1
        else:
            break
    warn_hot_threshold = max(int(args.warn_hot_threshold), 1)
    fail_hot_threshold = max(int(args.fail_hot_threshold), 1)
    fail_streak_hot_threshold = max(int(args.fail_streak_hot_threshold), 1)
    if fail_runs >= fail_hot_threshold or recent_fail_streak >= fail_streak_hot_threshold:
        risk_heat = "high"
    elif warn_runs >= warn_hot_threshold:
        risk_heat = "elevated"
    else:
        risk_heat = "normal"

    recent_runs = len(recent)
    warn_rate = float(warn_runs) / recent_runs if recent_runs else 0.0
    fail_rate = float(fail_runs) / recent_runs if recent_runs else 0.0
    suggested_warn = warn_hot_threshold
    suggested_fail = fail_hot_threshold
    suggested_streak = fail_streak_hot_threshold
    recommendation = "keep_current"
    reason = "history is balanced; keep thresholds."
    if fail_rate > 0.25:
        suggested_fail = min(fail_hot_threshold + 1, 5)
        suggested_streak = min(fail_streak_hot_threshold + 1, 3)
        recommendation = "relax_fail_thresholds"
        reason = f"fail_rate={round(fail_rate, 3)} is elevated."
    elif warn_rate > 0.4:
        suggested_warn = min(warn_hot_threshold + 1, 5)
        recommendation = "relax_warn_thresholds"
        reason = f"warn_rate={round(warn_rate, 3)} is elevated."
    elif recent_runs >= 5 and warn_rate == 0 and fail_rate == 0:
        suggested_warn = max(warn_hot_threshold - 1, 1)
        suggested_fail = max(fail_hot_threshold - 1, 1)
        recommendation = "tighten_thresholds"
        reason = "recent runs are stable pass with zero warn/fail."

    review_actions = []
    for item in alerts:
        if item.get("ok"):
            continue
        level = str(item.get("level", "warn")).lower()
        review_actions.append(
            {
                "name": item.get("name", ""),
                "priority": "P0" if level == "critical" else "P1",
                "detail": item.get("detail", ""),
                "action": item.get("action", ""),
            }
        )

    generated_at = datetime.now().isoformat()
    payload = {
        "generated_at": generated_at,
        "summary": {
            "overall_status": overall,
            "critical_count": critical_count,
            "warn_count": warn_count,
        },
        "trend": {
            "recent_runs": len(recent),
            "pass_runs": pass_runs,
            "warn_runs": warn_runs,
            "fail_runs": fail_runs,
            "recent_fail_streak": recent_fail_streak,
            "recent_statuses": [str(x.get("overall_status", "unknown")).lower() for x in recent],
            "risk_heat": risk_heat,
            "thresholds": {
                "warn_hot_threshold": warn_hot_threshold,
                "fail_hot_threshold": fail_hot_threshold,
                "fail_streak_hot_threshold": fail_streak_hot_threshold,
            },
        },
        "adaptive_thresholds": {
            "current_warn_hot_threshold": warn_hot_threshold,
            "current_fail_hot_threshold": fail_hot_threshold,
            "current_fail_streak_hot_threshold": fail_streak_hot_threshold,
            "suggested_warn_hot_threshold": suggested_warn,
            "suggested_fail_hot_threshold": suggested_fail,
            "suggested_fail_streak_hot_threshold": suggested_streak,
            "recommendation": recommendation,
            "reason": reason,
        },
        "review_action_count": len(review_actions),
        "alerts": alerts,
    }

    review_payload = {
        "generated_at": generated_at,
        "overall_status": overall,
        "actions": review_actions,
    }

    history.append(
        {
            "generated_at": generated_at,
            "overall_status": overall,
            "critical_count": critical_count,
            "warn_count": warn_count,
            "review_action_count": len(review_actions),
            "risk_heat": risk_heat,
            "turnover_ratio": turnover_ratio,
            "warn_hot_threshold": warn_hot_threshold,
            "fail_hot_threshold": fail_hot_threshold,
            "fail_streak_hot_threshold": fail_streak_hot_threshold,
        }
    )
    history = history[-max(int(args.history_limit), 5) :]

    latest_json = out_dir / "t12_alert_center_latest.json"
    latest_md = out_dir / "t12_alert_center_latest.md"
    review_json = out_dir / "t12_review_checklist_latest.json"
    review_md = out_dir / "t12_review_checklist_latest.md"

    latest_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_md.write_text(_build_markdown(payload), encoding="utf-8")
    review_json.write_text(json.dumps(review_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    review_md.write_text(_build_review_markdown(review_payload), encoding="utf-8")
    history_json.write_text(json.dumps({"items": history}, ensure_ascii=False, indent=2), encoding="utf-8")

    print(str(latest_json))
    print(str(latest_md))
    print(str(review_json))
    print(str(review_md))
    print(str(history_json))


if __name__ == "__main__":
    main()
