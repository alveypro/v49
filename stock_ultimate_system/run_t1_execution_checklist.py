import argparse
import json
from datetime import datetime
from pathlib import Path

from src.primary_result_candidate_basket import TARGET_MAX_INDUSTRY_WEIGHT
from src.utils.project_paths import resolve_project_path


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _item(name: str, ok: bool, detail: str, severity: str = "critical") -> dict:
    return {"name": name, "ok": bool(ok), "severity": severity, "detail": detail}


def _build_md(payload: dict) -> str:
    summary = payload.get("summary", {})
    lines = [
        "# A-share T+1 Daily Execution Checklist",
        "",
        f"Generated at: {payload.get('generated_at', '')}",
        f"Overall status: `{summary.get('overall_status', 'unknown')}`",
        "",
        "## Core Checks",
    ]
    for it in payload.get("items", []):
        status = "PASS" if it.get("ok") else ("WARN" if it.get("severity") == "warn" else "FAIL")
        lines.append(f"- [{status}] `{it.get('name', '')}`: {it.get('detail', '')}")

    actions = payload.get("next_session_sop", [])
    lines.extend(["", "## Next Session SOP"])
    for idx, action in enumerate(actions, start=1):
        lines.append(f"{idx}. {action}")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate daily T+1 execution checklist")
    parser.add_argument("--output-dir", default="data/experiments")
    args = parser.parse_args()

    out_dir = resolve_project_path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    buylist = _read_json(out_dir / "buylist_latest.json")
    audit = _read_json(out_dir / "governance_audit_latest.json")
    research = _read_json(out_dir / "daily_research_status_latest.json")
    update_status = _read_json(out_dir / "update_status_latest.json")

    target_count = int((buylist or {}).get("target_count", 0) or 0)
    buyable_count = int((buylist or {}).get("buyable_count", 0) or 0)
    turnover_ratio = float((buylist or {}).get("turnover_ratio", 0.0) or 0.0)
    max_turnover_ratio = float((buylist or {}).get("max_turnover_ratio", 0.8) or 0.8)
    constraints = (buylist or {}).get("constraints", {}) or {}
    max_industry_weight = float(
        constraints.get("max_industry_weight", TARGET_MAX_INDUSTRY_WEIGHT) or TARGET_MAX_INDUSTRY_WEIGHT
    )
    actual_industry_weight = float(constraints.get("actual_max_industry_weight", 1.0) or 1.0)

    audit_summary = (audit or {}).get("summary", {}) or {}
    audit_status = str(audit_summary.get("overall_status", "unknown")).strip().lower()
    research_state = str((research or {}).get("state", "")).strip().lower()
    post_candidates_ok = bool(((update_status or {}).get("post_candidates") or {}).get("ok"))
    post_daily_ok = bool(((update_status or {}).get("post_daily_research") or {}).get("ok"))

    items = [
        _item(
            "buylist_target_count",
            ok=(target_count > 0 and buyable_count >= target_count),
            detail=f"buyable_count={buyable_count}, target_count={target_count}",
        ),
        _item(
            "industry_concentration_control",
            ok=actual_industry_weight <= max_industry_weight,
            detail=(
                f"actual_max_industry_weight={round(actual_industry_weight, 4)}, "
                f"max_industry_weight={round(max_industry_weight, 4)}"
            ),
            severity="warn",
        ),
        _item(
            "turnover_control",
            ok=turnover_ratio <= max_turnover_ratio,
            detail=(
                f"turnover_ratio={round(turnover_ratio, 4)}, "
                f"max_turnover_ratio={round(max_turnover_ratio, 4)}"
            ),
            severity="warn",
        ),
        _item(
            "governance_gate_status",
            ok=audit_status in {"pass", "warn"},
            detail=f"audit_status={audit_status}",
        ),
        _item(
            "daily_research_status",
            ok=research_state in {"completed", "done"} and post_candidates_ok and post_daily_ok,
            detail=(
                f"research_state={research_state or 'missing'}, "
                f"post_candidates_ok={post_candidates_ok}, post_daily_research_ok={post_daily_ok}"
            ),
        ),
    ]

    fail_count = sum(1 for i in items if (not i["ok"]) and i["severity"] == "critical")
    warn_count = sum(1 for i in items if (not i["ok"]) and i["severity"] == "warn")
    overall = "pass" if fail_count == 0 and warn_count == 0 else ("warn" if fail_count == 0 else "fail")

    payload = {
        "generated_at": datetime.now().isoformat(),
        "summary": {
            "overall_status": overall,
            "pass_count": sum(1 for i in items if i["ok"]),
            "fail_count": fail_count,
            "warn_count": warn_count,
        },
        "inputs": {
            "target_count": target_count,
            "buyable_count": buyable_count,
            "turnover_ratio": turnover_ratio,
            "max_turnover_ratio": max_turnover_ratio,
            "actual_max_industry_weight": actual_industry_weight,
            "max_industry_weight": max_industry_weight,
            "audit_status": audit_status,
            "research_state": research_state,
        },
        "items": items,
        "next_session_sop": [
            "09:00 前复核 buylist 与调仓建议，确认保留/新增/剔除名单。",
            "09:25-09:30 检查隔夜公告、停复牌与涨跌停约束，过滤不可交易标的。",
            "09:30-09:45 按交易计划执行，单票与行业暴露不突破风控阈值。",
            "14:50 后复核当日持仓与风险状态，记录异常并准备次日迭代。",
            "16:00 后运行自动流水线，生成新一日清单与治理审计。",
        ],
    }

    out_json = out_dir / "t1_execution_checklist_latest.json"
    out_md = out_dir / "t1_execution_checklist_latest.md"
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    out_md.write_text(_build_md(payload), encoding="utf-8")
    print(str(out_json))
    print(str(out_md))


if __name__ == "__main__":
    main()
