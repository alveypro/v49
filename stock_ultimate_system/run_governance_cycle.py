import argparse
import json
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

from src.utils.project_paths import resolve_project_path


def _run(cmd: list[str]) -> tuple[bool, str]:
    try:
        completed = subprocess.run(cmd, check=True, capture_output=True, text=True)
        return True, completed.stdout.strip()
    except subprocess.CalledProcessError as exc:
        detail = (exc.stdout or "") + ("\n" + exc.stderr if exc.stderr else "")
        return False, detail.strip()


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def build_governance_cycle_summary(
    *,
    started_at: str,
    steps: list[dict[str, Any]],
    outputs_dir: Path,
    stable_release_reference_path: Path | None = None,
) -> dict[str, Any]:
    step_lookup = {str(step.get("name", "")): bool(step.get("ok")) for step in steps}
    governance_decision = _read_json(outputs_dir / "governance_decision.json")
    governance_audit = _read_json(outputs_dir / "governance_audit_latest.json")
    stable_release_reference = _read_json(stable_release_reference_path) if stable_release_reference_path else {}

    daily_research_ok = step_lookup.get("daily_research", False)
    artifact_bundle_ok = step_lookup.get("artifact_bundle", False)
    governance_audit_step_ok = step_lookup.get("governance_audit", False)
    audit_status = str((governance_audit.get("summary", {}) or {}).get("overall_status", "unknown") or "unknown").lower()
    decision = str(governance_decision.get("decision", "unknown") or "unknown").lower()

    ready_for_release = (
        daily_research_ok
        and artifact_bundle_ok
        and governance_audit_step_ok
        and audit_status == "pass"
        and decision == "promote_to_staging"
    )
    fully_release_ready = ready_for_release and bool(str(stable_release_reference.get("run_id", "") or "").strip())
    ready_for_observation = (
        daily_research_ok
        and artifact_bundle_ok
        and governance_audit_step_ok
        and audit_status in {"pass", "warn"}
        and decision == "observe"
    )

    if not daily_research_ok:
        cycle_state = "research_blocked"
        recommended_action = "rerun_daily_research"
        operator_message = "研究主链未完成，禁止继续晋级，先修复研究执行失败。"
    elif not artifact_bundle_ok:
        cycle_state = "artifact_blocked"
        recommended_action = "rebuild_artifact_bundle"
        operator_message = "治理产物包不完整，不能进入审计或发布。"
    elif not governance_audit_step_ok or audit_status == "fail":
        cycle_state = "audit_blocked"
        recommended_action = "review_governance_audit"
        operator_message = "治理审计未通过，维持上一稳定结果，不允许发布。"
    elif ready_for_release:
        cycle_state = "release_ready"
        recommended_action = "run_release_pipeline"
        operator_message = "治理门禁通过，可进入正式 release pipeline。"
    elif ready_for_observation:
        cycle_state = "observe_only"
        recommended_action = "hold_observation"
        operator_message = "当前仅满足观察条件，不满足正式发布条件。"
    elif decision == "reject":
        cycle_state = "rejected"
        recommended_action = "keep_previous_stable_release"
        operator_message = "当前候选已被治理拒绝，应保持上一稳定版本。"
    else:
        cycle_state = "governance_pending"
        recommended_action = "manual_review"
        operator_message = "治理结果未收敛，需要人工复核后再决定是否推进。"

    previous_stable_run_id = str(stable_release_reference.get("run_id", "") or "").strip()
    if previous_stable_run_id and recommended_action == "keep_previous_stable_release":
        operator_message = f"{operator_message} 当前稳定版本 {previous_stable_run_id}。"
    elif previous_stable_run_id and cycle_state in {"audit_blocked", "observe_only"}:
        operator_message = f"{operator_message} 若需兜底，优先保留稳定版本 {previous_stable_run_id}。"

    summary = {
        "started_at": started_at,
        "ended_at": datetime.now().isoformat(),
        "overall_ok": all(bool(x.get("ok")) for x in steps),
        "cycle_state": cycle_state,
        "recommended_action": recommended_action,
        "operator_message": operator_message,
        "release_readiness": {
            "ready_for_release": ready_for_release,
            "fully_release_ready": fully_release_ready,
            "ready_for_observation": ready_for_observation,
            "keep_previous_stable_release": not ready_for_release,
        },
        "governance_inputs": {
            "governance_decision": decision,
            "governance_audit_status": audit_status,
        },
        "previous_stable_release": stable_release_reference,
        "steps": steps,
    }
    return summary


def _build_cycle_markdown(summary: dict[str, Any]) -> str:
    readiness = summary.get("release_readiness", {}) or {}
    governance_inputs = summary.get("governance_inputs", {}) or {}
    lines = [
        "# Governance Cycle Summary",
        "",
        f"- started_at: {summary.get('started_at', '')}",
        f"- ended_at: {summary.get('ended_at', '')}",
        f"- cycle_state: {summary.get('cycle_state', 'unknown')}",
        f"- recommended_action: {summary.get('recommended_action', 'manual_review')}",
        f"- governance_decision: {governance_inputs.get('governance_decision', 'unknown')}",
        f"- governance_audit_status: {governance_inputs.get('governance_audit_status', 'unknown')}",
        f"- previous_stable_run_id: {(summary.get('previous_stable_release', {}) or {}).get('run_id', '-')}",
        f"- ready_for_release: {readiness.get('ready_for_release', False)}",
        f"- fully_release_ready: {readiness.get('fully_release_ready', False)}",
        f"- ready_for_observation: {readiness.get('ready_for_observation', False)}",
        "",
        "## Operator Message",
        summary.get("operator_message", ""),
        "",
        "## Steps",
    ]
    for step in summary.get("steps", []) or []:
        name = step.get("name", "")
        ok = bool(step.get("ok"))
        detail = str(step.get("detail", "") or "").strip()
        lines.append(f"- {'PASS' if ok else 'FAIL'} `{name}`: {detail[:200]}")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run governance cycle: research -> artifact bundle -> audit")
    parser.add_argument("--skip-daily-research", action="store_true", help="Skip daily research run")
    parser.add_argument("--profiles", nargs="*", default=["short", "medium"], help="Profiles for daily research")
    parser.add_argument("--stocks", nargs="*", default=["000001.SZ"], help="Stocks for daily research smoke run")
    parser.add_argument("--max-runs", type=int, default=1, help="Max runs per profile for cycle smoke")
    parser.add_argument("--batch-size", type=int, default=1, help="Batch size for cycle smoke")
    args = parser.parse_args()

    started_at = datetime.now().isoformat()
    outputs_dir = resolve_project_path("data/experiments")
    outputs_dir.mkdir(parents=True, exist_ok=True)
    cycle_report_path = outputs_dir / "governance_cycle_latest.json"
    stable_release_reference_path = resolve_project_path("artifacts/stock_release_pipeline/stable_release_reference.json")

    steps: list[dict[str, str | bool]] = []

    if not args.skip_daily_research:
        cmd = [
            "python",
            "run_daily_research.py",
            "--profiles",
            *args.profiles,
            "--stocks",
            *args.stocks,
            "--max-runs",
            str(args.max_runs),
            "--batch-size",
            str(args.batch_size),
            "--retry",
            "0",
            "--no-replay-failed-profiles",
        ]
        ok, detail = _run(cmd)
        steps.append({"name": "daily_research", "ok": ok, "detail": detail[:4000]})
    else:
        steps.append({"name": "daily_research", "ok": True, "detail": "skipped by flag"})

    ok_bundle, detail_bundle = _run(["python", "run_experiment_artifact_bundle.py"])
    steps.append({"name": "artifact_bundle", "ok": ok_bundle, "detail": detail_bundle[:4000]})

    ok_audit, detail_audit = _run(["python", "run_governance_audit.py"])
    steps.append({"name": "governance_audit", "ok": ok_audit, "detail": detail_audit[:4000]})

    summary = build_governance_cycle_summary(
        started_at=started_at,
        steps=steps,
        outputs_dir=outputs_dir,
        stable_release_reference_path=stable_release_reference_path,
    )
    cycle_report_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    (outputs_dir / "governance_cycle_latest.md").write_text(_build_cycle_markdown(summary), encoding="utf-8")

    print(
        json.dumps(
            {
                "overall_ok": summary["overall_ok"],
                "steps": len(steps),
                "cycle_state": summary["cycle_state"],
                "recommended_action": summary["recommended_action"],
            },
            ensure_ascii=False,
        )
    )
    print("Generated:")
    print(cycle_report_path)


if __name__ == "__main__":
    main()
