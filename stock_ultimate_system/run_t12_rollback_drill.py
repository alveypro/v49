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


def _to_int(value: object, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _build_md(payload: dict) -> str:
    lines = [
        "# T12 Rollback Drill Report",
        "",
        f"Generated at: {payload.get('generated_at', '')}",
        f"Mode: `{payload.get('mode', 'off')}`",
        f"Triggered: `{payload.get('triggered', False)}`",
        f"Would rollback apply: `{payload.get('would_rollback_apply', False)}`",
        f"Result: {payload.get('result', '')}",
        "",
        "## Simulated Inputs",
        f"- warn_runs={payload.get('simulated', {}).get('warn_runs', 0)}",
        f"- fail_runs={payload.get('simulated', {}).get('fail_runs', 0)}",
        f"- fail_streak={payload.get('simulated', {}).get('fail_streak', 0)}",
        "",
        "## Thresholds",
        f"- current: {payload.get('thresholds', {}).get('current_warn', '-')}/"
        f"{payload.get('thresholds', {}).get('current_fail', '-')}/"
        f"{payload.get('thresholds', {}).get('current_streak', '-')}",
        f"- stable: {payload.get('thresholds', {}).get('stable_warn', '-')}/"
        f"{payload.get('thresholds', {}).get('stable_fail', '-')}/"
        f"{payload.get('thresholds', {}).get('stable_streak', '-')}",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run rollback drill without mutating production thresholds")
    parser.add_argument("--output-dir", default="data/experiments")
    parser.add_argument("--mode", default="off", choices=["off", "warn", "fail"], help="Drill scenario mode")
    parser.add_argument("--stable-snapshot-file", default="data/experiments/t12_threshold_stable_snapshot.json")
    parser.add_argument("--rollback-events-file", default="data/experiments/t12_threshold_rollback_events.json")
    parser.add_argument("--rollback-warn-runs", type=int, default=3)
    parser.add_argument("--rollback-fail-runs", type=int, default=1)
    parser.add_argument("--rollback-fail-streak", type=int, default=1)
    args = parser.parse_args()

    out_dir = resolve_project_path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    stable_snapshot_file = Path(args.stable_snapshot_file)
    if not stable_snapshot_file.is_absolute():
        stable_snapshot_file = resolve_project_path(str(stable_snapshot_file))
    rollback_events_file = Path(args.rollback_events_file)
    if not rollback_events_file.is_absolute():
        rollback_events_file = resolve_project_path(str(rollback_events_file))

    threshold_control = _read_json(out_dir / "t12_threshold_control_latest.json")
    stable_snapshot = _read_json(stable_snapshot_file)
    rollback_events = _read_json(rollback_events_file)

    before = (threshold_control or {}).get("after", {}) or {}
    stable = (stable_snapshot or {}).get("thresholds", {}) or {}
    current_warn = _to_int(before.get("warn_hot_threshold"), 2)
    current_fail = _to_int(before.get("fail_hot_threshold"), 2)
    current_streak = _to_int(before.get("fail_streak_hot_threshold"), 1)
    stable_warn = _to_int(stable.get("warn_hot_threshold"), current_warn)
    stable_fail = _to_int(stable.get("fail_hot_threshold"), current_fail)
    stable_streak = _to_int(stable.get("fail_streak_hot_threshold"), current_streak)

    simulated_warn = 0
    simulated_fail = 0
    simulated_streak = 0
    if args.mode == "warn":
        simulated_warn = max(int(args.rollback_warn_runs), 1)
    elif args.mode == "fail":
        simulated_fail = max(int(args.rollback_fail_runs), 1)
        simulated_streak = max(int(args.rollback_fail_streak), 1)

    triggered = (
        simulated_warn >= max(int(args.rollback_warn_runs), 1)
        or simulated_fail >= max(int(args.rollback_fail_runs), 1)
        or simulated_streak >= max(int(args.rollback_fail_streak), 1)
    )
    has_stable = isinstance(stable, dict) and bool(stable)
    would_rollback_apply = triggered and has_stable and (
        (current_warn, current_fail, current_streak) != (stable_warn, stable_fail, stable_streak)
    )

    event_count = len((rollback_events or {}).get("items", [])) if isinstance((rollback_events or {}).get("items", []), list) else 0
    result = (
        "no drill in off mode"
        if args.mode == "off"
        else ("rollback would apply to stable snapshot" if would_rollback_apply else "rollback not needed or stable missing")
    )

    payload = {
        "generated_at": datetime.now().isoformat(),
        "mode": args.mode,
        "triggered": triggered,
        "would_rollback_apply": would_rollback_apply,
        "result": result,
        "simulated": {
            "warn_runs": simulated_warn,
            "fail_runs": simulated_fail,
            "fail_streak": simulated_streak,
        },
        "thresholds": {
            "current_warn": current_warn,
            "current_fail": current_fail,
            "current_streak": current_streak,
            "stable_warn": stable_warn,
            "stable_fail": stable_fail,
            "stable_streak": stable_streak,
        },
        "rollback_event_count": event_count,
    }

    out_json = out_dir / "t12_rollback_drill_latest.json"
    out_md = out_dir / "t12_rollback_drill_latest.md"
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    out_md.write_text(_build_md(payload), encoding="utf-8")
    print(str(out_json))
    print(str(out_md))


if __name__ == "__main__":
    main()
