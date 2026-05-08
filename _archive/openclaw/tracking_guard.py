#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.publish.notifier import NotificationPublisher


def _load_notify_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"enabled": False, "channels": ["stdout"], "outbox_path": "logs/openclaw/notify_outbox.log"}
    txt = path.read_text(encoding="utf-8", errors="ignore")
    try:
        import yaml  # type: ignore
        obj = yaml.safe_load(txt) or {}
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    return {"enabled": False, "channels": ["stdout"], "outbox_path": "logs/openclaw/notify_outbox.log"}


def _is_record_bad(record: Dict[str, Any]) -> bool:
    inserted = int(record.get("inserted") or 0)
    reason = str(record.get("reason") or "").strip().lower()
    if inserted > 0:
        return False
    if reason in {"no_picks", "no_run_summary"}:
        return False
    return True


def _load_exec_files(output_dir: Path, limit: int) -> List[Path]:
    return sorted(output_dir.glob("partner_execution_*.json"), reverse=True)[: max(1, limit)]


def _collect_bad_count(exec_files: List[Path], max_consecutive: int) -> Dict[str, Any]:
    bad_count = 0
    scan_details: List[Dict[str, Any]] = []
    for p in exec_files:
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue

        run_id = str(obj.get("run_id") or p.stem)
        run_bad = False
        by_strategy = obj.get("tracking_by_strategy") or []
        if isinstance(by_strategy, list) and by_strategy:
            any_bad = False
            all_no_pick = True
            for item in by_strategy:
                if not isinstance(item, dict):
                    continue
                result = item.get("result") or {}
                record = (result.get("record") or {}) if isinstance(result, dict) else {}
                reason = str(record.get("reason") or "").strip().lower()
                if reason not in {"no_picks", "no_run_summary"}:
                    all_no_pick = False
                if _is_record_bad(record):
                    any_bad = True
            run_bad = any_bad and not all_no_pick
        else:
            record = ((obj.get("tracking") or {}).get("record") or {})
            run_bad = _is_record_bad(record)

        scan_details.append({"run_id": run_id, "bad": run_bad})
        if run_bad:
            bad_count += 1
        else:
            break
        if bad_count >= max_consecutive:
            break

    return {"bad_consecutive": bad_count, "details": scan_details}


def _load_state(state_file: Path) -> Dict[str, Any]:
    if not state_file.exists():
        return {}
    try:
        data = json.loads(state_file.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def main() -> int:
    parser = argparse.ArgumentParser(description="Guard partner tracking quality and notify on consecutive anomalies.")
    parser.add_argument("--output-dir", default="logs/openclaw")
    parser.add_argument("--notify-config", default="openclaw/config/notify.yaml")
    parser.add_argument("--lookback-files", type=int, default=6)
    parser.add_argument("--consecutive-threshold", type=int, default=2)
    parser.add_argument("--state-file", default="logs/openclaw/tracking_guard_state.json")
    parser.add_argument("--status-file", default="logs/openclaw/tracking_guard_status.json")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    state_file = Path(args.state_file)
    status_file = Path(args.status_file)

    exec_files = _load_exec_files(output_dir, args.lookback_files)
    if not exec_files:
        status = {"ok": True, "reason": "no_partner_execution", "checked_at": datetime.now().isoformat(timespec="seconds")}
        status_file.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(status, ensure_ascii=False))
        return 0

    checked = _collect_bad_count(exec_files, args.consecutive_threshold)
    bad_count = int(checked.get("bad_consecutive") or 0)
    latest_run = ""
    for item in checked.get("details") or []:
        if isinstance(item, dict):
            latest_run = str(item.get("run_id") or latest_run)
            break

    should_alert = bad_count >= int(args.consecutive_threshold)
    state = _load_state(state_file)
    already_alerted = (
        str(state.get("last_alert_run_id") or "") == latest_run
        and int(state.get("last_alert_bad_count") or 0) == bad_count
    )

    notify_result: Dict[str, Any] = {"status": "skipped"}
    if should_alert and not already_alerted:
        cfg = _load_notify_config(Path(args.notify_config))
        publisher = NotificationPublisher(cfg)
        body = (
            f"tracking_guard 检测到连续异常。\n"
            f"- consecutive_bad_runs: {bad_count}\n"
            f"- threshold: {args.consecutive_threshold}\n"
            f"- latest_run: {latest_run}\n"
            f"- details: {json.dumps(checked.get('details') or [], ensure_ascii=False)}"
        )
        notify_result = publisher.publish(
            title=f"[OpenClaw] Tracking连续异常告警({bad_count})",
            body=body,
            metadata={"kind": "tracking_guard", "risk_level": "orange"},
        )
        state_file.parent.mkdir(parents=True, exist_ok=True)
        state_file.write_text(
            json.dumps(
                {
                    "last_alert_run_id": latest_run,
                    "last_alert_bad_count": bad_count,
                    "alerted_at": datetime.now().isoformat(timespec="seconds"),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    status = {
        "ok": True,
        "checked_at": datetime.now().isoformat(timespec="seconds"),
        "should_alert": should_alert,
        "bad_consecutive": bad_count,
        "threshold": int(args.consecutive_threshold),
        "latest_run": latest_run,
        "details": checked.get("details") or [],
        "notify": notify_result,
    }
    status_file.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(status, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
