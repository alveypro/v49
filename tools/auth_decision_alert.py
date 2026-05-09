#!/usr/bin/env python3
"""
Auth decision alert checker.

Purpose:
- Read auth decision JSONL log.
- Detect redirect-to-login storms in a rolling time window.
- Exit with non-zero code for monitoring integration.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List


DEFAULT_LOG_PATH = os.getenv("AIRIVO_AUTH_DECISION_LOG_PATH", "/tmp/airivo_auth_decision.jsonl")


@dataclass
class Record:
    ts: int
    decision: str
    reason: str
    path: str
    user_source: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Detect auth redirect storm from decision JSONL logs.")
    parser.add_argument("--log-path", default=DEFAULT_LOG_PATH, help="Path to auth decision jsonl log.")
    parser.add_argument("--window-seconds", type=int, default=300, help="Lookback window in seconds.")
    parser.add_argument(
        "--redirect-threshold",
        type=int,
        default=12,
        help="Alert if redirect_login count in window reaches this value.",
    )
    parser.add_argument(
        "--consecutive-threshold",
        type=int,
        default=4,
        help="Alert if max consecutive redirect_login reaches this value.",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=5,
        help="Max offending samples printed when alerting.",
    )
    parser.add_argument(
        "--allow-missing-log",
        action="store_true",
        help="If set, missing log file returns OK(0) instead of ALERT(2).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable summary as JSON.",
    )
    return parser.parse_args()


def load_records(path: Path) -> List[Record]:
    records: List[Record] = []
    with path.open("r", encoding="utf-8") as file_obj:
        for line in file_obj:
            raw = line.strip()
            if not raw:
                continue
            try:
                payload: Dict[str, Any] = json.loads(raw)
            except Exception:
                continue
            ts = int(payload.get("ts", 0) or 0)
            decision = str(payload.get("decision", "") or "")
            if ts <= 0 or not decision:
                continue
            records.append(
                Record(
                    ts=ts,
                    decision=decision,
                    reason=str(payload.get("reason", "") or ""),
                    path=str(payload.get("path", "") or ""),
                    user_source=str(payload.get("user_source", "") or ""),
                )
            )
    return records


def summarize(records: List[Record], window_seconds: int) -> Dict[str, Any]:
    now_ts = int(time.time())
    cutoff = now_ts - max(1, int(window_seconds))
    window_records = sorted([row for row in records if row.ts >= cutoff], key=lambda row: row.ts)

    redirect_count = 0
    max_consecutive_redirect = 0
    current_consecutive = 0
    recent_redirects: List[Dict[str, Any]] = []

    for row in window_records:
        if row.decision == "redirect_login":
            redirect_count += 1
            current_consecutive += 1
            if current_consecutive > max_consecutive_redirect:
                max_consecutive_redirect = current_consecutive
            recent_redirects.append(
                {
                    "ts": row.ts,
                    "reason": row.reason,
                    "path": row.path,
                    "user_source": row.user_source,
                }
            )
        else:
            current_consecutive = 0

    return {
        "now_ts": now_ts,
        "window_seconds": window_seconds,
        "total_records_in_window": len(window_records),
        "redirect_login_count": redirect_count,
        "max_consecutive_redirect_login": max_consecutive_redirect,
        "recent_redirect_samples": recent_redirects,
    }


def main() -> int:
    args = parse_args()
    log_path = Path(args.log_path)

    if not log_path.exists():
        message = {
            "status": "ok" if args.allow_missing_log else "alert",
            "reason": "missing_log",
            "log_path": str(log_path),
        }
        if args.json:
            print(json.dumps(message, ensure_ascii=False))
        else:
            print(f"[auth-alert] {message['status'].upper()}: log file missing: {log_path}")
        return 0 if args.allow_missing_log else 2

    records = load_records(log_path)
    summary = summarize(records, args.window_seconds)

    is_alert = (
        summary["redirect_login_count"] >= int(args.redirect_threshold)
        or summary["max_consecutive_redirect_login"] >= int(args.consecutive_threshold)
    )

    payload = {
        "status": "alert" if is_alert else "ok",
        "log_path": str(log_path),
        "redirect_threshold": int(args.redirect_threshold),
        "consecutive_threshold": int(args.consecutive_threshold),
        **summary,
    }
    payload["recent_redirect_samples"] = payload["recent_redirect_samples"][: max(0, int(args.sample_limit))]

    if args.json:
        print(json.dumps(payload, ensure_ascii=False))
    else:
        print(
            "[auth-alert] "
            f"{payload['status'].upper()} "
            f"window={payload['window_seconds']}s "
            f"records={payload['total_records_in_window']} "
            f"redirects={payload['redirect_login_count']} "
            f"max_consecutive={payload['max_consecutive_redirect_login']} "
            f"log={payload['log_path']}"
        )
        if is_alert and payload["recent_redirect_samples"]:
            for item in payload["recent_redirect_samples"]:
                print(
                    "[auth-alert] sample "
                    f"ts={item['ts']} reason={item['reason']} path={item['path']} source={item['user_source']}"
                )

    return 2 if is_alert else 0


if __name__ == "__main__":
    raise SystemExit(main())
