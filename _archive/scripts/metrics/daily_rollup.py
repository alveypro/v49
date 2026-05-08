#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Roll up daily OpenClaw metrics jsonl")
    parser.add_argument("--input", required=True, help="metrics_daily_YYYYMMDD.jsonl")
    parser.add_argument("--output", default="", help="output json path")
    args = parser.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        raise FileNotFoundError(in_path)

    rows = []
    with in_path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            rows.append(json.loads(s))

    if not rows:
        out = {"count": 0}
    else:
        risk_counter = Counter([r.get("risk_level", "unknown") for r in rows])
        publish_counter = Counter([r.get("publish_status", "unknown") for r in rows])
        fallback_counter = Counter([r.get("fallback_mode", "unknown") for r in rows])
        out = {
            "count": len(rows),
            "scan_count_sum": sum(int(r.get("scan_count", 0) or 0) for r in rows),
            "signal_density_avg": sum(float(r.get("signal_density", 0.0) or 0.0) for r in rows) / len(rows),
            "risk_level_dist": dict(risk_counter),
            "publish_status_dist": dict(publish_counter),
            "fallback_mode_dist": dict(fallback_counter),
            "latest_db_date": rows[-1].get("db_latest_date"),
        }

    out_path = Path(args.output) if args.output else in_path.with_suffix(".summary.json")
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
