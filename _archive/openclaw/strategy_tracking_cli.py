#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.strategy_tracking import (
    generate_scoreboard,
    record_signals_from_summary,
    refresh_performance,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Strategy tracking CLI for OC daily outputs")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_record = sub.add_parser("record")
    p_record.add_argument("--run-summary", required=True)
    p_record.add_argument("--db-path", default=None)

    p_refresh = sub.add_parser("refresh")
    p_refresh.add_argument("--db-path", default=None)
    p_refresh.add_argument("--lookback-days", type=int, default=240)

    p_board = sub.add_parser("scoreboard")
    p_board.add_argument("--db-path", default=None)
    p_board.add_argument("--output-dir", default="logs/openclaw")
    p_board.add_argument("--lookback-days", type=int, default=120)

    p_all = sub.add_parser("run-all")
    p_all.add_argument("--run-summary", required=True)
    p_all.add_argument("--db-path", default=None)
    p_all.add_argument("--output-dir", default="logs/openclaw")
    p_all.add_argument("--lookback-days", type=int, default=120)

    args = parser.parse_args()

    if args.cmd == "record":
        out = record_signals_from_summary(run_summary_path=args.run_summary, db_path=args.db_path)
    elif args.cmd == "refresh":
        out = refresh_performance(db_path=args.db_path, lookback_days=args.lookback_days)
    elif args.cmd == "scoreboard":
        out = generate_scoreboard(db_path=args.db_path, output_dir=args.output_dir, lookback_days=args.lookback_days)
    else:
        r1 = record_signals_from_summary(run_summary_path=args.run_summary, db_path=args.db_path)
        r2 = refresh_performance(db_path=args.db_path, lookback_days=max(240, args.lookback_days))
        r3 = generate_scoreboard(db_path=args.db_path, output_dir=args.output_dir, lookback_days=args.lookback_days)
        out = {"record": r1, "refresh": r2, "scoreboard": r3}

    print(json.dumps(out, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
