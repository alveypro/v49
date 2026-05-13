#!/usr/bin/env python3
"""Label Top5 audit snapshots with forward close-to-close returns from SQLite OHLC.

Exit codes:
  0  Evaluation written (optional) and optional evidence gates passed.
  1  Artifact / DB / IO error.
  2  Missing required CLI inputs (--artifact or --latest-artifact-dir).
  3  Evidence gate failure (--fail-on-*, coverage thresholds).

Example:

  python tools/evaluate_top5_forward_returns.py \\
    --latest-artifact-dir logs/openclaw/strategy_competition_audit \\
    --horizons 5,20,60 \\
    --output-json exports/top5_forward_eval_latest.json \\
    --fail-on-inferred-as-of \\
    --min-available-symbols-per-horizon 5
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from openclaw.paths import db_path as default_db_path  # noqa: E402
from openclaw.services.top5_forward_return_evaluation_service import (  # noqa: E402
    evaluate_top5_forward_returns,
    forward_evaluation_gate_failures,
    load_audit_artifact,
)


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate forward returns for a Top5 competition audit artifact.")
    parser.add_argument("--artifact", default="", help="Path to strategy_competition_portfolio_audit_*.json")
    parser.add_argument(
        "--latest-artifact-dir",
        default="",
        help="Pick newest strategy_competition_portfolio_audit_*.json in this directory.",
    )
    parser.add_argument("--db-path", default="", help="SQLite DB path (defaults via openclaw.paths.db_path).")
    parser.add_argument(
        "--horizons",
        default="5,20,60",
        help="Comma-separated forward horizons in trading days (positive integers).",
    )
    parser.add_argument("--subtract-roundtrip-cost-bps", type=float, default=0.0, help="Naive round-trip cost haircut.")
    parser.add_argument("--as-of-compact", default="", help="Override artifact trade_date (YYYYMMDD).")
    parser.add_argument("--output-json", default="", help="Write full evaluation payload to this path.")
    parser.add_argument(
        "--fail-on-eval-blocking",
        action="store_true",
        help="Exit 3 when evaluation blocking_reasons is non-empty (e.g. missing as_of).",
    )
    parser.add_argument(
        "--fail-on-inferred-as-of",
        action="store_true",
        help="Exit 3 when as_of was inferred from signal_refs run_id tokens (not explicit trade_date).",
    )
    parser.add_argument(
        "--min-available-symbols-per-horizon",
        type=int,
        default=0,
        help="Exit 3 if any horizon has fewer symbols with forward prices than this count (0=disabled).",
    )
    parser.add_argument(
        "--min-available-ratio-per-horizon",
        type=float,
        default=0.0,
        help="Exit 3 if availableSymbolCount/expectedSymbols < this ratio for any horizon (0=disabled).",
    )
    args = parser.parse_args()

    horizons = []
    for part in str(args.horizons or "").split(","):
        part = part.strip()
        if not part:
            continue
        horizons.append(int(part))

    artifact_path: Path | None = None
    if str(args.artifact or "").strip():
        artifact_path = Path(str(args.artifact).strip()).expanduser().resolve()
    elif str(args.latest_artifact_dir or "").strip():
        d = Path(str(args.latest_artifact_dir).strip()).expanduser().resolve()
        files = sorted(d.glob("strategy_competition_portfolio_audit_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not files:
            print(json.dumps({"error": "no_audit_artifacts", "directory": str(d)}, indent=2))
            return 1
        artifact_path = files[0]
    else:
        print(json.dumps({"error": "provide_--artifact_or_--latest-artifact-dir"}, indent=2))
        return 2

    assert artifact_path is not None
    if not artifact_path.is_file():
        print(json.dumps({"error": "artifact_not_found", "path": str(artifact_path)}, indent=2))
        return 1

    payload = load_audit_artifact(artifact_path)
    dbp = Path(str(args.db_path or "").strip() or default_db_path()).resolve()
    try:
        conn = sqlite3.connect(str(dbp), timeout=60)
    except Exception as exc:
        print(json.dumps({"error": "db_connect_failed", "detail": str(exc)}, indent=2))
        return 1
    try:
        evaluation = evaluate_top5_forward_returns(
            conn,
            artifact=payload,
            horizons=horizons,
            subtract_roundtrip_cost_bps=float(args.subtract_roundtrip_cost_bps or 0.0),
            override_as_of_compact=str(args.as_of_compact or "").strip(),
        )
    except Exception as exc:
        print(json.dumps({"error": "evaluation_failed", "detail": str(exc)}, indent=2))
        return 1
    finally:
        conn.close()

    gate_failures = forward_evaluation_gate_failures(
        evaluation,
        horizons=horizons,
        fail_on_eval_blocking=bool(args.fail_on_eval_blocking),
        fail_on_inferred_as_of=bool(args.fail_on_inferred_as_of),
        min_available_symbols_per_horizon=int(args.min_available_symbols_per_horizon or 0),
        min_available_ratio_per_horizon=float(args.min_available_ratio_per_horizon or 0.0),
    )
    evaluation["gate_failures"] = gate_failures
    evaluation["gate_passed"] = len(gate_failures) == 0

    evaluation["evaluation_meta"] = {
        "artifact_path": str(artifact_path),
        "artifact_sha256": _sha256_file(artifact_path),
        "db_path": str(dbp),
        "exit_policy": {
            "fail_on_eval_blocking": bool(args.fail_on_eval_blocking),
            "fail_on_inferred_as_of": bool(args.fail_on_inferred_as_of),
            "min_available_symbols_per_horizon": int(args.min_available_symbols_per_horizon or 0),
            "min_available_ratio_per_horizon": float(args.min_available_ratio_per_horizon or 0.0),
        },
    }
    text = json.dumps(evaluation, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if str(args.output_json or "").strip():
        out = Path(str(args.output_json).strip()).expanduser().resolve()
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text, encoding="utf-8")
    print(text)
    if gate_failures:
        print(json.dumps({"error": "gate_failed", "gate_failures": gate_failures}, indent=2), file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
