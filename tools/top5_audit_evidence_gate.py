#!/usr/bin/env python3
"""Aggregate Top5 audit evidence checks (artifact schema, relaxed semantics, manifest, forward gates).

Exit codes:
  0   All enabled checks passed.
  1   Input / IO error (no artifact, missing paths).
  10  Audit artifact schema/version rejected.
  11  Relaxed-mode production-block semantics violated (--enforce-relaxed-production-block).
  12  Trader-brief manifest missing or SHA256 mismatch (--require-manifest).
  13  Forward evaluation raised an exception.
  14  Forward evaluation gates failed (coverage / inferred as-of / eval blocking).

Environment (optional defaults for systemd/cron):
  TOP5_GATE_ARTIFACT_DIR          defaults to STRATEGY_COMPETITION_AUDIT_OUTPUT_DIR-style relative discovery
  TOP5_GATE_PROJECT_ROOT          defaults to repo root beside tools/
  TOP5_GATE_MANIFEST              path to top5_trader_brief_latest_manifest.json
  TOP5_GATE_ENFORCE_RELAXED       1 = enable relaxed semantics check
  TOP5_GATE_REQUIRE_MANIFEST      1 = require manifest + sha256 match
  TOP5_GATE_SKIP_FORWARD          1 = skip SQLite forward evaluation
  TOP5_GATE_HORIZONS              comma horizons (default 5,20,60)
  TOP5_GATE_FAIL_ON_INFERRED      1 = --fail-on-inferred-as-of
  TOP5_GATE_FAIL_ON_EVAL_BLOCKING 1 = --fail-on-eval-blocking
  TOP5_GATE_MIN_SYM_PER_H         integer for --min-available-symbols-per-horizon
  TOP5_GATE_MIN_RATIO_PER_H       float for --min-available-ratio-per-horizon
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
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


def _latest_audit_artifact(directory: Path) -> Path | None:
    files = sorted(directory.glob("strategy_competition_portfolio_audit_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def _parse_horizons(raw: str) -> list[int]:
    out: list[int] = []
    for part in str(raw or "").split(","):
        part = part.strip()
        if not part:
            continue
        out.append(int(part))
    return out


def main() -> int:
    env_root = os.getenv("TOP5_GATE_PROJECT_ROOT", "").strip()
    project_root = Path(env_root).expanduser().resolve() if env_root else ROOT

    parser = argparse.ArgumentParser(description="Top5 audit evidence gate (WO-P0/P1 aggregate).")
    parser.add_argument(
        "--artifact-dir",
        default=os.getenv(
            "TOP5_GATE_ARTIFACT_DIR",
            os.getenv("STRATEGY_COMPETITION_AUDIT_OUTPUT_DIR", str(project_root / "logs/openclaw/strategy_competition_audit")),
        ),
        help="Directory containing strategy_competition_portfolio_audit_*.json",
    )
    parser.add_argument(
        "--manifest-json",
        default=os.getenv("TOP5_GATE_MANIFEST", str(project_root / "exports/top5_trader_brief_latest_manifest.json")),
        help="Path to top5_trader_brief_latest_manifest.json (optional check).",
    )
    parser.add_argument(
        "--enforce-relaxed-production-block",
        action="store_true",
        default=os.getenv("TOP5_GATE_ENFORCE_RELAXED", "").strip() == "1",
        help="When audit_mode==relaxed, require passed==false and mode_relaxed_disallows_production_release in blocking_reasons.",
    )
    parser.add_argument(
        "--require-manifest",
        action="store_true",
        default=os.getenv("TOP5_GATE_REQUIRE_MANIFEST", "").strip() == "1",
        help="Require manifest file and matching artifact SHA256.",
    )
    parser.add_argument(
        "--skip-forward-eval",
        action="store_true",
        default=os.getenv("TOP5_GATE_SKIP_FORWARD", "").strip() == "1",
        help="Skip SQLite forward-return evaluation and downstream forward gates.",
    )
    parser.add_argument("--db-path", default=os.getenv("PERMANENT_DB_PATH", "").strip(), help="SQLite DB path override.")
    parser.add_argument("--horizons", default=os.getenv("TOP5_GATE_HORIZONS", "5,20,60"))
    parser.add_argument("--fail-on-inferred-as-of", action="store_true", default=os.getenv("TOP5_GATE_FAIL_ON_INFERRED", "") == "1")
    parser.add_argument("--fail-on-eval-blocking", action="store_true", default=os.getenv("TOP5_GATE_FAIL_ON_EVAL_BLOCKING", "") == "1")
    parser.add_argument(
        "--min-available-symbols-per-horizon",
        type=int,
        default=int(os.getenv("TOP5_GATE_MIN_SYM_PER_H", "0") or 0),
    )
    parser.add_argument(
        "--min-available-ratio-per-horizon",
        type=float,
        default=float(os.getenv("TOP5_GATE_MIN_RATIO_PER_H", "0") or 0),
    )
    parser.add_argument("--forward-output-json", default="", help="Write forward evaluation payload when run.")
    args = parser.parse_args()

    artifact_dir = Path(str(args.artifact_dir)).expanduser().resolve()
    latest = _latest_audit_artifact(artifact_dir)
    if latest is None:
        print(json.dumps({"error": "no_audit_artifact", "directory": str(artifact_dir)}, indent=2))
        return 1

    summary: dict[str, object] = {"artifact_path": str(latest), "checks": []}

    try:
        audit = load_audit_artifact(latest)
    except Exception as exc:
        print(json.dumps({"error": "artifact_load_failed", "detail": str(exc)}, indent=2))
        return 1

    if str(audit.get("artifact_version") or "") != "strategy_competition_portfolio_audit.v1":
        print(json.dumps({"error": "artifact_version_rejected", "artifact_version": audit.get("artifact_version")}, indent=2))
        return 10

    summary["checks"].append("artifact_version_ok")

    if args.enforce_relaxed_production_block:
        mode = str(audit.get("audit_mode") or "").strip().lower()
        if mode == "relaxed":
            blocking = [str(x) for x in (audit.get("blocking_reasons") or [])]
            if audit.get("passed") is not False:
                print(json.dumps({"error": "relaxed_requires_passed_false", "passed": audit.get("passed")}, indent=2))
                return 11
            if "mode_relaxed_disallows_production_release" not in blocking:
                print(json.dumps({"error": "relaxed_missing_expected_block_reason", "blocking_reasons": blocking}, indent=2))
                return 11
        summary["checks"].append("relaxed_production_block_ok")

    if args.require_manifest:
        man_path = Path(str(args.manifest_json)).expanduser().resolve()
        if not man_path.is_file():
            print(json.dumps({"error": "manifest_missing", "path": str(man_path)}, indent=2))
            return 12
        try:
            manifest = json.loads(man_path.read_text(encoding="utf-8"))
        except Exception as exc:
            print(json.dumps({"error": "manifest_invalid_json", "detail": str(exc)}, indent=2))
            return 12
        if not isinstance(manifest, dict):
            print(json.dumps({"error": "manifest_not_object"}, indent=2))
            return 12
        art_mp = str(manifest.get("artifact_path") or "").strip()
        exp_hash = str(manifest.get("artifact_sha256") or "").strip()
        if not art_mp or not exp_hash:
            print(json.dumps({"error": "manifest_missing_sha_or_path", "manifest_keys": sorted(manifest.keys())}, indent=2))
            return 12
        ap = Path(art_mp).expanduser().resolve()
        if ap != latest.resolve():
            print(json.dumps({"error": "manifest_artifact_path_mismatch", "manifest": art_mp, "latest": str(latest)}, indent=2))
            return 12
        actual = _sha256_file(latest)
        if actual.lower() != exp_hash.lower():
            print(
                json.dumps(
                    {"error": "manifest_sha256_mismatch", "expected": exp_hash, "actual": actual},
                    indent=2,
                )
            )
            return 12
        summary["checks"].append("manifest_sha256_ok")

    horizons = _parse_horizons(args.horizons)
    if args.skip_forward_eval:
        print(json.dumps({"ok": True, "summary": summary}, ensure_ascii=False, indent=2, sort_keys=True))
        return 0

    dbp = Path(str(args.db_path or "").strip() or default_db_path()).resolve()
    try:
        conn = sqlite3.connect(str(dbp), timeout=60)
    except Exception as exc:
        print(json.dumps({"error": "db_connect_failed", "detail": str(exc)}, indent=2))
        return 13

    try:
        evaluation = evaluate_top5_forward_returns(
            conn,
            artifact=audit,
            horizons=horizons,
            subtract_roundtrip_cost_bps=0.0,
            override_as_of_compact="",
        )
    except Exception as exc:
        print(json.dumps({"error": "forward_evaluation_exception", "detail": str(exc)}, indent=2))
        return 13
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
        "artifact_path": str(latest),
        "artifact_sha256": _sha256_file(latest),
        "db_path": str(dbp),
        "aggregate_gate": True,
    }

    out_path = str(args.forward_output_json or "").strip()
    if out_path:
        p = Path(out_path).expanduser().resolve()
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(evaluation, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    if gate_failures:
        print(json.dumps({"error": "forward_gate_failed", "gate_failures": gate_failures, "summary": summary}, indent=2))
        return 14

    summary["checks"].append("forward_evaluation_gates_ok")
    print(json.dumps({"ok": True, "summary": summary}, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
