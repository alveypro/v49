#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
import sys

sys.path.insert(0, str(ROOT))

from tools.check_top5_execution_observation_completeness import (  # noqa: E402
    CLOSING_STATUSES,
    OPEN_STATUSES,
    _key,
    _load_jsonl,
    build_report as build_completeness_report,
)


ARTIFACT_VERSION = "v9_canary_promotion_readiness.v1"


def _now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _load_import_manifests(path: Path) -> list[dict[str, Any]]:
    manifests: list[dict[str, Any]] = []
    if not path.exists():
        return manifests
    for item in sorted(path.glob("top5_exec_import_*.json")):
        try:
            payload = json.loads(item.read_text(encoding="utf-8"))
        except Exception as exc:
            manifests.append({"path": str(item), "parse_error": str(exc)})
            continue
        if isinstance(payload, dict):
            payload["path"] = str(item)
            manifests.append(payload)
    return manifests


def _date_stats(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    planned_keys_by_date: dict[str, set[tuple[str, str]]] = defaultdict(set)
    closed_keys_by_date: dict[str, set[tuple[str, str]]] = defaultdict(set)
    status_counts_by_date: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        if row.get("_parse_error"):
            continue
        trade_date = str(row.get("trade_date_compact") or "")
        if not trade_date:
            continue
        status = str(row.get("status") or "")
        if status in OPEN_STATUSES | CLOSING_STATUSES:
            planned_keys_by_date[trade_date].add(_key(row))
            status_counts_by_date[trade_date][status] += 1
        if status in CLOSING_STATUSES:
            closed_keys_by_date[trade_date].add(_key(row))
    out: list[dict[str, Any]] = []
    for trade_date in sorted(planned_keys_by_date):
        planned = len(planned_keys_by_date[trade_date])
        closed = len(closed_keys_by_date[trade_date])
        out.append(
            {
                "trade_date_compact": trade_date,
                "planned_signal_count": planned,
                "closed_signal_count": closed,
                "closure_rate": round((closed / planned) if planned else 0.0, 6),
                "status_counts": dict(sorted(status_counts_by_date[trade_date].items())),
            }
        )
    return out


def _verdict(
    *,
    evidence_days: int,
    min_review_days: int,
    min_promotion_days: int,
    min_closure_rate: float,
    closure_rate: float,
    quality_gap_count: int,
    parse_error_count: int,
    import_manifest_count: int,
) -> tuple[str, list[str], list[str]]:
    blockers: list[str] = []
    actions: list[str] = []
    if parse_error_count > 0:
        blockers.append("ledger_parse_errors_present")
        actions.append("Repair ledger parse errors before using execution evidence.")
    if quality_gap_count > 0:
        blockers.append("execution_quality_gaps_present")
        actions.append("Backfill missing prices, quantities, slippage, or failure attribution.")
    if evidence_days < min_review_days:
        blockers.append(f"insufficient_evidence_days:{evidence_days}<{min_review_days}")
        actions.append(f"Keep v9 in canary until at least {min_review_days} closed trading days are observed.")
    if closure_rate < min_closure_rate:
        blockers.append(f"closure_rate_below_min:{closure_rate:.2%}<{min_closure_rate:.2%}")
        actions.append("Close all planned/submitted rows via audited CSV import batches.")
    if import_manifest_count == 0:
        blockers.append("no_execution_import_manifests")
        actions.append("Use the CSV import tool so execution updates have batch audit manifests.")
    if blockers:
        return "blocked", blockers, actions
    if evidence_days < min_promotion_days:
        return (
            "reviewable_not_promotable",
            [f"promotion_window_not_complete:{evidence_days}<{min_promotion_days}"],
            [f"Continue collecting evidence until {min_promotion_days} trading days before production promotion review."],
        )
    return "eligible_for_human_review", [], ["Open a separate human approval artifact before changing registry tier."]


def build_report(
    *,
    ledger: Path,
    import_dir: Path,
    min_review_days: int,
    min_promotion_days: int,
    min_closure_rate: float,
) -> dict[str, Any]:
    rows = _load_jsonl(ledger)
    completeness_report = build_completeness_report(rows)
    valid = [row for row in rows if not row.get("_parse_error")]
    planned_keys = {_key(row) for row in valid if str(row.get("status") or "") in OPEN_STATUSES | CLOSING_STATUSES}
    closed_keys = {_key(row) for row in valid if str(row.get("status") or "") in CLOSING_STATUSES}
    date_rows = _date_stats(rows)
    closed_dates = [item for item in date_rows if float(item.get("closure_rate") or 0.0) >= min_closure_rate]
    closure_rate = (len(closed_keys) / len(planned_keys)) if planned_keys else 0.0
    manifests = _load_import_manifests(import_dir)
    manifest_parse_errors = [m for m in manifests if m.get("parse_error")]
    effective_manifests = [
        m
        for m in manifests
        if not m.get("parse_error") and m.get("dry_run") is not True and int(m.get("imported_count") or 0) > 0
    ]
    verdict, blockers, actions = _verdict(
        evidence_days=len(closed_dates),
        min_review_days=min_review_days,
        min_promotion_days=min_promotion_days,
        min_closure_rate=min_closure_rate,
        closure_rate=closure_rate,
        quality_gap_count=int(completeness_report.get("quality_gap_count") or 0),
        parse_error_count=int(completeness_report.get("parse_error_count") or 0) + len(manifest_parse_errors),
        import_manifest_count=len(effective_manifests),
    )
    return {
        "artifact_version": ARTIFACT_VERSION,
        "created_at": _now_text(),
        "strategy": "v9",
        "current_tier": "canary",
        "verdict": verdict,
        "blockers": blockers,
        "recommended_actions": actions,
        "criteria": {
            "min_review_days": min_review_days,
            "min_promotion_days": min_promotion_days,
            "min_closure_rate": min_closure_rate,
        },
        "ledger": str(ledger),
        "import_dir": str(import_dir),
        "planned_signal_count": len(planned_keys),
        "closed_signal_count": len(closed_keys),
        "overall_closure_rate": round(closure_rate, 6),
        "evidence_trade_days": len(date_rows),
        "closed_evidence_trade_days": len(closed_dates),
        "import_manifest_count": len(effective_manifests),
        "all_import_manifest_count": len([m for m in manifests if not m.get("parse_error")]),
        "import_manifest_parse_error_count": len(manifest_parse_errors),
        "parse_error_count": completeness_report.get("parse_error_count"),
        "quality_gap_count": completeness_report.get("quality_gap_count"),
        "open_observation_count": completeness_report.get("open_observation_count"),
        "date_stats": date_rows[-80:],
        "latest_import_manifests": [
            {
                "path": m.get("path"),
                "batch_id": m.get("batch_id"),
                "created_at": m.get("created_at"),
                "dry_run": m.get("dry_run"),
                "imported_count": m.get("imported_count"),
                "skipped_count": m.get("skipped_count"),
            }
            for m in manifests[-20:]
        ],
    }


def _markdown(report: dict[str, Any]) -> str:
    lines = [
        "# V9 Canary Promotion Readiness",
        "",
        f"- Created at: {report.get('created_at')}",
        f"- Verdict: {report.get('verdict')}",
        f"- Current tier: {report.get('current_tier')}",
        f"- Evidence trade days: {report.get('evidence_trade_days')}",
        f"- Closed evidence trade days: {report.get('closed_evidence_trade_days')}",
        f"- Overall closure rate: {float(report.get('overall_closure_rate') or 0.0):.2%}",
        f"- Planned signals: {report.get('planned_signal_count')}",
        f"- Closed signals: {report.get('closed_signal_count')}",
        f"- Import manifests: {report.get('import_manifest_count')}",
        "",
        "## Blockers",
    ]
    blockers = report.get("blockers") or []
    lines.extend([f"- {item}" for item in blockers] or ["- none"])
    lines.extend(["", "## Recommended Actions"])
    actions = report.get("recommended_actions") or []
    lines.extend([f"- {item}" for item in actions] or ["- none"])
    lines.extend(["", "## Recent Date Stats"])
    for item in (report.get("date_stats") or [])[-20:]:
        lines.append(
            f"- {item.get('trade_date_compact')}: "
            f"closed={item.get('closed_signal_count')}/{item.get('planned_signal_count')} "
            f"closure={float(item.get('closure_rate') or 0.0):.2%}"
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate whether v9 canary has enough execution evidence for promotion review.")
    parser.add_argument("--ledger", default="logs/openclaw/top5_execution_observations.jsonl")
    parser.add_argument("--import-dir", default="logs/openclaw/top5_execution_imports")
    parser.add_argument("--output-json", default="exports/v9_canary_promotion_readiness.json")
    parser.add_argument("--output-md", default="exports/v9_canary_promotion_readiness.md")
    parser.add_argument("--min-review-days", type=int, default=20)
    parser.add_argument("--min-promotion-days", type=int, default=60)
    parser.add_argument("--min-closure-rate", type=float, default=0.95)
    args = parser.parse_args()
    report = build_report(
        ledger=Path(args.ledger),
        import_dir=Path(args.import_dir),
        min_review_days=int(args.min_review_days),
        min_promotion_days=int(args.min_promotion_days),
        min_closure_rate=float(args.min_closure_rate),
    )
    out_json = Path(args.output_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.output_md:
        out_md = Path(args.output_md)
        out_md.parent.mkdir(parents=True, exist_ok=True)
        out_md.write_text(_markdown(report), encoding="utf-8")
    print(json.dumps({"output_json": str(out_json), "verdict": report["verdict"]}, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
