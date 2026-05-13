#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path
from typing import Any


ARTIFACT_VERSION = "top5_execution_court_record.v1"


def _now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _sha256_file(path: Path) -> str:
    if not path.is_file():
        return ""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"_missing": True, "_path": str(path)}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"_error": str(exc), "_path": str(path)}
    if isinstance(payload, dict):
        payload["_path"] = str(path)
        return payload
    return {"_error": "JSON payload is not an object", "_path": str(path)}


def _file_entry(label: str, path: Path) -> dict[str, Any]:
    return {
        "label": label,
        "path": str(path),
        "exists": path.is_file(),
        "size_bytes": path.stat().st_size if path.is_file() else 0,
        "sha256": _sha256_file(path),
    }


def _latest_import_manifest(import_dir: Path) -> Path | None:
    if not import_dir.exists():
        return None
    files = sorted(import_dir.glob("top5_exec_import_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def _verdict(*, sla: dict[str, Any], summary: dict[str, Any], readiness: dict[str, Any]) -> tuple[str, list[str]]:
    reasons: list[str] = []
    if sla.get("_missing") or summary.get("_missing") or readiness.get("_missing"):
        reasons.append("required_execution_evidence_artifact_missing")
    if sla.get("_error") or summary.get("_error") or readiness.get("_error"):
        reasons.append("required_execution_evidence_artifact_invalid")
    if str(sla.get("risk_level") or "") in {"red"}:
        reasons.append("execution_ops_sla_red")
    if int(sla.get("stale_open_observation_count") or 0) > 0:
        reasons.append("stale_open_observations_present")
    if str(summary.get("risk_level") or "") == "red":
        reasons.append("execution_evidence_summary_red")
    if str(readiness.get("verdict") or "") == "blocked":
        reasons.append("v9_promotion_readiness_blocked")
    if reasons:
        return "not_production_evidence", reasons
    if str(sla.get("risk_level") or "") in {"orange", "yellow"}:
        reasons.append("execution_ops_not_green")
    if int(summary.get("open_observation_count") or 0) > 0:
        reasons.append("open_observations_present")
    if reasons:
        return "candidate_only", reasons
    return "execution_evidence_clean_currently", []


def build_record(root: Path, *, output_json: Path, output_md: Path) -> dict[str, Any]:
    exports = root / "exports"
    logs = root / "logs/openclaw"
    top5_manifest = _load_json(exports / "top5_trader_brief_latest_manifest.json")
    sla = _load_json(exports / "top5_execution_ops_sla.json")
    summary = _load_json(exports / "top5_execution_evidence_summary.json")
    readiness = _load_json(exports / "v9_canary_promotion_readiness.json")
    latest_import = _latest_import_manifest(logs / "top5_execution_imports")
    verdict, reasons = _verdict(sla=sla, summary=summary, readiness=readiness)
    files = [
        _file_entry("top5_manifest", exports / "top5_trader_brief_latest_manifest.json"),
        _file_entry("open_observations_csv", exports / "top5_execution_open_observations.csv"),
        _file_entry("execution_ops_sla_json", exports / "top5_execution_ops_sla.json"),
        _file_entry("execution_ops_sla_md", exports / "top5_execution_ops_sla.md"),
        _file_entry("execution_evidence_summary_json", exports / "top5_execution_evidence_summary.json"),
        _file_entry("execution_evidence_summary_md", exports / "top5_execution_evidence_summary.md"),
        _file_entry("v9_promotion_readiness_json", exports / "v9_canary_promotion_readiness.json"),
        _file_entry("v9_promotion_readiness_md", exports / "v9_canary_promotion_readiness.md"),
        _file_entry("execution_ledger", logs / "top5_execution_observations.jsonl"),
    ]
    if latest_import is not None:
        files.append(_file_entry("latest_execution_import_manifest", latest_import))
    return {
        "artifact_version": ARTIFACT_VERSION,
        "created_at": _now_text(),
        "root": str(root),
        "verdict": verdict,
        "reasons": reasons,
        "trade_date_compact": top5_manifest.get("trade_date_compact"),
        "competition_run_id": top5_manifest.get("competition_run_id"),
        "top5_canary_gate_passed": (top5_manifest.get("top5_canary_gate") or {}).get("passed"),
        "execution_ops_sla_risk": sla.get("risk_level"),
        "execution_evidence_risk": summary.get("risk_level"),
        "v9_promotion_verdict": readiness.get("verdict"),
        "open_observation_count": summary.get("open_observation_count"),
        "stale_open_observation_count": sla.get("stale_open_observation_count"),
        "closed_evidence_trade_days": readiness.get("closed_evidence_trade_days"),
        "effective_import_manifest_count": readiness.get("import_manifest_count"),
        "files": files,
        "output_json": str(output_json),
        "output_md": str(output_md),
    }


def _markdown(record: dict[str, Any]) -> str:
    lines = [
        "# Top5 Execution Court Record",
        "",
        f"- Created at: {record.get('created_at')}",
        f"- Verdict: {record.get('verdict')}",
        f"- Reasons: {', '.join(record.get('reasons') or []) or 'none'}",
        f"- Trade date: {record.get('trade_date_compact') or 'n/a'}",
        f"- Competition run: {record.get('competition_run_id') or 'n/a'}",
        f"- Top5 canary gate passed: {record.get('top5_canary_gate_passed')}",
        f"- SLA risk: {record.get('execution_ops_sla_risk')}",
        f"- Execution evidence risk: {record.get('execution_evidence_risk')}",
        f"- V9 promotion verdict: {record.get('v9_promotion_verdict')}",
        f"- Open observations: {record.get('open_observation_count')}",
        f"- Stale open observations: {record.get('stale_open_observation_count')}",
        f"- Closed evidence trade days: {record.get('closed_evidence_trade_days')}",
        f"- Effective import manifests: {record.get('effective_import_manifest_count')}",
        "",
        "## Files",
    ]
    for item in record.get("files") or []:
        lines.append(
            f"- {item.get('label')}: exists={item.get('exists')} size={item.get('size_bytes')} "
            f"sha256={item.get('sha256') or 'n/a'} path={item.get('path')}"
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Build one court-of-record manifest for Top5 execution evidence.")
    parser.add_argument("--root", default=".")
    parser.add_argument("--output-json", default="exports/top5_execution_court_record.json")
    parser.add_argument("--output-md", default="exports/top5_execution_court_record.md")
    args = parser.parse_args()
    root = Path(args.root).resolve()
    output_json = root / args.output_json
    output_md = root / args.output_md
    output_json.parent.mkdir(parents=True, exist_ok=True)
    record = build_record(root, output_json=output_json, output_md=output_md)
    output_json.write_text(json.dumps(record, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    output_md.write_text(_markdown(record), encoding="utf-8")
    print(json.dumps({"output_json": str(output_json), "verdict": record["verdict"]}, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
