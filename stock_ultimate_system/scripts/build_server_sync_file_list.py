#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


FILE_LIST_VERSION = "server_sync_file_list.v1"


def _load_preflight(path: str | Path) -> dict[str, object]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"preflight payload is not a JSON object: {path}")
    return payload


def _validate_preflight(payload: dict[str, object]) -> None:
    if payload.get("preflight_version") != "server_sync_preflight.v1":
        raise ValueError("server sync file list requires a server_sync_preflight.v1 report")
    if payload.get("status") != "passed":
        raise ValueError("server sync file list requires a passed preflight report")
    allowed_files = payload.get("allowed_files")
    if not isinstance(allowed_files, list) or not all(isinstance(item, str) for item in allowed_files):
        raise ValueError("preflight report does not contain a valid allowed_files list")
    if not allowed_files:
        raise ValueError("preflight report allowed_files list is empty")


def build_server_sync_file_list(
    *,
    preflight_path: str | Path,
    output_path: str | Path,
    summary_output_path: str | Path | None = None,
) -> dict[str, object]:
    payload = _load_preflight(preflight_path)
    _validate_preflight(payload)

    allowed_files = sorted(set(payload["allowed_files"]))
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("".join(f"{path}\n" for path in allowed_files), encoding="utf-8")
    summary = {
        "file_list_version": FILE_LIST_VERSION,
        "status": "passed",
        "source_preflight": str(Path(preflight_path)),
        "output_path": str(output),
        "file_total": len(allowed_files),
        "sync_policy": payload.get("sync_policy"),
    }
    if summary_output_path:
        summary_output = Path(summary_output_path)
        summary_output.parent.mkdir(parents=True, exist_ok=True)
        summary_output.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Build an rsync --files-from list from a passed server sync preflight.")
    parser.add_argument("--preflight", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--summary-output")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    summary = build_server_sync_file_list(
        preflight_path=args.preflight,
        output_path=args.output,
        summary_output_path=args.summary_output,
    )
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        print(f"wrote {summary['file_total']} files to {summary['output_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
