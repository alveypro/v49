#!/usr/bin/env python3
"""Guardrail for OC proposal payloads before apply.

Rejects:
- missing/empty changed_files
- missing/placeholder unified_diff
- invalid status transition for apply stage
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple


PLACEHOLDER_PATTERNS = [
    r"existing code",
    r"old code",
    r"placeholder",
    r"index 12345\.\.67890",
    r"index 4b5c4b5\.\.1c9c9c9",
]


def _load_payload(path: str | None) -> Dict:
    if path:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    text = sys.stdin.read().strip()
    if not text:
        return {}
    return json.loads(text)


def validate(payload: Dict) -> Tuple[bool, List[str]]:
    errs: List[str] = []
    status = str(payload.get("status", "")).strip().lower()
    changed_files = payload.get("changed_files") or []
    unified_diff = str(payload.get("unified_diff", "") or "")

    if status in {"proposal_ready", "failed", ""}:
        errs.append(f"invalid_status:{status or 'missing'}")

    if not isinstance(changed_files, list) or not changed_files:
        errs.append("missing_changed_files")

    if not unified_diff.strip():
        errs.append("missing_unified_diff")
    else:
        if "@@" not in unified_diff or "diff --git" not in unified_diff:
            errs.append("invalid_unified_diff_format")
        lower = unified_diff.lower()
        for p in PLACEHOLDER_PATTERNS:
            if re.search(p, lower):
                errs.append("placeholder_unified_diff")
                break

    return len(errs) == 0, errs


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate OC proposal payload before apply.")
    parser.add_argument("--file", default=None, help="JSON payload file; default reads stdin")
    args = parser.parse_args()

    try:
        payload = _load_payload(args.file)
    except Exception as e:
        print(json.dumps({"ok": False, "errors": [f"invalid_json:{e}"]}, ensure_ascii=False))
        return 2

    ok, errors = validate(payload)
    out = {"ok": ok, "errors": errors}
    print(json.dumps(out, ensure_ascii=False))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
