#!/usr/bin/env python3
"""Local test for DingTalk bridge callback endpoint."""

from __future__ import annotations

import argparse
import json
import requests


def main() -> int:
    ap = argparse.ArgumentParser(description="Test local DingTalk callback bridge")
    ap.add_argument("--url", default="http://127.0.0.1:8601/dingtalk/callback")
    ap.add_argument("--text", default="@ClawAlpha 你好呀")
    args = ap.parse_args()

    payload = {
        "msgtype": "text",
        "text": {"content": args.text},
        "conversationId": "diag-group",
        "senderNick": "local-test",
    }

    r = requests.post(args.url, json=payload, timeout=30)
    print("status:", r.status_code)
    try:
        print(json.dumps(r.json(), ensure_ascii=False, indent=2))
    except Exception:
        print(r.text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
