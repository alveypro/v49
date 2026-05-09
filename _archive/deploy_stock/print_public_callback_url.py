#!/usr/bin/env python3
"""Print ngrok public callback URL for DingTalk."""

from __future__ import annotations

import json
import sys
from urllib.request import urlopen


def main() -> int:
    api = "http://127.0.0.1:4040/api/tunnels"
    try:
        with urlopen(api, timeout=5) as resp:  # noqa: S310
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: cannot read ngrok API at {api}: {exc}")
        return 1

    tunnels = data.get("tunnels") or []
    https_url = ""
    for t in tunnels:
        url = str(t.get("public_url", ""))
        if url.startswith("https://"):
            https_url = url
            break
    if not https_url:
        print("ERROR: no https tunnel found")
        return 1

    print("Public base URL:", https_url)
    print("DingTalk callback URL:", https_url.rstrip("/") + "/dingtalk/callback")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
