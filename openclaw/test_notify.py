#!/usr/bin/env python3
"""Send a test notification through existing notification_service."""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.runtime.root_dependency_bridge import load_notification_service_class

NotificationService = load_notification_service_class()


def main() -> int:
    parser = argparse.ArgumentParser(description="Send test notification")
    parser.add_argument("--config", default="notification_config.json")
    parser.add_argument("--title", default="[OpenClaw] Notification Test")
    parser.add_argument("--content", default="OpenClaw test message")
    parser.add_argument("--urgent", action="store_true")
    args = parser.parse_args()

    service = NotificationService(config_file=args.config)
    content = f"{args.content}\n\nTime: {datetime.now().isoformat()}"
    service.send_notification(args.title, content, urgent=args.urgent)

    print("test notification invoked")
    print(f"config: {args.config}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
