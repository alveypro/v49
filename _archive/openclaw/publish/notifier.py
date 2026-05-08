"""Notification publisher with explicit safety gates.

Supported channels:
- stdout
- file (append to local outbox)
- webhook (optional, POST JSON)
- legacy_service (bridge to local notification_service.py)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
import json
import hashlib
import hmac
import os
import time
import uuid
import urllib.request

from openclaw.runtime.root_dependency_bridge import load_notification_service_class


JsonDict = Dict[str, Any]


@dataclass
class NotificationPublisher:
    config: JsonDict

    def publish(self, title: str, body: str, metadata: JsonDict | None = None) -> JsonDict:
        metadata = metadata or {}
        channels: List[str] = list(self.config.get("channels", ["stdout"]))
        results: List[JsonDict] = []

        payload = {
            "title": title,
            "body": body,
            "metadata": metadata,
            "sent_at": datetime.now().isoformat(),
        }

        for channel in channels:
            if channel == "stdout":
                results.append(self._publish_stdout(payload))
            elif channel == "file":
                results.append(self._publish_file(payload))
            elif channel == "webhook":
                results.append(self._publish_webhook(payload))
            elif channel == "legacy_service":
                results.append(self._publish_legacy_service(payload))
            else:
                results.append({"channel": channel, "ok": False, "error": "unsupported channel"})

        ok = all(r.get("ok") for r in results) if results else False
        return {"ok": ok, "results": results, "payload": payload}

    def _publish_stdout(self, payload: JsonDict) -> JsonDict:
        print("[openclaw notify]", payload["title"])
        print(payload["body"])
        return {"channel": "stdout", "ok": True}

    def _publish_file(self, payload: JsonDict) -> JsonDict:
        outbox = Path(self.config.get("outbox_path", "logs/openclaw/notify_outbox.log"))
        outbox.parent.mkdir(parents=True, exist_ok=True)
        with outbox.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        return {"channel": "file", "ok": True, "path": str(outbox)}

    def _publish_webhook(self, payload: JsonDict) -> JsonDict:
        url = str(self.config.get("webhook_url", "")).strip()
        if not url:
            return {"channel": "webhook", "ok": False, "error": "webhook_url missing"}

        timeout = float(self.config.get("webhook_timeout_sec", 5.0))
        retries = int(self.config.get("webhook_retries", 2))
        secret = self._resolve_webhook_secret()
        signed_payload = dict(payload)
        ts = str(int(time.time()))
        nonce = uuid.uuid4().hex
        body = json.dumps(signed_payload, ensure_ascii=False).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if secret:
            sig = hmac.new(secret.encode("utf-8"), f"{ts}.{nonce}.".encode("utf-8") + body, hashlib.sha256).hexdigest()
            headers.update(
                {
                    "X-OpenClaw-Timestamp": ts,
                    "X-OpenClaw-Nonce": nonce,
                    "X-OpenClaw-Signature": sig,
                    "X-OpenClaw-Signature-Alg": "HMAC-SHA256",
                }
            )

        last_err = ""
        for i in range(max(1, retries + 1)):
            req = urllib.request.Request(
                url=url,
                data=body,
                headers=headers,
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
                    status = int(resp.status)
                ok = 200 <= status < 300
                if ok:
                    return {"channel": "webhook", "ok": True, "status": status, "attempt": i + 1}
                last_err = f"unexpected status={status}"
            except Exception as exc:  # noqa: BLE001
                last_err = str(exc)
            time.sleep(min(2.0, 0.5 * (i + 1)))
        return {"channel": "webhook", "ok": False, "error": last_err, "attempts": retries + 1}

    def _resolve_webhook_secret(self) -> str:
        env_name = str(self.config.get("webhook_secret_env", "")).strip()
        if env_name:
            val = os.getenv(env_name, "").strip()
            if val:
                return val
        secret_file = str(self.config.get("webhook_secret_file", "")).strip()
        if secret_file:
            p = Path(secret_file)
            if p.exists():
                try:
                    return p.read_text(encoding="utf-8").strip()
                except Exception:
                    return ""
        return ""

    def _publish_legacy_service(self, payload: JsonDict) -> JsonDict:
        """Use existing local NotificationService implementation."""
        config_file = str(self.config.get("legacy_config_file", "notification_config.json"))
        try:
            NotificationService = load_notification_service_class()
            urgent = bool(payload.get("metadata", {}).get("risk_level") in {"orange", "red"})
            service = NotificationService(config_file=config_file)
            service.send_notification(
                title=str(payload.get("title", "")),
                content=str(payload.get("body", "")),
                urgent=urgent,
            )
            return {
                "channel": "legacy_service",
                "ok": True,
                "config_file": config_file,
                "urgent": urgent,
            }
        except Exception as exc:  # noqa: BLE001
            return {"channel": "legacy_service", "ok": False, "error": str(exc), "config_file": config_file}
