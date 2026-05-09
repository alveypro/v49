import argparse
import json
import os
import smtplib
from email.mime.text import MIMEText
from pathlib import Path
from urllib import error, request

from src.utils.project_paths import resolve_project_path


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _send_webhook(message: str) -> tuple[bool, str]:
    webhook = os.getenv("ALERT_WEBHOOK_URL", "").strip()
    if not webhook:
        return False, "missing ALERT_WEBHOOK_URL"

    mode = os.getenv("ALERT_WEBHOOK_MODE", "generic").strip().lower()
    if mode == "dingtalk":
        payload = {"msgtype": "text", "text": {"content": message}}
    elif mode == "feishu":
        payload = {"msg_type": "text", "content": {"text": message}}
    else:
        payload = {"text": message}

    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = request.Request(
        webhook,
        data=body,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=10) as resp:
            return True, f"webhook status={resp.getcode()}"
    except error.URLError as exc:
        return False, f"webhook error={exc}"


def _send_email(subject: str, message: str) -> tuple[bool, str]:
    host = os.getenv("ALERT_SMTP_HOST", "").strip()
    port = int(os.getenv("ALERT_SMTP_PORT", "465"))
    user = os.getenv("ALERT_SMTP_USER", "").strip()
    password = os.getenv("ALERT_SMTP_PASSWORD", "").strip()
    to_addr = os.getenv("ALERT_EMAIL_TO", "").strip()
    from_addr = os.getenv("ALERT_EMAIL_FROM", user).strip()

    if not host or not user or not password or not to_addr:
        return False, "missing smtp/email envs"

    msg = MIMEText(message, _charset="utf-8")
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_addr

    try:
        with smtplib.SMTP_SSL(host, port, timeout=12) as server:
            server.login(user, password)
            server.sendmail(from_addr, [to_addr], msg.as_string())
        return True, "email sent"
    except Exception as exc:
        return False, f"email error={exc}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Dispatch alert for governance warn/fail status")
    parser.add_argument("--audit-json", default="data/experiments/governance_audit_latest.json")
    parser.add_argument("--buylist-json", default="data/experiments/buylist_latest.json")
    parser.add_argument("--allow-status", nargs="*", default=["warn", "fail"], help="Statuses that trigger alert")
    args = parser.parse_args()

    audit = _read_json(resolve_project_path(args.audit_json))
    buylist = _read_json(resolve_project_path(args.buylist_json))

    summary = (audit or {}).get("summary", {}) or {}
    status = str(summary.get("overall_status", "unknown")).strip().lower()
    if status not in {str(x).strip().lower() for x in args.allow_status}:
        print(json.dumps({"sent": False, "reason": f"status={status} not in allow list"}, ensure_ascii=False))
        return

    target_count = int((buylist or {}).get("target_count", 0) or 0)
    buyable_count = int((buylist or {}).get("buyable_count", 0) or 0)
    turnover_ratio = float((buylist or {}).get("turnover_ratio", 0.0) or 0.0)
    overlap_ratio = float((buylist or {}).get("overlap_with_previous_ratio", 0.0) or 0.0)

    subject = f"[T12 Alert] governance={status.upper()}"
    message = (
        f"T12治理告警: overall_status={status}\n"
        f"pass={summary.get('pass_count', 0)}, fail={summary.get('fail_count', 0)}, warn={summary.get('warn_count', 0)}\n"
        f"buyable/target={buyable_count}/{target_count}, turnover_ratio={turnover_ratio:.2%}, overlap_ratio={overlap_ratio:.2%}\n"
        "详情: https://airivo.online/T12/docs/governance_audit_latest.json"
    )

    webhook_ok, webhook_detail = _send_webhook(message)
    email_ok, email_detail = _send_email(subject, message)
    print(
        json.dumps(
            {
                "sent": webhook_ok or email_ok,
                "status": status,
                "webhook_ok": webhook_ok,
                "webhook_detail": webhook_detail,
                "email_ok": email_ok,
                "email_detail": email_detail,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
