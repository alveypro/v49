#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
from typing import Any
from urllib import request as urlrequest


REQUEST_PROTOCOL_VERSION = "benchmark_verify_request.v1"
RESPONSE_PROTOCOL_VERSION = "benchmark_verify_response.v1"
IAM_POLICY_VERSION = "benchmark_kms_iam_policy.v1"


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _response(*, verified: bool, reason: str = "", severity: str = "high", **extra: Any) -> dict:
    payload = {
        "protocol_version": RESPONSE_PROTOCOL_VERSION,
        "verified": bool(verified),
        **extra,
    }
    if not verified:
        payload["reason"] = str(reason or "verification_failed")
        payload["severity"] = str(severity or "high")
    return payload


def _load_json_file(path_text: str) -> dict:
    path = Path(str(path_text or "").strip())
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _load_iam_policy() -> dict:
    inline = os.getenv("AIRIVO_KMS_IAM_POLICY_INLINE", "").strip()
    if inline:
        try:
            payload = json.loads(inline)
        except Exception:
            return {"schema_version": "invalid_inline_policy"}
        return payload if isinstance(payload, dict) else {"schema_version": "invalid_inline_policy"}
    return _load_json_file(os.getenv("AIRIVO_KMS_IAM_POLICY_FILE", ""))


def _principal_allowed(policy: dict, *, principal: str, key_id: str, algo: str) -> tuple[bool, str]:
    if not policy:
        return True, ""
    if str(policy.get("schema_version") or "") != IAM_POLICY_VERSION:
        return False, "iam_policy_schema_invalid"
    principals = policy.get("principals") if isinstance(policy.get("principals"), dict) else {}
    meta = principals.get(principal) if isinstance(principals.get(principal), dict) else {}
    if not meta:
        return False, f"iam_principal_not_allowed:{principal}"
    actions = {str(item) for item in meta.get("allowed_actions") or []}
    if actions and "verify" not in actions:
        return False, f"iam_action_not_allowed:{principal}:verify"
    allowed_keys = {str(item) for item in meta.get("allowed_key_ids") or []}
    if allowed_keys and key_id not in allowed_keys:
        return False, f"iam_key_not_allowed:{principal}:{key_id}"
    allowed_algos = {str(item) for item in meta.get("allowed_algos") or []}
    if allowed_algos and algo not in allowed_algos:
        return False, f"iam_algo_not_allowed:{principal}:{algo}"
    return True, ""


def _secret_signature(payload: dict, secret: str) -> str:
    body = _stable_hash(payload)
    return hashlib.sha256(f"{body}|{str(secret or '').strip()}".encode("utf-8")).hexdigest()


def _pseudo_pki_signature(payload: dict, *, key_id: str, algo: str, public_key_pem: str) -> str:
    body = _stable_hash(payload)
    token = f"{body}|{key_id}|{algo}|{public_key_pem}"
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _verify_offline(request_payload: dict) -> tuple[bool, str]:
    algo = str(request_payload.get("algo") or "").strip()
    signature = str(request_payload.get("signature") or "").strip()
    payload = request_payload.get("payload") if isinstance(request_payload.get("payload"), dict) else {}
    key_id = str(request_payload.get("key_id") or "").strip()
    public_key_pem = str(request_payload.get("public_key_pem") or "")
    if algo == "sha256_secret_v1":
        secret = os.getenv("AIRIVO_BENCHMARK_CONTRACT_SIGNING_SECRET", "").strip()
        if not secret:
            return False, "missing_signing_secret"
        expected = _secret_signature(payload, secret)
    elif algo in {"pkcs1_sha256_detached_v1", "kms_hmac_sha256_v1"}:
        expected = _pseudo_pki_signature(payload, key_id=key_id, algo=algo, public_key_pem=public_key_pem)
    else:
        return False, f"unsupported_algo:{algo}"
    if signature != expected:
        return False, "signature_mismatch"
    return True, ""


def _verify_http(request_payload: dict) -> tuple[bool, str]:
    endpoint = os.getenv("AIRIVO_KMS_DELEGATE_ENDPOINT", "").strip()
    if not endpoint:
        return False, "missing_kms_delegate_endpoint"
    token = os.getenv("AIRIVO_KMS_DELEGATE_TOKEN", "").strip()
    timeout = int(os.getenv("AIRIVO_KMS_DELEGATE_TIMEOUT_SECONDS", "10") or "10")
    body = json.dumps(request_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urlrequest.Request(endpoint, data=body, headers=headers, method="POST")
    try:
        with urlrequest.urlopen(req, timeout=max(1, min(60, timeout))) as resp:
            response_payload = json.loads(resp.read().decode("utf-8") or "{}")
    except Exception as exc:
        return False, f"kms_http_error:{exc}"
    if not isinstance(response_payload, dict):
        return False, "kms_http_payload_not_object"
    if response_payload.get("verified") is True:
        return True, ""
    return False, str(response_payload.get("reason") or "kms_http_unverified")


def _append_audit_log(*, request_payload: dict, response_payload: dict, principal: str) -> None:
    path_text = os.getenv("AIRIVO_KMS_AUDIT_LOG_PATH", "").strip()
    if not path_text:
        return
    path = Path(path_text)
    path.parent.mkdir(parents=True, exist_ok=True)
    previous_hash = ""
    if path.exists():
        try:
            lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
            if lines:
                previous_hash = str(json.loads(lines[-1]).get("entry_hash") or "")
        except Exception:
            previous_hash = ""
    entry_core = {
        "schema_version": "benchmark_kms_audit_log.v1",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "principal": principal,
        "key_id": str(request_payload.get("key_id") or ""),
        "algo": str(request_payload.get("algo") or ""),
        "request_hash": _stable_hash(
            {
                "protocol_version": request_payload.get("protocol_version"),
                "key_id": request_payload.get("key_id"),
                "algo": request_payload.get("algo"),
                "signature_hash": _stable_hash(str(request_payload.get("signature") or "")),
                "payload_hash": _stable_hash(request_payload.get("payload") or {}),
            }
        ),
        "verified": bool(response_payload.get("verified") is True),
        "reason": str(response_payload.get("reason") or ""),
        "previous_hash": previous_hash,
    }
    entry = {**entry_core, "entry_hash": _stable_hash(entry_core)}
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\n")


def main() -> int:
    try:
        request_payload = json.loads(sys.stdin.read() or "{}")
    except Exception as exc:
        print(json.dumps(_response(verified=False, reason=f"invalid_stdin_json:{exc}", severity="critical")))
        return 1
    if not isinstance(request_payload, dict):
        print(json.dumps(_response(verified=False, reason="stdin_payload_not_object", severity="critical")))
        return 1
    if str(request_payload.get("protocol_version") or "") != REQUEST_PROTOCOL_VERSION:
        print(json.dumps(_response(verified=False, reason="request_protocol_version_invalid", severity="critical")))
        return 1
    principal = (
        str((request_payload.get("context") or {}).get("caller_principal") or "").strip()
        if isinstance(request_payload.get("context"), dict)
        else ""
    ) or os.getenv("AIRIVO_KMS_CALLER_PRINCIPAL", "benchmark_governance_gate").strip()
    key_id = str(request_payload.get("key_id") or "").strip()
    algo = str(request_payload.get("algo") or "").strip()
    policy = _load_iam_policy()
    allowed, reason = _principal_allowed(policy, principal=principal, key_id=key_id, algo=algo)
    if not allowed:
        response_payload = _response(verified=False, reason=reason, severity="critical", mode="iam")
        _append_audit_log(request_payload=request_payload, response_payload=response_payload, principal=principal)
        print(json.dumps(response_payload, ensure_ascii=False, sort_keys=True))
        return 0
    mode = os.getenv("AIRIVO_KMS_DELEGATE_MODE", "offline").strip().lower()
    if mode == "http":
        verified, reason = _verify_http(request_payload)
    elif mode == "offline":
        verified, reason = _verify_offline(request_payload)
    else:
        verified, reason = False, f"delegate_mode_invalid:{mode}"
    response_payload = (
        _response(verified=True, mode=mode, principal=principal)
        if verified
        else _response(verified=False, reason=reason, severity="high", mode=mode, principal=principal)
    )
    _append_audit_log(request_payload=request_payload, response_payload=response_payload, principal=principal)
    print(json.dumps(response_payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
