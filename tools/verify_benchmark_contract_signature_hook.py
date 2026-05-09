#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import shlex
import subprocess
import sys
from typing import Any

REQUEST_PROTOCOL_VERSION = "benchmark_verify_request.v1"
RESPONSE_PROTOCOL_VERSION = "benchmark_verify_response.v1"


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _sign_with_secret(payload: dict, secret: str) -> str:
    body = _stable_hash(payload)
    return hashlib.sha256(f"{body}|{str(secret or '').strip()}".encode("utf-8")).hexdigest()


def _pseudo_pki_signature(payload: dict, *, key_id: str, algo: str, public_key_pem: str) -> str:
    body = _stable_hash(payload)
    token = f"{body}|{key_id}|{algo}|{public_key_pem}"
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _verify_with_delegate(command_text: str, request: dict) -> tuple[bool, str]:
    command = str(command_text or "").strip()
    if not command:
        return False, "missing_delegate_verify_command"
    try:
        result = subprocess.run(
            shlex.split(command),
            input=json.dumps(request, ensure_ascii=False, sort_keys=True),
            text=True,
            capture_output=True,
            timeout=20,
            check=False,
        )
    except Exception as exc:
        return False, f"delegate_execution_error:{exc}"
    if result.returncode != 0:
        return False, f"delegate_nonzero_exit:{result.returncode}"
    body = str(result.stdout or "").strip()
    if not body:
        return False, "delegate_empty_output"
    try:
        payload = json.loads(body)
    except Exception:
        return False, "delegate_invalid_json_output"
    if not isinstance(payload, dict):
        return False, "delegate_invalid_payload_object"
    version = str(payload.get("protocol_version") or "")
    if version != RESPONSE_PROTOCOL_VERSION:
        return False, f"delegate_protocol_version_invalid:{version}"
    if payload.get("verified") is True:
        return True, ""
    return False, str(payload.get("reason") or "delegate_reported_unverified")


def main() -> int:
    try:
        request = json.loads(sys.stdin.read() or "{}")
    except Exception as exc:
        print(json.dumps({"verified": False, "reason": f"invalid_stdin_json:{exc}"}))
        return 1
    if not isinstance(request, dict):
        print(json.dumps({"verified": False, "reason": "stdin_payload_not_object"}))
        return 1
    algo = str(request.get("algo") or "").strip()
    signature = str(request.get("signature") or "").strip()
    payload = request.get("payload") if isinstance(request.get("payload"), dict) else {}
    key_id = str(request.get("key_id") or "").strip()
    public_key_pem = str(request.get("public_key_pem") or "").strip()
    request_protocol_version = str(request.get("protocol_version") or "").strip()
    if request_protocol_version and request_protocol_version != REQUEST_PROTOCOL_VERSION:
        print(
            json.dumps(
                {
                    "protocol_version": RESPONSE_PROTOCOL_VERSION,
                    "verified": False,
                    "reason": f"request_protocol_version_invalid:{request_protocol_version}",
                    "severity": "high",
                }
            )
        )
        return 1
    if not algo or not signature or not payload:
        print(
            json.dumps(
                {
                    "protocol_version": RESPONSE_PROTOCOL_VERSION,
                    "verified": False,
                    "reason": "missing_required_signature_fields",
                    "severity": "high",
                }
            )
        )
        return 1
    delegate_command = os.getenv("AIRIVO_BENCHMARK_HOOK_DELEGATE_VERIFY_COMMAND", "").strip()
    if delegate_command:
        verified, reason = _verify_with_delegate(delegate_command, request)
        if not verified:
            print(
                json.dumps(
                    {
                        "protocol_version": RESPONSE_PROTOCOL_VERSION,
                        "verified": False,
                        "reason": reason,
                        "severity": "high",
                    }
                )
            )
            return 0
        print(json.dumps({"protocol_version": RESPONSE_PROTOCOL_VERSION, "verified": True, "mode": "delegate"}))
        return 0
    if algo == "sha256_secret_v1":
        secret = os.getenv("AIRIVO_BENCHMARK_CONTRACT_SIGNING_SECRET", "").strip()
        if not secret:
            print(
                json.dumps(
                    {
                        "protocol_version": RESPONSE_PROTOCOL_VERSION,
                        "verified": False,
                        "reason": "missing_signing_secret",
                        "severity": "high",
                    }
                )
            )
            return 1
        expected = _sign_with_secret(payload, secret)
    elif algo in {"pkcs1_sha256_detached_v1", "kms_hmac_sha256_v1"}:
        # Offline drill-compatible verifier: deterministic surrogate for PKI/KMS pipelines.
        expected = _pseudo_pki_signature(payload, key_id=key_id, algo=algo, public_key_pem=public_key_pem)
    else:
        print(
            json.dumps(
                {
                    "protocol_version": RESPONSE_PROTOCOL_VERSION,
                    "verified": False,
                    "reason": f"unsupported_algo:{algo}",
                    "severity": "high",
                }
            )
        )
        return 1
    if signature != expected:
        print(
            json.dumps(
                {
                    "protocol_version": RESPONSE_PROTOCOL_VERSION,
                    "verified": False,
                    "reason": "signature_mismatch",
                    "severity": "high",
                }
            )
        )
        return 0
    print(json.dumps({"protocol_version": RESPONSE_PROTOCOL_VERSION, "verified": True}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
