from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
HOOK = ROOT / "_archive" / "tools" / "verify_benchmark_contract_signature_hook.py"


def _stable_hash(value: object) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    import hashlib

    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _signature(payload: dict, secret: str) -> str:
    import hashlib

    return hashlib.sha256(f"{_stable_hash(payload)}|{secret}".encode("utf-8")).hexdigest()


def test_benchmark_signature_hook_verifies_sha256_secret_signature():
    payload = {"k": "v", "n": 1}
    secret = "unit-test-secret"
    request = {
        "protocol_version": "benchmark_verify_request.v1",
        "algo": "sha256_secret_v1",
        "signature": _signature(payload, secret),
        "payload": payload,
        "key_id": "benchmark_default_key",
        "public_key_pem": "",
    }
    env = os.environ.copy()
    env["AIRIVO_BENCHMARK_CONTRACT_SIGNING_SECRET"] = secret
    result = subprocess.run(
        [sys.executable, str(HOOK)],
        input=json.dumps(request),
        text=True,
        capture_output=True,
        check=False,
        env=env,
    )
    assert result.returncode == 0
    body = json.loads(result.stdout)
    assert body["verified"] is True
    assert body["protocol_version"] == "benchmark_verify_response.v1"


def test_benchmark_signature_hook_supports_delegate_verifier():
    request = {
        "protocol_version": "benchmark_verify_request.v1",
        "algo": "pkcs1_sha256_detached_v1",
        "signature": "any",
        "payload": {"k": "v"},
        "key_id": "benchmark_default_key",
        "public_key_pem": "pem",
    }
    env = os.environ.copy()
    env["AIRIVO_BENCHMARK_HOOK_DELEGATE_VERIFY_COMMAND"] = (
        f"{sys.executable} -c "
        "\"import json,sys; json.load(sys.stdin); print(json.dumps({'protocol_version':'benchmark_verify_response.v1','verified': True}))\""
    )
    result = subprocess.run(
        [sys.executable, str(HOOK)],
        input=json.dumps(request),
        text=True,
        capture_output=True,
        check=False,
        env=env,
    )
    assert result.returncode == 0
    body = json.loads(result.stdout)
    assert body["verified"] is True
    assert body["mode"] == "delegate"
    assert body["protocol_version"] == "benchmark_verify_response.v1"


def test_benchmark_signature_hook_reports_delegate_failure_reason():
    request = {
        "protocol_version": "benchmark_verify_request.v1",
        "algo": "pkcs1_sha256_detached_v1",
        "signature": "any",
        "payload": {"k": "v"},
        "key_id": "benchmark_default_key",
        "public_key_pem": "pem",
    }
    env = os.environ.copy()
    env["AIRIVO_BENCHMARK_HOOK_DELEGATE_VERIFY_COMMAND"] = (
        f"{sys.executable} -c "
        "\"import json,sys; json.load(sys.stdin); print(json.dumps({'protocol_version':'benchmark_verify_response.v1','verified': False, 'reason': 'kms_denied', 'severity': 'high'}))\""
    )
    result = subprocess.run(
        [sys.executable, str(HOOK)],
        input=json.dumps(request),
        text=True,
        capture_output=True,
        check=False,
        env=env,
    )
    assert result.returncode == 0
    body = json.loads(result.stdout)
    assert body["verified"] is False
    assert body["reason"] == "kms_denied"
    assert body["protocol_version"] == "benchmark_verify_response.v1"
