from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
DELEGATE = ROOT / "tools" / "kms_pki_delegate_verifier.py"


def _stable_hash(value: object) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _pseudo_pki_signature(payload: dict, *, key_id: str, algo: str, public_key_pem: str) -> str:
    body = _stable_hash(payload)
    token = f"{body}|{key_id}|{algo}|{public_key_pem}"
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _request(*, key_id: str = "benchmark_rotating_key_2026q3") -> dict:
    payload = {"contract_hash": "abc", "benchmark_trade_date": "20260502"}
    algo = "pkcs1_sha256_detached_v1"
    public_key_pem = "pem"
    return {
        "protocol_version": "benchmark_verify_request.v1",
        "request_type": "benchmark_contract_signature_verification",
        "key_id": key_id,
        "algo": algo,
        "signature": _pseudo_pki_signature(payload, key_id=key_id, algo=algo, public_key_pem=public_key_pem),
        "payload": payload,
        "public_key_pem": public_key_pem,
        "context": {"caller_principal": "benchmark_governance_gate"},
    }


def test_kms_delegate_verifies_with_iam_policy_and_writes_audit_log(tmp_path):
    policy_path = tmp_path / "iam_policy.json"
    policy_path.write_text(
        json.dumps(
            {
                "schema_version": "benchmark_kms_iam_policy.v1",
                "principals": {
                    "benchmark_governance_gate": {
                        "allowed_actions": ["verify"],
                        "allowed_algos": ["pkcs1_sha256_detached_v1"],
                        "allowed_key_ids": ["benchmark_rotating_key_2026q3"],
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    audit_path = tmp_path / "kms_audit.jsonl"
    env = os.environ.copy()
    env["AIRIVO_KMS_IAM_POLICY_FILE"] = str(policy_path)
    env["AIRIVO_KMS_AUDIT_LOG_PATH"] = str(audit_path)
    env["AIRIVO_KMS_DELEGATE_MODE"] = "offline"

    result = subprocess.run(
        [sys.executable, str(DELEGATE)],
        input=json.dumps(_request()),
        text=True,
        capture_output=True,
        check=False,
        env=env,
    )

    assert result.returncode == 0
    body = json.loads(result.stdout)
    assert body["protocol_version"] == "benchmark_verify_response.v1"
    assert body["verified"] is True
    rows = [json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 1
    assert rows[0]["schema_version"] == "benchmark_kms_audit_log.v1"
    assert rows[0]["verified"] is True
    assert rows[0]["entry_hash"]


def test_kms_delegate_blocks_iam_key_boundary_and_audits_failure(tmp_path):
    policy_path = tmp_path / "iam_policy.json"
    policy_path.write_text(
        json.dumps(
            {
                "schema_version": "benchmark_kms_iam_policy.v1",
                "principals": {
                    "benchmark_governance_gate": {
                        "allowed_actions": ["verify"],
                        "allowed_algos": ["pkcs1_sha256_detached_v1"],
                        "allowed_key_ids": ["benchmark_rotating_key_2026q3"],
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    audit_path = tmp_path / "kms_audit.jsonl"
    env = os.environ.copy()
    env["AIRIVO_KMS_IAM_POLICY_FILE"] = str(policy_path)
    env["AIRIVO_KMS_AUDIT_LOG_PATH"] = str(audit_path)

    result = subprocess.run(
        [sys.executable, str(DELEGATE)],
        input=json.dumps(_request(key_id="benchmark_default_key")),
        text=True,
        capture_output=True,
        check=False,
        env=env,
    )

    assert result.returncode == 0
    body = json.loads(result.stdout)
    assert body["verified"] is False
    assert body["reason"] == "iam_key_not_allowed:benchmark_governance_gate:benchmark_default_key"
    rows = [json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines()]
    assert rows[0]["verified"] is False
    assert rows[0]["reason"] == "iam_key_not_allowed:benchmark_governance_gate:benchmark_default_key"
