# Benchmark Signature Integration Runbook

## Goal

Bridge benchmark contract verification from local deterministic checks to production-grade KMS/PKI verification with strict governance gate blocking.

## Required contract fields

- `approval_signature_algo`
- `approval_key_id`
- `approval_signature`
- `provider_receipt_hash`
- `provider_batch_id`
- `provider_snapshot_id`

All fields above are covered by `contract_hash` and validated by `tools/governance_gate.py`.

## Verification modes

### 1) Secret mode (transition)

Use for controlled rollout only:

- Set `AIRIVO_BENCHMARK_CONTRACT_SIGNING_SECRET`
- Keep `approval_signature_algo=sha256_secret_v1`

Gate verifies deterministic signature hash.

### 2) Hook mode (recommended pre-production)

Set keyring `verify_mode=hook` and provide verifier command:

- `verify_hook_command` in keyring entry, or
- `AIRIVO_BENCHMARK_CONTRACT_VERIFY_HOOK`

The command receives JSON on stdin and must return protocol v1 JSON:

- request: `{"protocol_version":"benchmark_verify_request.v1", ...}`
- success: `{"protocol_version":"benchmark_verify_response.v1","verified":true}`
- failure: `{"protocol_version":"benchmark_verify_response.v1","verified":false,"reason":"...","severity":"low|medium|high|critical"}`

Legacy responses without `protocol_version` are blocked by gate.

Reference implementation:

- `tools/verify_benchmark_contract_signature_hook.py`
- `tools/kms_pki_delegate_verifier.py`

## Key governance controls

- Allowed algos: `AIRIVO_BENCHMARK_CONTRACT_ALLOWED_ALGOS`
- Active keys: `AIRIVO_BENCHMARK_CONTRACT_ACTIVE_KEY_IDS`
- Revoked keys: `AIRIVO_BENCHMARK_CONTRACT_REVOKED_KEY_IDS`
- Keyring file: `AIRIVO_BENCHMARK_CONTRACT_KEYRING_FILE`
- Hook timeout: `AIRIVO_BENCHMARK_CONTRACT_VERIFY_HOOK_TIMEOUT_SECONDS`
- Hook failure policy: `AIRIVO_BENCHMARK_CONTRACT_VERIFY_HOOK_FAILURE_POLICY` (`block` or `warn`)
- Hook alert threshold: `AIRIVO_BENCHMARK_CONTRACT_VERIFY_HOOK_ALERT_MIN_LEVEL`
- Delegate command: `AIRIVO_BENCHMARK_HOOK_DELEGATE_VERIFY_COMMAND`
- KMS delegate mode: `AIRIVO_KMS_DELEGATE_MODE` (`offline` or `http`)
- KMS IAM policy: `AIRIVO_KMS_IAM_POLICY_FILE` or `AIRIVO_KMS_IAM_POLICY_INLINE`
- KMS audit log path: `AIRIVO_KMS_AUDIT_LOG_PATH`
- HTTP KMS endpoint: `AIRIVO_KMS_DELEGATE_ENDPOINT`

If any check fails, governance gate blocks production.

## IAM Boundary

Use `docs/benchmark_kms_iam_policy.example.json` to bind:

- principal
- allowed actions
- allowed key ids
- allowed signature algorithms

The default production principal should be `benchmark_governance_gate`. Do not allow broad wildcard key access in production.

## Audit Retention

`tools/kms_pki_delegate_verifier.py` can append hash-chain JSONL audit entries through `AIRIVO_KMS_AUDIT_LOG_PATH`.

Each entry records:

- principal
- key id
- algo
- request hash
- verification result
- previous hash
- entry hash

Ship this file to centralized retention storage or SIEM after each release window.

## Security drill

Run:

`python3 tools/run_benchmark_contract_security_drill.py`

Scenarios:

- valid signature
- key not active
- key revoked
- invalid signature
- dual-key overlap accepts rotating key
- post-cutover blocks legacy key
- KMS IAM policy blocks unauthorized key

Use this drill before release window cutover.

## Migration to real KMS/PKI

1. Keep current governance fields unchanged.
2. Switch key entries to `verify_mode=hook`.
3. Point `verify_hook_command` to KMS/PKI verifier binary/service wrapper.
4. Enable active/revoked key policy envs in CI gate.
5. Set `benchmark_hook_timeout_seconds` and `benchmark_hook_failure_policy` in `portfolio_constraints`.
6. Set `AIRIVO_BENCHMARK_HOOK_DELEGATE_VERIFY_COMMAND="python3 tools/kms_pki_delegate_verifier.py"`.
7. Configure IAM policy and audit log path.
8. Validate with security drill + stage gate.
