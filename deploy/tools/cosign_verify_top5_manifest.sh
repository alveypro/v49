#!/usr/bin/env bash
# Verify Sigstore / Cosign keyless bundle for scheduler manifest (Phase C companion to CI optional signing).
#
# Prerequisites: cosign on PATH (https://docs.sigstore.dev/cosign/installation/).
#
# Typical GitHub Actions keyless issuer:
#   export COSIGN_CERT_OIDC_ISSUER=https://token.actions.githubusercontent.com
#   export COSIGN_CERT_IDENTITY_REGEXP='https://github.com/OWNER/REPO/.github/workflows/top5_audit_manifest_publish.yml@refs/'
#
# If you sign from **tag** releases (see top5_audit_manifest_publish.yml release_assets job),
# the ref suffix is `refs/tags/…`, not only `refs/heads/main` — widen the regexp accordingly,
# e.g. `@refs/(heads/main|tags/)` or separate verify paths for main vs release assets.
# Tighten the regexp once you freeze workflow path + branch/ref policy.
set -euo pipefail

MANIFEST="${1:-}"
BUNDLE="${2:-}"
if [[ -z "$MANIFEST" || -z "$BUNDLE" ]]; then
  echo "用法: COSIGN_CERT_IDENTITY_REGEXP=… $0 TOP5_audit_scheduler_manifest.json top5_audit_scheduler_manifest.cosign.bundle" >&2
  exit 2
fi
if [[ ! -f "$MANIFEST" ]] || [[ ! -f "$BUNDLE" ]]; then
  echo "[cosign-verify-manifest] ERROR: missing file manifest=$MANIFEST bundle=$BUNDLE" >&2
  exit 4
fi
if ! command -v cosign >/dev/null 2>&1; then
  echo "[cosign-verify-manifest] ERROR: cosign not on PATH — install Sigstore Cosign CLI" >&2
  exit 4
fi
if [[ -z "${COSIGN_CERT_IDENTITY_REGEXP:-}" ]]; then
  echo "[cosign-verify-manifest] ERROR: set COSIGN_CERT_IDENTITY_REGEXP to your workload identity regexp (workflow subject)" >&2
  exit 4
fi
ISS="${COSIGN_CERT_OIDC_ISSUER:-https://token.actions.githubusercontent.com}"

exec cosign verify-blob "$MANIFEST" --bundle "$BUNDLE" \
  --certificate-identity-regexp="${COSIGN_CERT_IDENTITY_REGEXP}" \
  --certificate-oidc-issuer-regexp="${ISS}"
