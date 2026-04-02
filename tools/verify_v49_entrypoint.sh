#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

fail() {
  echo "[entrypoint] FAIL: $1" >&2
  exit 1
}

echo "[entrypoint] checking canonical entrypoint guards..."

[[ -f "v49_app.py" ]] || fail "missing v49_app.py"
[[ -f "start_v49_full.sh" ]] || fail "missing start_v49_full.sh"
[[ -f "launchd/com.airivo.v49.streamlit.plist" ]] || fail "missing launchd plist"
[[ -f "终极量价暴涨系统_v49.0_长期稳健版.py" ]] || fail "missing legacy shim file"

grep -q 'CANONICAL_APP_FILE="v49_app.py"' start_v49_full.sh || fail "start script missing canonical app guard"
grep -q '/Users/mac/2026Qlin/start_v49_full.sh' launchd/com.airivo.v49.streamlit.plist || fail "launchd plist not pointing to start_v49_full.sh"
grep -q 'from v49_app import \*' 终极量价暴涨系统_v49.0_长期稳健版.py || fail "legacy file is not shimmed to v49_app.py"
grep -q 'Single source of truth: ./v49_app.py' 终极量价暴涨系统_v49.0_长期稳健版.py || fail "legacy shim missing source-of-truth marker"

echo "[entrypoint] OK canonical=v49_app.py"
