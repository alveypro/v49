#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_OUT="$ROOT_DIR/logs/v49.streamlit.launchd.log"
LOG_ERR="$ROOT_DIR/logs/v49.streamlit.launchd.err.log"
HEALTH_URL="http://127.0.0.1:8501/_stcore/health"
LABEL="com.airivo.v49.streamlit"
UID_NUM="$(id -u)"
FORCE_RESTART="${FORCE_RESTART:-1}"

cd "$ROOT_DIR"

printf "[recover] root=%s\n" "$ROOT_DIR"

if [[ ! -x "$ROOT_DIR/start_v49_full.sh" ]]; then
  echo "[recover][error] missing start_v49_full.sh"
  exit 1
fi

if [[ "${FORCE_RESTART}" == "1" ]]; then
  pids="$(lsof -ti:8501 2>/dev/null || true)"
  if [[ -n "${pids}" ]]; then
    echo "[recover] force restart: stopping existing :8501 pid(s): ${pids}"
    # shellcheck disable=SC2086
    kill ${pids} 2>/dev/null || true
    sleep 2
    remain="$(lsof -ti:8501 2>/dev/null || true)"
    if [[ -n "${remain}" ]]; then
      echo "[recover] force restart: kill -9 remaining pid(s): ${remain}"
      # shellcheck disable=SC2086
      kill -9 ${remain} 2>/dev/null || true
      sleep 1
    fi
  fi
fi

# Compatibility wrappers for orchestrators that may call legacy names.
if [[ ! -f "$ROOT_DIR/start_v49.sh" ]]; then
  cat > "$ROOT_DIR/start_v49.sh" <<'EOS'
#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
exec "$ROOT_DIR/start_v49_full.sh" "$@"
EOS
  chmod +x "$ROOT_DIR/start_v49.sh"
fi

if [[ ! -f "$ROOT_DIR/start_v49_streamlit.sh" ]]; then
  cat > "$ROOT_DIR/start_v49_streamlit.sh" <<'EOS'
#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
exec "$ROOT_DIR/start_v49_full.sh" "$@"
EOS
  chmod +x "$ROOT_DIR/start_v49_streamlit.sh"
fi

bash "$ROOT_DIR/tools/install_v49_streamlit_launchd.sh"

printf "[recover] waiting for streamlit health...\n"
mkdir -p "$ROOT_DIR/logs"
: > "$LOG_OUT"
: > "$LOG_ERR"

for i in {1..90}; do
  body="$(curl -sS --max-time 2 "$HEALTH_URL" 2>/dev/null || true)"
  if [[ "$body" == *'"ok"'* ]] || [[ "$body" == *'"status":"ok"'* ]]; then
    printf "[recover] health ok: %s\n" "$body"
    exit 0
  fi
  # Fallback: some environments block loopback probe but service is already listening.
  if lsof -i :8501 -nP 2>/dev/null | rg -q "LISTEN"; then
    printf "[recover] health probe pending but port 8501 is LISTEN; treat as running\n"
    launchctl print "gui/${UID_NUM}/${LABEL}" 2>/dev/null | rg -n "state =|pid =|last exit code" -S || true
    exit 0
  fi
  sleep 1
done

echo "[recover][error] streamlit health check failed: $HEALTH_URL"
launchctl print "gui/${UID_NUM}/${LABEL}" 2>/dev/null | rg -n "state =|pid =|last exit code|program =|arguments =|path =" -S || true
[[ -f "$LOG_ERR" ]] && { echo "----- tail err -----"; tail -n 120 "$LOG_ERR"; }
[[ -f "$LOG_OUT" ]] && { echo "----- tail out -----"; tail -n 120 "$LOG_OUT"; }
exit 2
