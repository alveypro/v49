#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f ".env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source ".env"
  set +a
fi

if [[ -z "${TELEGRAM_BOT_TOKEN:-}" ]]; then
  echo "[telegram-chatid] ERROR: TELEGRAM_BOT_TOKEN is missing in .env"
  exit 2
fi

API_BASE="https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}"

echo "[telegram-chatid] bot profile:"
BOT_JSON="$(curl -sS "${API_BASE}/getMe")"
python3 - "$BOT_JSON" <<'PY'
import json, sys
d=json.loads(sys.argv[1])
if not d.get("ok"):
    print(d)
    raise SystemExit(1)
r=d.get("result") or {}
print(json.dumps({"id":r.get("id"),"username":r.get("username"),"first_name":r.get("first_name")},ensure_ascii=False))
PY

echo
echo "[telegram-chatid] reading recent updates..."
UPDATES_JSON="$(curl -sS "${API_BASE}/getUpdates?limit=100&timeout=1")"
python3 - "$UPDATES_JSON" <<'PY'
import json, sys
d=json.loads(sys.argv[1])
if not d.get("ok"):
    print("[telegram-chatid] ERROR:", d)
    raise SystemExit(1)

rows=[]
for u in d.get("result") or []:
    msg = u.get("message") or u.get("edited_message") or {}
    chat = msg.get("chat") or {}
    cid = chat.get("id")
    if cid is None:
        continue
    rows.append({
        "chat_id": int(cid),
        "type": chat.get("type"),
        "title": chat.get("title"),
        "username": chat.get("username"),
        "first_name": chat.get("first_name"),
        "last_name": chat.get("last_name"),
    })

uniq={}
for r in rows:
    uniq[r["chat_id"]] = r

if not uniq:
    print("[telegram-chatid] no updates yet.")
    print("Send one message to your bot, then rerun this script.")
    raise SystemExit(0)

print("[telegram-chatid] discovered chats:")
for cid in sorted(uniq):
    r=uniq[cid]
    label = r.get("title") or r.get("username") or r.get("first_name") or "unknown"
    print(f"- {cid} ({r.get('type')}): {label}")

ids=",".join(str(x) for x in sorted(uniq))
print()
print("Suggested .env value:")
print(f"TELEGRAM_ALLOWED_CHAT_IDS={ids}")
PY
