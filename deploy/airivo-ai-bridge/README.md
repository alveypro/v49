# Airivo AI Bridge (serialized upstream)

Node.js bridge: Web API → OpenClaw `http://127.0.0.1:18789/v1/chat/completions`.  
**Serializes requests** so only one call to OpenClaw is in-flight at a time, avoiding `TypeError: fetch failed` when the upstream refuses connections while busy.

## Deploy to server

```bash
# From repo (or copy app.mjs to server)
scp deploy/airivo-ai-bridge/app.mjs root@YOUR_SERVER:/opt/airivo-ai-bridge/app.mjs

# On server
sudo systemctl restart airivo-ai-bridge
journalctl -u airivo-ai-bridge -f
```

## override.conf

Keep `StartLimitIntervalSec` under `[Unit]`, not `[Service]`. Use `Environment=` for `NODE_OPTIONS`:

- `/etc/systemd/system/airivo-ai-bridge.service.d/override.conf`:

```ini
[Unit]
StartLimitIntervalSec=0

[Service]
Restart=always
RestartSec=2
StandardOutput=journal
StandardError=journal
Environment="NODE_OPTIONS=--dns-result-order=ipv4first --max-old-space-size=512"
Environment="UPSTREAM_TIMEOUT_MS=35000"
Environment="UPSTREAM_RETRIES=3"
Environment="UPSTREAM_RETRY_DELAY_MS=800"
Environment="MAX_MESSAGES=16"
```

Then: `sudo systemctl daemon-reload && sudo systemctl restart airivo-ai-bridge`.

## Env (optional)

- `PORT` (default 3443), `HOST` (default 127.0.0.1)
- `OPENCLAW_URL` (default http://127.0.0.1:18789/v1/chat/completions)
- `UPSTREAM_TIMEOUT_MS`, `UPSTREAM_RETRIES`, `UPSTREAM_RETRY_DELAY_MS`, `MAX_MESSAGES`

## Verify

Same curl as before; with serialization you should get 200 for all, with later requests waiting in queue:

```bash
for i in 1 2 3 4 5; do
  echo -n "$i "
  curl -skS -o /tmp/out_$i.json -w "code=%{http_code} time=%{time_total}s\n" \
    -H "Content-Type: application/json" \
    -d '{"sessionId":"test","messages":[{"role":"user","content":"你好"}]}' \
    "https://47.90.160.87/api/ai/chat"
done
```

Expect: all `code=200`, and `time` increasing (e.g. ~20s, ~40s, ~60s...) as requests are processed one by one.
