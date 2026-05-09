#!/usr/bin/env node
/**
 * Airivo AI Bridge: HTTPS API -> OpenClaw (single upstream).
 * Serializes requests to OpenClaw so only one is in-flight at a time,
 * avoiding "fetch failed" when the upstream refuses connections while busy.
 */
const http = require('http');
const https = require('https');

const PORT = Number(process.env.PORT || 3443);
const HOST = process.env.HOST || '127.0.0.1';
const OPENCLAW_URL = process.env.OPENCLAW_URL || 'http://127.0.0.1:18789/v1/chat/completions';
const UPSTREAM_TIMEOUT_MS = Number(process.env.UPSTREAM_TIMEOUT_MS || 35000);
const UPSTREAM_RETRIES = Math.max(1, Number(process.env.UPSTREAM_RETRIES || 3));
const UPSTREAM_RETRY_DELAY_MS = Number(process.env.UPSTREAM_RETRY_DELAY_MS || 800);
const MAX_MESSAGES = Math.min(50, Math.max(1, Number(process.env.MAX_MESSAGES || 16)));

const requestQueue = [];
let inFlight = false;

function randomId() {
  return Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 8);
}

function parseUrl(url) {
  const u = new URL(url);
  const isHttps = u.protocol === 'https:';
  return { isHttps, hostname: u.hostname, port: u.port || (isHttps ? 443 : 80), path: u.pathname + u.search };
}

function fetchUpstream(body) {
  return new Promise((resolve, reject) => {
    const { isHttps, hostname, port, path } = parseUrl(OPENCLAW_URL);
    const lib = isHttps ? https : http;
    const payload = JSON.stringify(body);
    const req = lib.request({
      hostname,
      port: Number(port) || (isHttps ? 443 : 80),
      path: path || '/v1/chat/completions',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(payload, 'utf8'),
      },
      timeout: UPSTREAM_TIMEOUT_MS,
    }, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        const raw = Buffer.concat(chunks).toString('utf8');
        try {
          const data = JSON.parse(raw);
          if (res.statusCode >= 200 && res.statusCode < 300) {
            resolve({ statusCode: res.statusCode, data });
          } else {
            reject(new Error(`upstream ${res.statusCode}: ${raw.slice(0, 200)}`));
          }
        } catch (e) {
          reject(new Error(`upstream invalid json: ${raw.slice(0, 200)}`));
        }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('upstream timeout')); });
    req.setTimeout(UPSTREAM_TIMEOUT_MS);
    req.write(payload);
    req.end();
  });
}

async function doOneRequest(job) {
  const { reqId, body, resolve, reject } = job;
  const start = Date.now();
  let lastErr;
  for (let attempt = 1; attempt <= UPSTREAM_RETRIES; attempt++) {
    try {
      const { data } = await fetchUpstream(body);
      const ms = Date.now() - start;
      console.log(`[airivo-ai-bridge] req=${reqId} ok ms=${ms}`);
      resolve(data);
      return;
    } catch (e) {
      lastErr = e;
      if (attempt < UPSTREAM_RETRIES) {
        console.log(`[airivo-ai-bridge] req=${reqId} attempt=${attempt} fetch_error=${e?.message || e}`);
        await new Promise((r) => setTimeout(r, UPSTREAM_RETRY_DELAY_MS));
      }
    }
  }
  const ms = Date.now() - start;
  console.log(`[airivo-ai-bridge] req=${reqId} fail status=503 err=${lastErr?.message || lastErr}`);
  reject(Object.assign(lastErr || new Error('fetch failed'), { statusCode: 503 }));
}

function runNext() {
  if (inFlight || requestQueue.length === 0) return;
  inFlight = true;
  const job = requestQueue.shift();
  doOneRequest(job).finally(() => {
    inFlight = false;
    runNext();
  });
}

function enqueue(body) {
  return new Promise((resolve, reject) => {
    const reqId = randomId();
    requestQueue.push({ reqId, body, resolve, reject });
    runNext();
  });
}

function buildOpenClawBody(messages) {
  const list = Array.isArray(messages) ? messages.slice(0, MAX_MESSAGES) : [];
  return {
    model: process.env.OPENCLAW_MODEL || 'gpt-4o',
    stream: false,
    max_tokens: Number(process.env.OPENCLAW_MAX_TOKENS || 2048),
    messages: list.map((m) => ({
      role: (m.role || 'user').toLowerCase(),
      content: typeof m.content === 'string' ? m.content : JSON.stringify(m.content || ''),
    })),
  };
}

function parseIncoming(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      try {
        const raw = Buffer.concat(chunks).toString('utf8');
        resolve(raw ? JSON.parse(raw) : {});
      } catch (e) {
        reject(e);
      }
    });
    req.on('error', reject);
  });
}

const server = http.createServer(async (req, res) => {
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  if (req.method !== 'POST') {
    res.writeHead(404);
    res.end(JSON.stringify({ error: 'Not Found' }));
    return;
  }
  let payload;
  try {
    payload = await parseIncoming(req);
  } catch (e) {
    res.writeHead(400);
    res.end(JSON.stringify({ error: 'Invalid JSON' }));
    return;
  }
  const messages = payload.messages;
  if (!Array.isArray(messages) || messages.length === 0) {
    res.writeHead(400);
    res.end(JSON.stringify({ error: 'messages required' }));
    return;
  }
  const body = buildOpenClawBody(messages);
  try {
    const data = await enqueue(body);
    res.writeHead(200);
    res.end(JSON.stringify(data));
  } catch (e) {
    const code = e.statusCode || 503;
    res.writeHead(code);
    res.end(JSON.stringify({ error: e?.message || 'Upstream error', statusCode: code }));
  }
});

server.listen(PORT, HOST, () => {
  console.log(`[airivo-ai-bridge] listening on ${HOST}:${PORT}`);
});
