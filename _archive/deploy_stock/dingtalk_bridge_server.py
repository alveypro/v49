#!/usr/bin/env python3
"""DingTalk inbound bridge: @message -> local stock agent reply."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import re
import sys
import urllib.parse
from random import SystemRandom
from pathlib import Path
from typing import Any, Dict, Optional

from flask import Flask, Response, jsonify, request

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

def _load_dotenv_if_exists() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    try:
        for raw in env_path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            key = k.strip()
            if not key:
                continue
            value = v.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)
    except Exception:
        return


_load_dotenv_if_exists()

from openclaw.assistant.remote_bridge import query_openclaw_remote

app = Flask(__name__)

LOG_LEVEL = os.getenv("DINGTALK_BRIDGE_LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
logger = logging.getLogger("dingtalk_bridge")

SIGN_SECRET = os.getenv("DINGTALK_CALLBACK_SECRET", "").strip()
CALLBACK_TOKEN = os.getenv("DINGTALK_CALLBACK_TOKEN", "").strip()
CALLBACK_AES_KEY = os.getenv("DINGTALK_CALLBACK_AES_KEY", "").strip()
CALLBACK_APP_KEY = os.getenv("DINGTALK_CALLBACK_APP_KEY", "").strip()
DEFAULT_TIMEOUT = int(os.getenv("DINGTALK_AGENT_TIMEOUT_SEC", "18"))
DEFAULT_SESSION = os.getenv("DINGTALK_DEFAULT_SESSION", "dingtalk-group")
_RAND = SystemRandom()
_PKCS7_BLOCK_SIZE = 32


def _is_encrypted_callback(payload: Dict[str, Any]) -> bool:
    return isinstance(payload.get("encrypt"), str) and bool(
        request.args.get("msg_signature") or request.args.get("signature")
    )


def _pkcs7_unpad(data: bytes) -> bytes:
    if not data:
        return data
    pad_len = data[-1]
    if pad_len < 1 or pad_len > _PKCS7_BLOCK_SIZE:
        raise ValueError("invalid pkcs7 padding")
    return data[:-pad_len]


def _pkcs7_pad(data: bytes) -> bytes:
    pad_len = _PKCS7_BLOCK_SIZE - (len(data) % _PKCS7_BLOCK_SIZE)
    if pad_len == 0:
        pad_len = _PKCS7_BLOCK_SIZE
    return data + bytes([pad_len]) * pad_len


def _aes_key_bytes() -> bytes:
    if not CALLBACK_AES_KEY:
        raise ValueError("missing DINGTALK_CALLBACK_AES_KEY")
    raw = CALLBACK_AES_KEY.strip()
    if len(raw) == 43:
        raw += "="
    return base64.b64decode(raw)


def _sha1_signature(token: str, timestamp: str, nonce: str, encrypt_text: str) -> str:
    items = sorted([token, timestamp, nonce, encrypt_text])
    return hashlib.sha1("".join(items).encode("utf-8")).hexdigest()


def _decrypt_encrypt_field(encrypt_text: str) -> str:
    from Crypto.Cipher import AES

    key = _aes_key_bytes()
    cipher = AES.new(key, AES.MODE_CBC, key[:16])
    plain = cipher.decrypt(base64.b64decode(encrypt_text))
    plain = _pkcs7_unpad(plain)

    if len(plain) < 20:
        raise ValueError("decrypted payload too short")
    msg_len = int.from_bytes(plain[16:20], byteorder="big")
    msg = plain[20 : 20 + msg_len]
    app_key = plain[20 + msg_len :].decode("utf-8", errors="ignore")
    if CALLBACK_APP_KEY and app_key and app_key != CALLBACK_APP_KEY:
        raise ValueError("app_key mismatch")
    return msg.decode("utf-8", errors="ignore")


def _encrypt_reply(text: str, timestamp: str, nonce: str) -> Dict[str, Any]:
    from Crypto.Cipher import AES

    key = _aes_key_bytes()
    rand16 = bytes(_RAND.randrange(0, 256) for _ in range(16))
    msg = text.encode("utf-8")
    app_key = CALLBACK_APP_KEY.encode("utf-8")
    raw = rand16 + len(msg).to_bytes(4, byteorder="big") + msg + app_key
    encrypted = AES.new(key, AES.MODE_CBC, key[:16]).encrypt(_pkcs7_pad(raw))
    encrypt_text = base64.b64encode(encrypted).decode("utf-8")
    sign = _sha1_signature(CALLBACK_TOKEN, timestamp, nonce, encrypt_text)
    return {
        "msg_signature": sign,
        "encrypt": encrypt_text,
        "timeStamp": timestamp,
        "nonce": nonce,
    }


def _verify_signature(req) -> bool:
    """Verify DingTalk callback signature when secret is configured."""
    payload = req.get_json(silent=True) or {}
    if _is_encrypted_callback(payload):
        if not (CALLBACK_TOKEN and CALLBACK_AES_KEY):
            return False
        timestamp = str(req.args.get("timestamp", "")).strip()
        nonce = str(req.args.get("nonce", "")).strip()
        given = str(req.args.get("msg_signature") or req.args.get("signature") or "").strip()
        encrypt_text = str(payload.get("encrypt", "")).strip()
        if not (timestamp and nonce and given and encrypt_text):
            return False
        expected = _sha1_signature(CALLBACK_TOKEN, timestamp, nonce, encrypt_text)
        return hmac.compare_digest(expected, given)

    if not SIGN_SECRET:
        return True
    timestamp = (req.headers.get("timestamp") or req.headers.get("Timestamp") or "").strip()
    signature = (req.headers.get("sign") or req.headers.get("Sign") or "").strip()
    if not timestamp or not signature:
        return False
    string_to_sign = f"{timestamp}\n{SIGN_SECRET}"
    digest = hmac.new(SIGN_SECRET.encode("utf-8"), string_to_sign.encode("utf-8"), digestmod=hashlib.sha256).digest()
    expected = base64.b64encode(digest).decode("utf-8")
    candidates = {signature, urllib.parse.unquote_plus(signature)}
    return any(hmac.compare_digest(expected, c) for c in candidates)


def _extract_text(payload: Dict[str, Any]) -> str:
    text = ""
    if isinstance(payload.get("text"), dict):
        text = str(payload["text"].get("content", "")).strip()
    if not text:
        text = str(payload.get("content", "")).strip()
    if not text and isinstance(payload.get("msg"), dict):
        text = str(payload["msg"].get("content", "")).strip()
    return text


def _normalize_question(text: str) -> str:
    # Remove @mentions to avoid confusing the agent prompt.
    text = re.sub(r"@\S+", "", text or "").strip()
    return text


def _callback_text(content: str) -> Dict[str, Any]:
    return {
        "msgtype": "text",
        "text": {"content": content},
    }


@app.get("/health")
def health():
    return jsonify(
        {
            "ok": True,
            "service": "dingtalk-bridge",
            "sign_secret_enabled": bool(SIGN_SECRET),
            "encrypted_callback_enabled": bool(CALLBACK_TOKEN and CALLBACK_AES_KEY and CALLBACK_APP_KEY),
        }
    )


@app.get("/dingtalk/callback")
def dingtalk_callback_get():
    """Handle DingTalk publish-time reachability probe (GET)."""
    return Response("success", mimetype="text/plain")


@app.post("/dingtalk/callback")
def dingtalk_callback():
    payload = request.get_json(silent=True) or {}
    if "challenge" in payload:
        # Some callback gateways validate endpoint ownership with challenge.
        return jsonify({"challenge": payload.get("challenge")})

    if not _verify_signature(request):
        logger.warning("signature verification failed")
        if _is_encrypted_callback(payload):
            return jsonify({"err": "signature verification failed"}), 401
        return jsonify(_callback_text("签名校验失败，请检查回调密钥配置。")), 401

    encrypted_mode = _is_encrypted_callback(payload)
    decrypted_payload: Dict[str, Any] = {}
    if encrypted_mode:
        try:
            plain = _decrypt_encrypt_field(str(payload.get("encrypt", "")))
            plain = plain.strip()
            if plain.startswith("{") and plain.endswith("}"):
                import json

                decrypted_payload = json.loads(plain)
            else:
                decrypted_payload = {"text": {"content": plain}}
        except Exception as exc:  # noqa: BLE001
            logger.exception("decrypt callback failed: %s", exc)
            return jsonify({"err": "decrypt failed"}), 400

    source_payload = decrypted_payload or payload
    original_text = _extract_text(source_payload)
    question = _normalize_question(original_text)
    logger.info(
        "callback received: encrypted=%s keys=%s text=%s question=%s",
        encrypted_mode,
        ",".join(sorted(list(source_payload.keys()))[:12]),
        (original_text or "")[:120],
        (question or "")[:120],
    )
    timestamp = str(request.args.get("timestamp") or "")
    nonce = str(request.args.get("nonce") or "")

    def _finalize_response(content: str, force_success: bool = False):
        if encrypted_mode:
            if force_success:
                plain = "success"
            else:
                # DingTalk encrypted callbacks expect message JSON plaintext.
                plain = json.dumps(_callback_text(content), ensure_ascii=False)
            try:
                resp = _encrypt_reply(plain, timestamp, nonce)
                return jsonify(resp)
            except Exception as exc:  # noqa: BLE001
                logger.exception("encrypt response failed: %s", exc)
                return jsonify({"err": "encrypt failed"}), 500
        return jsonify(_callback_text(content))

    if not question:
        # For encrypted callback checks and non-text event pushes, DingTalk expects success ack.
        return _finalize_response("已收到消息，请直接输入要咨询的问题（可包含股票代码或公司名）。", force_success=True)

    session_id = str(
        source_payload.get("conversationId")
        or source_payload.get("sessionWebhook")
        or source_payload.get("openConversationId")
        or DEFAULT_SESSION
    )
    answer: Optional[Dict[str, Any]] = query_openclaw_remote(
        question=question,
        timeout=DEFAULT_TIMEOUT,
        session_id=session_id,
    )
    if not answer or not answer.get("answer"):
        return _finalize_response("已收到问题，但本地智能体暂时不可达，请稍后重试。")

    reply = str(answer["answer"]).strip()
    if len(reply) > 1800:
        reply = reply[:1800] + "\n...(已截断)"
    return _finalize_response(reply)


if __name__ == "__main__":
    host = os.getenv("DINGTALK_BRIDGE_HOST", "0.0.0.0")
    port = int(os.getenv("DINGTALK_BRIDGE_PORT", "8601"))
    app.run(host=host, port=port)
