from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import pandas as pd


def summarize_holdings(holdings: pd.DataFrame) -> Dict[str, float]:
    if holdings is None or holdings.empty:
        return {"count": 0.0, "total_cost": 0.0, "total_value": 0.0, "total_profit": 0.0, "total_profit_pct": 0.0}
    total_cost = float(pd.to_numeric(holdings.get("cost_total", 0), errors="coerce").fillna(0.0).sum())
    total_value = float(pd.to_numeric(holdings.get("current_value", 0), errors="coerce").fillna(0.0).sum())
    total_profit = float(pd.to_numeric(holdings.get("profit_loss", 0), errors="coerce").fillna(0.0).sum())
    total_profit_pct = (total_profit / total_cost) if total_cost > 0 else 0.0
    return {
        "count": float(len(holdings)),
        "total_cost": total_cost,
        "total_value": total_value,
        "total_profit": total_profit,
        "total_profit_pct": float(total_profit_pct),
    }


def summarize_trade_periods(trades: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    if trades is None or trades.empty:
        z = {"profit": 0.0, "pct": 0.0, "amount": 0.0}
        return {"daily": dict(z), "weekly": dict(z), "monthly": dict(z)}

    t = trades.copy()
    t["trade_date"] = pd.to_datetime(t.get("trade_date"), errors="coerce")
    sells = t[t.get("action") == "sell"].copy()
    if sells.empty:
        z = {"profit": 0.0, "pct": 0.0, "amount": 0.0}
        return {"daily": dict(z), "weekly": dict(z), "monthly": dict(z)}

    sells["amount"] = pd.to_numeric(sells.get("amount", 0), errors="coerce").fillna(0.0)
    sells["profit_loss"] = pd.to_numeric(sells.get("profit_loss", 0), errors="coerce").fillna(0.0)
    sells["cost_basis"] = sells["amount"] - sells["profit_loss"]
    sells = sells.dropna(subset=["trade_date"])

    def _period_stats(df: pd.DataFrame) -> Dict[str, float]:
        if df.empty:
            return {"profit": 0.0, "pct": 0.0, "amount": 0.0}
        profit = float(df["profit_loss"].sum())
        cost = float(df["cost_basis"].sum())
        amount = float(df["amount"].sum())
        pct = (profit / cost) if cost > 0 else 0.0
        return {"profit": profit, "pct": float(pct), "amount": amount}

    today = pd.Timestamp.now().normalize()
    week_start = today - pd.Timedelta(days=today.weekday())
    month_start = today.replace(day=1)

    return {
        "daily": _period_stats(sells[sells["trade_date"] >= today]),
        "weekly": _period_stats(sells[sells["trade_date"] >= week_start]),
        "monthly": _period_stats(sells[sells["trade_date"] >= month_start]),
    }


def load_notification_config(config_file: str = "notification_config.json") -> Dict[str, Any]:
    p = Path(config_file)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def build_notification_config(
    *,
    enable_email: bool,
    smtp_server: str,
    smtp_user: str,
    smtp_password: str,
    email_address: str,
    enable_wechat: bool,
    wechat_webhook: str,
    enable_dingtalk: bool,
    dingtalk_webhook: str,
) -> Dict[str, Any]:
    return {
        "enabled": bool(enable_email or enable_wechat or enable_dingtalk),
        "email": {
            "enabled": bool(enable_email),
            "smtp_server": str(smtp_server or "smtp.qq.com"),
            "smtp_port": 465 if "qq.com" in str(smtp_server or "") else 587,
            "sender_email": str(smtp_user or ""),
            "sender_password": str(smtp_password or ""),
            "receiver_emails": [str(email_address)] if email_address else [],
        },
        "wechat_work": {
            "enabled": bool(enable_wechat),
            "webhook_url": str(wechat_webhook or ""),
        },
        "dingtalk": {
            "enabled": bool(enable_dingtalk),
            "webhook_url": str(dingtalk_webhook or ""),
            "secret": "",
        },
    }

