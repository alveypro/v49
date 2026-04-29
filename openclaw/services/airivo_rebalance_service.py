from __future__ import annotations

from datetime import datetime
import glob
import json
import os
from typing import Any, Callable, Dict, List, Tuple

import pandas as pd


def production_rollback_state_path(app_root: str) -> str:
    evo_dir = os.path.join(app_root, "evolution")
    os.makedirs(evo_dir, exist_ok=True)
    return os.path.join(evo_dir, "production_auto_rollback_state.json")


def load_production_rollback_state(app_root: str) -> Dict[str, Any]:
    path = production_rollback_state_path(app_root)
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return dict(json.load(f) or {})
    except Exception:
        pass
    return {}


def save_production_rollback_state(app_root: str, payload: Dict[str, Any]) -> None:
    path = production_rollback_state_path(app_root)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload or {}, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def evaluate_production_rollback_trigger(
    *,
    load_latest_production_backtest_audit: Callable[[], Tuple[pd.DataFrame, str]],
) -> Dict[str, Any]:
    df, _ = load_latest_production_backtest_audit()
    if df is None or df.empty:
        return {"triggered": False, "reason": "no_audit_data", "targets": [], "signature": ""}
    mapping = {"v5": "V5", "v8": "V8", "v9": "V9", "combo": "COMBO"}
    targets: List[str] = []
    red_count = 0
    no_count = 0
    for _, row in df.iterrows():
        sk = str(row.get("策略", "")).strip().lower()
        if sk not in mapping:
            continue
        risk = str(row.get("评估风险", "GREEN")).strip().upper()
        approved = str(row.get("建议晋升", "NO")).strip().upper() == "YES"
        if risk == "RED":
            red_count += 1
        if not approved:
            no_count += 1
        if (risk == "RED") and (not approved) and sk in {"v8", "v9", "combo"}:
            targets.append(mapping[sk])
    triggered = bool((red_count >= 2 and no_count >= 3) or len(targets) >= 2)
    sig = f"{datetime.now().strftime('%Y%m%d')}|red={red_count}|no={no_count}|targets={','.join(sorted(set(targets)))}"
    reason = "red_no_gate_breach" if triggered else "within_risk_budget"
    return {
        "triggered": triggered,
        "reason": reason,
        "targets": sorted(set(targets)),
        "red_count": int(red_count),
        "no_count": int(no_count),
        "signature": sig,
    }


def resolve_market_regime(
    regime_choice: str,
    *,
    market_environment_provider: Callable[[], str],
) -> str:
    c = str(regime_choice or "").strip().lower()
    if c in {"bull", "bear", "oscillation"}:
        return c
    try:
        env = str(market_environment_provider()).strip().lower()
        if env in {"bull", "bear", "oscillation"}:
            return env
    except Exception:
        pass
    return "oscillation"


def compute_production_allocation_plan(
    capital_total: float,
    *,
    regime_choice: str = "auto",
    load_latest_production_backtest_audit: Callable[[], Tuple[pd.DataFrame, str]],
    market_environment_provider: Callable[[], str],
    load_portfolio_risk_budget: Callable[[], Dict[str, Any]],
) -> Dict[str, Any]:
    audit_df, _ = load_latest_production_backtest_audit()
    if audit_df is None or audit_df.empty:
        return {"ok": False, "error": "no_audit_data"}

    regime = resolve_market_regime(regime_choice, market_environment_provider=market_environment_provider)
    base_map = {
        "bull": {"v5": 0.45, "v8": 0.10, "v9": 0.25, "combo": 0.20},
        "oscillation": {"v5": 0.35, "v8": 0.10, "v9": 0.35, "combo": 0.20},
        "bear": {"v5": 0.20, "v8": 0.20, "v9": 0.35, "combo": 0.25},
    }
    base = dict(base_map.get(regime, base_map["oscillation"]))

    rb = load_portfolio_risk_budget()
    max_positions = int(rb.get("max_positions", 20))
    max_ind_ratio = float(rb.get("max_industry_ratio", 0.35))

    rows: List[Dict[str, Any]] = []
    for sk in ("v5", "v8", "v9", "combo"):
        row = audit_df[audit_df["策略"].astype(str).str.lower() == sk]
        if row.empty:
            continue
        r = row.iloc[0]
        wr = float(r.get("胜率(%)", 0.0) or 0.0)
        dd = float(r.get("最大回撤(%)", 0.0) or 0.0)
        risk = str(r.get("评估风险", "GREEN")).upper()
        promo = str(r.get("建议晋升", "NO")).upper() == "YES"

        wr_mul = max(0.50, min(1.25, wr / 52.0))
        dd_mul = max(0.30, min(1.20, (25.0 / max(1.0, dd))))
        risk_mul = 1.00 if risk == "GREEN" else (0.65 if risk == "YELLOW" else 0.35)
        promo_mul = 1.00 if promo else 0.80

        raw = float(base.get(sk, 0.0)) * wr_mul * dd_mul * risk_mul * promo_mul
        rows.append(
            {
                "strategy": sk,
                "win_rate_pct": wr,
                "max_drawdown_pct": dd,
                "risk": risk,
                "promo": promo,
                "base_weight": float(base.get(sk, 0.0)),
                "score_weight_raw": raw,
            }
        )

    if not rows:
        return {"ok": False, "error": "empty_allocation_rows"}

    plan_df = pd.DataFrame(rows)
    raw_sum = float(plan_df["score_weight_raw"].sum())
    if raw_sum <= 1e-9:
        plan_df["target_weight"] = 1.0 / max(1, len(plan_df))
    else:
        plan_df["target_weight"] = plan_df["score_weight_raw"] / raw_sum
    plan_df["target_weight"] = plan_df["target_weight"].clip(lower=0.05, upper=0.65)
    clipped_sum = float(plan_df["target_weight"].sum())
    if clipped_sum > 1e-9:
        plan_df["target_weight"] = plan_df["target_weight"] / clipped_sum

    plan_df["capital_alloc"] = plan_df["target_weight"] * float(max(0.0, capital_total))
    plan_df["slot_count"] = (plan_df["target_weight"] * int(max_positions)).round().astype(int).clip(lower=1)
    plan_df["single_ticket_cap"] = plan_df["capital_alloc"] / plan_df["slot_count"].replace(0, 1)

    return {
        "ok": True,
        "regime": regime,
        "capital_total": float(capital_total),
        "risk_budget": {
            "max_positions": int(max_positions),
            "max_industry_ratio": float(max_ind_ratio),
        },
        "plan_df": plan_df.sort_values("target_weight", ascending=False).reset_index(drop=True),
    }


def write_production_allocation_report(
    *,
    app_root: str,
    plan: Dict[str, Any],
    now: datetime | None = None,
) -> Tuple[bool, str, str]:
    try:
        out_dir = os.path.join(app_root, "logs", "openclaw")
        os.makedirs(out_dir, exist_ok=True)
        now_dt = now or datetime.now()
        ts = now_dt.strftime("%Y%m%d_%H%M%S")
        md_path = os.path.join(out_dir, f"production_allocation_report_{ts}.md")
        csv_path = os.path.join(out_dir, f"production_allocation_report_{ts}.csv")
        df = pd.DataFrame(plan.get("plan_df", pd.DataFrame()))
        if df.empty:
            return False, "", ""
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")

        lines: List[str] = []
        lines.append("# 生产策略仓位分配简报")
        lines.append("")
        lines.append(f"- 生成时间: {now_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"- 市场状态: {plan.get('regime', 'oscillation')}")
        lines.append(f"- 总资金: {float(plan.get('capital_total', 0.0)):.2f}")
        rb = plan.get("risk_budget", {}) or {}
        lines.append(f"- 风险预算: max_positions={rb.get('max_positions', 20)}, max_industry_ratio={rb.get('max_industry_ratio', 0.35)}")
        lines.append("")
        lines.append("## 分配建议")
        for _, r in df.iterrows():
            lines.append(
                f"- {str(r.get('strategy', 'N/A'))}: 权重{float(r.get('target_weight', 0))*100:.1f}% | "
                f"资金{float(r.get('capital_alloc', 0)):.0f} | 预估仓位{int(r.get('slot_count', 1))} | "
                f"单票上限{float(r.get('single_ticket_cap', 0)):.0f}"
            )
        lines.append("")
        lines.append(f"- 明细CSV: `{csv_path}`")
        with open(md_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        return True, md_path, csv_path
    except Exception:
        return False, "", ""


def execute_production_auto_rollback(
    *,
    app_root: str,
    force: bool,
    evaluate_rollback_trigger: Callable[[], Dict[str, Any]],
    rollback_latest_promoted_params: Callable[[str], Tuple[bool, str]],
    now_text: Callable[[], str],
    retrain_enabled: bool,
    trigger_auto_evolve_optimize: Callable[..., Tuple[bool, str]],
) -> Tuple[bool, str, Dict[str, Any]]:
    eval_info = evaluate_rollback_trigger()
    if not force and not bool(eval_info.get("triggered", False)):
        return False, "未触发自动回滚条件。", eval_info

    targets = list(eval_info.get("targets") or [])
    if not targets:
        return False, "没有可回滚目标策略。", eval_info

    state = load_production_rollback_state(app_root)
    sig = str(eval_info.get("signature", "") or "")
    if (not force) and sig and sig == str(state.get("last_signature", "")):
        return False, "当前触发签名已执行过回滚，已跳过。", eval_info

    done: List[str] = []
    failed: List[str] = []
    for sk in targets:
        ok, msg = rollback_latest_promoted_params(sk)
        if ok:
            done.append(sk)
        else:
            failed.append(f"{sk}:{msg}")
    state_payload = {
        "last_run_at": now_text(),
        "last_signature": sig,
        "last_targets": targets,
        "last_done": done,
        "last_failed": failed,
        "reason": eval_info.get("reason"),
    }
    try:
        save_production_rollback_state(app_root, state_payload)
    except Exception:
        pass
    if failed and not done:
        return False, f"回滚失败：{' | '.join(failed)}", eval_info
    retrain_msg = ""
    if done and retrain_enabled:
        ok_rt, msg_rt = trigger_auto_evolve_optimize(force_now=True)
        retrain_msg = f" | 自动重训：{msg_rt}" if ok_rt else f" | 自动重训未启动：{msg_rt}"
    if failed and done:
        return True, f"部分回滚完成：成功{','.join(done)}；失败{' | '.join(failed)}{retrain_msg}", eval_info
    return True, f"自动回滚完成：{','.join(done)}{retrain_msg}", eval_info


def score_production_rebalance_execution(exec_result: Dict[str, Any]) -> Dict[str, Any]:
    s = (exec_result or {}).get("summary", {}) or {}
    total = max(1, int(s.get("total", 0)))
    done = int(s.get("buy_done", 0)) + int(s.get("reduce_done", 0))
    success_rate = float(done / total)
    buy_amount = float(s.get("buy_amount_total", 0.0) or 0.0)
    sell_amount = float(s.get("sell_amount_total", 0.0) or 0.0)
    turnover = float(buy_amount + sell_amount)
    intensity = min(1.0, turnover / 1_000_000.0)
    score = (success_rate * 85.0) + (intensity * 15.0)
    level = "A" if score >= 85 else ("B" if score >= 70 else ("C" if score >= 55 else "D"))
    return {
        "score": round(float(score), 2),
        "level": str(level),
        "success_rate_pct": round(success_rate * 100.0, 2),
        "turnover": round(turnover, 2),
    }


def production_rebalance_audit_log_path(app_root: str) -> str:
    out_dir = os.path.join(app_root, "logs", "openclaw")
    os.makedirs(out_dir, exist_ok=True)
    return os.path.join(out_dir, "production_rebalance_audit.jsonl")


def append_production_rebalance_audit_log(app_root: str, exec_result: Dict[str, Any], now_text: Callable[[], str]) -> None:
    try:
        p = production_rebalance_audit_log_path(app_root)
        payload = {
            "run_at": now_text(),
            "batch_id": str((exec_result or {}).get("batch_id", "")),
            "execute_reduce": bool((exec_result or {}).get("execute_reduce", False)),
            "summary": (exec_result or {}).get("summary", {}) or {},
            "quality": (exec_result or {}).get("quality", {}) or {},
        }
        with open(p, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        pass


def load_latest_production_rebalance_audit(app_root: str) -> Dict[str, Any]:
    p = production_rebalance_audit_log_path(app_root)
    if not os.path.exists(p):
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            lines = [x.strip() for x in f.readlines() if str(x).strip()]
        if not lines:
            return {}
        return dict(json.loads(lines[-1]) or {})
    except Exception:
        return {}


def load_recent_production_rebalance_audits(
    app_root: str,
    *,
    days: int = 7,
    max_rows: int = 300,
    safe_parse_dt: Callable[[str], Any],
) -> pd.DataFrame:
    p = production_rebalance_audit_log_path(app_root)
    if not os.path.exists(p):
        return pd.DataFrame()
    rows: List[Dict[str, Any]] = []
    try:
        with open(p, "r", encoding="utf-8") as f:
            lines = [x.strip() for x in f.readlines() if str(x).strip()]
        for ln in lines[-max(1, int(max_rows)):]:
            try:
                item = json.loads(ln) or {}
                run_at = str(item.get("run_at", "") or "")
                dt = safe_parse_dt(run_at)
                if not dt:
                    continue
                if (datetime.now() - dt).days > int(days):
                    continue
                q = item.get("quality", {}) or {}
                s = item.get("summary", {}) or {}
                rows.append(
                    {
                        "run_at": run_at,
                        "trade_date": dt.strftime("%Y%m%d"),
                        "score": float(q.get("score", 0.0) or 0.0),
                        "success_rate_pct": float(q.get("success_rate_pct", 0.0) or 0.0),
                        "turnover": float(q.get("turnover", 0.0) or 0.0),
                        "total": int(s.get("total", 0) or 0),
                        "buy_done": int(s.get("buy_done", 0) or 0),
                        "reduce_done": int(s.get("reduce_done", 0) or 0),
                        "skipped": int(s.get("skipped", 0) or 0),
                        "status_dist": s.get("status_dist", {}) or {},
                    }
                )
            except Exception:
                continue
    except Exception:
        return pd.DataFrame()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values("run_at")


def build_weekly_rebalance_quality_dashboard(
    *,
    app_root: str,
    days: int = 7,
    safe_parse_dt: Callable[[str], Any],
) -> Dict[str, Any]:
    df = load_recent_production_rebalance_audits(app_root, days=days, max_rows=400, safe_parse_dt=safe_parse_dt)
    if df is None or df.empty:
        return {"ok": False, "error": "no_recent_audit"}
    daily = (
        df.groupby("trade_date", as_index=False)
        .agg(
            score=("score", "mean"),
            success_rate_pct=("success_rate_pct", "mean"),
            turnover=("turnover", "sum"),
            total=("total", "sum"),
            skipped=("skipped", "sum"),
            buy_done=("buy_done", "sum"),
            reduce_done=("reduce_done", "sum"),
        )
        .sort_values("trade_date")
    )
    skip_reason_counter: Dict[str, int] = {}
    for _, r in df.iterrows():
        sd = r.get("status_dist", {}) or {}
        if isinstance(sd, dict):
            for k, v in sd.items():
                if str(k).startswith("SKIP_"):
                    skip_reason_counter[str(k)] = skip_reason_counter.get(str(k), 0) + int(v or 0)
    skip_top = sorted(skip_reason_counter.items(), key=lambda x: x[1], reverse=True)[:8]
    return {
        "ok": True,
        "daily_df": daily,
        "runs": int(len(df)),
        "avg_score": float(df["score"].mean()),
        "avg_success_rate": float(df["success_rate_pct"].mean()),
        "turnover_total": float(df["turnover"].sum()),
        "skip_top": skip_top,
    }


def auto_rebalance_log_path(app_root: str) -> str:
    out_dir = os.path.join(app_root, "logs", "openclaw")
    os.makedirs(out_dir, exist_ok=True)
    return os.path.join(out_dir, "production_auto_rebalance.jsonl")


def append_auto_rebalance_log(app_root: str, payload: Dict[str, Any]) -> None:
    try:
        p = auto_rebalance_log_path(app_root)
        with open(p, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload or {}, ensure_ascii=False) + "\n")
    except Exception:
        pass


def load_latest_auto_rebalance_log(app_root: str) -> Dict[str, Any]:
    p = auto_rebalance_log_path(app_root)
    if not os.path.exists(p):
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            lines = [x.strip() for x in f.readlines() if str(x).strip()]
        if not lines:
            return {}
        return dict(json.loads(lines[-1]) or {})
    except Exception:
        return {}


def load_latest_strategy_pool_candidates(
    *,
    app_root: str,
    strategy: str,
    top_n: int = 30,
    parse_price_to_float: Callable[[Any], float],
) -> pd.DataFrame:
    sk = str(strategy or "").strip().lower()
    if sk not in {"v5", "v8", "v9", "combo"}:
        return pd.DataFrame()
    pool_dirs = [
        os.path.join(app_root, "logs", "openclaw", "stock_pool"),
        os.path.join(os.getcwd(), "logs", "openclaw", "stock_pool"),
    ]
    pool_dir = ""
    for d in pool_dirs:
        if os.path.isdir(d):
            pool_dir = d
            break
    if not pool_dir:
        return pd.DataFrame()

    meta_files = sorted(glob.glob(os.path.join(pool_dir, "*.meta.json")), reverse=True)
    for mf in meta_files:
        try:
            with open(mf, "r", encoding="utf-8") as f:
                meta = json.load(f) or {}
            m_sk = str(meta.get("strategy", "")).strip().lower()
            if m_sk != sk:
                continue
            csv_path = str(meta.get("csv_path", "")).strip() or mf.replace(".meta.json", ".csv")
            if not os.path.isabs(csv_path):
                csv_path = os.path.join(app_root, csv_path)
            if not os.path.exists(csv_path):
                csv_path = mf.replace(".meta.json", ".csv")
            if not os.path.exists(csv_path):
                continue
            df = pd.read_csv(csv_path)
            if df is None or df.empty:
                continue
            code_col = "股票代码" if "股票代码" in df.columns else ("ts_code" if "ts_code" in df.columns else "")
            name_col = "股票名称" if "股票名称" in df.columns else ("name" if "name" in df.columns else "")
            ind_col = "行业" if "行业" in df.columns else ("industry" if "industry" in df.columns else "")
            score_col = ""
            for c in ["共识评分", "综合评分", "评分", "score"]:
                if c in df.columns:
                    score_col = c
                    break
            price_col = ""
            for c in ["最新价格", "close_price", "close", "价格"]:
                if c in df.columns:
                    price_col = c
                    break
            if not code_col:
                continue
            out = pd.DataFrame(
                {
                    "strategy": sk,
                    "ts_code": df[code_col].astype(str).str.strip(),
                    "name": (df[name_col].astype(str) if name_col else ""),
                    "industry": (df[ind_col].astype(str) if ind_col else "未知"),
                    "score": pd.to_numeric(df[score_col], errors="coerce") if score_col else float("nan"),
                    "price": df[price_col].apply(parse_price_to_float) if price_col else 0.0,
                }
            )
            out = out.dropna(subset=["ts_code"]).drop_duplicates(subset=["ts_code"], keep="first")
            if "score" in out.columns:
                out = out.sort_values("score", ascending=False)
            return out.head(max(1, int(top_n))).reset_index(drop=True)
        except Exception:
            continue
    return pd.DataFrame()


def build_production_rebalance_orders(
    plan: Dict[str, Any],
    *,
    load_strategy_pool_candidates: Callable[[str, int], pd.DataFrame],
    get_sim_positions: Callable[[], Dict[str, Dict[str, Any]]],
) -> Dict[str, Any]:
    if not isinstance(plan, dict) or not plan.get("ok"):
        return {"ok": False, "error": "invalid_plan"}
    plan_df = pd.DataFrame(plan.get("plan_df", []))
    if plan_df.empty:
        return {"ok": False, "error": "empty_plan_df"}

    positions = get_sim_positions()
    order_rows: List[Dict[str, Any]] = []
    target_codes: set[str] = set()
    buy_count = 0
    hold_count = 0

    for _, r in plan_df.iterrows():
        sk = str(r.get("strategy", "")).strip().lower()
        slot_cnt = max(1, int(r.get("slot_count", 1) or 1))
        cap_per_ticket = float(r.get("single_ticket_cap", 0.0) or 0.0)
        candidates = load_strategy_pool_candidates(sk, max(slot_cnt * 2, 10))
        if candidates.empty:
            order_rows.append(
                {
                    "action": "NO_DATA",
                    "strategy": sk,
                    "ts_code": "",
                    "name": "",
                    "industry": "",
                    "score": float("nan"),
                    "price": 0.0,
                    "target_amount": 0.0,
                    "reason": "未找到该策略最新股票池候选",
                }
            )
            continue
        picked = candidates.head(slot_cnt).copy()
        for _, c in picked.iterrows():
            ts_code = str(c.get("ts_code", "")).strip()
            if not ts_code:
                continue
            target_codes.add(ts_code)
            in_pos = ts_code in positions
            action = "HOLD" if in_pos else "BUY"
            if action == "BUY":
                buy_count += 1
            else:
                hold_count += 1
            order_rows.append(
                {
                    "action": action,
                    "strategy": sk,
                    "ts_code": ts_code,
                    "name": str(c.get("name", "")),
                    "industry": str(c.get("industry", "未知")),
                    "score": float(c.get("score", float("nan"))) if pd.notna(c.get("score", float("nan"))) else float("nan"),
                    "price": float(c.get("price", 0.0) or 0.0),
                    "target_amount": float(cap_per_ticket),
                    "reason": "策略建议仓位内且评分靠前",
                }
            )

    reduce_count = 0
    for ts_code, pos in positions.items():
        if ts_code in target_codes:
            continue
        reduce_count += 1
        order_rows.append(
            {
                "action": "REDUCE",
                "strategy": "portfolio",
                "ts_code": str(ts_code),
                "name": str(pos.get("name", "")),
                "industry": "",
                "score": float("nan"),
                "price": 0.0,
                "target_amount": 0.0,
                "reason": "不在当前目标池，建议减仓/退出",
            }
        )

    orders_df = pd.DataFrame(order_rows)
    if orders_df.empty:
        return {"ok": False, "error": "empty_orders"}
    return {
        "ok": True,
        "orders_df": orders_df,
        "summary": {
            "buy_count": int(buy_count),
            "hold_count": int(hold_count),
            "reduce_count": int(reduce_count),
            "total_orders": int(len(orders_df)),
        },
    }


def write_production_rebalance_report(
    *,
    app_root: str,
    plan: Dict[str, Any],
    rebalance: Dict[str, Any],
    now: datetime | None = None,
) -> Tuple[bool, str, str]:
    try:
        if not isinstance(plan, dict) or not isinstance(rebalance, dict) or not rebalance.get("ok"):
            return False, "", ""
        out_dir = os.path.join(app_root, "logs", "openclaw")
        os.makedirs(out_dir, exist_ok=True)
        now_dt = now or datetime.now()
        ts = now_dt.strftime("%Y%m%d_%H%M%S")
        md_path = os.path.join(out_dir, f"production_rebalance_report_{ts}.md")
        csv_path = os.path.join(out_dir, f"production_rebalance_report_{ts}.csv")
        odf = pd.DataFrame(rebalance.get("orders_df", []))
        if odf.empty:
            return False, "", ""
        odf.to_csv(csv_path, index=False, encoding="utf-8-sig")
        s = rebalance.get("summary", {}) or {}
        lines = [
            "# 生产调仓指令简报",
            "",
            f"- 生成时间: {now_dt.strftime('%Y-%m-%d %H:%M:%S')}",
            f"- 市场状态: {plan.get('regime', 'oscillation')}",
            f"- 指令总数: {int(s.get('total_orders', 0))}",
            f"- BUY/HOLD/REDUCE: {int(s.get('buy_count', 0))}/{int(s.get('hold_count', 0))}/{int(s.get('reduce_count', 0))}",
            "",
            "## 执行要点",
            "- BUY: 按目标金额分批建仓（优先流动性好的标的）",
            "- HOLD: 维持仓位，等待下一次调仓窗口",
            "- REDUCE: 先减弱势与偏离目标池标的",
            "",
            f"- 指令CSV: `{csv_path}`",
        ]
        with open(md_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        return True, md_path, csv_path
    except Exception:
        return False, "", ""


def precheck_production_rebalance_orders(
    rebalance: Dict[str, Any],
    *,
    execute_reduce: bool = False,
    get_sim_account: Callable[[], Dict[str, Any]],
    get_sim_positions: Callable[[], Dict[str, Dict[str, Any]]],
    get_latest_prices: Callable[[List[str]], Dict[str, Dict[str, Any]]],
    expand_ts_code_keys: Callable[[str], List[str]],
    parse_price_to_float: Callable[[Any], float],
) -> Dict[str, Any]:
    if not isinstance(rebalance, dict) or not rebalance.get("ok"):
        return {"ok": False, "error": "invalid_rebalance_orders"}
    orders_df = pd.DataFrame(rebalance.get("orders_df", []))
    if orders_df.empty:
        return {"ok": False, "error": "empty_orders_df"}

    account = get_sim_account()
    cash = float(account.get("cash", 0.0) or 0.0)
    positions = get_sim_positions()

    buy_df = orders_df[orders_df.get("action", "").astype(str).str.upper() == "BUY"].copy()
    reduce_df = orders_df[orders_df.get("action", "").astype(str).str.upper() == "REDUCE"].copy()

    all_codes = [str(x).strip() for x in orders_df.get("ts_code", pd.Series([], dtype=str)).dropna().astype(str).tolist()]
    latest_map = get_latest_prices(all_codes) if all_codes else {}

    need_cash = 0.0
    buy_ready = 0
    buy_no_price = 0
    buy_exists = 0
    buy_too_small = 0
    for _, r in buy_df.iterrows():
        ts_code = str(r.get("ts_code", "")).strip()
        if not ts_code:
            continue
        if ts_code in positions:
            buy_exists += 1
            continue
        price = parse_price_to_float(r.get("price", 0.0))
        if price <= 0:
            for k in expand_ts_code_keys(ts_code):
                info = latest_map.get(k)
                if info:
                    price = float(info.get("price", 0.0) or 0.0)
                    break
        if price <= 0:
            buy_no_price += 1
            continue
        target_amount = float(r.get("target_amount", 0.0) or 0.0)
        if target_amount <= 0:
            target_amount = float(account.get("per_buy_amount", 0.0) or 0.0)
        shares = int((target_amount / price) // 100 * 100)
        amount = float(shares * price)
        if shares <= 0 or amount <= 0:
            buy_too_small += 1
            continue
        need_cash += amount
        buy_ready += 1

    reduce_ready = 0
    reduce_no_pos = 0
    if bool(execute_reduce):
        for _, r in reduce_df.iterrows():
            ts_code = str(r.get("ts_code", "")).strip()
            if not ts_code:
                continue
            if ts_code not in positions:
                reduce_no_pos += 1
            else:
                reduce_ready += 1

    cash_gap = max(0.0, need_cash - cash)
    status = "GREEN"
    issues: List[str] = []
    if buy_ready <= 0 and reduce_ready <= 0:
        status = "RED"
        issues.append("无可执行指令")
    if cash_gap > 0:
        status = "RED"
        issues.append(f"资金缺口 {cash_gap:.2f}")
    if buy_no_price > 0:
        if status != "RED":
            status = "YELLOW"
        issues.append(f"{buy_no_price} 条BUY缺少价格")
    if buy_exists > 0:
        if status == "GREEN":
            status = "YELLOW"
        issues.append(f"{buy_exists} 条BUY与现持仓重复")

    return {
        "ok": True,
        "status": status,
        "cash": cash,
        "need_cash": need_cash,
        "cash_gap": cash_gap,
        "buy_ready": buy_ready,
        "buy_no_price": buy_no_price,
        "buy_exists": buy_exists,
        "buy_too_small": buy_too_small,
        "reduce_ready": reduce_ready,
        "reduce_no_pos": reduce_no_pos,
        "issues": issues,
        "execute_reduce": bool(execute_reduce),
    }


def execute_production_rebalance_orders(
    rebalance: Dict[str, Any],
    *,
    execute_reduce: bool = False,
    get_sim_account: Callable[[], Dict[str, Any]],
    get_sim_positions: Callable[[], Dict[str, Dict[str, Any]]],
    get_latest_prices: Callable[[List[str]], Dict[str, Dict[str, Any]]],
    expand_ts_code_keys: Callable[[str], List[str]],
    parse_price_to_float: Callable[[Any], float],
    upsert_sim_position: Callable[..., None],
    add_sim_trade: Callable[..., None],
    delete_sim_position: Callable[[str], None],
    update_sim_account: Callable[..., None],
    now: datetime | None = None,
    score_execution: Callable[[Dict[str, Any]], Dict[str, Any]] | None = None,
    append_rebalance_audit_log: Callable[[Dict[str, Any]], None] | None = None,
) -> Dict[str, Any]:
    if not isinstance(rebalance, dict) or not rebalance.get("ok"):
        return {"ok": False, "error": "invalid_rebalance_orders"}
    orders_df = pd.DataFrame(rebalance.get("orders_df", []))
    if orders_df.empty:
        return {"ok": False, "error": "empty_orders_df"}

    account = get_sim_account()
    cash = float(account.get("cash", 0.0) or 0.0)
    positions = get_sim_positions()
    now_dt = now or datetime.now()
    now_date = now_dt.strftime("%Y%m%d")
    batch_id = f"prod_rebalance_{now_dt.strftime('%Y%m%d_%H%M%S')}"

    all_codes = [str(x).strip() for x in orders_df.get("ts_code", pd.Series([], dtype=str)).dropna().astype(str).tolist()]
    latest_map = get_latest_prices(all_codes) if all_codes else {}

    executed_rows: List[Dict[str, Any]] = []
    buy_done = 0
    reduce_done = 0
    skipped = 0
    buy_amount_total = 0.0
    sell_amount_total = 0.0

    for _, row in orders_df.iterrows():
        action = str(row.get("action", "")).strip().upper()
        ts_code = str(row.get("ts_code", "")).strip()
        if not ts_code:
            skipped += 1
            continue
        name = str(row.get("name", "") or ts_code)
        target_amount = float(row.get("target_amount", 0.0) or 0.0)
        if target_amount <= 0:
            target_amount = float(account.get("per_buy_amount", 0.0) or 0.0)

        price = parse_price_to_float(row.get("price", 0.0))
        if price <= 0:
            hit = None
            for k in expand_ts_code_keys(ts_code):
                hit = latest_map.get(k)
                if hit:
                    break
            if hit:
                price = float(hit.get("price", 0.0) or 0.0)

        if action == "BUY":
            if ts_code in positions:
                skipped += 1
                executed_rows.append({"action": action, "ts_code": ts_code, "status": "SKIP_EXISTS", "shares": 0, "amount": 0.0})
                continue
            if price <= 0:
                skipped += 1
                executed_rows.append({"action": action, "ts_code": ts_code, "status": "SKIP_NO_PRICE", "shares": 0, "amount": 0.0})
                continue
            shares = int((target_amount / price) // 100 * 100)
            amount = float(shares * price)
            if shares <= 0 or amount <= 0:
                skipped += 1
                executed_rows.append({"action": action, "ts_code": ts_code, "status": "SKIP_TOO_SMALL", "shares": 0, "amount": 0.0})
                continue
            if cash < amount:
                skipped += 1
                executed_rows.append({"action": action, "ts_code": ts_code, "status": "SKIP_NO_CASH", "shares": shares, "amount": amount})
                continue
            upsert_sim_position(ts_code=ts_code, name=name, shares=shares, avg_cost=price, buy_date=now_date)
            add_sim_trade(
                trade_date=now_date,
                ts_code=ts_code,
                name=name,
                side="BUY",
                price=price,
                shares=shares,
                amount=amount,
                pnl=0.0,
                batch_id=batch_id,
                source="prod_rebalance",
            )
            cash -= amount
            buy_done += 1
            buy_amount_total += amount
            executed_rows.append({"action": action, "ts_code": ts_code, "status": "DONE", "shares": shares, "amount": amount})
            continue

        if action == "REDUCE":
            if not execute_reduce:
                skipped += 1
                executed_rows.append({"action": action, "ts_code": ts_code, "status": "SKIP_REDUCE_DISABLED", "shares": 0, "amount": 0.0})
                continue
            pos = positions.get(ts_code)
            if not pos:
                skipped += 1
                executed_rows.append({"action": action, "ts_code": ts_code, "status": "SKIP_NO_POSITION", "shares": 0, "amount": 0.0})
                continue
            if price <= 0:
                skipped += 1
                executed_rows.append({"action": action, "ts_code": ts_code, "status": "SKIP_NO_PRICE", "shares": 0, "amount": 0.0})
                continue
            shares = int(pos.get("shares", 0) or 0)
            if shares <= 0:
                skipped += 1
                executed_rows.append({"action": action, "ts_code": ts_code, "status": "SKIP_ZERO_SHARES", "shares": 0, "amount": 0.0})
                continue
            amount = float(shares * price)
            avg_cost = float(pos.get("avg_cost", 0.0) or 0.0)
            pnl = float((price - avg_cost) * shares)
            add_sim_trade(
                trade_date=now_date,
                ts_code=ts_code,
                name=name,
                side="SELL",
                price=price,
                shares=shares,
                amount=amount,
                pnl=pnl,
                batch_id=batch_id,
                source="prod_rebalance",
            )
            delete_sim_position(ts_code)
            cash += amount
            reduce_done += 1
            sell_amount_total += amount
            executed_rows.append({"action": action, "ts_code": ts_code, "status": "DONE", "shares": shares, "amount": amount})
            continue

        skipped += 1
        executed_rows.append({"action": action, "ts_code": ts_code, "status": "SKIP_UNSUPPORTED", "shares": 0, "amount": 0.0})

    update_sim_account(cash=float(cash))
    exec_df = pd.DataFrame(executed_rows)
    status_dist: Dict[str, int] = {}
    if not exec_df.empty and "status" in exec_df.columns:
        vc = exec_df["status"].astype(str).value_counts()
        status_dist = {str(k): int(v) for k, v in vc.to_dict().items()}
    out = {
        "ok": True,
        "batch_id": batch_id,
        "execute_reduce": bool(execute_reduce),
        "summary": {
            "buy_done": int(buy_done),
            "reduce_done": int(reduce_done),
            "skipped": int(skipped),
            "total": int(len(executed_rows)),
            "cash_after": float(cash),
            "buy_amount_total": float(buy_amount_total),
            "sell_amount_total": float(sell_amount_total),
            "status_dist": status_dist,
        },
        "executed_df": exec_df.to_dict("records"),
    }
    if score_execution is not None:
        out["quality"] = score_execution(out)
    if append_rebalance_audit_log is not None:
        append_rebalance_audit_log(out)
    return out


def run_auto_rebalance_pipeline(
    *,
    capital_total: float,
    regime_choice: str = "auto",
    execute_reduce: bool = False,
    now_text: Callable[[], str],
    compute_production_allocation_plan: Callable[..., Dict[str, Any]],
    build_production_rebalance_orders: Callable[[Dict[str, Any]], Dict[str, Any]],
    precheck_production_rebalance_orders: Callable[[Dict[str, Any], bool], Dict[str, Any]],
    execute_production_rebalance_orders: Callable[[Dict[str, Any], bool], Dict[str, Any]],
    append_auto_rebalance_log: Callable[[Dict[str, Any]], None],
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "run_at": now_text(),
        "capital_total": float(capital_total),
        "regime_choice": str(regime_choice),
        "execute_reduce": bool(execute_reduce),
        "ok": False,
        "stage": "init",
    }
    plan = compute_production_allocation_plan(capital_total=float(capital_total), regime_choice=str(regime_choice))
    if not plan.get("ok"):
        out.update({"stage": "allocation", "error": str(plan.get("error", "allocation_failed"))})
        append_auto_rebalance_log(out)
        return out

    reb = build_production_rebalance_orders(plan)
    if not reb.get("ok"):
        out.update({"stage": "build_orders", "error": str(reb.get("error", "build_orders_failed"))})
        append_auto_rebalance_log(out)
        return out

    pre = precheck_production_rebalance_orders(reb, bool(execute_reduce))
    if not pre.get("ok"):
        out.update({"stage": "precheck", "error": str(pre.get("error", "precheck_failed"))})
        append_auto_rebalance_log(out)
        return out
    if str(pre.get("status", "RED")).upper() == "RED":
        out.update({"stage": "precheck", "error": "precheck_red_blocked", "precheck": pre})
        append_auto_rebalance_log(out)
        return out

    exec_ret = execute_production_rebalance_orders(reb, bool(execute_reduce))
    if not exec_ret.get("ok"):
        out.update({"stage": "execute", "error": str(exec_ret.get("error", "execute_failed")), "precheck": pre})
        append_auto_rebalance_log(out)
        return out

    out.update(
        {
            "ok": True,
            "stage": "done",
            "plan": {
                "regime": plan.get("regime"),
                "risk_budget": plan.get("risk_budget"),
            },
            "precheck": pre,
            "execute": {
                "batch_id": exec_ret.get("batch_id"),
                "summary": exec_ret.get("summary", {}),
                "quality": exec_ret.get("quality", {}),
            },
        }
    )
    append_auto_rebalance_log(out)
    return out


def auto_rebalance_scheduler_tick(
    *,
    enabled: bool,
    hhmm: str,
    capital: float,
    regime_choice: str,
    execute_reduce: bool,
    now: datetime,
    get_sim_meta: Callable[[str, Any], Any],
    set_sim_meta: Callable[[str, Any], None],
    now_text: Callable[[], str],
    run_auto_rebalance_pipeline: Callable[[float, str, bool], Dict[str, Any]],
) -> Dict[str, Any]:
    today = now.strftime("%Y%m%d")
    if not enabled:
        return {"ok": False, "status": "disabled"}

    m = __import__("re").fullmatch(r"([01]\d|2[0-3]):([0-5]\d)", str(hhmm or "").strip())
    if not m:
        return {"ok": False, "status": "invalid_time", "time": hhmm}
    run_h, run_m = int(m.group(1)), int(m.group(2))
    due = (now.hour, now.minute) >= (run_h, run_m)
    if not due:
        return {"ok": False, "status": "not_due", "time": hhmm}

    last_run_date = get_sim_meta("prod_auto_rebalance_last_run_date")
    if str(last_run_date or "") == today:
        return {"ok": False, "status": "already_ran_today", "date": today}

    ret = run_auto_rebalance_pipeline(float(capital), str(regime_choice), bool(execute_reduce))
    set_sim_meta("prod_auto_rebalance_last_run_date", today)
    set_sim_meta("prod_auto_rebalance_last_run_at", now_text())
    set_sim_meta("prod_auto_rebalance_last_ok", "1" if ret.get("ok") else "0")
    return {"ok": bool(ret.get("ok")), "status": "ran", "result": ret}


def production_strategy_health_multipliers(
    load_latest_production_backtest_audit: Callable[[], Tuple[pd.DataFrame, str]],
) -> Dict[str, float]:
    df, _ = load_latest_production_backtest_audit()
    if df is None or df.empty:
        return {"v5": 1.0, "v8": 1.0, "v9": 1.0, "combo": 1.0}
    out = {"v5": 1.0, "v8": 1.0, "v9": 1.0, "combo": 1.0}
    for _, row in df.iterrows():
        try:
            sk = str(row.get("策略", "")).strip().lower()
            if sk not in out:
                continue
            risk = str(row.get("评估风险", "GREEN")).strip().upper()
            promo = str(row.get("建议晋升", "NO")).strip().upper() == "YES"
            if risk == "RED":
                mul = 0.20
            elif risk == "YELLOW":
                mul = 0.60
            else:
                mul = 1.00
            if not promo:
                mul = min(mul, 0.80)
            out[sk] = float(mul)
        except Exception:
            continue
    return out
