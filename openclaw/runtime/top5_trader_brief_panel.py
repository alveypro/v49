# -*- coding: utf-8 -*-
"""Streamlit panel for the Top5 trader brief.

The calculation/rebuild path lives in ``openclaw.services``.  This module is
presentation-only so ``v49_app.py`` can remain an entry shell.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Tuple

import pandas as pd

from openclaw.services.top5_brief_manifest_freshness_service import (
    evaluate_top5_brief_stale_banner,
    resolve_top5_brief_stale_alert_hours_threshold,
)
from openclaw.services.top5_trader_brief_rebuild_service import (
    compute_top5_advice_accuracy_payload,
    load_top5_advice_version_state,
    rebuild_top5_trader_brief_exports,
    safe_float_any,
    save_top5_advice_version_state,
)


GuardAction = Callable[..., bool]


def _exports_dir(repo_root: Path) -> Path:
    env_dir = os.getenv("TOP5_TRADER_BRIEF_EXPORTS_DIR", "").strip()
    if env_dir:
        return Path(env_dir).expanduser().resolve()
    return repo_root / "exports"


def latest_top5_trader_brief_exports(repo_root: Path | str) -> Dict[str, str]:
    exports_dir = _exports_dir(Path(repo_root).resolve())
    if not exports_dir.exists():
        return {}
    manifest_path = exports_dir / "top5_trader_brief_latest_manifest.json"
    if manifest_path.is_file():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            if isinstance(manifest, dict):
                out: Dict[str, str] = {}
                for key in ("markdown", "csv"):
                    raw = str(manifest.get(key) or "").strip()
                    if raw and Path(raw).is_file():
                        out[key] = raw
                if out:
                    return out
        except Exception:
            pass
    md_files = sorted(
        exports_dir.glob("top5_trader_brief_*.md"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    csv_files = sorted(
        exports_dir.glob("top5_trader_brief_*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    payload: Dict[str, str] = {}
    if md_files:
        payload["markdown"] = str(md_files[0])
    if csv_files:
        payload["csv"] = str(csv_files[0])
    return payload


def _localize_top5_markdown(text: str) -> str:
    if not text:
        return text
    replacements = {
        "# Top5 Trader Brief": "# Top5 交易员执行清单",
        "## Top5 List": "## Top5 列表",
        "## Execution Priority (Actionable)": "## 执行优先级（可执行）",
        "## Pre-open Checks (Must Pass)": "## 开盘前检查（必须通过）",
        "- Source artifact:": "- 来源审计文件：",
        "- Audit mode:": "- 审计模式：",
        "- Universe count:": "- 股票数量：",
        "industry=": "行业=",
        "weight=": "权重=",
        "score=": "得分=",
        "est_cost_bps=": "预估成本bp=",
    }
    out = str(text)
    for src, dst in replacements.items():
        out = out.replace(src, dst)
    return out


def _normalize_top5_trader_dataframe(trader_df: pd.DataFrame) -> pd.DataFrame:
    if trader_df is None or trader_df.empty:
        return trader_df
    out = trader_df.copy()
    if "ts_code" in out.columns:
        out = out.rename(
            columns={
                "rank": "序号",
                "ts_code": "股票代码",
                "name": "股票名称",
                "industry": "行业",
                "target_weight": "目标权重",
                "final_stock_score": "综合得分",
                "liquidity_amount": "流动性金额",
                "pct_chg": "涨跌幅",
                "estimated_cost_bps": "预估成本(bp)",
                "risk_contribution_share": "风险贡献占比",
                "execution_priority": "执行优先级",
                "risk_tag": "风险标签",
                "action_note": "操作建议",
            }
        )
    if "清单状态" not in out.columns:
        out["清单状态"] = "盘前复核后可执行" if "参考买入价" in out.columns else "旧版清单：仅人工复核"
    return out


def _run_full_top5_evidence_pipeline(repo_root: Path, permanent_db_path: str) -> Tuple[bool, str]:
    script = repo_root / "tools" / "run_daily_v9_evidence_pipeline.py"
    if not script.exists():
        return rebuild_top5_trader_brief_exports(repo_root=repo_root)
    env = dict(os.environ)
    env.update(
        {
            "PERMANENT_DB_PATH": str(permanent_db_path),
            "OPENCLAW_DB_PATH": str(permanent_db_path),
            "AIRIVO_DB_PATH": str(permanent_db_path),
            "TOP5_AUDIT_MODE": "strict",
            "TOP5_AUDIT_AUTO_SHADOW_INPUT": "0",
            "TOP5_GATE_ENFORCE_RELAXED": "0",
            "AIRIVO_RECORD_PLANNED_OBSERVATIONS": "1",
            "AIRIVO_REQUIRE_EXECUTION_CLOSURE": "0",
            "AIRIVO_REQUIRE_NO_STALE_OPEN": "0",
        }
    )
    proc = subprocess.run(
        [
            sys.executable or "python3",
            str(script),
            "--db-path",
            str(permanent_db_path),
            "--record-planned-observations",
            "--site-health-url",
            "https://airivo.online/",
        ],
        cwd=str(repo_root),
        env=env,
        capture_output=True,
        text=True,
        timeout=1800,
    )
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        return False, f"完整证据流水线执行失败：{err[:500]}"
    out = (proc.stdout or "").strip()
    return True, f"完整证据流水线已执行：{out[:500]}"


def _load_top5_execution_json(exports_dir: Path, name: str) -> Dict[str, Any]:
    path = exports_dir / name
    if not path.exists():
        return {"_missing": True, "_path": str(path)}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            payload["_path"] = str(path)
            return payload
    except Exception as exc:
        return {"_error": str(exc), "_path": str(path)}
    return {"_error": "JSON payload is not an object", "_path": str(path)}


def _render_top5_execution_evidence_status(st: Any, exports_dir: Path) -> None:
    sla = _load_top5_execution_json(exports_dir, "top5_execution_ops_sla.json")
    summary = _load_top5_execution_json(exports_dir, "top5_execution_evidence_summary.json")
    readiness = _load_top5_execution_json(exports_dir, "v9_canary_promotion_readiness.json")
    if sla.get("_missing") and summary.get("_missing") and readiness.get("_missing"):
        st.warning("执行证据状态缺失：尚未生成 SLA / 证据摘要 / v9 晋级评估。Top5 不应被视为已具备实盘质量证据。")
        return

    sla_risk = str(sla.get("risk_level") or "unknown")
    evidence_risk = str(summary.get("risk_level") or "unknown")
    verdict = str(readiness.get("verdict") or "unknown")
    stale_open = int(safe_float_any(sla.get("stale_open_observation_count"), 0.0))
    current_open = int(safe_float_any(sla.get("current_trade_date_open_count"), 0.0))
    open_count = int(safe_float_any(summary.get("open_observation_count"), sla.get("open_observation_count", 0)))
    closure_rate = float(safe_float_any(summary.get("closure_rate"), readiness.get("overall_closure_rate", 0.0)))
    closed_days = int(safe_float_any(readiness.get("closed_evidence_trade_days"), 0))
    import_count = int(safe_float_any(readiness.get("import_manifest_count"), 0))

    if stale_open > 0 or evidence_risk == "red" or verdict == "blocked":
        st.error("执行证据状态：不可作为生产/晋级依据。请先关闭 open observations 并完成正式导入。")
    elif sla_risk in {"orange", "yellow"} or evidence_risk in {"orange", "yellow"}:
        st.warning("执行证据状态：当天运营未闭环。Top5 只能作为候选清单，不能宣称实盘有效。")
    else:
        st.success("执行证据状态：当前账本无 open observations。仍需满足 20-60 个交易日证据窗口后再讨论晋级。")

    st.markdown("#### 执行证据与运营 SLA")
    col_a, col_b, col_c, col_d = st.columns(4)
    with col_a:
        st.metric("SLA 风险", sla_risk)
    with col_b:
        st.metric("未闭环", open_count)
    with col_c:
        st.metric("跨日欠账", stale_open)
    with col_d:
        st.metric("闭环率", f"{closure_rate * 100:.1f}%")

    col_e, col_f, col_g, col_h = st.columns(4)
    with col_e:
        st.metric("当天待办", current_open)
    with col_f:
        st.metric("闭环交易日", closed_days)
    with col_g:
        st.metric("正式导入批次", import_count)
    with col_h:
        st.metric("v9 晋级状态", verdict)

    evidence_files = [
        ("下载未闭环待办 CSV", "top5_execution_open_observations.csv", "text/csv"),
        ("下载 SLA 报告", "top5_execution_ops_sla.md", "text/markdown"),
        ("下载执行证据摘要", "top5_execution_evidence_summary.md", "text/markdown"),
        ("下载 v9 晋级评估", "v9_canary_promotion_readiness.md", "text/markdown"),
        ("下载证据法庭记录", "top5_execution_court_record.md", "text/markdown"),
    ]
    cols = st.columns(len(evidence_files))
    for idx, (label, filename, mime) in enumerate(evidence_files):
        path = exports_dir / filename
        with cols[idx]:
            if path.exists():
                data = path.read_bytes() if filename.endswith(".csv") else path.read_text(encoding="utf-8")
                st.download_button(label, data=data, file_name=filename, mime=mime, key=f"download_{filename}")
            else:
                st.caption(f"{label}：未生成")


def _runtime_consistency_snapshot(repo_root: Path, exports_dir: Path) -> Dict[str, Any]:
    state = load_top5_advice_version_state(repo_root=repo_root)
    active_version = str(state.get("active_version") or "A").upper()
    latest_brief_version = ""
    latest_brief_file = ""
    latest_brief_age_min = -1
    brief_ok = False
    try:
        csv_files = sorted(
            exports_dir.glob("top5_trader_brief_*.csv"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if csv_files:
            latest = csv_files[0]
            latest_brief_file = str(latest)
            latest_brief_age_min = int(max(time.time() - latest.stat().st_mtime, 0.0) // 60)
            brief_df = pd.read_csv(latest)
            if "参数版本" in brief_df.columns and not brief_df.empty:
                latest_brief_version = str(brief_df.iloc[0].get("参数版本") or "").upper()
            brief_ok = bool(latest_brief_version == active_version and latest_brief_age_min <= 240)
    except Exception:
        pass

    latest_audit_file = ""
    latest_audit_age_min = -1
    audit_ok = False
    try:
        audit_files = sorted(
            exports_dir.glob("top5_advice_version_audit_*.md"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if audit_files:
            latest_audit = audit_files[0]
            latest_audit_file = str(latest_audit)
            latest_audit_age_min = int(max(time.time() - latest_audit.stat().st_mtime, 0.0) // 60)
            audit_ok = bool(latest_audit_age_min <= 240)
    except Exception:
        pass
    return {
        "active_version": active_version,
        "latest_brief_version": latest_brief_version,
        "latest_brief_file": latest_brief_file,
        "latest_brief_age_min": latest_brief_age_min,
        "latest_audit_file": latest_audit_file,
        "latest_audit_age_min": latest_audit_age_min,
        "brief_ok": brief_ok,
        "audit_ok": audit_ok,
        "overall_ok": bool(brief_ok and audit_ok),
    }


def _guarded(guard_action: Optional[GuardAction], role: str, action: str, target: str, reason: str = "") -> bool:
    if guard_action is None:
        return True
    return bool(guard_action(role, action, target=target, reason=reason))


def _render_accuracy_dashboard(
    *,
    st: Any,
    repo_root: Path,
    active_version: str,
    min_samples_for_switch: int,
    auto_degrade_enabled: bool,
    guard_action: Optional[GuardAction],
    exports_dir: Path,
) -> None:
    st.markdown("#### 建议准确率看板（近20/60/120交易日）")
    try:
        accuracy_payload = compute_top5_advice_accuracy_payload(repo_root=repo_root)
    except Exception as exc:
        st.warning(f"建议准确率看板计算失败：{exc}")
        return

    score_df = accuracy_payload.get("dashboard", pd.DataFrame())
    if isinstance(score_df, pd.DataFrame) and not score_df.empty:
        st.dataframe(score_df, use_container_width=True, hide_index=True)
    else:
        st.caption("暂无可用于评估的历史 Top5 建议样本。")

    for key, caption in (
        ("summary", ""),
        ("execution_sync", "成交回报增量入库状态："),
        ("missing_reasons", "缺失原因统计（用于定位为何部分样本无法纳入评估）："),
        ("industry_segments", "分行业准确率（近60日）："),
        ("liquidity_segments", "分流动性准确率（近60日）："),
        ("volatility_segments", "分波动状态准确率（近60日）："),
        ("ab_compare", "参数版本 A/B 滚动对比："),
    ):
        df = accuracy_payload.get(key, pd.DataFrame())
        if isinstance(df, pd.DataFrame) and not df.empty:
            if caption:
                st.caption(caption)
            st.dataframe(df, use_container_width=True, hide_index=True)

    ab_compare_df = accuracy_payload.get("ab_compare", pd.DataFrame())
    if isinstance(ab_compare_df, pd.DataFrame) and not ab_compare_df.empty:
        col_v1, col_v2, col_v3, col_v4 = st.columns(4)
        with col_v1:
            target_version = st.selectbox(
                "切换目标版本",
                options=["A", "B"],
                index=0 if active_version == "A" else 1,
                key="top5_advice_target_version",
            )
        with col_v2:
            new_min_samples = st.number_input(
                "切换最小样本门槛",
                min_value=1,
                max_value=500,
                value=int(min_samples_for_switch),
                step=1,
                key="top5_advice_min_samples",
            )
        with col_v3:
            if st.button("保存门槛", key="top5_save_min_samples"):
                if _guarded(guard_action, "admin", "top5_save_min_samples", "top5_advice_version"):
                    save_top5_advice_version_state({"min_samples_for_switch": int(new_min_samples)}, repo_root=repo_root)
                    st.success(f"已保存切换最小样本门槛：{int(new_min_samples)}")
        with col_v4:
            auto_degrade_choice = st.selectbox(
                "自动降级",
                options=["开启", "关闭"],
                index=0 if auto_degrade_enabled else 1,
                key="top5_auto_degrade_choice",
            )
            if st.button("保存降级开关", key="top5_save_auto_degrade"):
                if _guarded(guard_action, "admin", "top5_save_auto_degrade", "top5_advice_version"):
                    save_top5_advice_version_state(
                        {"auto_degrade_enabled": auto_degrade_choice == "开启"},
                        repo_root=repo_root,
                    )
                    st.success(f"自动降级已{auto_degrade_choice}。")

    consistency = _runtime_consistency_snapshot(repo_root, exports_dir)
    st.caption("线上/离线一致性监控：")
    st.dataframe(
        pd.DataFrame(
            [
                {
                    "检查项": "参数版本一致性",
                    "结果": "通过" if consistency.get("brief_ok") else "异常",
                    "详情": (
                        f"激活版本={consistency.get('active_version')} | "
                        f"最新清单版本={consistency.get('latest_brief_version') or '未知'} | "
                        f"清单年龄={consistency.get('latest_brief_age_min')}分钟"
                    ),
                },
                {
                    "检查项": "版本审计新鲜度",
                    "结果": "通过" if consistency.get("audit_ok") else "异常",
                    "详情": (
                        f"审计文件={consistency.get('latest_audit_file') or '无'} | "
                        f"年龄={consistency.get('latest_audit_age_min')}分钟"
                    ),
                },
                {
                    "检查项": "总体验证",
                    "结果": "通过" if consistency.get("overall_ok") else "需关注",
                    "详情": f"最新清单={consistency.get('latest_brief_file') or '无'}",
                },
            ]
        ),
        use_container_width=True,
        hide_index=True,
    )

    cost_consistency_df = accuracy_payload.get("cost_consistency", pd.DataFrame())
    if isinstance(cost_consistency_df, pd.DataFrame) and not cost_consistency_df.empty:
        st.caption("交易成本分解一致性（信号收益 vs 执行后收益 vs 实际成交收益）：")
        st.dataframe(cost_consistency_df, use_container_width=True, hide_index=True)

    export_csv = str(accuracy_payload.get("export_csv") or "")
    updated_at = str(accuracy_payload.get("updated_at") or "")
    if export_csv:
        st.caption(f"评估导出文件：{export_csv}")
    if updated_at:
        st.caption(f"最近计算时间：{updated_at}")


def render_top5_trader_brief_panel(
    *,
    st: Any,
    repo_root: Path | str,
    permanent_db_path: str,
    config: Optional[Mapping[str, Any]] = None,
    guard_action: Optional[GuardAction] = None,
) -> None:
    repo_root = Path(repo_root).resolve()
    exports_dir = _exports_dir(repo_root)
    with st.expander("Top5 交易员执行清单", expanded=False):
        version_state = load_top5_advice_version_state(repo_root=repo_root)
        active_version = str(version_state.get("active_version") or "A").upper()
        previous_version = str(version_state.get("previous_version") or "").upper()
        min_samples_for_switch = max(1, int(safe_float_any(version_state.get("min_samples_for_switch"), 30.0)))
        auto_degrade_enabled = bool(version_state.get("auto_degrade_enabled", True))

        refresh_sec = 0
        try:
            refresh_sec = max(0, int(float(os.getenv("TOP5_TRADER_BRIEF_PAGE_AUTO_REFRESH_SECONDS", "0") or 0)))
        except (TypeError, ValueError):
            refresh_sec = 0
        if refresh_sec > 0:
            refresh_sec = max(30, min(refresh_sec, 3600))
        if refresh_sec > 0 and getattr(st, "autorefresh", None):
            st.autorefresh(interval=refresh_sec * 1000, key="top5_trader_brief_page_autorefresh")
        refresh_hint = (
            f" | 页面自动重载：整页每 `{refresh_sec}s`（仅刷新展示；清单以 exports/manifest 为准）"
            if refresh_sec > 0
            else " | 页面自动重载：关闭（`TOP5_TRADER_BRIEF_PAGE_AUTO_REFRESH_SECONDS`，建议 30-600）"
        )
        st.caption(
            f"当前参数版本：`{active_version}` | 上一版本：`{previous_version or '无'}` | "
            f"切换最小样本门槛：`{min_samples_for_switch}` | 自动降级：`{'开启' if auto_degrade_enabled else '关闭'}`"
            f"{refresh_hint}"
        )

        if st.button("运行完整证据流水线并刷新 Top5", key="refresh_top5_trader_brief", use_container_width=True):
            if _guarded(guard_action, "operator", "refresh_top5_trader_brief", "top5_trader_brief", "manual_refresh"):
                with st.spinner("正在执行数据健康、v9 evidence、Top5 gate、manifest 与执行证据检查..."):
                    ok, msg = _run_full_top5_evidence_pipeline(repo_root, permanent_db_path)
                st.success(msg) if ok else st.error(msg)

        artifacts = latest_top5_trader_brief_exports(repo_root)
        if not artifacts:
            st.caption("暂无 `exports/top5_trader_brief_*.md/.csv`，请先执行 top5 清单生成。")
            return

        _render_top5_execution_evidence_status(st, exports_dir)
        md_path = Path(str(artifacts.get("markdown", "") or ""))
        csv_path = Path(str(artifacts.get("csv", "") or ""))
        fallback_paths = [p for p in (md_path, csv_path) if p.name]
        _, stale_msg = evaluate_top5_brief_stale_banner(
            exports_dir=exports_dir,
            manifest_fallback_paths=fallback_paths,
            secondary_config=config,
        )
        if stale_msg:
            st.warning(stale_msg)
        threshold_h = resolve_top5_brief_stale_alert_hours_threshold(secondary_config=config)
        if threshold_h > 0:
            st.caption(
                f"新鲜度阈值：超过 **{threshold_h:.0f} 小时** 显示降级横幅（展示层，`TOP5_BRIEF_STALE_ALERT_HOURS` / `config.json` 可调；≤0 关闭）。"
            )

        if md_path.exists():
            try:
                st.markdown(_localize_top5_markdown(md_path.read_text(encoding="utf-8")))
            except Exception as exc:
                st.warning(f"读取文档清单失败：{exc}")
        if csv_path.exists():
            try:
                trader_df = _normalize_top5_trader_dataframe(pd.read_csv(csv_path))
                preferred_cols = [
                    "序号",
                    "股票代码",
                    "股票名称",
                    "清单状态",
                    "行业",
                    "基准价格",
                    "参考买入价",
                    "建议持有天数",
                    "参考卖出价",
                    "止损价",
                    "目标权重",
                    "综合得分",
                    "流动性金额",
                    "涨跌幅",
                    "预估成本(bp)",
                    "风险贡献占比",
                    "执行优先级",
                    "风险标签",
                    "委托方式",
                    "首波上限",
                    "交易台硬门禁",
                    "操作建议",
                    "触发应对",
                ]
                visible_cols = [col for col in preferred_cols if col in trader_df.columns]
                st.dataframe(trader_df[visible_cols] if visible_cols else trader_df, use_container_width=True, hide_index=True)
            except Exception as exc:
                st.warning(f"读取表格清单失败：{exc}")

        _render_accuracy_dashboard(
            st=st,
            repo_root=repo_root,
            active_version=active_version,
            min_samples_for_switch=min_samples_for_switch,
            auto_degrade_enabled=auto_degrade_enabled,
            guard_action=guard_action,
            exports_dir=exports_dir,
        )

        col_left, col_right = st.columns(2)
        with col_left:
            if csv_path.exists():
                st.download_button(
                    "下载表格清单（CSV）",
                    data=csv_path.read_bytes(),
                    file_name=csv_path.name,
                    mime="text/csv",
                    key="download_top5_trader_csv",
                )
        with col_right:
            if md_path.exists():
                st.download_button(
                    "下载文档清单（Markdown）",
                    data=md_path.read_text(encoding="utf-8"),
                    file_name=md_path.name,
                    mime="text/markdown",
                    key="download_top5_trader_md",
                )
        st.caption(
            f"当前清单文件：文档={md_path if md_path.exists() else '无'} | 表格={csv_path if csv_path.exists() else '无'}"
        )


__all__ = ["latest_top5_trader_brief_exports", "render_top5_trader_brief_panel"]
