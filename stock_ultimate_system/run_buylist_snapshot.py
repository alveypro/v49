import argparse
import json
from datetime import datetime

import pandas as pd
from src.primary_result_candidate_basket import TARGET_MAX_INDUSTRY_WEIGHT

from src.utils.project_paths import resolve_project_path


def _to_float(value: object, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _normalise_risk(value: object) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return "unknown"
    if "high" in text or "高" in text:
        return "high"
    if "low" in text or "低" in text:
        return "low"
    if "mid" in text or "med" in text or "中" in text:
        return "medium"
    return text


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate latest buylist snapshot from candidates output")
    parser.add_argument("--input-csv", default="data/experiments/candidates_top_latest.csv", help="Latest candidates csv path")
    parser.add_argument("--output-dir", default="data/experiments", help="Output directory")
    parser.add_argument("--target-count", type=int, default=5, help="Fixed target buyable count")
    parser.add_argument(
        "--max-industry-weight",
        type=float,
        default=TARGET_MAX_INDUSTRY_WEIGHT,
        help="Max share per industry within buylist",
    )
    parser.add_argument("--max-high-risk-count", type=int, default=1, help="Max high risk stocks within buylist")
    parser.add_argument("--min-retain-ratio", type=float, default=0.4, help="Soft target for retaining previous picks")
    args = parser.parse_args()

    input_csv = resolve_project_path(args.input_csv)
    out_dir = resolve_project_path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    output_json = out_dir / "buylist_latest.json"
    output_md = out_dir / "buylist_latest.md"
    previous_payload = {}
    if output_json.exists():
        try:
            previous_payload = json.loads(output_json.read_text(encoding="utf-8"))
        except Exception:
            previous_payload = {}

    if not input_csv.exists():
        payload = {
            "generated_at": datetime.now().isoformat(),
            "target_count": int(args.target_count),
            "buyable_count": 0,
            "reason": f"missing input csv: {input_csv}",
            "items": [],
        }
        output_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        output_md.write_text("# Buylist Snapshot\n\nNo candidate file found.\n", encoding="utf-8")
        print(str(output_json))
        print(str(output_md))
        return

    df = pd.read_csv(input_csv)
    if "signal" not in df.columns:
        df["signal"] = "unknown"
    if "rank" not in df.columns:
        df["rank"] = range(1, len(df) + 1)
    if "ts_code" not in df.columns:
        df["ts_code"] = ""
    if "final_score" not in df.columns:
        df["final_score"] = 0.0
    if "stock_name" not in df.columns:
        if "name" in df.columns:
            df["stock_name"] = df["name"]
        else:
            df["stock_name"] = ""
    if "industry" not in df.columns:
        df["industry"] = ""
    if "risk_level" not in df.columns:
        df["risk_level"] = "unknown"
    df["risk_level"] = df["risk_level"].map(_normalise_risk)
    df["industry"] = df["industry"].fillna("").astype(str)

    buyable = df[df["signal"].astype(str).isin(["strong_buy", "buy"])].copy()
    buyable = buyable.sort_values(["rank", "final_score"], ascending=[True, False]).reset_index(drop=True)
    target_count = max(int(args.target_count), 1)
    max_industry_weight = min(max(float(args.max_industry_weight), 0.1), 1.0)
    max_high_risk_count = max(int(args.max_high_risk_count), 0)
    min_retain_ratio = min(max(float(args.min_retain_ratio), 0.0), 1.0)

    max_per_industry = max(1, int(target_count * max_industry_weight))
    min_retain_count = min(target_count, int(round(target_count * min_retain_ratio)))

    previous_items = previous_payload.get("items", []) if isinstance(previous_payload, dict) else []
    previous_codes_ordered = [
        str(x.get("ts_code", "")).strip() for x in previous_items if str(x.get("ts_code", "")).strip()
    ]
    previous_codes_set = set(previous_codes_ordered)
    row_by_code = {str(r.get("ts_code", "")).strip(): r for _, r in buyable.iterrows()}

    selected_codes: list[str] = []
    industry_counter: dict[str, int] = {}
    high_risk_count = 0

    def _can_add(ts_code: str, strict: bool = True) -> bool:
        nonlocal high_risk_count
        if not ts_code or ts_code in selected_codes:
            return False
        row = row_by_code.get(ts_code)
        if row is None:
            return False
        industry = str(row.get("industry", "")).strip() or "unknown"
        risk = _normalise_risk(row.get("risk_level", "unknown"))
        if strict and industry_counter.get(industry, 0) >= max_per_industry:
            return False
        if risk == "high" and high_risk_count >= max_high_risk_count:
            return False
        return True

    def _add_code(ts_code: str) -> None:
        nonlocal high_risk_count
        row = row_by_code[ts_code]
        industry = str(row.get("industry", "")).strip() or "unknown"
        risk = _normalise_risk(row.get("risk_level", "unknown"))
        selected_codes.append(ts_code)
        industry_counter[industry] = industry_counter.get(industry, 0) + 1
        if risk == "high":
            high_risk_count += 1

    # Phase 1: retain previous picks (soft turnover control) with strict constraints.
    for code in previous_codes_ordered:
        if len(selected_codes) >= min_retain_count:
            break
        if _can_add(code, strict=True):
            _add_code(code)

    # Phase 2: fill remaining using current ranking with strict constraints.
    for _, row in buyable.iterrows():
        if len(selected_codes) >= target_count:
            break
        code = str(row.get("ts_code", "")).strip()
        if _can_add(code, strict=True):
            _add_code(code)

    # Phase 3: relaxation fallback to ensure target_count can still be reached.
    if len(selected_codes) < target_count:
        for _, row in buyable.iterrows():
            if len(selected_codes) >= target_count:
                break
            code = str(row.get("ts_code", "")).strip()
            if _can_add(code, strict=False):
                _add_code(code)

    picked = buyable[buyable["ts_code"].astype(str).isin(selected_codes)].copy()
    picked["__order"] = picked["ts_code"].astype(str).map({c: i for i, c in enumerate(selected_codes)})
    picked = picked.sort_values(["__order"], ascending=[True]).drop(columns=["__order"]).reset_index(drop=True)

    items = []
    for _, row in picked.iterrows():
        items.append(
            {
                "rank": int(row.get("rank", 0)),
                "ts_code": str(row.get("ts_code", "")),
                "stock_name": str(row.get("stock_name", "")),
                "industry": str(row.get("industry", "")),
                "risk_level": str(row.get("risk_level", "unknown")),
                "signal": str(row.get("signal", "")),
                "final_score": float(row.get("final_score", 0.0) or 0.0),
            }
        )

    previous_codes = {str(x.get("ts_code", "")).strip() for x in previous_items if str(x.get("ts_code", "")).strip()}
    current_codes = {str(x.get("ts_code", "")).strip() for x in items if str(x.get("ts_code", "")).strip()}
    overlap_count = len(previous_codes & current_codes)
    overlap_ratio = round(float(overlap_count) / max(len(current_codes), 1), 4) if current_codes else 0.0
    add_codes = sorted(current_codes - previous_codes)
    remove_codes = sorted(previous_codes - current_codes)
    retain_codes = [code for code in previous_codes_ordered if code in current_codes]
    turnover_ratio = round(float(len(add_codes)) / max(len(current_codes), 1), 4) if current_codes else 0.0

    code_to_name = {str(x.get("ts_code", "")): str(x.get("stock_name", "")) for x in items}
    for x in previous_items:
        c = str(x.get("ts_code", ""))
        if c not in code_to_name:
            code_to_name[c] = str(x.get("stock_name", ""))

    actual_max_industry_weight = 0.0
    if items:
        industry_counts: dict[str, int] = {}
        for item in items:
            key = str(item.get("industry", "")).strip() or "unknown"
            industry_counts[key] = industry_counts.get(key, 0) + 1
        actual_max_industry_weight = round(max(industry_counts.values()) / len(items), 4)

    payload = {
        "generated_at": datetime.now().isoformat(),
        "target_count": target_count,
        "buyable_count": int(len(items)),
        "total_candidates": int(len(df)),
        "strong_buy_count": int((df["signal"].astype(str) == "strong_buy").sum()),
        "buy_count": int((df["signal"].astype(str) == "buy").sum()),
        "previous_generated_at": str(previous_payload.get("generated_at", "")) if isinstance(previous_payload, dict) else "",
        "overlap_with_previous_count": int(overlap_count),
        "overlap_with_previous_ratio": float(overlap_ratio),
        "turnover_count": int(len(add_codes)),
        "turnover_ratio": float(turnover_ratio),
        "max_turnover_ratio": float(max(1.0 - min_retain_ratio, 0.0)),
        "rebalance_actions": {
            "retain": [{"ts_code": c, "stock_name": code_to_name.get(c, "")} for c in retain_codes],
            "add": [{"ts_code": c, "stock_name": code_to_name.get(c, "")} for c in add_codes],
            "remove": [{"ts_code": c, "stock_name": code_to_name.get(c, "")} for c in remove_codes],
        },
        "constraints": {
            "max_industry_weight": float(max_industry_weight),
            "max_high_risk_count": int(max_high_risk_count),
            "min_retain_ratio": float(min_retain_ratio),
            "actual_max_industry_weight": float(actual_max_industry_weight),
            "actual_high_risk_count": int(sum(1 for x in items if _normalise_risk(x.get("risk_level")) == "high")),
        },
        "items": items,
    }
    output_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# Buylist Snapshot",
        "",
        f"generated_at: {payload['generated_at']}",
        f"target_count: {payload['target_count']}",
        f"buyable_count: {payload['buyable_count']}",
        f"total_candidates: {payload['total_candidates']}",
        f"strong_buy_count: {payload['strong_buy_count']}",
        f"buy_count: {payload['buy_count']}",
        f"overlap_with_previous_count: {payload['overlap_with_previous_count']}",
        f"overlap_with_previous_ratio: {payload['overlap_with_previous_ratio']:.2%}",
        f"turnover_count: {payload['turnover_count']}",
        f"turnover_ratio: {payload['turnover_ratio']:.2%}",
        f"actual_max_industry_weight: {payload['constraints']['actual_max_industry_weight']:.2%}",
        f"actual_high_risk_count: {payload['constraints']['actual_high_risk_count']}",
        "",
        "| rank | ts_code | stock_name | industry | risk_level | signal | final_score |",
        "|------|---------|------------|----------|------------|--------|-------------|",
    ]
    for item in items:
        lines.append(
            f"| {item['rank']} | {item['ts_code']} | {item.get('stock_name', '')} | "
            f"{item.get('industry', '')} | {item.get('risk_level', 'unknown')} | "
            f"{item['signal']} | {item['final_score']:.2f} |"
        )
    lines.append("")
    output_md.write_text("\n".join(lines), encoding="utf-8")

    print(str(output_json))
    print(str(output_md))


if __name__ == "__main__":
    main()
