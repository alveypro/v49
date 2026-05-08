from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from src.artifact_registry import sha256_file
from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_CANDIDATE_BASKET_VERSION = "primary_result_candidate_basket.v1"
PRIMARY_RESULT_CANDIDATE_BASKET_POINTER_VERSION = "primary_result_candidate_basket_pointer.v1"
REQUIRED_COLUMNS = ("ts_code",)
DEFAULT_MAX_SINGLE_WEIGHT = 0.35
DEFAULT_MAX_HIGH_RISK_WEIGHT = 0.25
TARGET_MAX_INDUSTRY_WEIGHT = 0.50
CONDITIONAL_MAX_INDUSTRY_WEIGHT = 0.65


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_float(value: object, default: float = 0.0) -> float:
    text = _normalize_text(value)
    if not text:
        return default
    try:
        return float(text)
    except ValueError:
        return default


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _read_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _check(name: str, passed: bool, detail: str, details: dict[str, object] | None = None) -> dict[str, object]:
    return {"name": name, "passed": passed, "detail": detail, "details": details or {}}


def _load_csv(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        rows = [{key: _normalize_text(value) for key, value in row.items()} for row in reader]
    return rows, fieldnames


def _row_weight(row: dict[str, str]) -> float:
    for key in ("portfolio_weight_after_risk", "basket_weight_pct", "position_pct"):
        value = _normalize_float(row.get(key), default=-1.0)
        if value > 0:
            return value
    return 0.0


def _normalize_weights(rows: list[dict[str, str]]) -> list[float]:
    raw_weights = [_row_weight(row) for row in rows]
    total = sum(raw_weights)
    if total <= 0:
        return [round(1.0 / len(rows), 6) for _ in rows] if rows else []
    weights = [round(weight / total, 6) for weight in raw_weights]
    if weights:
        weights[-1] = round(1.0 - sum(weights[:-1]), 6)
    return weights


def _basket_items(rows: list[dict[str, str]], weights: list[float]) -> list[dict[str, object]]:
    items = []
    for idx, row in enumerate(rows):
        items.append(
            {
                "rank": int(_normalize_float(row.get("rank"), idx + 1)),
                "ts_code": _normalize_text(row.get("ts_code")),
                "stock_name": _normalize_text(row.get("stock_name")) or None,
                "industry": _normalize_text(row.get("industry")) or None,
                "signal": _normalize_text(row.get("signal")) or None,
                "risk_level": _normalize_text(row.get("risk_level")).lower() or "unknown",
                "final_score": _normalize_float(row.get("final_score")),
                "weight": weights[idx],
                "basket_role": _normalize_text(row.get("basket_role")) or None,
                "basket_risk_flag": _normalize_text(row.get("basket_risk_flag")) or "ok",
                "stop_loss": _normalize_float(row.get("stop_loss")) or None,
                "take_profit": _normalize_float(row.get("take_profit")) or None,
            }
        )
    return items


def _identity_quality(items: list[dict[str, object]]) -> dict[str, object]:
    missing_name = [str(item.get("ts_code") or "") for item in items if not _normalize_text(item.get("stock_name"))]
    missing_industry = [str(item.get("ts_code") or "") for item in items if not _normalize_text(item.get("industry"))]
    return {
        "missing_name_count": len(missing_name),
        "missing_industry_count": len(missing_industry),
        "missing_name_codes": missing_name[:20],
        "missing_industry_codes": missing_industry[:20],
    }


def _risk_budget(items: list[dict[str, object]]) -> dict[str, object]:
    industry_weights: dict[str, float] = {}
    high_risk_weight = 0.0
    for item in items:
        weight = float(item["weight"])
        industry = str(item.get("industry") or "unknown")
        industry_weights[industry] = round(industry_weights.get(industry, 0.0) + weight, 6)
        if str(item.get("risk_level") or "").lower() in {"high", "critical"}:
            high_risk_weight += weight
    top_industry = None
    top_industry_weight = 0.0
    if industry_weights:
        top_industry, top_industry_weight = max(industry_weights.items(), key=lambda pair: pair[1])
    concentration_hhi = round(sum(float(item["weight"]) ** 2 for item in items), 6)
    return {
        "item_total": len(items),
        "weight_sum": round(sum(float(item["weight"]) for item in items), 6),
        "max_single_weight": max((float(item["weight"]) for item in items), default=0.0),
        "high_risk_weight": round(high_risk_weight, 6),
        "industry_weights": industry_weights,
        "top_industry": top_industry,
        "top_industry_weight": round(top_industry_weight, 6),
        "concentration_hhi": concentration_hhi,
    }


def _validation_quality(validation_payload: dict[str, object]) -> dict[str, object]:
    summary = validation_payload.get("summary") if isinstance(validation_payload, dict) else {}
    if not isinstance(summary, dict):
        summary = {}
    try:
        rebalance_dates = int(float(summary.get("rebalance_dates") or 0))
    except (TypeError, ValueError):
        rebalance_dates = 0
    return {
        "rebalance_dates": rebalance_dates,
        "avg_basket_return_5d": _normalize_float(summary.get("avg_basket_return_5d")),
        "avg_excess_return_5d": _normalize_float(summary.get("avg_excess_return_5d")),
        "basket_win_rate_5d": _normalize_float(summary.get("basket_win_rate_5d")),
    }


def _default_basket_id(run_id: str, generated_at: str) -> str:
    safe_run_id = "".join(ch if ch.isalnum() or ch in "-_" else "-" for ch in run_id.strip()) or "candidate-basket"
    stamp = generated_at.replace(":", "").replace("-", "").replace("+00:00", "Z")
    return f"{safe_run_id}-{stamp}"


def build_primary_result_candidate_basket_snapshot(
    *,
    candidates_csv_path: str | Path = "data/experiments/candidates_top_latest.csv",
    summary_json_path: str | Path | None = "data/experiments/candidates_top_summary_latest.json",
    validation_json_path: str | Path | None = None,
    basket_id: str | None = None,
    run_id: str = "candidate-basket",
    top_n: int = 20,
    max_single_weight: float = DEFAULT_MAX_SINGLE_WEIGHT,
    max_high_risk_weight: float = DEFAULT_MAX_HIGH_RISK_WEIGHT,
    target_max_industry_weight: float = TARGET_MAX_INDUSTRY_WEIGHT,
    max_industry_weight: float = CONDITIONAL_MAX_INDUSTRY_WEIGHT,
    min_items: int = 1,
    formal_release: bool = False,
    min_validation_rebalance_dates: int = 0,
    output_path: str | Path | None = None,
) -> tuple[int, dict[str, object]]:
    generated_at = _utc_now_iso()
    resolved_candidates_csv = resolve_project_path(candidates_csv_path)
    resolved_summary_json = resolve_project_path(summary_json_path) if summary_json_path else None
    resolved_validation_json = resolve_project_path(validation_json_path) if validation_json_path else None
    checks = [
        _check("candidates_csv_exists", resolved_candidates_csv.exists(), "candidate CSV must exist"),
        _check("top_n_valid", int(top_n) > 0, "top_n must be positive"),
        _check("min_items_valid", int(min_items) > 0, "min_items must be positive"),
    ]
    rows: list[dict[str, str]] = []
    fieldnames: list[str] = []
    source_hash = sha256_file(resolved_candidates_csv) if resolved_candidates_csv.exists() else None
    summary_hash = sha256_file(resolved_summary_json) if resolved_summary_json and resolved_summary_json.exists() else None
    validation_hash = (
        sha256_file(resolved_validation_json) if resolved_validation_json and resolved_validation_json.exists() else None
    )
    source_summary = _read_json(resolved_summary_json) if resolved_summary_json and resolved_summary_json.exists() else {}
    source_validation = _read_json(resolved_validation_json) if resolved_validation_json and resolved_validation_json.exists() else {}
    error: str | None = None

    if resolved_candidates_csv.exists():
        try:
            raw_rows, fieldnames = _load_csv(resolved_candidates_csv)
            missing_columns = [column for column in REQUIRED_COLUMNS if column not in fieldnames]
            checks.append(
                _check(
                    "required_columns_present",
                    not missing_columns,
                    "candidate CSV must include required columns",
                    {"required_columns": list(REQUIRED_COLUMNS), "missing_columns": missing_columns},
                )
            )
            rows = [row for row in raw_rows if _normalize_text(row.get("ts_code"))][: int(top_n)]
        except Exception as exc:
            error = str(exc)
            checks.append(_check("candidates_csv_readable", False, "candidate CSV must be readable", {"error": error}))

    weights = _normalize_weights(rows)
    items = _basket_items(rows, weights)
    identity = _identity_quality(items)
    budget = _risk_budget(items)
    validation = _validation_quality(source_validation)
    generation_degraded = bool(source_summary.get("generation_degraded"))
    guardrail_mode = _normalize_text(source_summary.get("guardrail_mode")).lower()
    checks.extend(
        [
            _check(
                "min_items_present",
                len(items) >= int(min_items),
                "candidate basket must include enough items",
                {"required": int(min_items), "actual": len(items)},
            ),
            _check("weights_sum_to_one", abs(float(budget["weight_sum"]) - 1.0) <= 0.00001, "basket weights must sum to 1.0", budget),
            _check(
                "single_name_weight_within_limit",
                float(budget["max_single_weight"]) <= float(max_single_weight),
                "single-name weight must stay within policy",
                {"actual": budget["max_single_weight"], "limit": max_single_weight},
            ),
            _check(
                "high_risk_weight_within_limit",
                float(budget["high_risk_weight"]) <= float(max_high_risk_weight),
                "high-risk weight must stay within policy",
                {"actual": budget["high_risk_weight"], "limit": max_high_risk_weight},
            ),
            _check(
                "candidate_identity_complete",
                int(identity["missing_name_count"]) == 0 and int(identity["missing_industry_count"]) == 0,
                "candidate basket items must include stock name and industry",
                identity,
            ),
            _check(
                "industry_weight_within_target",
                float(budget["top_industry_weight"]) <= float(target_max_industry_weight),
                "top industry weight exceeds target operating band",
                {
                    "actual": budget["top_industry_weight"],
                    "target_limit": target_max_industry_weight,
                    "industry": budget["top_industry"],
                },
            ),
            _check(
                "industry_weight_within_hard_limit",
                float(budget["top_industry_weight"]) <= float(max_industry_weight),
                "top industry weight must stay within hard policy limit",
                {
                    "actual": budget["top_industry_weight"],
                    "hard_limit": max_industry_weight,
                    "industry": budget["top_industry"],
                },
            ),
        ]
    )
    formal_gate_enabled = bool(formal_release or resolved_validation_json or int(min_validation_rebalance_dates) > 0)
    if formal_gate_enabled:
        checks.extend(
            [
                _check(
                    "candidate_generation_complete",
                    not generation_degraded,
                    "candidate generation must complete without degradation",
                    {
                        "generation_degraded": generation_degraded,
                        "generation_reason": _normalize_text(source_summary.get("generation_reason")) or None,
                    },
                ),
                _check(
                    "guardrail_mode_not_defensive",
                    guardrail_mode not in {"defensive", "blocked"},
                    "candidate basket guardrail mode must not be defensive for formal release",
                    {
                        "guardrail_mode": guardrail_mode or None,
                        "guardrail_reasons": source_summary.get("guardrail_reasons") or [],
                    },
                ),
            ]
        )
    if formal_gate_enabled:
        checks.extend(
            [
                _check(
                    "validation_json_exists",
                    bool(resolved_validation_json and resolved_validation_json.exists()),
                    "candidate basket validation artifact must exist for formal release",
                    {"path": str(resolved_validation_json) if resolved_validation_json else None},
                ),
                _check(
                    "validation_rebalance_dates_sufficient",
                    int(validation["rebalance_dates"]) >= int(min_validation_rebalance_dates),
                    "candidate basket validation sample count must meet formal release threshold",
                    {
                        "actual": validation["rebalance_dates"],
                        "minimum": int(min_validation_rebalance_dates),
                    },
                ),
            ]
        )
    conditional_checks = [check for check in checks if check["passed"] is not True and check["name"] == "industry_weight_within_target"]
    blocking_checks = [check for check in checks if check["passed"] is not True and check["name"] != "industry_weight_within_target"]
    status = "approved"
    if blocking_checks:
        status = "blocked"
    elif conditional_checks:
        status = "conditional"
    resolved_basket_id = basket_id or _default_basket_id(run_id, generated_at)
    payload = {
        "basket_version": PRIMARY_RESULT_CANDIDATE_BASKET_VERSION,
        "basket_id": resolved_basket_id,
        "run_id": run_id,
        "generated_at": generated_at,
        "status": status,
        "source_candidates_csv_path": str(resolved_candidates_csv),
        "source_candidates_csv_hash": source_hash,
        "source_summary_json_path": str(resolved_summary_json) if resolved_summary_json else None,
        "source_summary_json_hash": summary_hash,
        "source_summary": source_summary,
        "source_validation_json_path": str(resolved_validation_json) if resolved_validation_json else None,
        "source_validation_json_hash": validation_hash,
        "validation_quality": validation,
        "top_n": int(top_n),
        "policy": {
            "min_items": int(min_items),
            "max_single_weight": float(max_single_weight),
            "max_high_risk_weight": float(max_high_risk_weight),
            "target_max_industry_weight": float(target_max_industry_weight),
            "max_industry_weight": float(max_industry_weight),
            "formal_release": bool(formal_release),
            "min_validation_rebalance_dates": int(min_validation_rebalance_dates),
        },
        "items": items,
        "risk_budget": budget,
        "identity_quality": identity,
        "checks": checks,
        "blocking_reasons": [str(check["detail"]) for check in blocking_checks],
        "conditional_reasons": [str(check["detail"]) for check in conditional_checks],
        "governance_notes": (
            [
                (
                    f"industry concentration {float(budget['top_industry_weight']):.2%} exceeds target band "
                    f"{float(target_max_industry_weight):.2%} but remains within conditional hard limit "
                    f"{float(max_industry_weight):.2%}"
                )
            ]
            if status == "conditional"
            else []
        ),
        "error": error,
        "production_boundary": (
            "candidate basket is a governed candidate portfolio artifact; it does not replace the /stock primary result, "
            "trade, close observations, record terminal outcomes, or promote baselines"
        ),
    }
    if output_path:
        _write_json(resolve_project_path(output_path), payload)
    return (0 if payload["status"] in {"approved", "conditional"} else 1), payload


class PrimaryResultCandidateBasketRegistry:
    def __init__(self, *, baskets_dir: str | Path = "artifacts/primary_result_candidate_baskets") -> None:
        self.baskets_dir = resolve_project_path(baskets_dir)
        self.history_dir = self.baskets_dir / "history"
        self.current_path = self.baskets_dir / "current.json"

    def register_snapshot(self, snapshot: dict[str, object]) -> dict[str, object]:
        if snapshot.get("basket_version") != PRIMARY_RESULT_CANDIDATE_BASKET_VERSION:
            raise ValueError("candidate basket snapshot version is invalid")
        basket_id = _normalize_text(snapshot.get("basket_id"))
        if not basket_id:
            raise ValueError("candidate basket snapshot missing basket_id")
        snapshot_path = self.history_dir / f"{basket_id}.json"
        if snapshot_path.exists():
            raise FileExistsError(f"candidate basket snapshot already exists: {snapshot_path}")
        _write_json(snapshot_path, snapshot)
        pointer = {
            "pointer_version": PRIMARY_RESULT_CANDIDATE_BASKET_POINTER_VERSION,
            "updated_at": _utc_now_iso(),
            "basket_id": basket_id,
            "snapshot_path": str(snapshot_path),
            "snapshot_hash": sha256_file(snapshot_path),
            "status": snapshot.get("status"),
        }
        _write_json(self.current_path, pointer)
        return pointer

    def current(self) -> dict[str, object]:
        if not self.current_path.exists():
            return {
                "pointer_version": PRIMARY_RESULT_CANDIDATE_BASKET_POINTER_VERSION,
                "basket_id": None,
                "snapshot_path": None,
                "snapshot_hash": None,
                "status": None,
            }
        return _read_json(self.current_path)

    def load_snapshot(self, basket_id: str) -> dict[str, object]:
        snapshot_path = self.history_dir / f"{_normalize_text(basket_id)}.json"
        if not snapshot_path.exists():
            raise FileNotFoundError(f"candidate basket snapshot not found: {snapshot_path}")
        return _read_json(snapshot_path)

    def rollback_current(self, basket_id: str) -> dict[str, object]:
        snapshot = self.load_snapshot(basket_id)
        snapshot_path = self.history_dir / f"{_normalize_text(basket_id)}.json"
        pointer = {
            "pointer_version": PRIMARY_RESULT_CANDIDATE_BASKET_POINTER_VERSION,
            "updated_at": _utc_now_iso(),
            "basket_id": snapshot["basket_id"],
            "snapshot_path": str(snapshot_path),
            "snapshot_hash": sha256_file(snapshot_path),
            "status": snapshot.get("status"),
            "rollback": True,
        }
        _write_json(self.current_path, pointer)
        return pointer
