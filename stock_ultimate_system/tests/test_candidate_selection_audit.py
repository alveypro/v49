import pandas as pd

from run_top_candidates import rank_candidates
from src.candidate_selection.audit import build_candidate_audit_rows


def _sample_results():
    results = []
    stock_basic_map = {}
    payloads = [
        ("AAA", "Oil", 86, 0.76, 0.050, 0.78, 0.045),
        ("BBB", "Oil", 85, 0.75, 0.049, 0.77, 0.044),
        ("CCC", "Tech", 82, 0.71, 0.041, 0.73, 0.034),
        ("DDD", "Tech", 80, 0.69, 0.038, 0.71, 0.030),
    ]
    for code, industry, score, prob, pred_ret, cal_prob, cal_ret in payloads:
        results.append({
            "ts_code": code,
            "signal_result": {"signal": "strong_buy", "score": score, "reason": "test"},
            "forecast_result": {
                "direction_prob_up": prob,
                "pred_return": pred_ret,
                "confidence": 0.75,
                "calibrated_upside_win_rate": cal_prob,
                "calibrated_avg_return": cal_ret,
                "calibration_sample_size": 120,
                "model_agreement": 0.8,
                "prediction_dispersion": 0.03,
            },
            "risk_info": {"risk_level": "low", "stop_loss": 9.0, "take_profit": 11.0},
            "position_result": {"position_pct": 0.2},
        })
        stock_basic_map[code] = {"stock_name": code, "industry": industry, "market": "Main", "area": "SZ"}
    return results, stock_basic_map


def test_candidate_audit_rows_include_scoring_and_reason_fields():
    results, stock_basic_map = _sample_results()
    ranked = rank_candidates(results, top_n=3, stock_basic_map=stock_basic_map)

    audit_rows = build_candidate_audit_rows(ranked)

    assert len(audit_rows) == 3
    first = audit_rows[0]
    assert first["ts_code"] == "AAA"
    assert first["selected"] is True
    assert first["final_score"] >= first["selection_score"]
    assert "selected_for_basket" in first["selection_reason"]
    assert {"cross_section_score", "diversification_penalty", "risk_overlay_penalty", "basket_risk_flag"} <= set(first)


def test_candidate_audit_rows_return_empty_list_for_empty_frame():
    assert build_candidate_audit_rows(pd.DataFrame()) == []
