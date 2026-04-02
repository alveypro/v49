---
name: signal-arbiter
description: Merge and arbitrate outputs from multiple strategy versions (v4/v5/v6/v7) into one ranked list with conflict explanations and confidence scoring.
---

# Signal Arbiter

## Use this skill when
- User asks for one unified list from multiple strategy outputs.
- Signals disagree and need interpretable conflict resolution.

## Inputs
- Strategy output list with fields: `ts_code`, `score`, `strategy`, optional features.
- Weight config and confidence thresholds.

## Arbitration rules
1. Normalize scores to `[0, 100]` per strategy.
2. Apply strategy weights from config.
3. Penalize unstable names (high disagreement or high volatility).
4. Produce final rank with reason codes:
   - `consensus_strong`
   - `high_score_low_consensus`
   - `theme_supported`
   - `risk_penalty_applied`

## Output contract
- `ranked_list`
- `conflicts`
- `reason_codes`
- `confidence`

## Guardrails
- Keep the raw strategy scores in output for auditability.
