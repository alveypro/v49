---
name: market-context
description: Build daily market context for A-share trading from local DB and strategy outputs. Use for market regime summaries, sector heatmaps, risk flags, and concise pre-market or post-market briefs.
---

# Market Context

## Use this skill when
- User asks for market state summary, risk-on/risk-off judgment, or sector rotation snapshot.
- User needs a short daily note to support v4/v5/v6/v7 strategy decisions.

## Inputs
- Trade date (`YYYY-MM-DD`)
- Data source paths (DB/log/CSV)
- Optional lookback window (default 20 trading days)

## Workflow
1. Read market breadth and turnover proxies from configured source tables.
2. Compute a compact state vector:
   - trend_strength
   - breadth_ratio
   - volatility_state
   - liquidity_state
   - leadership_concentration
3. Assign one of: `risk_on`, `neutral`, `risk_off`.
4. Emit top 3 opportunities and top 3 risks with explicit evidence fields.

## Output contract
Return a dict with:
- `trade_date`
- `regime`
- `signals`
- `risks`
- `evidence`
- `confidence` (0-1)

## Guardrails
- Never output directional trading orders.
- If key inputs are missing, return `regime=unknown` with explicit missing fields.
