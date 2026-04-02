# OpenClaw Partner Mode Baseline

## Goal
Run OpenClaw as a stable collaborator for v49 optimization with one clear entrypoint:
- `5101` stock-agent API (`/chat`)
- `8601` dingtalk-bridge (only message forwarding)

## One-Command Reset
Run:

```bash
bash tools/openclaw_reset_runtime.sh
```

This script:
1. Restarts core services (`stock-agent`, `dingtalk-bridge`)
2. Prints launchd status
3. Checks `/health` and `/debug/llm?probe=1`
4. Sends one `/chat` probe and prints returned `mode`
5. Prints whether LLM is truly ready (`openai_ok=true`)

## Ready Criteria
System is ready only when all conditions are true:
1. `http://127.0.0.1:5101/health` returns `ok=true`
2. `http://127.0.0.1:8601/health` returns `ok=true`
3. `http://127.0.0.1:5101/debug/llm?probe=1` returns `probe.openai_ok=true`
4. `/chat` response includes `mode=agent_llm` (not `agent_expert`)

## Common Failure
If `/debug/llm?probe=1` shows:
- `openai_status=401`
- `Incorrect API key provided`

Then OpenClaw will fall back to `agent_expert`.
Fix by updating `.env` `OPENAI_API_KEY`, then rerun reset script.

## Daily Operating Loop (v49)
1. Ask OpenClaw for today's v49 optimization focus.
2. Run one scoped change proposal.
3. Run backtest/risk check.
4. Save report artifact and keep only one accepted change per cycle.

## Daily Dispatch Kit
1. Dispatch template: `docs/V49_PARTNER_DAILY_TASK_TEMPLATE.md`
2. Local execution wrapper: `tools/openclaw_partner_daily_run.sh`
3. Execution artifacts:
   - `logs/openclaw/partner_execution_*.json`
   - `logs/openclaw/partner_execution_*.md`
   - `logs/openclaw/partner_daily_*.log`
