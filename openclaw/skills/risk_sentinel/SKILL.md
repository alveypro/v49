---
name: risk-sentinel
description: Monitor strategy health and execution risk in near real time. Trigger warnings, degrade automation level, and propose safe fallback actions.
---

# Risk Sentinel

## Use this skill when
- User asks for risk monitoring, drawdown alarms, or strategy health checks.
- System runs in semi-automated mode and needs kill-switch behavior.

## Checks
- Rolling win-rate drift
- Max drawdown breach
- Signal density collapse
- Data freshness and null ratio
- Strategy disagreement spike

## Risk levels
- `green`: normal
- `yellow`: caution
- `orange`: reduce aggressiveness
- `red`: stop automation and require manual review

## Output contract
- `risk_level`
- `triggered_rules`
- `recommended_actions`
- `evidence`

## Guardrails
- `red` must include at least one hard-threshold breach.
- Recommend action, do not execute trade actions directly.
