---
name: report-writer
description: Generate concise daily and weekly reports from strategy outputs, risk status, and market context, including markdown and CSV artifacts.
---

# Report Writer

## Use this skill when
- User asks for daily brief, post-market recap, or strategy review.
- Team needs publish-ready markdown plus machine-readable CSV.

## Sections
1. Executive summary (5 lines max)
2. Market regime and key changes
3. Top opportunities with reasons
4. Risk alerts and action checklist
5. Strategy health panel (v4/v5/v6/v7)

## Output contract
- `markdown`
- `csv_paths`
- `metadata` (`trade_date`, `run_id`, `source_versions`)

## Guardrails
- Cite exact metrics used in conclusions.
- Keep recommendations explicit and testable.
