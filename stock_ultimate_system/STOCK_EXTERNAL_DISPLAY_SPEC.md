# /stock External Display Spec

## Goal

`/stock` is an external decision interface, not an internal research console.

The first screen must let a non-internal user understand three things without reading logs, status codes, or model jargon:

1. Current judgment
2. Current boundary
3. Next allowed action

## First Screen Contract

The overview first screen must be organized as:

1. One judgment: current system state in human language.
2. One boundary: what is not allowed yet.
3. One next step: the next dated check or action gate.

For the current waiting state, the external layer must say:

- Current judgment: `受控等待`
- Boundary: observation window has not started and sample-closing actions are locked.
- Next step: re-check on `2026-04-20`, then verify real market data coverage.

Raw lifecycle names such as `pending_window`, `blocked`, `manual_review`, and `running_daily_research` must not appear in the external first screen.

## Information Hierarchy

Public layer:

- Human judgment
- Evidence summary
- Boundary
- Next step

Expert layer:

- Artifact names
- Ledger names
- Raw lifecycle states
- Action-lock details
- Diagnostic sample details

Expert-layer content must sit inside disclosure or lower-priority sections by default.

## Visual Density Rules

The overview first screen must avoid card inflation.

Required behavior:

- Keep the external decision spine visible.
- Collapse run, candidate, prefilter, and governance summary into a single expandable system summary.
- Do not show the old view banner, headline bar, daily brief, hero, or command deck on the overview first screen.
- Do not repeat the same metric in multiple first-screen cards.

## Status Language

External labels must use stable Chinese product language:

- `pending_window` -> `受控等待`
- `blocked` -> `已阻断`
- `manual_review` -> `人工复核`
- `up_to_date` -> `已更新`
- `done` / `completed` -> `已完成`
- `locked` -> `已锁定`

Status color should follow the same semantic map everywhere:

- waiting / controlled: amber
- pass / ready: green
- blocked / locked: red
- neutral evidence: gray
- current focus: teal

## Module Contract

External-facing modules should follow the same display contract:

1. `结论`
2. `证据`
3. `边界`
4. `下一步`

Modules that cannot state all four parts should remain secondary or expert-only until their language is complete.

## Non-Goals

This spec does not authorize:

- Closing primary-result samples before real market data exists.
- Closing candidate-basket samples before real market data exists.
- Adding new strategy modules.
- Adding promotional claims that are not backed by evidence.
- Exposing internal raw status codes as product copy.
