# Grid Backtest Presets

This guide provides practical command templates for parameter-grid backtesting.

## Profiles

- `short`: fast smoke run on a short window (default 2025-01 to 2025-08)
- `medium`: balanced research run with independent validation window
- `long`: larger search space with independent validation window, better for overnight runs

## Recommended Commands

### 1) Fast Validation (Short Profile)

```bash
python3 run_grid_backtest.py \
  --profile short \
  --stocks 000001.SZ \
  --max-runs 8 \
  --batch-size 2
```

### 2) Balanced Research (Medium Profile)

```bash
python3 run_grid_backtest.py \
  --profile medium \
  --stocks 000001.SZ 600036.SH \
  --max-runs 24 \
  --batch-size 4 \
  --early-stop-patience 6 \
  --replay-top-k 3
```

Default window:

- Search: `2024-01-01` ~ `2024-12-31`
- Validation: `2025-01-01` ~ `2025-06-30`

### 3) Full Search (Long Profile)

```bash
python3 run_grid_backtest.py \
  --profile long \
  --stocks 000001.SZ 600036.SH \
  --max-runs 48 \
  --batch-size 6 \
  --early-stop-patience 10 \
  --min-improve 0.00005 \
  --replay-top-k 5
```

Default window:

- Search: `2023-01-01` ~ `2024-12-31`
- Validation: `2025-01-01` ~ `2025-12-31`

## Outputs

- Ranked latest:
  - `data/experiments/grid_search/grid_backtest_latest.csv`
  - `data/experiments/grid_search/grid_backtest_latest.md`
- Replay validation latest:
  - `data/experiments/grid_search/grid_backtest_replay_latest.csv`
  - `data/experiments/grid_search/grid_backtest_replay_latest.md`

## Notes

- For tiny windows, model warnings are expected due to very small samples.
- Prefer medium/long profiles for robust model ranking.
- `replay` validation must not overlap the search window; overlapping replay is rejected.
