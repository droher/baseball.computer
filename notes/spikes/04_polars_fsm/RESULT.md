# Spike 4 — Polars `forward_fill().over()` rewrite of `event_pitching_flags`

**Verdict:** GO with **pure Polars**. Numba fallback (D4 in the evaluation doc) is **not needed**.

The save / hold / blown-save / blown-long-save FSM in `bc/models/intermediate/flags/event_pitching_flags.sql` (167L) decomposes cleanly to columnar Polars expressions. Row-equivalent output across all 13 derived columns on a 225K-row season slice. No `map_groups` required; no Numba `@njit` required.

## Mapping

The dbt model's load-bearing primitive is `LAG(X IGNORE NULLS) OVER (PARTITION BY game_id, batting_side, pitcher_id ORDER BY event_id)`. Polars equivalent:

```python
pl.col(X)
  .forward_fill().over(group, order_by=event_id)   # propagate last-seen non-null up to current row
  .shift(1).over(group, order_by=event_id)          # then lag by one — "non-null at strictly prior row"
```

`LEAD(pitcher_id) IS NULL` (`pitcher_finish_flag`):

```python
pl.col("pitcher_id").shift(-1).over(["game_id", "batting_side"], order_by="event_id").is_null()
```

Plain `LAG(X)` (no IGNORE NULLS, used for `lag_conditional_blown_save_flag`): just `shift(1).over(...)`.

The full FSM rewrite is in `spike.py`. Two-stage:

1. `init_flags` block — per-row flags computed under the `(game_id, batting_side)` window, including `previous_pitcher_id`, `next_pitcher_id`, `save_situation_start_flag` (NULL except on `new_pitcher_flag` rows), etc.
2. `save_flags` block — re-sort by `(game_id, batting_side, pitcher_id, event_id)` and apply the four `LAG(... IGNORE NULLS)` lookups via the `forward_fill().shift(1)` chain.

`pl.when(...).then(...).otherwise(None)` faithfully reproduces SQL's `CASE WHEN cond THEN val` (the no-`ELSE` form returning NULL).

## Diff result (2019 season, 225,535 rows)

```
INFO polars version: 1.40.1
INFO loaded event_states_full season=2019 rows=225535
INFO expected rows=225535, actual rows=225535
INFO col hold_flag: ok
INFO col save_flag: ok
INFO col blown_save_flag: ok
INFO col blown_long_save_flag: ok
INFO col pitcher_exit_flag: ok
INFO col pitcher_finish_flag: ok
INFO col new_relief_pitcher_flag: ok
INFO col save_situation_start_flag: ok
INFO col starting_pitcher_flag: ok
INFO col starting_pitcher_exit_flag: ok
INFO col starting_pitcher_early_exit_flag: ok
INFO col inherited_runners: ok
INFO col bequeathed_runners: ok
```

13/13 columns row-equivalent. Including all four FSM-output flags and the LEAD-derived `pitcher_finish_flag` + `bequeathed_runners` (which uses `shift(-1)` for the LEAD-style lookahead).

## Build-time

Quick timing on the same 2019 slice:

| Step | Time |
|---|---|
| `duckdb.sql(...).arrow()` (load 225K rows) | 0.08s |
| Polars FSM compute | 0.09s |
| (Reference) DuckDB single `LAG OVER` window scan | 0.01s |

Polars is ~10× slower than a single SQL window scan on the slice, but the dbt model runs the full FSM (≥4 windowed scans plus all init_flags case-when work). Per-season cost in Polars stays well under 1s. Production-scale extrapolation: ~20s for the whole 26M-row event_states_full sweep — comparable to or faster than the dbt SQL build (which materializes intermediate tables).

## Verdict

**GO** with pure Polars for axis-D Phase 5. The codebase's most LAG-heavy FSM works without a `map_groups`/Numba escape hatch. Phase 5 starts with `event_pitching_flags`; this spike already has a working port that can be lifted into a SQLMesh Python model directly.

Numba's role per the evaluation doc was a fallback for "multi-flag interdependence forces map_groups." This spike proves that interdependence (4 `LAG IGNORE NULLS` lookups feeding 4 different output flags, plus a `LAG(plain)` not-blown-save-yet check) decomposes into columnar `forward_fill().shift().over()` chains. So Numba stays available for genuinely row-iterative state machines, but it's not load-bearing for the planned axis-D port.
