# Phase 5 — Open Follow-ups

Branch: `phase-5-pitching-flags`. Goal: port stacked-CTE FSMs from SQL
to SQLMesh Python `@model`s backed by Polars `forward_fill().shift(1)`
windows.

## What landed (first wave)

- `bc/python_models/event_locality/` — new package. `pitching_flags.py`
  holds the pure-Polars FSM transform (input: 10 columns from
  `event_states_full`; output: the 18-column shape of
  `event_pitching_flags`).
- `bc/models/intermediate/flags/event_pitching_flags.py` — SQLMesh
  Python `@model` (`is_sql=False`, `kind=FULL`, `grain=[event_key]`,
  matching column types and `download_parquet` physical property to the
  prior SQL model). Reads upstream via
  `context.engine_adapter.cursor.sql(...).pl()`, runs the transform,
  returns `to_pandas()` for SQLMesh's DuckDB ingest path.
- `bc/models/intermediate/flags/event_pitching_flags.sql` — deleted.
- `bc/tests/test_event_pitching_flags.py` — six unit tests against the
  pure transform (no warehouse needed): clean 3-IP save, blown save +
  later save credit, hold + downstream save, save situation start
  mid-inning, plus shape and null-flag invariants.
- `scripts/diff_models.py` — `event_pitching_flags` added to
  `PHASE_2_MODELS` / `GRAINS` so the harness recognizes it. Skips with
  "no prod counterpart" in this checkout (no `main_models` schema
  populated locally) — the equivalence check was done directly against
  the prior `sqlmesh__main_models.main_models__event_pitching_flags__*`
  snapshot pair, **0 drift on all 17 non-grain columns over 18.1M rows**.
- `notes/migration-evaluation.md` — Phase 5 row split into 5.1
  shipped / 5.2 queued / 5.3 stays-in-SQL.

## Verification log

```
sqlmesh plan dev --auto-apply
  -> main_models__dev.event_pitching_flags built in 42s, 18,141,020 rows
sqlmesh audit --model main_models.event_pitching_flags
  -> 0 audit(s) (model has no inline audits, same as the SQL it replaces)
pytest bc/tests/test_event_pitching_flags.py
  -> 6 passed
duckdb diff sql_snapshot vs python_snapshot
  -> rows_sql=18141020 rows_py=18141020, 0 missing either way,
     0 IS DISTINCT FROM rows on every non-grain column
```

The `sqlmesh plan dev --auto-apply` cascade also surfaces the
pre-existing DEV_ONLY `relationships` audit failures on
`player_team_season_offense_stats` / `player_game_pitching_stats` (they
reference `main_models.people` / `main_models.game_results` which don't
exist in dev). Independent of this work — see `bc/audits/relationships.sql`
for the bug; it's been called out across prior phase notes.

## Second wave — queued candidates

Recon during this wave widened the candidate list beyond what
`migration-evaluation.md` originally listed:

### `team_game_results`

Two `LAG ... IGNORE NULLS` windows feeding a streak FSM. Same shape as
`event_pitching_flags` save_flags CTE but at game grain rather than
event grain. Likely smallest second-wave port; clean port of the
forward_fill+shift idiom. Confirm partition key is `(team_id, season)`
and ordering is `game_date, game_number`.

### `team_game_start_info`

One `LAG ... IGNORE NULLS` for series-id derivation (carrying the
running series identifier across consecutive games against the same
opponent). Single FSM column; smaller diff. Phase-1 nondeterminism
catalogue (`notes/phase-1-followups.md`) already calls out
doubleheader-induced churn here — the Polars port is not expected to
fix that, but it shouldn't regress it either. Capture allowlist before
diffing.

### Skipped / not Phase 5

- `event_baserunning_stats` — bitfield `bit_and` over `base_state`, no
  `LAG`/`LEAD`. Not stacked-CTE pain.
- `calc_fielding_play_agg` — plain `LAG`, no FSM complexity.

### Orthogonal one-shot — SQL ergonomics

`QUALIFY` and recursive CTE `USING KEY` (DuckDB May 2025) shorten a
handful of existing models without changing the Python-vs-SQL
question. Ship as its own PR.

## Surprises from the first wave

1. **No `is_sql=False` flag actually needed.** The SQLMesh `@model`
   decorator defaults to `is_sql=False` and only flips to SQL mode when
   the entrypoint returns a string. The two existing repo Python
   models (`calc_park_factors_basic.py` and `_advanced.py`) pass
   `is_sql=True` explicitly because they emit SQL. The new
   `event_pitching_flags.py` is the first DataFrame-returning Python
   model in the repo.
2. **Polars is not directly accepted by SQLMesh's DuckDB adapter.**
   Per `sqlmesh/core/engine_adapter/base.py:_native_df_to_pandas_df`,
   only pandas is converted. Polars → pandas conversion happens in the
   model entrypoint via `to_pandas()`. Cheap (Arrow-backed); fine here.
3. **`diff_models.py` skip path.** When prod counterpart doesn't
   exist locally (no `main_models.<table>`), the harness logs
   `SKIP ... (no prod counterpart — new view)` and exits clean. The
   real equivalence check used the SQLMesh snapshot pair from the
   plan-dev run instead. If a fresh prod build is ever run, the
   harness will pick the model up automatically.
4. **`MODEL` block had no `audits`.** The plan suggested preserving
   audits "verbatim", but the original SQL had only `grain (event_key)`
   and a `download_parquet` physical property. Both preserved in the
   Python decorator; no audits to port.
