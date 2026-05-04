# Performance deep dive — build time, memory, disk

Branch: `perf-deep-dive`. Tooling lives in `scripts/perf_run.py` (build
harness) and `scripts/disk_analysis.py` (post-build storage audit);
both env-gated by `BC_PERF_MODE=1`, normal builds untouched.

## Method

- Wiped `bc.db` + `bc/bc_state.db`, ran the harness with
  `concurrent_tasks=1` (also pinned the DuckDB connection pool to
  size 1 so per-snapshot pragma SETs stick to the same connection),
  full backfill on the `dev` environment.
- `SnapshotEvaluator.evaluate` is monkey-patched to capture, per
  snapshot: wall time, peak own-process RSS (0.5 s sampler), `bc.db`
  size delta, `bc.db.tmp/` spill size delta. Each evaluation also
  redirects DuckDB `profile_output` to
  `logs/perf/profiles/<model>.json` so the model's final query plan
  survives (DuckDB overwrites the profile file every query, and
  audits + virtual-layer DDL are always cheap, so the captured plan
  is the build CTAS).
- `SnapshotEvaluator.audit` is no-op'd for the perf run: built-in
  `relationships(... to_model := main_models.x)` audits render
  cross-model refs against the virtual `main_models` schema, which
  is only created in `VirtualLayerSchemaCreationStage` after
  backfill. On a cold state DB the virtual schema does not exist
  during backfill, so audits crash with `Catalog Error: schema
  "main_models" does not exist`. Build cost is what we want; audits
  can run separately once virtuals exist.
- A final `CHECKPOINT bc` runs at end of plan so `bc.db.wal` flushes
  into the data file. Without it subsequent reopens crash on WAL
  replay because the WAL's trailing `ALTER COLUMN … SET DATA TYPE
  <enum>` entries (from `alter_types`) reference custom types that
  cannot be resolved without the user catalog already loaded.

Per-snapshot timings: `logs/perf/perf_<UTCstamp>.jsonl`.
Per-table / per-column disk: `logs/disk/disk_{tables,columns}.csv`.

## The headline number was misleading

`bc.db` was 90 GB before this work. The fresh build is **13 GB**
(11.6 GB compressed data, 1.4 GB free blocks). The 77 GB difference
was retained-snapshot bloat, not real model data — every
`sqlmesh plan dev` creates new hash-suffixed physical tables and the
old ones linger under `sqlmesh__main_models` until SQLMesh GCs them.
Recent commit `799e736` cut the **publish-side** retention to
keep-last-5 but did nothing about the build-side equivalent.

So the disk-saving conversation is fighting over **13 GB**, not 90:

| schema             | tables | bytes (MB) |
| ------------------ | -----: | ---------: |
| sqlmesh__main_models | 106 | ~11200 |
| event              | 7   | ~2700 (post-init_db source load) |
| box_score          | 17  | ~340 |
| game               | 4   | ~210 |
| misc/baseballdatabank/biodata | 17 | ~10 |

The ~3 GB of source schema bytes survive every build (`init_db`
materializes the R2 parquet into bc.db). They are not query-time
overhead — staging models read from them — but they show up in
`bc.db` size.

## Build time + memory

Total per-model wall time (`concurrent_tasks=1`, run 3 of 3, all
runs within ±5 s of each other): **178 s** of CTAS evaluation across
162 models. `before_all` (`init_db` + `create_enums` + `alter_types` +
`load_seeds`) adds **~57 s**, dominated by the 219 `ALTER COLUMN …
SET DATA TYPE <enum>` rewrites of the source columns. Plus virtual
layer view creation + state writes ~30 s. End-to-end cold build:
**under 5 minutes** at concurrency 1, **2-3 minutes** projected at the
default `concurrent_tasks=2`.

No model spilled to disk (`bc.db.tmp/` stayed at 0 throughout). Peak
own-process RSS climbs to ~30 GB late in the build and stays there;
the 48 GB cap in `bc/config.py` is comfortable.

### Top by wall time

| model | sec | peak RSS GB | db delta MB |
| ----- | --: | --: | --: |
| event_offense_stats          | 17.4 | 25.9 | 433 |
| event_player_fielding_stats  | 15.4 | 29.8 | 2018 |
| event_pitching_stats         | 14.4 | 29.8 | 339 |
| event_states_full            | 7.5  | 23.3 | 932 |
| player_game_offense_stats    | 7.2  | 30.8 | 233 |
| event_baserunning_stats      | 5.5  | 16.7 | 321 |
| event_base_out_states        | 5.4  | 16.6 | 386 |
| player_game_pitching_stats   | 4.2  | 25.2 | 112 |
| calc_park_factors_advanced   | 3.6  | 29.2 | 0 (view) |
| event_fielders_flat          | 3.4  | 20.2 | 499 |

Note on RSS: it's the absolute high-water-mark during each snapshot's
eval, including DuckDB heap retained from earlier snapshots (DuckDB
does not give memory back between queries). It tells you "did this
model need a lot of resident memory at some point" but not "this
model alone allocated this much." For per-model attribution we'd
need DuckDB's profile JSON metric `TOTAL_MEMORY_ALLOCATED` — the
profile capture is in place, parsing it is a followup if needed.

## Disk — the real picture

Sum of per-model `db_growth_mb` from the harness = **9.7 GB**, ~85% of
final compressed model data. Top tables by on-disk bytes (DuckDB
unique-block × 256 KB; matches `pragma database_size` `used_blocks`
to within a few %):

| model | rows (M) | bytes (MB) |
| ----- | -------: | ---------: |
| event_player_fielding_stats | 152.0 | 2046 |
| event_states_full           | 18.1  | 992 |
| event_offense_stats         | 26.3  | 646 |
| event_pitching_stats        | 16.5  | 554 |
| event_fielders_flat         | 16.3  | 507 |
| event_transition_values     | 17.9  | 488 |
| player_game_offense_stats   | 4.7   | 467 |
| event_states_batter_pitcher | 18.1  | 410 |
| event_base_out_states       | 18.1  | 409 |
| stg_events                  | 18.1  | 395 |

### What's actually on disk: keys, repeated everywhere

Aggregating bytes by **column name** across all 106 model tables:

| column | total MB | tables | what it is |
| ------ | -------: | -----: | ---------- |
| event_key                | 1685 | 27 | UINTEGER 4-byte event surrogate |
| game_id                  | 1620 | 60 | VARCHAR (12-char Retrosheet ID) |
| player_id                |  632 | 25 | VARCHAR (8-char Retrosheet ID) |
| team_id                  |  388 | 30 | ENUM with ~310 levels |
| pitcher_id               |  235 | 12 | VARCHAR |
| batter_id                |  190 | 7  | VARCHAR |
| event_id                 |  177 | 9  | UINTEGER (within-game) |
| personnel_fielding_key   |  155 | 4  | INTEGER surrogate |
| batting_team_id          |  139 | 6  | ENUM |
| fielding_team_id         |  133 | 6  | ENUM |
| win_expectancy_end_key   |  115 | 2  | VARCHAR composite |
| win_expectancy_start_key |  114 | 3  | VARCHAR composite |
| fielding_position        |  112 | 10 | UTINYINT |
| personnel_lineup_key     |   91 | 2  | BIGINT |

**~5 GB out of ~11 GB of model data is the same handful of key
columns repeated across tables.** Two failure modes here:

1. **High-fanout tables blow up the grain side**:
   `event_player_fielding_stats` is 152 M rows because each event
   gets ~9 rows (one per fielder). Of the 2046 MB that table costs:
   - event_key: 619 MB (already 4-byte int, can't shrink without
     dropping rows)
   - game_id (12-char varchar): 447 MB
   - player_id (8-char varchar): 311 MB
   - team_id (ENUM): 297 MB
   - all 23 numeric stat columns combined: ~370 MB
   The keys and IDs are 75% of this table. A `LIST<STRUCT<position,
   player_id, …>>` reshape that goes from 152 M rows back down to
   ~16 M (one row per event) deduplicates `event_key`/`game_id`
   /`team_id` 9-fold. Estimated savings: **1.4-1.6 GB on a 2 GB
   table** — biggest single disk win in the repo.

2. **Composite VARCHARs where INTs would do**:
   `event_states_full` has `win_expectancy_start_key`,
   `win_expectancy_end_key`, `run_expectancy_start_key`,
   `run_expectancy_end_key` all as VARCHAR composites
   (~58 MB each, ~230 MB total in this one table, plus ~115 MB more
   in `event_transition_values`). These are lookup keys into tiny
   matrices. Encoding them as `UBIGINT` packed bits (bases × outs ×
   inning × … fits in 64 bits) drops storage to 8 bytes/row pre-
   compression and lets DuckDB use bitpacking. Estimated savings:
   **~250 MB across event_states_full + event_transition_values**.

## Joins + fanout (event-level intermediates)

Sources verified by reading the SQL; the run-3 profile JSONs in
`logs/perf/profiles/` confirm wall-time ranking but column-level
operator detail is what `profiling_mode=detailed` should give us
once parsed.

- **`event_player_fielding_stats`** (`bc/models/intermediate/event_level/event_player_fielding_stats.sql:142`)
  inner-joins 16 M-row event_fielding_stats × 1.5 M-row
  personnel_fielding_states on `personnel_fielding_key` to produce
  152 M rows. Lookup table is on the right (probe side); should be
  on the left (build side). With `preserve_insertion_order=False`
  in `bc/config.py:29` DuckDB will not pick build side from
  insertion order, so the SQL has to express it. Rewrite as
  `personnel_fielding_states pfs INNER JOIN event_fielding_stats efs
  USING (personnel_fielding_key)` and re-measure.

- **`event_states_batter_pitcher`** (`bc/models/intermediate/states/event_states_batter_pitcher.sql:79`)
  has a hidden non-equi BETWEEN join against
  `stg_game_fielding_appearances`:
  `events.event_id BETWEEN batter_field.start_event_id AND
  batter_field.end_event_id`. DuckDB's planner does not push range
  predicates into a zonemap-backed scan, so this becomes a hash join
  followed by a per-row range filter. Shape is exactly an `ASOF
  JOIN`: event valid within a fielder appearance interval, ordered by
  `(game_id, event_id)`. Worth a spike — `ASOF` with that key order
  should be a clear win, especially since the model takes 3.2 s and
  the upstream ranges are pre-sorted.

- **`event_baserunning_stats`** (16 M rows, 392 MB) is consumed by
  `event_offense_stats` (FULL OUTER + UNION ALL of remaining
  baserunners), `event_pitching_stats` (GROUP BY aggregate for
  pitcher attribution), and `event_run_assignment_stats` (INNER
  JOIN on responsible pitcher). The same 60-90 M-row fanout is
  produced and discarded twice. Pre-aggregating per-event
  baserunning summary (counts of runs scored, outs on basepaths,
  steal events, etc.) into a 1:1 event-grain table would let two of
  those three consumers probe a 16 M-row table instead of joining
  60 M rows. Both `event_offense_stats` and `event_pitching_stats`
  are top-3 by wall time, so this is a meaningful build-time win.

- **`event_transition_values`** (`bc/models/intermediate/expectancy/event_transition_values.sql:109`)
  joins the run-expectancy and win-expectancy matrices four times
  per event (start/end keys for runs and wins). The matrices are
  tiny (~100 / ~5-10 k rows). The cost is not the matrix scan; it's
  that the four lookup keys are recomputed in the model body
  (string concat) instead of being projected once in
  `event_states_full`. Consolidating those 4 keys upstream and
  joining once per matrix would let the planner reuse the
  expectancy hash tables.

## Python / Ibis chunking

- SQLMesh exposes `Iterator[DataFrame]` yield support for FULL
  Python `@model`s
  (`.venv/.../sqlmesh/core/snapshot/evaluator.py:1040`). Incremental
  kinds `pd.concat` the yields back together
  (`evaluator.py:1007-1038`), so chunking only helps FULL models.
  No PyArrow RecordBatch streaming surface, no partition-aware
  write API.
- Every Python `@model` in this repo (`bc/models/metrics/*.py`,
  `bc/models/analyses/calc_park_factors.py`) compiles to one
  Ibis-or-raw SQL CTAS. DuckDB does the work; Python memory is not
  the constraint. This means **chunking the Python side does
  nothing here** — the in-Python materialization is one row of SQL,
  not a DataFrame. Any chunking would have to be expressed inside
  the SQL itself (UNION ALL of per-partition CTAS into the same
  table, manual partition write loop in the @model).
- `calc_park_factors_advanced` peaks at 29 GB in 3.6 s. The body
  (`bc/python_models/park_factors/builder.py:160`) has a
  `with_priors AS this … with_priors AS other` self-join that looks
  Cartesian-shaped on a static read. The captured profile JSON
  should say definitively whether DuckDB is hash-joining the full
  product or pruning it; if the former, partitioning the self-join
  by (season, league) is the obvious fix.

No iterative-loop / Markov / row-by-row models exist. Polars / numba
are not the right tools here — every model is relational SQL on
DuckDB and that is the right abstraction.

## Lazy materialization

Trivial-projection staging models suitable for `kind: VIEW` (rename
+ cast + filter, no join, no agg, no fanout):

`stg_parks`, `stg_event_flags`, `stg_event_comments`,
`stg_event_fielding_plays`, `stg_event_pitch_sequences`, `stg_rosters`,
`stg_gamelog`.

Sizes are in the 1-30 MB range each — the disk savings are tiny.
The case for converting them is more about reducing the number of
materialized snapshots SQLMesh has to track and rebuild on every
plan (each is a small but real CTAS that re-runs unconditionally
because its hash changes whenever the source loader changes).

`stg_events`, `stg_event_baserunners`, all event-level intermediates,
and all `personnel_*_states` must stay FULL — every one is heavily
re-scanned downstream and a VIEW would re-run the underlying query
per consumer.

## Nested types

DuckDB 1.5 has solid `LIST<STRUCT>` support: columnar storage,
zero-copy `UNNEST`, list aggregates. The ergonomics question is on
the consumer side, not the writer side.

- **Highest-leverage candidate:** `event_player_fielding_stats`
  (152 M rows, 2 GB, 9-row fanout per event). Reshape into one row
  per event with `LIST<STRUCT<position UTINYINT, player_id, team_id,
  outs_played, …>>`. Two downstream consumers
  (`player_position_game_fielding_stats.sql`,
  `fielder_advance_expectancy.sql`) need an `UNNEST` lateral.
  **Estimated savings: 1.4-1.6 GB. Effort: medium.**

- **Skip:** `stg_event_baserunners` is high-fanout (~3 runners) but
  has 8 consumers including expectancy and ML pipelines. Refactor
  cost outweighs disk savings unless disk pressure becomes acute.

## Actionable changes — ranked

1. **Reshape `event_player_fielding_stats` to `LIST<STRUCT>`** —
   biggest single disk win (1.4-1.6 GB) and cuts row count of the
   biggest table 9× which helps any downstream that scans it.
   Update two consumers to `UNNEST`. Medium effort. _Disk + time._
2. **Pre-aggregate baserunning summary into a 1:1 event-grain
   table** — replaces 2 of 3 probes against 60-90 M fanout rows
   with probes against 16 M. Two of the top-3 wall-time models
   benefit. Medium effort. _Time + memory._
3. **Reorder `event_player_fielding_stats` join: build side =
   `personnel_fielding_states`** — small SQL change, measurable
   wall-time win on top-2 model. Trivial effort. _Time._
4. **`ASOF JOIN` for `event_states_batter_pitcher`'s BETWEEN
   range** — removes a hash + filter pattern that DuckDB can't
   optimize. Small effort. _Time._
5. **Encode expectancy lookup keys as packed `UBIGINT` instead of
   VARCHAR composites** in `event_states_full` and propagate to
   `event_transition_values`. ~250 MB disk + faster joins. Small
   effort. _Disk + time._
6. **Consolidate expectancy lookup keys into `event_states_full`**
   so 4 matrix joins run once instead of being recomputed per
   downstream model. Small-medium effort. _Time._
7. **Convert trivial staging models to `kind: VIEW`** — small disk
   win (~50 MB) but cuts snapshot churn on plan re-runs. Trivial
   effort. _Disk (small)._
8. **Profile `calc_park_factors_advanced`'s `with_priors AS this …
   with_priors AS other` self-join** via the captured profile JSON;
   partition by (season, league) if the profile shows full-product
   materialization. _Memory._

## Outcome — landed on `perf-deep-dive` branch

End-to-end perf_run wall-clock: 4m06s baseline → 3m48s after preload →
**~2m13s after grid-tuned config** (t=14, w=6).

- SQL model rewrites: QUALIFY → narrow CTE, window-frame elimination,
  ENUM/INT type alignment, FILTER vs CASE pivot, `min_by` for first
  fielding position, `ANY_VALUE` for FD-determined columns. Total
  model-build sum: 177.8s → 170.9s on (7,2) baseline.
- `scripts/preload_sources.py`: parallel CREATE TABLE for 45 source
  parquet, default 8 workers. Init phase 63s → 46s.
- Grid search over `(BC_DUCKDB_THREADS, BC_CONCURRENT_TASKS)` —
  winner `t=14, w=6` (~6× core overcommit) at 133s plan,
  −26% vs previous default `(7, 2)`. Bumped defaults in
  `bc/config.py`. Higher overcommit (28×6, 14×8) regresses.
- `scripts/grid_search.py` + `scripts/_grid_one.py` are the harness;
  results in `logs/perf/grid/grid_results.json`.

## Followups

- **Build-side snapshot retention** is what was eating the 90 GB.
  `799e736` set publish retention to keep-last-5; build-side
  equivalent is needed. SQLMesh has `--gc` and `janitor`; wire one
  to the build path or the post-build cleanup script.
- **Cold-state audit failure**: `relationships(... to_model :=
  main_models.x)` audits crash on first plan from empty state
  because the virtual layer doesn't exist during backfill. The
  documented `sqlmesh plan dev --auto-apply` command in `CLAUDE.md`
  silently relies on prior state. Either change the audit to
  resolve to physical at audit time, or document the two-step
  build (`plan dev --skip-audits` → `audit dev`).
- **DuckDB WAL after plan**: SQLMesh does not run `CHECKPOINT` at
  end of plan, so `bc.db.wal` retains unflushed `ALTER COLUMN …
  SET DATA TYPE <enum>` entries. Subsequent reopens via the CLI or
  a fresh Python connection crash on WAL replay because custom
  enum types can't be looked up without the user catalog already
  loaded. Workaround in `scripts/perf_run.py` is a final
  `CHECKPOINT bc`; the same call belongs in
  `scripts/publish_ducklake.py` for the same reason — file from
  `799e736` already does this for the publish step but the build
  pipeline itself still emits a fragile WAL.
- **Profile JSON parsing**: `logs/perf/profiles/<model>.json` has
  the per-operator plan tree with `OPERATOR_TIMING`,
  `OPERATOR_CARDINALITY`, `TOTAL_MEMORY_ALLOCATED`, etc., for every
  evaluated model. Worth a small parser that surfaces the top-cost
  operator per model — would let us validate the join-side / range-
  predicate hypotheses above without manual SQL re-runs.
