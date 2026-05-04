# Per-model hot-operator analysis

Hand-curated analysis of DuckDB JSON profile traces for the 15 slowest
SQLMesh models in the latest perf run
(`logs/perf/perf_20260503T035843Z.jsonl`). Profiles captured by
`scripts/profile_hotspots.py` running each model's rendered SELECT body
as `CREATE OR REPLACE TEMPORARY TABLE _profile_tmp AS <select>` against
the already-built `bc.db`, with `enable_profiling='json'` and
`profiling_mode='detailed'`. Profile JSONs at
`logs/perf/profiles_analyze/`.

`operator_timing` numbers below are summed wall-time across the 7 worker
threads, so single-thread elapsed is roughly `operator_timing / 7`. Even
so, a 14s figure here is roughly the same order as the original build's
~7s wall clock (FullKind models also pay materialization on top).

## Cross-cutting findings

Pulled from per-model digs below; these are the themes that recur and
have the highest payoff if fixed once.

### 1. ENUM↔VARCHAR cast friction breaks hash joins

The single most expensive misfeature in the corpus. Two distinct
manifestations:

- **`player_team_season_pitching_stats` (build 3.5s, NL join 17.1s)** —
  the join predicate is
  `(CAST(game_type AS VARCHAR) = 'RegularSeason') AND (player_id =
  player_id) AND (season = season) AND (CAST(team_id AS VARCHAR) =
  team_id)`. Both `game_type` and `team_id` are ENUM on the retrosheet
  side, VARCHAR on the databank side. DuckDB refuses to hash on
  `CAST(enum AS VARCHAR)` and falls back to BLOCKWISE_NL — a
  ~3.1B-pair Cartesian filter over (63K × 49K) groups. Aligning the
  types in the staging layer turns a 17s NL join into a sub-second
  hash join.
- **`event_player_fielding_stats` (build 15.4s, 152M output rows)** —
  the inner hash join on `personnel_fielding_key` carries a
  `personnel_fielding_key = CAST(personnel_fielding_key AS BIGINT)`
  predicate (different types upstream). DuckDB has to hash on the cast
  expression, so the dynamic filter degenerates to
  `[-1.6e9, 1.6e9]` — i.e. no zone-map pruning. Fixing the upstream
  type alignment restores useful min/max filters on the 16M-row
  scans.

Worth a one-shot audit of `bc/external_models.yaml` and the staging
layer to make sure ENUM columns flow through intermediates as ENUM, not
silently casted to VARCHAR.

### 2. Wide trailing projections over already-materialized event rows

`event_offense_stats` (138 cols × 26.3M rows = 6.65s PROJECTION) and
`event_pitching_stats` (133 cols × 16.5M rows = 4.82s PROJECTION) both
spend their second-largest chunk of time on a single final projection
that runs `@EACH(@stats(), s -> COALESCE(@s, 0)::INT1)` over the
unioned wide row. Cutting this is structural:

- Push the COALESCE+INT1 cast into each upstream CTE (one per UNION
  leg) before the union, so each leg projects only the columns it can
  populate. UNION ALL BY NAME zero-fills the rest implicitly. Halves
  the wide-row pass on each model.
- Or move from FULL OUTER + UNION ALL to LEFT JOIN + anti-join + a
  separate non-matching leg, so `event_baserunning_stats` is scanned
  once instead of twice (offense scans it 5.81M filtered to
  `baserunner='Batter'` and 10.54M unfiltered).

### 3. `QUALIFY ROW_NUMBER ... = 1` packs every column into a STRUCT

`event_states_batter_pitcher` build 3.2s, of which **10.1s of HASH_GROUP_BY
work** is one operator: the optimizer rewrote
`QUALIFY ROW_NUMBER() OVER (PARTITION BY event_key ORDER BY
batter_fielding_position DESC) = 1` into
`arg_max_nulls_last(STRUCT(...all output cols...),
batter_fielding_position) GROUP BY event_key`. Input cardinality
(~18.1M) ≈ output cardinality (~18.1M), so 99% of the work is building
a STRUCT of every output column for every row only to keep the unique
row most of the time.

The duplicates only arise from a range LEFT JOIN to
`stg_game_fielding_appearances` on
`event_id BETWEEN start_event_id AND end_event_id`. Two fixes:
- Pre-aggregate that staging input to one row per
  `(game_id, batter_id, event_id)` choosing the max
  `fielding_position`, so the QUALIFY disappears. Saves ~10s.
- Or rewrite the BETWEEN as an ASOF LEFT JOIN
  (`MATCH_CONDITION events.event_id >= start_event_id` + post-filter
  on `end_event_id`). DuckDB handles ASOF natively as a sort-merge,
  cheaper than the current hash join + window.

### 4. GROUP BY columns that are functionally dependent on the key

`event_pitching_stats` `baserunning_agg` CTE: GROUP BY `event_key` with
`MIN(game_id), MIN(current_pitcher_id), MIN(fielding_team_id)` plus
~30 SUMs. The three identifiers are functionally determined by
`event_key` — every row with the same `event_key` has the same
game/pitcher/team. Pulling them through the hash table is pure
overhead; resolve them with a downstream join from
`event_batting_stats` (which already has them). Drops three columns
from the per-group state; expected several-hundred-ms savings on the
8.1s HASH_GROUP_BY.

### 5. Wide windows that compute redundant aggregates

- **`event_base_out_states`** — the 8-expression `game_key`-partitioned
  WINDOW (11.2s) computes both
  `score_*_start = SUM(...) OVER (... ROWS UNBOUNDED PRECEDING AND 1
  PRECEDING)` and
  `score_*_end = SUM(...) OVER (... ROWS UNBOUNDED PRECEDING AND
  CURRENT ROW)`. The two differ by exactly the current row, so
  `start = end - runs_on_play * (batting_side = ...)`. Drops 4 of 8
  expressions in the dominator operator.
- **`calc_park_factors_advanced`** — a 21-column 3-row trailing
  `SUM(... ) OVER (PARTITION BY park_id, batter_id, pitcher_id, league
  ORDER BY season ROWS 2 PRECEDING)` window (13.65s, 5.6M rows). Only
  ~5 of the 21 windowed columns are consumed in raw form by
  downstream `with_priors`; the rest feed `averages` ratios that
  could just as well be derived from the 3 raw counts. Trim the
  window expression list, and the operator scales down
  proportionately.

### 6. Ordered LIST aggregates pay per-group sort buffering

`player_game_appearances` `fielding_agg`: 21.9s on a single
`LIST(fielding_position ORDER BY position_order, fielding_position)`
GROUP BY `(game_id, player_id)` over 5.28M rows → 1.22M groups. The
`ORDER BY` inside `LIST(...)` is the multiplier — without it the agg
finishes in well under a second.

The ordering exists so `fielding_positions[1]` resolves to the starter.
Two fixes:
- Replace with `min_by(fielding_position, position_order)` for the
  starter scalar, and use an unordered `LIST(...)` for the rest.
- Or pre-sort the `fielding_union` CTE once via a window
  (`ROW_NUMBER` already partitioned by `(game_id, player_id)` ordered
  by `position_order`), then just `LIST(fielding_position)` —
  unordered LIST keeps physical order.

### 7. The `event_player_fielding_stats` 152M-row fanout

Confirmed from the profile: an inner HASH_JOIN on
`personnel_fielding_key` blows from 15.4M × 16.3M inputs to 152M output
rows because each event row gets crossed with all 9 fielding positions.
The two stacked PROJECTIONs over 152M rows then do ~10 nested CASE
expressions per row (3.3s + 0.95s). The model's own TODO already calls
out that 8 of every 9 position-rows per event are zero-filled.

The flagged Phase 4.5 split — one row per
`(event × actually-involved-fielder)` plus a small per-game position
summary — is the structural fix. Cuts the table from 152M rows to ~16M
+ a small auxiliary, eliminates the wide CASE projections, and stops
this from being the largest table in the database.

### 8. UTINYINT → BIGINT widening before wide SUM aggregations

`player_game_offense_stats` widens 26.3M event-level UTINYINT counters
to BIGINT in a PROJECTION before the 133-aggregate HASH_GROUP_BY (14.1s,
4.43M groups). DuckDB's `sum_no_overflow` accumulator is BIGINT-typed,
so the cast is forced — but the *input* row width can stay UTINYINT.
Removing the explicit BIGINT cast lets DuckDB keep input rows narrow
and only widen inside the accumulator. Same antipattern in
`player_team_season_pitching_stats`'s 152-SUM agg. Modest individual
win, but it's free.

### 9. Wide CONCAT_WS-built composite keys

`event_states_full`'s 3.4s final PROJECTION includes four
`CONCAT_WS(...)`-built `*_expectancy_key` VARCHARs over 18.1M rows.
These are stringified composites of integer/short columns, used solely
as join keys against the run/win-expectancy lookup tables. Either store
them as `STRUCT` and join on members (no string materialization), or
hash them once via DuckDB's `hash()` (BIGINT, 8 bytes) and join the
lookup tables on the hash. Saves ~3s + the on-disk bytes for those four
columns.

## Per-model summary table

| model | build_s | top op | top op_s | rows | key issue |
|---|---:|---|---:|---:|---|
| `event_offense_stats` | 17.4 | PROJECTION (138 cols) | 6.65 | 26.3M | wide trailing projection (#2) |
| `event_player_fielding_stats` | 15.4 | HASH_JOIN | 6.60 | 152M | 152M-row fanout + cast on join key (#1, #7) |
| `event_pitching_stats` | 14.4 | HASH_GROUP_BY (baserunning_agg) | 8.12 | 10.6M | functional-dependency cols in GROUP BY (#4) |
| `event_states_full` | 7.5 | PROJECTION (CONCAT_WS keys) | 3.41 | 18.1M | wide stringified composite keys (#9) |
| `player_game_offense_stats` | 7.2 | HASH_GROUP_BY (133 SUMs) | 14.14 | 4.4M | 5.9× reduction with 133 aggs; BIGINT cast (#8) |
| `event_baserunning_stats` | 5.5 | HASH_JOIN | 1.11 | 16.3M | per-row enrichment, no obvious win |
| `event_base_out_states` | 5.4 | WINDOW (game_key, 8 exprs) | 11.21 | 18.1M | redundant start/end frames (#5) |
| `player_game_pitching_stats` | 4.2 | HASH_GROUP_BY | 4.81 | 1.2M | reasonable for grain; no obvious win |
| `player_game_appearances` | 3.8 | HASH_GROUP_BY (LIST ORDER BY) | 21.89 | 5.3M | ordered LIST aggregate (#6) |
| `calc_park_factors_advanced` | 3.6 | WINDOW (21 cols) | 13.65 | 5.6M | overbroad window expression list (#5) |
| `player_team_season_pitching_stats` | 3.5 | BLOCKWISE_NL_JOIN | 17.13 | 64K | ENUM/VARCHAR cast forced NL join (#1) |
| `event_fielders_flat` | 3.4 | HASH_GROUP_BY (PIVOT) | 1.24 | 1.6M | 9-CASE pivot; FILTER could push predicate |
| `event_batting_stats` | 3.3 | TABLE_SCAN | 0.54 | 15.8M | write-bound; would need INCREMENTAL_BY_TIME_RANGE |
| `player_position_game_fielding_stats` | 3.3 | HASH_GROUP_BY | 9.69 | 5.1M | upstream 152M-row table (see #7) |
| `event_states_batter_pitcher` | 3.2 | HASH_GROUP_BY (arg_max struct) | 10.15 | 18.1M | QUALIFY → struct-arg_max rewrite (#3) |

## Per-model detail

### `event_offense_stats` — wide projection over wide UNION row

Build 17.4s; analyze 4.2s wall-clock; peak buffer 21.7 GB.

Operator hot-spots (top 5):
1. d=2 `PROJECTION` 6.65s, 26.3M rows × **138 cols** — final
   `@EACH(@stats(), s -> COALESCE(@s, 0)::INT1)` cast over the UNIONed
   wide row.
2. d=8 `HASH_JOIN LEFT (event_key)` 1.57s, build=event_batting_stats
   (15.76M, 32 cols), probe=event_batted_ball_stats (12.04M, 40 cols).
3. d=6 `HASH_JOIN FULL (event_key)` 1.52s, build=upstream join (15.76M,
   ~129 cols), probe=batter_baserunning (5.81M, 46 cols).
4. d=7 `HASH_JOIN LEFT (event_key)` 1.03s, probe=event_pitch_sequence_stats
   (7.33M, 24 cols).
5. Two TABLE_SCANs of `event_baserunning_stats` (5.81M filtered to
   `baserunner='Batter'`, plus 10.54M unfiltered for the UNION leg).

Antipatterns: scans `event_baserunning_stats` twice; the wide cast
projection runs once over the full unioned 26M-row × 138-col stream.

Concrete actions:
- Push the `COALESCE(@stat, 0)::INT1` casts into each UNION leg so each
  leg only projects its own columns. Halves the wide pass.
- Replace FULL OUTER + UNION ALL with LEFT JOIN + anti-join +
  non-matching leg, sharing one scan of `event_baserunning_stats`.
- Narrow `batter_baserunning` to the cols actually needed downstream
  before the FULL JOIN.

### `event_player_fielding_stats` — 152M-row fanout, casted join key

Build 15.4s; analyze 5.4s; peak buffer 73.4 GB; output 152M rows × 27
cols.

Hot-spots:
1. d=4 `HASH_JOIN LEFT (event_key, fielding_position)` 6.60s, output
   152M, probe=`calc_fielding_play_agg` (15.23M).
2. d=5 `HASH_JOIN INNER (personnel_fielding_key)` 5.49s, output 152M.
   Build=personnel_fielding_states (15.38M), probe=event_fielding_stats
   (16.32M). Output cardinality ≈ 10× either input — the
   cross-with-9-fielding-positions explosion.
3. d=2/d=3 stacked `PROJECTION` 3.26s + 0.95s, both over 152M rows,
   evaluating ~10 nested CASE/COALESCE expressions per row.

Extra_info evidence: the inner-join condition includes
`personnel_fielding_key = CAST(personnel_fielding_key AS BIGINT)`. Type
mismatch upstream → DuckDB hashes on the cast expression → dynamic
filter degenerates.

Concrete actions:
- Fix `personnel_fielding_key` type alignment in upstream models so the
  raw column hashes correctly and zone-maps prune.
- Honor the model's TODO: split into one (event × actually-involved
  fielder) row + a small per-(game, position, player) summary. Cuts
  this from 152M rows to ~16M + small.
- If the wide table must stay, fuse the two stacked PROJECTIONs into
  one over 152M rows (saves ~1s).
- This is also the largest table on disk (~2 GB written) — incremental
  kind candidate.

### `event_pitching_stats` — functional-dependency MINs in big GROUP BY

Build 14.4s; analyze 5.3s; peak buffer 73.4 GB.

Hot-spots:
1. d=11 `HASH_GROUP_BY` **8.12s**, 10.56M groups — `baserunning_agg`
   over 16.3M baserunning rows: `MIN(game_id), MIN(player_id),
   MIN(team_id), SUM(...) per pitching_baserunning_col` GROUP BY
   `event_key`.
2. d=2 `PROJECTION` 4.82s, 16.46M rows × 133 cols — same wide cast
   pattern as offense.
3. Three large HASH_JOINs (1.0–1.8s each) building the wide row.
4. Hash-table state for the GROUP BY is the dominant memory consumer
   (peak 76.9 GB system-wide buffer).

Concrete actions:
- Drop `MIN(game_id), MIN(current_pitcher_id), MIN(fielding_team_id)`
  from `baserunning_agg` — they're functionally determined by
  `event_key`. Resolve them downstream by joining from
  `event_batting_stats`. Shrinks per-group state from ~30 cols to ~25.
- Push the INT1 cast into each UNION leg (same as offense).
- Narrow the build-side wide row before the LEFT JOIN to
  `event_run_assignment_stats` (1.6M-row probe doesn't need 130-col
  build).
- INCREMENTAL_BY_TIME_RANGE candidate (append-only by game_date).

### `event_states_full` — CONCAT_WS-built composite keys

Build 7.5s; pure JOIN+PROJECT, no windows or aggs.

Hot-spots:
1. d=1 `PROJECTION` 3.41s, 18.1M rows × ~70 cols — the final SELECT,
   dominated by four `CONCAT_WS(...)`-built `*_expectancy_key` VARCHARs
   plus several CASE branches.
2. Three sequential INNER hash joins on `event_key` / `game_id`
   totalling ~3.1s.

Concrete actions:
- Drop the four CONCAT_WS keys from this model. Compute them on demand
  in the run/win-expectancy lookup models, or store as STRUCT and join
  on members. Saves ~3s + on-disk space for four wide VARCHARs over
  18.1M rows.

### `player_game_offense_stats` — 133-SUM agg with BIGINT widening

Build 7.2s; analyze 2.9s.

Hot-spots:
1. d=4 `HASH_GROUP_BY` 14.14s, 26.3M → 4.43M (5.9× reduction), **133
   SUM aggregates** GROUP BY `(game_id, team_id, player_id)`.
2. d=5 `PROJECTION` 0.34s, 26.3M rows — pre-agg cast UTINYINT → BIGINT.
3. d=10 box-score branch (330K rows, 22 aggs) — cheap.

Concrete actions:
- Drop the explicit BIGINT cast — DuckDB's `sum_no_overflow` already
  widens inside the accumulator; the input row can stay UTINYINT, which
  shrinks the hash-table key+value width.
- Pre-stage as event → game-half → game so each aggregator processes a
  smaller hash table.
- INCREMENTAL_BY_TIME_RANGE (per season) candidate — already flagged in
  Phase 4.5.

### `event_baserunning_stats` — per-row enrichment, no clear win

Build 5.5s; mostly write-bound.

Hot-spots:
1. d=1 `CREATE_TABLE_AS` 4.12s — write phase for 16.3M rows × ~50
   narrow UTINYINT cols.
2. d=13 `HASH_JOIN INNER` 1.11s — `event_key = event_key`.
3. d=2 `PROJECTION` 0.79s — ~50 boolean/case-cast expressions.

No obvious win. Already efficient for what it does.

### `event_base_out_states` — redundant window frames

Build 5.4s; WINDOW dominator.

Hot-spots:
1. d=4 `WINDOW` 11.21s, partitioned by `game_key` — 8 expressions over
   ~225K partitions (~80 events each) on 18.1M rows. Includes both
   `score_*_start` (frame `... 1 PRECEDING`) and `score_*_end` (frame
   `... CURRENT ROW`) variants.
2. d=3 `WINDOW` 5.8s, partitioned by `(game_key, inning, frame_key)` —
   6 LEAD/LAG calls.
3. d=8 `HASH_GROUP_BY` 1.0s, 11.2M groups — `runners` CTE.

Concrete actions:
- Compute only `score_*_end` and derive `score_*_start = score_*_end -
  runs_on_play * (batting_side = ...)`. Cuts 4 of 8 expressions in
  the dominant WINDOW operator.
- The `frame_start_flag` / `game_start_flag` `LAG IS NULL` pattern can
  swap to `event_key = MIN(event_key) OVER partition` for co-location
  with the running aggregates. Modest.

### `player_game_pitching_stats` — reasonable for grain

Build 4.2s.

Hot-spots:
1. d=16 `HASH_GROUP_BY` 4.81s, 16.5M → 1.19M, `min` + 5+
   `sum_no_overflow` aggregates.
2. d=6 `WINDOW` 1.18s, partitioned by `(team_id, game_id)` — team-game
   running totals.

No obvious structural win. The 1.2M-row output can't shrink without
changing grain.

### `player_game_appearances` — ordered LIST aggregate

Build 3.8s; HASH_GROUP_BY 21.9s on `LIST(... ORDER BY ...)`.

Hot-spots:
1. d=6 `HASH_GROUP_BY` 21.89s, 5.28M → 1.22M groups —
   `LIST(fielding_position ORDER BY position_order, fielding_position)`
   + 2 BOOL_OR. The `ORDER BY` is the multiplier.
2. The `box_offense` LEFT JOINs use `CAST(side AS VARCHAR) = side` —
   ENUM/VARCHAR mismatch forcing a per-join expression on the build
   side.

Concrete actions:
- Replace ordered LIST with `min_by(fielding_position, position_order)`
  for the starter scalar + unordered LIST for the rest. Or pre-sort
  via `ROW_NUMBER` window in `fielding_union` and drop the ORDER BY
  inside LIST.
- Cast `side` once in a CTE rather than per-join, or align the staging
  type.

### `calc_park_factors_advanced` — overbroad window expression list

Build 3.6s; analyze 4.0s.

Hot-spots:
1. d=6 `WINDOW` 13.65s, 5.6M rows — `SUM(...) OVER (PARTITION BY
   park_id, batter_id, pitcher_id, league ORDER BY season ROWS 2
   PRECEDING)` over **21 stat columns**.
2. d=9 `HASH_GROUP_BY` 5.46s, 5.6M rows — `batting_agg` GROUP BY
   `(park_id, season, league, batter_id, pitcher_id)`.
3. d=12 `HASH_JOIN` 1.67s, 24.6M rows — `event_states_full` ⋈
   `event_offense_stats` on `event_key`.
4. Two more WINDOWs (1.45s + 1.67s) for `self_joined` rate calcs.

Concrete action:
- Trim the window expression list. Only ~5 of the 21 windowed stats
  are consumed in raw form by `with_priors`; the remaining 16 feed
  ratio derivations in `averages` that can be computed from the 5
  raw counts. Saves proportional time on the 13.65s operator.

### `player_team_season_pitching_stats` — ENUM cast forces NL join

Build 3.5s; BLOCKWISE_NL_JOIN 17.1s.

Hot-spots:
1. d=6 `BLOCKWISE_NL_JOIN LEFT` 17.13s, 63.6K output — predicate
   `((CAST(game_type AS VARCHAR) = 'RegularSeason') AND (player_id =
   player_id) AND (season = season) AND (CAST(team_id AS VARCHAR) =
   team_id))`.
2. d=9 `HASH_GROUP_BY` 1.85s, 1.23M → 63.6K with 152 SUMs.

Concrete action:
- Materialize `team_id::VARCHAR` and `game_type::VARCHAR` once in a CTE
  (or fix the databank side to ENUM). Predicate becomes
  `team_id = team_id AND game_type = game_type` over equal types →
  HASH_JOIN, sub-second.

### `event_fielders_flat` — PIVOT-then-rejoin

Build 3.4s.

Hot-spots:
1. d=1 `CREATE_TABLE_AS` 2.71s — write 1.6M rows.
2. d=4 `HASH_GROUP_BY` 1.24s — pivot `personnel_fielding_states` into
   9 ANY_VALUE columns grouped by `personnel_fielding_key`.
3. d=3 `HASH_JOIN INNER` 0.59s — re-join 16.3M event rows back to the
   1.65M aggregate.
4. d=5 `PROJECTION` 0.37s — 9 `CASE WHEN fielding_position = N`
   branches feeding the GROUP BY.

Concrete action:
- Replace the 9 CASE expressions feeding ANY_VALUE with
  `ANY_VALUE(player_id) FILTER (WHERE fielding_position = 1)` etc.
  Pushes the predicate into the aggregate so DuckDB skips
  non-matching rows. Likely sub-second improvement, but easy.

### `event_batting_stats` — write-bound

Build 3.3s; pure row-shape transform.

Hot-spots:
1. d=1 `CREATE_TABLE_AS` 2.27s — write 15.8M rows.
2. d=7 `TABLE_SCAN` 0.54s on `stg_events` with predicate
   `plate_appearance_result IS NOT NULL`.
3. Wide PROJECTION + LEFT JOIN combo, ~1s total.

Already efficient. Only structural lever is incremental kind so daily
runs don't re-write 15.8M rows.

### `player_position_game_fielding_stats` — upstream-bound

Build 3.3s; aggregating the 152M-row `event_player_fielding_stats`.

Hot-spots:
1. d=8 `HASH_GROUP_BY` 9.69s, 152M → 5.07M with ~22 SUMs, GROUP BY
   `(game_id, player_id, fielding_position)`.
2. d=11 `TABLE_SCAN` 2.15s on `event_player_fielding_stats` (152M).

The work is intrinsic to the source size. The lever is upstream — if
`event_player_fielding_stats` is split per the §7 recommendation, this
GROUP BY shrinks dramatically.

### `event_states_batter_pitcher` — QUALIFY ROW_NUMBER struct-pack

Build 3.2s; HASH_GROUP_BY 10.15s.

Hot-spots:
1. d=3 `HASH_GROUP_BY` 10.15s, 18.1M groups —
   `arg_max_nulls_last(STRUCT(...all output cols...),
   batter_fielding_position) GROUP BY event_key`. Group cardinality ≈
   input cardinality; 99% overhead.
2. d=7 `HASH_JOIN LEFT` 0.54s — range LEFT JOIN to
   `stg_game_fielding_appearances` on `(game_id, batter_id, event_id
   BETWEEN start_event_id AND end_event_id)` — produces the duplicates
   that the QUALIFY then collapses.

Concrete actions (any of these eliminates the 10s op):
- Pre-aggregate `stg_game_fielding_appearances` to one row per
  `(game_id, batter_id, event_id)` choosing the max `fielding_position`
  before the join. QUALIFY disappears.
- Or rewrite the BETWEEN as `ASOF LEFT JOIN` with `MATCH_CONDITION
  events.event_id >= start_event_id` + post-filter on
  `end_event_id`. ASOF is sort-merge; cheaper than the current
  hash-join + window.

## Implementation status

Non-structural findings landed on branch `perf-deep-dive`. Re-run
`scripts/perf_run.py` against the rebuilt `bc.db` to capture the new
hot-operator profile.

Implemented:

- **#1 ENUM/VARCHAR cast in `player_team_season_pitching_stats`**: added
  `retrosheet_keyed` CTE that materializes `team_id::VARCHAR AS
  team_id_str` and `game_type::VARCHAR AS game_type_str`, used as the
  join keys against the databank side. Outer SELECT excludes the
  helpers so the published schema is unchanged.
- **#1 `personnel_fielding_key` / `personnel_lineup_key` type
  alignment**: changed both keys from BIGINT to INTEGER end-to-end
  (`personnel_fielding_states`, `personnel_lineup_states`,
  `event_personnel_lookup`, `event_fielding_stats`,
  `event_fielders_flat`). Construction expression now `(...)::INTEGER`
  with a comment on the range bound. Eliminates the `CAST AS BIGINT`
  the planner was injecting on the join predicate. As a side-fix
  cleaned up an apparent precedence bug in `personnel_lineup_states`
  where `* CASE ... -1 END::INT` only cast the `-1`.
- **#3 `event_states_batter_pitcher` QUALIFY rewrite**: extracted a
  narrow `batter_field_at_event` CTE that joins `stg_events` to
  `stg_game_fielding_appearances` and computes
  `MAX(batter_field.fielding_position)` GROUP BY `event_key`. The wide
  `joined` CTE now LEFT JOINs that lookup. Removes the
  `arg_max(STRUCT(...all output cols...))` rewrite the planner
  produced from the old QUALIFY.
- **#4 FD MINs in `event_pitching_stats.baserunning_agg`**: swapped
  `MIN(game_id) / MIN(current_pitcher_id) / MIN(fielding_team_id)` to
  `ANY_VALUE(...)`. Same correctness (functionally determined by
  `event_key`), cheaper accumulator op.
- **#5a `event_base_out_states` window trim**: dropped the
  `start_event` window (UNBOUNDED..1 PRECEDING). Compute only
  `score_*_end` via `end_event`, derive `score_*_start` arithmetically
  from `score_*_end - runs_on_play * (batting_side matches)`. Reuses
  `end_event` for `game_start_flag` (LAG ignores frame).
- **#6 `player_game_appearances` `LIST(... ORDER BY ...)`**: kept the
  sort (the column ships to external parquet consumers and array
  order matters) but replaced the downstream `fielding_positions[1]`
  with `min_by(fielding_position, (position_order,
  fielding_position))` to eliminate the indexing. Modest win — most
  of #6's potential needs the non-deterministic LIST that we
  consciously rejected.
- **`event_fielders_flat` FILTER pivot**: 9 `ANY_VALUE(CASE WHEN
  fielding_position = N THEN player_id END)` → `ANY_VALUE(player_id)
  FILTER (WHERE fielding_position = N)`.
- **Side-effect fixes for #6**: `stg_box_score_pinch_running_lines`
  and `stg_box_score_pinch_hitting_lines` now declare `side` as
  `SIDE` ENUM with explicit `side::SIDE` cast in the rename CTE,
  matching the batting-lines staging type. Removes per-join CASTs in
  `player_game_appearances.box_offense`.

Deferred (not implemented this round):

- **#2 push INT1 cast into UNION legs in `event_offense_stats` /
  `event_pitching_stats`**: requires splitting
  `EVENT_LEVEL_OFFENSE_STATS` and `EVENT_LEVEL_PITCHING_STATS` into
  per-leg subsets and explicit `0::INT1 AS missing_col` defaults to
  preserve UNION ALL BY NAME's zero-fill semantics. Estimated win
  ~25–30%; reorganization touches a 138-col model with regression
  risk. Worth a focused PR.
- **#5b `calc_park_factors_advanced` window trim**: re-reading
  `with_priors` → `self_joined` → `rate_calculation`, every windowed
  stat is referenced raw downstream (`this_{s}_per_pa = this_{s} /
  this_plate_appearances`). The "5 of 21 raw" claim doesn't match
  the SQL flow; trimming would require rewriting the rate calc to
  derive ratios from a different source. Needs deeper analysis.
- **#7 split `event_player_fielding_stats` into involved-fielder + per-
  position summary**: structural (changes table grain) per the user's
  scope rule. Defers the largest single win.
- **#8 BIGINT widening in `player_game_offense_stats` /
  `player_team_season_pitching_stats`**: there is no explicit BIGINT
  cast in the current SQL; the widening is DuckDB's accumulator
  promoting input INT1/UTINYINT to BIGINT internally. Lowering the
  upstream cast on `event_offense_stats` from INT1 to UTINYINT would
  let DuckDB use UBIGINT accumulators, but that changes the published
  table's column type — structural.
- **#9 CONCAT_WS keys in `event_states_full`**: removes columns from
  the output, structural.

## Methodology and caveats

- Profile capture pinned the build-time DuckDB pool to a single
  connection (`BC_PERF_MODE=1` in `bc/config.py`) so that per-snapshot
  PRAGMAs stick.
- Rendered SELECT bodies were rewritten from
  `"sqlmesh__main_models"."main_models__<X>__<id>__dev"` to
  `"main_models__dev"."<X>"` (virtual layer) because snapshot IDs in
  `Context.render` output drift from what's on disk after a
  monkey-patched-audit build.
- `EXPLAIN ANALYZE` does not write `profile_output`. Plain SELECT does;
  CREATE TEMPORARY TABLE AS works in read-only mode and was picked.
- `operator_timing` is wall-time across all worker threads — useful for
  ranking ops, but multiply by `1/threads` for single-thread elapsed.
- DuckDB does not populate per-operator `total_memory_allocated`
  (always 0); `system_peak_buffer_memory` at the top level is the real
  memory signal.
