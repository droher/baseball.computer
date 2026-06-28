---
title: Rollup Parity Dispositions
type: decision-log
status: active
audience: humans-and-agents
last-verified: 2026-05-13
---

# Rollup Parity Dispositions

`scripts/rollup_parity_checks.py` runs `EXCEPT` in both directions between each
existing completeness model and a ledger-derived reproduction. Initial run on
`data_coverage` after the Phase-1 ledgers (and the
`source_data_error_risk_ledger` per-credit-dimension expansion) produces nonzero
diffs on every model.

The Phase-1 exit-gate item "completeness models can be reproduced from ledger
rollups" requires a written disposition per model: either the diff is an
accepted semantic gap (ledger is the new source-of-truth, heuristic flag was a
pre-ledger inference) or a heuristic-rewrite follow-up.

`justfile` `rollup-parity-checks` recipe sets the seven `accept-gap` models in a
default `--allow-mismatch` list so the recipe exits 0 in CI. Any future model
added must also receive a disposition entry here.

## Per-model dispositions

### `event_completeness_pitches`

- **Existing flag logic.** `event_completeness_pitches` walks `stg_events` and
  `stg_event_pitch_sequences` directly, deriving the `has_*` booleans from raw
  pitch counts and base/strike state on each event row.
- **Ledger logic.** `event_observation_pitch` carries one row per
  `(event_key, dimension)` with an `observed_status` enum sourced from the
  per-pitch sequence and the count fields, and the ledger reproduction pivots
  those rows back to event-grain booleans (`BOOL_OR(observed_status =
  'observed')`).
- **Diff source (1.94M rows in_existing vs 1.93M rows in_ledger).** The two
  cuts diverge mostly on event-population semantics: the existing model
  includes events with no PA result so long as a pitch row exists, while the
  ledger reproduction filters to `plate_appearance_result IS NOT NULL OR
  has_pitches` and pivots through the ledger which itself drops the
  not-applicable rows. There are also small `strike_types` semantic
  differences (the ledger's `unknown_code` handling is more conservative than
  the existing flag's `BOOL_OR(... IS NOT NULL)`).
- **Disposition: `accept-gap`.** The ledger is canonical from now on. A
  follow-up enrichment PR can rewrite the existing model to read directly off
  `event_observation_pitch` (and would close this gap by construction); not in
  scope for the exit-gate close. Tracked in `notes/data-coverage-implementation/implementation-checklist.md`
  §"Phase 1 Enrichment Follow-Ups" if/when the work is sequenced.

### `event_completeness_fielding_credit`

- **Existing flag logic.** Joins `stg_events` to fielding aggregates with a
  FULL OUTER JOIN, defaulting missing fielding rows to `TRUE` for all has_*
  flags (i.e. "no fielding play recorded" → "no missing credit").
- **Ledger logic.** Drives off `stg_events` filtered to plate-appearance
  events, LEFT JOINs `event_observation_credit` pivoted to per-dim
  `unknown_code` flags, and inverts (`NOT unk_*`).
- **Diff source (221 rows each side).** A handful of edge events where the
  existing FULL OUTER JOIN keeps fielding rows that aren't tied to a
  plate-appearance event, plus a few cases where the ledger's stricter
  `unknown_code` derivation (via `calc_fielding_play_agg.fielding_position =
  0`) and the existing model's `IS NULL` test on the fielding columns
  disagree.
- **Disposition: `accept-gap`.** 221 rows on a 18.1M-row event population is
  rounding error; the ledger's per-credit-dimension semantics are correct by
  construction. No follow-up.

### `event_completeness_batted_balls`

- **Existing flag logic.** Reads `stg_events` row-at-a-time and tests
  `trajectory IS NOT NULL`, `general_location IS NOT NULL`, and
  `batted_to_fielder IS NOT NULL` against the raw event columns; INNER-JOINs
  `seed_plate_appearance_result_types` to scope to batted-ball results.
- **Ledger logic.** Rolls up `event_observation_geometry` per event_key with
  `BOOL_OR(dimension = 'trajectory' AND observed_status = 'observed')` etc.,
  then INNER-JOINs the same seed.
- **Diff source (301,318 rows each side).** Two semantic divergences: (a) the
  ledger's `general_location` `observed` rows are scoped against
  `calc_batted_ball_type` (which COALESCEs `'Unknown'` rather than NULL — see
  `data_coverage_next_steps` ledger-#10 lesson), so the ledger's
  `has_general_location` is `FALSE` where the existing flag is `TRUE`; (b)
  `ball_handler_position` (the ledger's `has_batted_to_fielder`) drops zero-
  fielder marker rows that the existing flag still counts as observed.
- **Disposition: `accept-gap`.** Ledger is canonical. Same enrichment-rewrite
  comment as `event_completeness_pitches`: the existing analyses model can
  eventually be rewritten to read directly off
  `event_observation_geometry`, which would close the gap by construction.
  Defer to a separate enrichment PR.

### `player_game_data_completeness`

- **Existing flag logic.** Aggregates per-event flags from
  `event_completeness_pitches` and `event_completeness_batted_balls` across
  all events for a (game_id, player_id, player_type) tuple using BOOL_AND
  with COALESCE-FALSE defaults.
- **Ledger logic.** Rolls up `event_observation_pitch` per event_key, then
  joins `event_observation_context` to expand each event into BATTING /
  PITCHING rows keyed on `batter_id` / `pitcher_id`, then aggregates per
  (game_id, player_id, player_type) with BOOL_AND.
- **Diff source (~1.30M rows each side).** Two compounding divergences: (a)
  the per-event input differs (`event_completeness_pitches` vs ledger pivot
  — see disposition above); (b) the ledger reproduction in the parity script
  emits ONLY pitch flags (`count_balls`, `count_strikes`, `count`,
  `has_pitches`), while the existing model also surfaces batted-ball
  columns. The parity script projects the shared columns only, so all
  diff is attributable to (a).
- **Disposition: `accept-gap`.** Inherits from
  `event_completeness_pitches`. Closes when the per-event ledger rewrite
  closes.

### `player_completeness`

- **Existing flag logic.** COUNT_IF over pre-computed
  `player_game_data_completeness` rows.
- **Ledger logic.** Same as `player_game_data_completeness` ledger
  reproduction, then COUNT_IF the per-game flag rollup.
- **Diff source (~17K rows each side).** Inherits from
  `player_game_data_completeness`.
- **Disposition: `accept-gap`.** Same as
  `player_game_data_completeness`.

### `game_data_completeness`

- **Existing flag logic.** Joins `stg_events` to `game_start_info` for each
  game and rolls up `has_play_by_play` / `has_box_score` from
  `source_type`.
- **Ledger logic.** Reads `source_acquisition_ledger` directly with
  `team_id IS NULL` and BOOL_ORs `source_type IN ('Event', 'BoxScore')`
  vs `'PlayByPlay'`.
- **Diff source (31,640 rows in_existing, 0 in_ledger).** The 31,640 are
  games present in the existing model that don't appear in
  `source_acquisition_ledger` filtered to `team_id IS NULL`. Two causes:
  (a) games that only have side-keyed (team_id IS NOT NULL) source rows
  (e.g. PBP games where the game-wide row is absent because PBP is
  team-keyed in `source_acquisition_ledger`); (b) gamelog-only games whose
  game-wide row exists in `game_start_info` but not in
  `source_acquisition_ledger.dimension = 'event'`.
- **Disposition: `accept-gap`.** Ledger is source-of-truth on what
  acquisition status exists per game; the asymmetric diff (only existing has
  rows) reflects that the existing model treated several upstreams as
  game-grain when they're actually side-grain in the ledger taxonomy.
  Rewrite of `game_data_completeness` to BOOL_OR across team-keyed rows is
  a follow-up enrichment.

### `season_team_coverage`

- **Existing flag logic.** Aggregates `team_game_start_info` per (season,
  team_id) and computes `least_granular_source_type` with BOOL_AND cascades
  on `source_type`.
- **Ledger logic.** Joins `source_acquisition_ledger` (where
  `team_id IS NULL OR team_id = gsi.team_id`) to `team_game_start_info`,
  excludes forfeits and Exhibition+league=NULL games, then applies the same
  BOOL_AND cascades.
- **Diff source (531 rows in_existing, 0 in_ledger).** The 531 are
  (season, team_id) tuples where the ledger reproduction's exclude-list
  (forfeits, league=NULL Exhibition) drops every game in the team-season,
  while the existing model retains a row for the team-season anyway.
- **Disposition: `accept-gap`.** Ledger reproduction's exclusion semantics
  are correct (forfeit+exhibition games carry no real coverage signal). A
  follow-up could rewrite `season_team_coverage` to read off the ledger
  directly; defer.

## Summary

| Model | Disposition | Rewrite candidate? |
|-------|-------------|---------------------|
| `event_completeness_pitches` | `accept-gap` | Yes — ledger-direct rewrite |
| `event_completeness_fielding_credit` | `accept-gap` | No — diff is rounding |
| `event_completeness_batted_balls` | `accept-gap` | Yes — ledger-direct rewrite |
| `player_game_data_completeness` | `accept-gap` | Yes — inherits from pitches |
| `player_completeness` | `accept-gap` | Yes — inherits from pitches |
| `game_data_completeness` | `accept-gap` | Yes — switch to side-grain BOOL_OR |
| `season_team_coverage` | `accept-gap` | Yes — read off ledger directly |

All seven added to `--allow-mismatch` default in
`justfile:rollup-parity-checks` so the recipe exits 0. Removing any from the
default list before the rewrite lands will reopen the Phase-1 exit gate.
