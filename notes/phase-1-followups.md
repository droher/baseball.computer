# Phase 1 — Open Follow-ups

Items surfaced during Phase 1 but deferred. None block declaring the
SQLMesh transition done; all are real and worth picking up.

## Migration scope: known-not-done

### `_init_db.py` macros never actually run

`bc/macros/_init_db.py` defines three SQLMesh `@macro` functions
(`init_db`, `create_enums`, `alter_types`) intended to fire from
`before_all`. **They never registered.** `sqlmesh.dbt.loader.DbtLoader._load_scripts()`
only globs `**/*.sql` in `macros/` — it never imports `.py` files.
`sqlmesh info` reports `Macros: 0`.

The source schemas (`event`, `game`, `box_score`, `misc`,
`baseballdatabank`, `biodata`) currently exist in `bc.db` only because
historical `dbt run-operation init_db` invocations populated them.
SQLMesh consumes pre-existing tables.

This means Phase 1 is **not** a clean dbt → SQLMesh cutover for the
source-loading step. dbt is still required as a prerequisite for a
fresh build.

**Fix options:**
- (a) Lift `_init_db.py` to a plain Python script (`scripts/init_sources.py`)
  invoked before `sqlmesh run`. Walk `source.yml`, write directly to
  duckdb. No SQLMesh macro machinery. Document as build prerequisite.
- (b) Find SQLMesh's actual `.py` macro discovery mechanism on the dbt-import
  path (may not exist; `SqlMeshLoader` would, but switching off `DbtLoader`
  loses the dbt model parser).
- (c) Stay as-is, document `dbt run-operation init_db` as Phase 1 prerequisite,
  punt to Phase 4 / DuckLake which will rebuild the source layer anyway.

Recommend (a) — small, honest, no platform fight.

## Surfaced model-quality bugs (not migration bugs)

After fixing the seed-loader NA bug (PatchedDbtLoader), 14 tables still
diverge between dbt and SQLMesh. All trace to **non-deterministic SQL
in the model layer or upstream data quality issues**, not to the
transition itself. Both engines run the same SQL; both produce valid
(different) outputs because the SQL is underspecified.

dbt's answer is not canonically more correct than SQLMesh's. The diff
harness is exposing latent non-determinism that has been silently
fluctuating between dbt builds — nobody noticed because dbt always
produced the same answer on the same machine.

### 1. `team_game_start_info` — window ties on bad upstream dh_status

Window `ORDER BY date, doubleheader_status` ties → `LAG()` picks an
arbitrary neighbor → `days_since_last_game` differs between engines.

Concrete example: BRO @ BSN doubleheader 1904-05-30. Both
`BRO190405301` and `BRO190405302` have `doubleheader_status = 'SingleGame'`
in `stg_games` (event-derived) — but `stg_gamelog` correctly labels them
`DoubleHeaderGame1` / `DoubleHeaderGame2`. `game_start_info` UNIONs
games over gamelog, so the wrong event-derived value wins.

Root cause is in upstream Retrosheet `1904.EBN` (Event Box) file or the
parser at `baseball.computer.rs`. EBN files are box-score-derived (no
play-by-play exists for 1904); the doubleheader-number header may not
be populated, parser falls through to `SingleGame` default.

**Fix options:**
- File issue against `baseball.computer.rs` to derive dh from game_id
  suffix when missing from header.
- In `game_start_info`, prefer `stg_gamelog.doubleheader_status` over
  `stg_games.doubleheader_status` when they disagree (gamelog is the
  authoritative scheduling source).
- Add `game_id` tiebreaker to windows in `team_game_start_info` —
  band-aid, doesn't fix the underlying wrong dh_status.

### 2. Other 13 mismatched tables

Not yet root-caused individually. Likely categories:

- **Park-factor cascade**: `calc_park_factors_basic`, `park_factors`,
  `unknown_fielding_play_shares`, downstream `metrics_*_fielding`.
- **Game-level cascade**: `game_scorekeeping`, `team_game_results`
  (has `LAG()` over win/loss streaks — same window-tie risk),
  `standings`, `leverage_index`.
- **Metrics cascade** (5 tables): `metrics_player_career_fielding`,
  `metrics_player_career_pitching`, `metrics_player_season_league_fielding`,
  `metrics_player_season_league_pitching`, `metrics_team_season_fielding`,
  `metrics_team_season_pitching`.

Investigation pattern: pick a table, dump diff rows via
`SELECT … EXCEPT SELECT …`, identify which column varies, trace back
to the responsible window or join.

## Cleanup punch-list (Phase 2)

These are deferred from Phase 1 plan as previously documented:

- Delete the 9 thin wrapper `metrics_*.sql` files — replaced by the
  blueprint-style `metric_table_body` macro but kept for now.
- Move `bc/` → `bc/_legacy/` — full directory restructure.
- Rename `spikes-sqlmesh` dep group → `migration`.
- Delete archived macros `init_db.sql`, `metrics_table_generator.sql`
  (`-- ARCHIVED` prefix kept for dbt-cli fallback during cutover).
- Remove `bc/macros/_init_db.py` if option (a) above is taken (it's dead
  code today; lifting to `scripts/` makes it explicitly a build script).
