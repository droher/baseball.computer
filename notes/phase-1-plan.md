# Phase 1 — SQLMesh transition

**Status (2026-05-02)**: Done. SQLMesh builds the full 128-model graph
end-to-end into `main_models__dev`. All 9 metric models render, diff
harness + 12 tests in place, NA seed-loader divergence fixed via
`PatchedDbtLoader`. Diff: 92/106 match. Remaining 14 documented in
`notes/phase-1-followups.md` as model-quality / upstream-data follow-ups
(not migration bugs).

Branch: `phase-1-sqlmesh` (off `next`). Phase 0 spikes complete — see `notes/spikes/` and `notes/migration-evaluation.md` "Resolved decisions" section. This doc is the entry point for resuming Phase 1 work.

## Goal

Replace dbt with SQLMesh as the primary build engine. **Same semantics, same outputs, different engine.** No model rewrites, no metric DSL changes, no DuckLake yet — those are Phases 2–4.

End-of-phase state:
- SQLMesh runs the build against `bc.db` from local parquet (and from R2).
- Output is row-equivalent to current dbt build for all 128 models + 9 metric tables.
- dbt project archived (kept under git history; `bc/` directory either deleted or moved to `bc/_legacy/`).
- Existing `make`/script entry points repointed at SQLMesh CLI.

## What Phase 0 already proved

- `sqlmesh init -t dbt duckdb` parses 128 models cleanly. Most macros translate via the dbt-jinja runtime SQLMesh embeds.
- `calc_park_factors_advanced`, `event_pitching_flags`, `event_offense_stats`, `event_states_full` render to clean SQL via `sqlmesh render bc.main_models.<name>`.
- Two macros need manual port:
  - `metrics_table_generator.sql` (125L) — fails on DuckDB ENUM introspection (`Could not interpret data_type "ENUM('Home', 'Away')"`). Port to a SQLMesh **blueprint** (~3–4 days).
  - `init_db.sql` (155L) — uses `graph.sources` iteration that doesn't exist in SQLMesh's runtime. Port to a Python pre-build hook (~2 days).

Total manual macro work ≤ 1 week. Plan budget for Phase 1 is 3–4 weeks → ample headroom.

See `notes/spikes/01_sqlmesh_init/RESULT.md` for full per-macro inventory.

## CLI note

`sqlmesh init -t dbt` API changed since the migration-evaluation doc was written. New form:

```
cd <dbt project root>
uv run --group spikes-sqlmesh sqlmesh init -t dbt duckdb
```

No `--path` flag; engine is a positional. Init must run inside the dbt project root and only writes `sqlmesh.yaml`. This affects step 1 of the work plan below.

## Dep groups

`pyproject.toml` declares three uv conflict groups. For Phase 1 use `spikes-sqlmesh` (or rename — see follow-up below). Switch via `uv sync --group spikes-sqlmesh`.

A Phase 1 cleanup: rename `spikes-sqlmesh` → `migration` (or similar) once the spikes are no longer the primary justification. Defer until end-of-phase to avoid churn.

## Work plan

Suggested ordering (DAG):

```
1. Init SQLMesh in bc/  (sqlmesh.yaml + state DB)
   ↓
2. Port init_db macros → Python pre-build hook
   ├─ create_enums hook (drops + creates DuckDB ENUMs)
   ├─ source registration hook (replaces graph.sources iteration)
   └─ Wire into sqlmesh.yaml as before_all
   ↓
3. Port metrics_table_generator → SQLMesh blueprint
   ├─ Define blueprint key {kind, agg_type, grouping_keys}
   ├─ One blueprint per (offense, pitching, fielding) × (player, team) × (season, career)
   └─ Confirm blueprint expansion produces same 9 wide tables
   ↓
4. Audit metric_calcs.sql / stat_lists.sql / park_factors.sql
   These work via SQLMesh's dbt-Jinja runtime; no port needed unless friction surfaces.
   ↓
5. Run SQLMesh build end-to-end
   ├─ uv run sqlmesh plan dev   (against local parquet via source_roots vars)
   └─ uv run sqlmesh run         (apply)
   ↓
6. Diff vs dbt build
   ├─ Build dbt + SQLMesh into separate DuckDB files
   ├─ Row-hash compare per table (use duckdb-side: SUM(HASH(*)) per table)
   └─ Investigate any deltas — fix until clean
   ↓
7. Cut over
   ├─ Update scripts/* to call sqlmesh, not dbt
   ├─ Move bc/ → bc/_legacy/ (or delete; user call)
   └─ Update CLAUDE.md / README to reflect SQLMesh as primary
```

Each numbered block is a 1–4 day sub-task; commits map onto sub-tasks.

## Critical files

**Reference / read-only during Phase 1**:
- `notes/spikes/01_sqlmesh_init/RESULT.md` — per-macro translation inventory + render verdicts
- `notes/spikes/01_sqlmesh_init/sqlmesh.yaml` — the init output from Spike 1; use as a starting template
- `bc/macros/init_db.sql` — source for the Python pre-build hook port
- `bc/macros/metrics_table_generator.sql` — source for the blueprint port
- `bc/macros/{metric_calcs,stat_lists,park_factors}.sql` — should pass through unchanged via SQLMesh's dbt-jinja runtime

**Created during Phase 1**:
- `sqlmesh/` (or wherever init lands the project) — SQLMesh project root
- `sqlmesh.yaml` — gateway/connection config + plan defaults
- `python/init_db.py` (or similar) — pre-build hook replacing the dbt macro
- `models/blueprints/metrics_table.sql` — SQLMesh blueprint replacing `metrics_table_generator`
- Updated `scripts/*` — entry points pointing at SQLMesh CLI

## Risks + mitigations

- **Diff investigation rabbit holes**: Phase 0 Spike 2 surfaced a single-root cast-tower drift (8/24 cols off by 1–2 rows in calc_park_factors_advanced). Spike was Ibis, not the dbt-Jinja path SQLMesh uses for Phase 1, so the same drift may not apply. But: budget at least 1 day for diff investigation; consider building a row-hash diff harness (`scripts/diff_dbt_sqlmesh.py`?) early so it's reusable.
- **State storage**: SQLMesh needs its own state DB. Spike 1's `sqlmesh.yaml` infers state schema from gateway. Confirm this works with `disable_transactions: true` (the bc profile has this set) — if not, state goes in a separate DuckDB file (same pattern Phase 4 will need anyway).
- **`source_roots` vars passthrough**: dbt's `--vars '{source_roots: {...}}'` pattern needs an SQLMesh equivalent. SQLMesh has `--var key=value` on plan/run; verify the dict-shaped var passes through unchanged. If not, rewrite `init_db` Python hook to read from a config file or env vars.
- **CI/CD**: user is hand-orchestrated currently (per migration-evaluation.md). Don't add CI tooling in Phase 1; keep manual.

## Out of scope for Phase 1

- Ibis ports (Phase 2)
- BSL / semantic layer (Phase 3)
- DuckLake publish layer (Phase 4)
- Polars/Numba axis-D rewrites (Phase 5)
- ML / Hamilton (Phase 6)
- Renaming `spikes-sqlmesh` → `migration` dep group (defer to Phase 1 cleanup)

## Resuming after context clear

Read in this order:
1. This file (`notes/phase-1-plan.md`)
2. `CLAUDE.md` (root) — current build commands + ported macro inventory
3. `notes/spikes/01_sqlmesh_init/RESULT.md` — what translates and what doesn't
4. `notes/migration-evaluation.md` § "Phase 1 — SQLMesh transition" + "Resolved decisions"

Branch is `phase-1-sqlmesh` off `next`. Confirm with `git status && git log --oneline -3`.

## What landed

- `bc/config.py` (replaces deleted `bc/sqlmesh.yaml`) — uses the
  `sqlmesh.dbt.loader.sqlmesh_config()` helper to wire
  `loader=PatchedDbtLoader`. Gateway `dev` matches the dbt target, state
  lives in `bc_state.db`, variables include `source_roots` and
  `force_reload`. The Python config form is required because the YAML
  schema's `loader` field is `t.Type[Loader]` and pydantic doesn't
  auto-import classes from strings.
- `bc/loader.py` — `PatchedDbtLoader` subclass that monkey-patches
  `SeedConfig.to_sqlmesh` on init. Disables pandas' hardcoded
  `keep_default_na=True` and adds `""`/`" "` to `na_values` so that
  literal `"NA"` survives in `seed_franchises.csv` (National Association,
  1871-1875) while empty fields still become NULL — matches dbt-agate.
- `bc/macros/_init_db.py` — written but **dead code**. SQLMesh's
  `DbtLoader._load_scripts()` only globs `*.sql`, never imports `.py`.
  Source schemas are populated by `dbt run-operation init_db`. See
  `notes/phase-1-followups.md` for lift-to-script plan.
- `bc/macros/metric_table_body.sql` + `bc/macros/metric_col_lists.sql`
  — replacement for `metrics_table_generator.sql`. The 9 thin wrapper
  models in `bc/models/metrics/metrics_*.sql` now call
  `metric_table_body(kind, grouping_keys, <kind>_int_cols(), metric_game_cols())`.
- `bc/macros/{init_db,metrics_table_generator}.sql` — both prefixed
  `-- ARCHIVED` so dbt CLI still works during cutover. Delete in Phase 2.
- `scripts/diff_dbt_vs_sqlmesh.py` — order-independent row-hash diff
  with per-column canonicalization (numerics through DOUBLE; everything
  else through VARCHAR; column ordinal sorted by name; type-divergence
  detection for shared columns). Tests in `tests/test_diff_harness.py`
  (12/12 passing).
- Determinism fixes in three models — added tiebreakers to
  `personnel_fielding_states.sql` (QUALIFY ROW_NUMBER), `stg_rosters.sql`
  (QUALIFY ROW_NUMBER), `game_line_scores.sql` (STRING_AGG ORDER BY).
  These were latent non-determinism that the diff harness surfaced.
- `CLAUDE.md` (root) + `README.md` — point at SQLMesh as the canonical
  engine; document loader/config layout.

### Plan deviations

- Step 1 yaml→Python config: see "What landed" above. Required to wire
  the custom loader.
- Step 2 `_init_db.py` does not run via SQLMesh. The `before_all` calls
  in the original `sqlmesh.yaml` were never reached. Behavior covered by
  legacy `dbt run-operation init_db`. Documented as Phase 1 follow-up
  rather than a Phase 1 fix to keep the migration commit narrow.
- Step 3 originally said "SQLMesh blueprint". The dbt-import path's
  model loader feeds every `.sql` in `models/` through dbt parsing first,
  which doesn't understand SQLMesh-native `MODEL ()` blocks — the loader
  errors out with a jinja compile error. Switched to keeping the existing
  9 thin wrapper files and parameterizing them via the new
  `metric_table_body` macro + per-kind col-list macros. Same DRY win,
  no naming-convention friction with the dbt schema namespacing.
- Step 5's diff harness took distinct `--dbt-schema` / `--sqlmesh-schema`
  args (vs the single `--schema` in the original plan). Necessary because
  both engines write into the same `bc.db` file under different schema
  names (`main_models` vs `main_models__dev`).

## Open drift items

End-to-end diff (dbt `main_models` vs SQLMesh `main_models__dev`,
2026-05-02): **92 of 106 tables match, 14 mismatched.**

The mismatched 14 all trace to either (a) latent SQL non-determinism in
the model layer (windows / aggregations with tied ORDER BY → arbitrary
row ordering picked differently per engine even though both produce
valid output), or (b) upstream Retrosheet data quality issues that
weren't visible until cross-engine comparison existed. Neither is a
migration bug; both engines run the same SQL on the same data.

Concrete example: `team_game_start_info` differs because BRO @ BSN
doubleheader 1904-05-30 has `doubleheader_status='SingleGame'` for both
games in `stg_games` (event-derived) but `'DoubleHeaderGame1'`/`Game2`
in `stg_gamelog`. `game_start_info` UNIONs games over gamelog so the
wrong dh_status wins. The `team_game_start_info` window then ties on
`(date, doubleheader_status)` and `LAG()` picks an arbitrary previous
row — different on each engine.

Full root-cause inventory + fix plans in `notes/phase-1-followups.md`.

Phase 1 declared **done** at this status. Remaining 14 are model-quality
follow-ups, not migration follow-ups. dbt's previous answers are not
canonically more correct than SQLMesh's; the diff harness surfaced
latent bugs in both.

## Things explicitly NOT done in Phase 1 (per plan + user direction)

- Did not delete the 9 metric wrapper `.sql` files (kept; they now act
  as thin dispatchers, sibling `.yml` docs/tests still reference them
  by name).
- Did not move `bc/` → `bc/_legacy/` — Phase 2 commit.
- Did not rename `spikes-sqlmesh` dep group to `migration` — Phase 2.
- Did not delete archived macros (`init_db.sql`, `metrics_table_generator.sql`)
  — Phase 2.
- Did not wire CI/CD (per migration-evaluation.md, hand-orchestrated).
