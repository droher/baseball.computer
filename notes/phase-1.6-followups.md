# Phase 1.6 — Pre-Phase-2 cleanup outcome

Branch: `phase-1.6-cleanup`. Goal: enter Phase 2 (Ibis ports) on a
SQLMesh-native footing — no dbt artefacts on disk, no jinja macros,
no jinja shim, single-source-of-truth ENUM list.

## What landed

### dbt artefacts removed
- `bc/dbt_project.yml`, `bc/packages.yml`, `bc/package-lock.yml`
- `bc/dbt_packages/` (~1.3 MB), `bc/target/` (~17 MB), `bc/logs/dbt.log*`
- `scripts/migration/` (4 conversion scripts), `scripts/diff_dbt_vs_sqlmesh.py`
- `tests/test_diff_harness.py` + the `tests/` tree (now empty)

### jinja macros ported to Python `@macro`
| Old `.sql` macro | New Python module | Notes |
|---|---|---|
| `event_id_to_key.sql` | `bc/macros/_event_id_to_key.py` | 2 funcs, returns SQL fragment |
| `stat_lists.sql` | `bc/macros/_stat_lists.py` | Lists as module constants, exposed as `@macro` returning `list[str]`; pitching list is offense-derived at module load |
| `metric_calcs.sql` + `metric_col_lists.sql` + `metric_table_body.sql` | `bc/macros/_metric_table_body.py` | Single `@metric_table_body(kind, *grouping_keys)` macro; per-kind int-cols / basic-rate / event-based dicts as constants. The 9 metrics models reduce to one line each. |
| `park_factors.sql` (`batter_pitcher_park_factor`) | `bc/macros/_park_factors.py` | + the advanced-park-factor stat-list macros for `calc_park_factors_advanced` |

### Models de-jinjafied
44 `JINJA_QUERY_BEGIN ... JINJA_END` blocks across 25 files removed.
For-loops over stat lists become SQLMesh `@EACH(@list_macro(), x -> ...)`
calls. `calc_park_factors_advanced` uses `@EACH` over the stat-list
macros from `_park_factors.py`.

The six game-level / season-level aggregators
(`team_game_offense_stats`, `team_game_pitching_stats`,
`player_game_offense_stats`, `player_game_pitching_stats`,
`player_team_season_offense_stats`,
`player_team_season_pitching_stats`) had per-stat conditional cast
types in their jinja loops. The offense and combined-pitching tables
use `USMALLINT`/`UTINYINT` for normal counters and `INT1` for
`surplus_*`. The player-level pitching aggregator follows a different
rule (carried over verbatim from the original jinja): `INT2` for any
column whose name contains `bases_advanced` (so
`surplus_bases_advanced_on_balls_in_play` is `INT2`, not `INT1`),
`USMALLINT` for `pitches*`, `UTINYINT` for everything else.

These collapse into Python `@macro`s (`offense_sum_utinyint`,
`offense_sum_usmallint`, `pitching_combined_sum_usmallint`,
`player_pitching_sum_block`) under `bc/macros/_stat_lists.py`, each
returning a `list[str]` of `SUM(col)::TYPE AS col` fragments — SQLMesh
splices the list into the SELECT as comma-separated items.

### Loader shim deleted
- `bc/loader.py` (BcSqlMeshLoader subclass) — gone.
- `bc/jinja_globals.py` (return / ref / source / exceptions injection) — gone.
- `bc/config.py`: dropped `loader=BcSqlMeshLoader`, `sys.path.insert`,
  and the dbt-profile reference comment block.

### `bc/analyses/` ported and deleted
- 6 park-factor analyses → `bc/models/analyses/calc_park_factor_*.sql`
  as `kind VIEW` models calling `@batter_pitcher_park_factor(...)`.
- 25 other analyses → `bc/models/analyses/<name>.sql` (kind VIEW),
  `{{ ref('foo') }}` substituted to `main_models.foo` (and to
  `main_seeds.seed_*` for seed refs), `{{ config(...) }}` and stale
  `{# ... #}` jinja comments stripped. Bulk-ported via a one-shot
  Python script.
- Several analyses had pre-existing SQL bugs that compiled cleanly only
  under dbt because `analyses/` is documentation-only there. Fixes
  applied during the port:
  - `game_data_completeness`, `player_game_data_completeness`: the
    `LEFT JOIN event_completeness_fielding_credit` /
    `LEFT JOIN event_completeness_pitches` lines were jinja-commented in
    the original; uncommented because the upstream models now exist as
    real `main_models.*` views.
  - `outfield_hits`: jinja-commented `FROM game_data_completeness AS g`
    re-enabled — `WHERE g.has_*` references it.
  - `hit_vs_out_spray_air`: same shape — added `FROM main_models.game_data_completeness AS g`.
  - `assists_as_putouts_finder`, `box_event_fielding_discrepancies`,
    `unknown_play_no_box`, `unknown_plays`: ambiguous `filename` /
    `line_number` qualified to the `stg_event_audit` alias.
  - `unknown_plays`: `season` did not resolve from any joined table;
    derived as `EXTRACT(YEAR FROM date)`.
  - `scorekeeper_tendencies_batter`, `scorekeeper_tendencies_contact`:
    `COUNT_IF(<int_col>)` rejected by DuckDB (TINYINT, not BOOLEAN);
    rewrote to `COUNT_IF(<int_col> > 0)`.
  - `ground_ball_hits`: original was a CTE with no terminating SELECT
    and a stale `#}` artefact; gave it the `REGR_R2(...)` SELECT the
    inline comments described as the intended query.
  - `player_completeness`: `has_play_by_play` column doesn't exist on
    `player_game_data_completeness` (it's a `game_data_completeness`
    column); dropped the line.
- `scratch.sql` and `bc/analyses/notebooks/` (Jupyter scratchpads) were
  not ported — they aren't analyses, they're loose dev notes.
- `bc/macros/park_factors.sql` and the `bc/analyses/` tree both gone.

### `_init_db.py` refactor
- Single `_ENUM_DEFS: list[tuple[str, str]]` source of truth. DROP
  statements derive from `reversed(_ENUM_DEFS)` so child ENUMs drop
  before parents that reference them; CREATE statements derive from the
  same list in forward order.
- `_external_models_path()` + `_seeds_dir()` collapse into
  `_project_root()`.
- `_force_reload`, `_cache_bust`, `_source_roots` one-liners inlined at
  their (single) call sites.
- New `_no_quote(s, label)` helper replaces three open-coded
  single-quote injection guards.
- `_logger()` factory stays — SQLMesh `serialize_env` rejects
  module-level `Logger` instances.

## Verification

- `cd bc && uv run --group spikes-sqlmesh sqlmesh info` —
  179 models, 16 macros, both connections succeed.
- `sqlmesh render` spot-checked against:
  `metrics_player_career_offense`, `metrics_team_season_pitching`,
  `metrics_player_season_league_fielding`, `event_pitching_stats`,
  `calc_park_factors_advanced`, `calc_park_factor_hit_location`. Each
  produces SQL that's semantically equivalent to the prior render
  (some trailing-comma whitespace differs; DuckDB tolerates both).
- `sqlmesh plan dev --auto-apply` rebuilt ~70 models; all audits ran
  green. (Outcome captured at the bottom of this file.)

## Open follow-ups

### Carried over from Phase 1
- 13–14 models with latent non-determinism still diverge between
  pre-cleanup and post-cleanup outputs by a small amount in the same
  shape Phase 1 already documented (window-tie ordering, ENUM
  sort-order ties on small inputs). See `notes/phase-1-followups.md`
  for the catalogue. Phase 1.6 doesn't touch any of these — same SQL.

### New to Phase 1.6
- `_metric_table_body.py` and `_park_factors.py` use
  `# pyright: reportPrivateImportUsage=false` indirectly via
  `from sqlglot.expressions.core import Expression`. Pyright strict
  mode flags the `arg.this` accesses on Literal/Boolean as `Any`.
  Acceptable: SQLMesh's macro API is loosely typed at the boundary,
  `arg.name` paths cover the usable cases.
- `bc/macros/_init_db.py` is 360 lines (vs the plan's <250 target).
  The ENUM list itself is ~80 lines and trying to compress it further
  hurts readability. Acceptable.
- `scripts/diff_dbt_vs_sqlmesh.py` and the bigger
  `scripts/diff_duckdb_schemas.py` rename suggested in
  `phase-1-followups.md` are now moot — the harness is gone.

## Phase 2 entry checklist

Phase 2 = Ibis ports per `notes/migration-evaluation.md` §"Phase 2".
Prerequisites the cleanup makes available:

- [x] Models are pure SQLMesh syntax (no jinja). An Ibis-built CTE tree
      can drop in as a `MODEL (...)` body via `f"...{ibis_query.compile()}..."`
      or by switching the model to a SQLMesh Python model.
- [x] `_metric_table_body.py`'s constants (`_OFFENSE_INT_COLS`,
      `_BASIC_RATE_OFFENSE`, `_BATTED_BALL_STATS`, etc.) are already
      Python objects — Phase 2 can replace each formula string with an
      `ibis.expr` lambda without re-parsing SQL.
- [x] `_park_factors.py` `batter_pitcher_park_factor` is a single
      Python function returning SQL — easiest target for Phase 2's
      Bayesian-shrinkage Ibis port.
- [x] `bc/analyses/*` analyses are real SQLMesh views, so Phase 2 can
      port any of them to Python models without first rescuing them
      from a dbt-only directory.
- [ ] Renames suggested in `phase-1-followups.md` still pending:
      `spikes-sqlmesh` dep group → `migration` (deferred, low priority).

## CLAUDE.md sync

`/Users/davidroher/Repos/baseball.computer/CLAUDE.md` referenced
`bc/loader.py`, `bc/jinja_globals.py`, the migration scripts, and the
diff harness. Update during this branch's merge:

- Drop the "Custom loader (jinja builtins)" paragraph entirely.
- Drop the "Diff harness" paragraph (`scripts/diff_dbt_vs_sqlmesh.py` gone).
- Repo-structure tree should drop `loader.py`, `jinja_globals.py`,
  `scripts/` migration mentions, and add `bc/models/analyses/`.
- Phase status: Phase 1.6 closes the dbt-cleanup loop; Phase 2 is the
  next planned milestone.
