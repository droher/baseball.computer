# Phase 2 — Open Follow-ups

Branch: `phase-2-ibis`. Goal: replace the two macro-based SQL-codegen
surfaces (`@metric_table_body`, `@batter_pitcher_park_factor`, plus the
two `calc_park_factors_*` custom-SQL models) with SQLMesh Python models
that compose Ibis expressions and a Pydantic Metric registry.

## What landed

### Library code (`bc/python_models/`)
- `_doc_lookup.py` — plain-Python re-export of `_doc_dict` for use in
  `@model(column_descriptions=...)`.
- `_enum_types.py` — `PARK_ID`, `TEAM_ID`, `PLAYER_ID`, `GAME_ID`,
  `HAND` as `exp.DataType(udt=True)` instances. The Python `@model`
  columns dict refuses raw `"PARK_ID"` strings — the SQL MODEL block
  parser is more permissive.
- `metrics/{registry,builders,_metric_registrations,_ibis_helpers,
  _constants}.py` — Pydantic `Metric` (formula or
  numerator+denominator), 148 registrations across (offense, pitching,
  fielding) × (season, event), and `build_metric_sql(kind, keys)`
  reproducing the `season → event → basic_stats / event_agg → final`
  CTE shape.
- `park_factors/{builder,advanced,basic}.py` — three SQL builders.
  `builder.batter_pitcher_park_factor(...)` keeps the macro's
  parameter shape (`rate_stats`, `denominator_stat`,
  `prior_sample_size`, `prev_years`, `filter_exp`, `batter_hand_split`,
  `use_odds`).

### Models migrated (17)
- 9 `bc/models/metrics/metrics_*.py` (career / season-league / team-season
  × offense / pitching / fielding)
- 6 `bc/models/analyses/calc_park_factor_*.py`
- 2 `bc/models/intermediate/park_factors/calc_park_factors_{advanced,basic}.py`

### Macros removed (stage G)
- `bc/macros/_metric_table_body.py`
- `bc/macros/_park_factors.py`

### Diff harness
- `scripts/diff_models.py` — row-by-row diff with per-column tolerances
  (`int=0`, `rate=1e-9`, `*_park_factor=1e-2`, `sqrt_sample_size=1`).
  Uses `IS DISTINCT FROM` for primary mismatch detection; tolerance
  applies only to finite-vs-finite pairs (so NaN/NaN and inf/inf
  count as equal).
- `scripts/diff_known_flaky.json` — baseline allowlist. Empty on
  first capture because the Phase 2 dev build happened to match
  prod for that run. If a later dev build surfaces drift on the
  models in "Carried over from Phase 1" below, run
  `python scripts/diff_models.py --capture-baseline` to seed the
  allowlist before re-running diff in CI.

## Carried over from Phase 1

The 13–14 latent-nondeterminism tables flagged in
`notes/phase-1-followups.md` (window-tie ordering, ENUM sort-order ties)
are not addressed. Phase 2 produces the same SQL semantics; the flake
remains. Allowlisted in `scripts/diff_known_flaky.json`.

In Phase 2 scope, this affects (per phase-1-followups.md "Park-factor
cascade" and "Metrics cascade"):
- `calc_park_factors_basic`
- `metrics_player_career_pitching`
- `metrics_player_career_fielding`
- `metrics_player_season_league_pitching`
- `metrics_player_season_league_fielding`
- `metrics_team_season_pitching`
- `metrics_team_season_fielding`

## New to Phase 2

### Decisions worth flagging

- The Pydantic `Metric` registry keys on `(name, kind)` rather than
  `name`. `walk_rate`, `strikeout_rate`, `home_run_rate`, and every
  event-based metric ship under both `offense` and `pitching` with
  the same name but different formulas.
- `bc/python_models/metrics/builders.py::build_metric_sql` imports
  `metrics_for` lazily inside the function body. SQLMesh's
  `serialize_env` walks function globals when packaging a model; a
  module-level reference to the `METRICS` dict (which contains
  Pydantic objects with lambda closures) produces unparseable
  `repr()` text at hydration time. Lazy import keeps the registry
  off the serialization path.
- "Ratio of ratios" metrics (e.g. `known_trajectory_out_hit_ratio =
  known_trajectory_rate_outs / known_trajectory_rate_hits`) inline
  the dependency in the lambda. The plan explicitly defers `derived`
  composition to Phase 3, when the BSL semantic layer turns it on.
- `calc_park_factors_advanced` cast-tower note: the spike's
  recommended fix (cast both UNION branches to `INT`) was applied
  during I2, but byte-equivalence against prod required reverting
  it — `INT` truncated the fractional priors and shifted the rare-
  stat park factors by up to 2.95. The Phase 2 build now reproduces
  the macro literally (`avg_X_per_pa * 1000::SMALLINT`, prior
  branch is DOUBLE; `multi_year_range` branch is INT; UNION widens
  to DOUBLE). The drift the spike documented on `sqrt_sample_size`
  was within the harness's ±1 ROUND-0 tolerance — no fix needed.

### Still flaky (allowlisted, future work)

- The 7 in-scope flaky models above. Still upstream-induced; rooted
  in `team_game_start_info` doubleheader handling (open issue against
  `baseball.computer.rs`) and ENUM sort-order ties.
- `team_game_start_info` doubleheader fix is upstream. Once the
  parser disambiguates dh-status from the `game_id` suffix, several
  cascade tables should stabilize.

### Imports

- `bc/python_models` modules use `from python_models.X` (the SQLMesh
  loader puts the `bc/` config dir on `sys.path`, not its parent).
  Scripts run from repo root must add `bc/` to `sys.path` themselves
  if they import the package directly.
- `bc/python_models/_doc_lookup.py` imports `from macros._docs import
  _doc_dict` for the same reason. The leading underscore is a
  reach-into; intentional, scoped to this package.

## Phase 3 entry checklist

Phase 3 = BSL semantic layer per `notes/migration-evaluation.md`
§"Phase 3". The `Metric` class already has a `derived` field; Phase 3
turns it on so e.g. `ops = obp + slg` becomes a true composition. That
refactor will:

- Replace inlined `formula=` lambdas for ratio-of-ratio metrics with
  `derived=lambda t, m: m["known_trajectory_rate_outs"] /
  m["known_trajectory_rate_hits"]`.
- Rewrite `build_metric_sql` to compute building-block metrics in
  pass 1 and derived metrics in pass 2 via Ibis `mutate()`.
- Reuse the Metric objects for the BSL semantic layer; the registry
  becomes the source of truth for both build-time and runtime
  metrics.

Until then, Phase 2 produces row-equivalent output through inlining.

## Audit sweep (sqlmesh-audit-sweep-event-level)

Branch `sqlmesh-audit-sweep-event-level` carried out three coordinated
pieces of work on top of the leverage cleanup:

1. **Audit coverage broadened**. 46 model files gained explicit `audits`
   blocks (event_level, season+game_level, box_score staging). Custom
   audit `bc/audits/unique_grain.sql` introduced — composite-key uniqueness
   (the built-in `unique_values((a, b))` checks each column individually,
   not the tuple). 27 existing usages of `unique_values((a, b))` migrated.
2. **Audit infra bugs fixed**:
   - `unique_values` per-column footgun (above).
   - `valid_baseball_season(birth_year)` removed from `people.sql` —
     1803 false positives on pre-1871 birth years (Cap Anson born 1832).
3. **Real data integrity findings (A–E) addressed**:
   - **A**: `seed_franchises.csv` got 5 rows for ATH (Sacramento 2025),
     BFA, CUS, HOE, LCB (Negro Leagues). OAK row's `date_end` set to
     `2024-09-26`.
   - **B**: `bc/models/intermediate/bio/people.sql:93` regex changed from
     `[a-z]{5}[01][0-9]{2}` to `[a-z\-]{5}[01][0-9]{2}` to accept the
     dashed retrosheet ID format used for ≤4-letter surnames
     (`lee-b104`, `puk-a001`, `lo--c001`). Was zeroing out
     `player_id` on 2602 people rows.
   - **C**: `player_team_season_pitching_stats.sql` Lahman supplement
     restructure. Was wholesale-replacing only seasons absent from
     `stg_games`; now FULL JOINs Lahman per `(season, team_id, player_id)`
     and COALESCEs 15 supplement-eligible stats per row. Fixes 1872 + 1874
     `batters_faced` NULLs (box source had all-NULL bfp; Lahman has the
     value). Same pattern not yet applied to
     `player_team_season_offense_stats` — known follow-up below.
   - **D**: `bc/python_models/park_factors/builder.py` got
     `HAVING SUM({denominator_stat}) > 0` on `lines_agg` to drop
     batter-pitcher pairs with 0 in the denominator (NaN source for
     `outs` variant — 2593 row failures gone). All `(1 - rate)` and
     ratio denominators in the odds calc wrapped with `NULLIF`.
   - **E**: `registration.py` got a `bounded_max` parameter
     (default `10.0`); the `hit_location` and `out_location` variants set
     `bounded_max=20.0` to accommodate genuine Negro League NN1/NN2
     small-sample park-factor outliers (sqrt_sample_size ~506; rates
     genuinely hit 1.0 for spatial-distribution stats).

### New open follow-ups

- **Partial-coverage SUMs**. The Lahman supplement only catches
  *all-NULL* upstream cases. For partial-coverage years (1901-07 BFP
  ~15-25% NULL rows in `stg_box_score_pitching_lines`, 1903-09 ER
  ~50-100% NULL), `SUM(stat)` produces a biased-low partial total
  (NULLs treated as 0) that is non-NULL, so COALESCE doesn't fire.
  Fix needs `IF(BOOL_OR(col IS NULL), NULL, SUM(col))` at both
  `box_agg` (in `player_game_pitching_stats.sql`) and the
  per-season SUM in `player_team_season_pitching_stats.sql` (the
  `@EACH(@combined_pitching_stats(), s -> SUM(@s)::INT AS @s)` macro
  expansion). Probably wants a `@nullable_sum` helper macro since
  the same pattern would apply to batting/fielding.
- **Apply Lahman supplement to batting**.
  `player_team_season_offense_stats.sql` likely has the same shape
  bug for early-NA SO/CS/SH (and the post-1920s SH/SF era when
  SF wasn't separated). Audits don't fail today (PA is non-null
  everywhere), but the latent NULLs propagate to downstream metrics.
  Mirror the pitching restructure.
- **Park-factor priors for sparse leagues**. Even with `bounded_max=20`,
  the 6 residual NN1/NN2 spatial-distribution outliers represent real
  data but extreme park factors. Could bump `prior_sample_size` per-league
  (e.g. 5000 for NN1/NN2 vs 1000 default) to dampen further if downstream
  use cases need it.

