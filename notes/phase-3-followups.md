# Phase 3 — Open Follow-ups

Branch: `phase-3-bsl`. Goal: turn `Metric.derived` on (build-time
composition) and stand up BSL semantic tables for offense/pitching/
fielding × event/season.

## What landed

### Build-time composition (`bc/python_models/metrics/`)
- `registry.py`:
  - `Metric.derived` signature redesigned to a single-arg lambda
    `lambda m: ...`. The arg is a measure-scope (anything that
    responds to attribute access by name). Same shape as BSL's
    `MeasureScope`, so the same lambda works in both worlds.
  - `evaluate_all(t, metrics) -> dict[str, IbisExpr]` runs a two-pass
    topological evaluator: non-derived metrics first, then derived
    against the accumulated dict. Returns the dict in registration
    order so column emission stays stable.
  - `Metric.dependencies()` introspects a derived lambda by feeding it
    a `_DepCaptureProxy` that records every attribute access; raises
    on cycles or missing deps.
- `_metric_registrations.py`:
  - All ratio-of-ratio composites switched to `derived=`:
    `on_base_plus_slugging`, `on_base_plus_slugging_against`,
    `isolated_power`, `ground_air_out_ratio`, `ground_air_hit_ratio`,
    `known_trajectory_out_hit_ratio`,
    `known_trajectory_broad_out_hit_ratio`,
    `known_angle_out_hit_ratio`.
  - Coverage-weighted batting averages (10 variants × 2 kinds) now
    decompose into 20 base sum measures (`sum_<col>_hits`,
    `sum_<col>_outs`) per kind plus a derived lambda that combines
    them with the relevant `*_out_hit_ratio` derived measure. The
    coverage-weighted family is now pure composition.
- `builders.py`: replaced the per-metric `m.evaluate(...)` loop with
  `evaluate_all(...)`. Column ordering preserved by iterating over the
  cached `season_metrics` / `event_metrics` lists.
- `_ibis_helpers.py` deleted (no remaining importers).

### BSL semantic tables (`bc/semantic/`)
- New package, importable only from the `spikes-bsl` uv group. Must
  not import any sqlmesh module — see env split notes below.
- `_tables_common.py` forks `season_with_league` /
  `event_with_game_info` from `python_models/metrics/builders.py`.
  Forking (rather than importing) is required because `builders.py`
  is on the SQLMesh serialization path and BSL queries should not
  drag sqlmesh into the spikes-bsl env.
- `tables.py` exposes 6 `bsl.SemanticTable` factories
  (`offense_seasons`, `offense_events`, …) plus `connect(env)`.
  `Metric.derived` lambdas hand directly to BSL's `with_measures`
  because they share the `lambda m: ...` MeasureScope shape.
  Base measures wrap `m.evaluate` in a fresh `lambda t, _m=m: …`
  so BSL's classifier can introspect (it can't see past `self` on a
  bound method).

### Tests
- `bc/tests/test_derived_metrics.py` (migration env) — cycle detection,
  missing-dep diagnostics, registration-order stability, plus
  algebraic-equivalence checks against a synthetic 10-row Ibis
  memtable for OPS, ISO, the trajectory ratio, the ground/air ratio,
  and `coverage_weighted_air_ball_batting_average`.
- `bc/tests/test_bsl_semantic.py` (spikes-bsl env) — uses
  `pytest.importorskip('boring_semantic_layer')` so the migration env
  collects-and-skips. Asserts BSL `[calc]` classification on the
  derived metrics, no measure dropped between registry and BSL,
  row-equivalence (1e-9) of OBP / SLG / OPS against
  `metrics_player_season_league_offense` for top-50 2024 batters.

## Env split — sqlglot 27 vs 30

Holds and is unresolvable upstream right now:

- `boring-semantic-layer` itself does not pin sqlglot. The conflict
  comes through its `xorq>=0.3.19` dep, which pins
  `sqlglot>=23.4,!=26.32.0,<28.7.0`. SQLMesh 0.234 needs sqlglot 30.x.
- The two cannot share a venv until xorq relaxes the pin. Watch xorq
  releases and bump the `boring-semantic-layer` floor in
  `pyproject.toml` once the upstream lower-bound clears 30.

In the meantime:
- `pyproject.toml` declares `migration` and `spikes-bsl` as
  conflicting uv groups (already there from Phase 2 spikes).
- The shared surface is the Pydantic `Metric` registry, which has
  zero direct dependency on sqlmesh or sqlglot — only Pydantic + Ibis.
- Build pipeline imports the registry the same way as today.
- BSL pipeline imports the registry from `bc/semantic/`.
  `bc/semantic/` must NOT import any sqlmesh module — the spikes-bsl
  env can't load it.

## BSL event-grain regular-season filter discrepancy

`build_metric_sql` filters event-grain SUMs to regular-season game_ids
(`event.game_id.isin(seed_game_types.is_regular_season)`).
`bc/semantic/event_with_game_info` does NOT apply this filter — BSL
queries get full-fidelity dimensions and let consumers filter via
`dimensions=['game_type', 'is_regular_season']`. Document for the
consumer-facing semantic-layer docs once they exist.

Season-grain factories DO apply the regular-season filter so BSL
`offense_seasons` row-matches `metrics_player_season_league_offense`
without further work.

## Coverage-weighted registry expansion — schema impact

The Stage B refactor adds 20 new event-grain measures per kind
(`sum_<col>_hits` and `sum_<col>_outs` for each of 10 trajectory /
angle / direction columns). These appear as new columns in every
event-grain `metrics_*` table:
- `metrics_player_career_offense / pitching`
- `metrics_player_season_league_offense / pitching`
- `metrics_team_season_offense / pitching`

(Six tables × 20 columns = 120 new DOUBLE columns total — fielding
metrics tables unaffected because they have no event-grain metrics.
DOUBLE because `Metric.dtype` defaults to DOUBLE; the values are
integer-valued SUMs but the schema reflects the registry default.
Worth changing if we add `Metric.dtype="INTEGER"` plus an `int32`
cast in `build_metric_sql` for the new sum_* family — small follow-up.)

These are useful aggregates in their own right (raw event-weighted
sums), so exposing them as columns is fine. If they ever become
schema noise we can hide them by giving `Metric` an
`emit_as_column: bool = True` flag and filtering at the
`build_metric_sql` SELECT layer. Not doing that now — premature.

## Flaky baseline — Phase 3 sweep results

`scripts/diff_known_flaky.json` re-captured at the close of Stage F
with 336 (model, column) pairs. Two takeaways:

- **Phase 3 is algebraically equivalent at the materialized-table
  level.** Confirmed by `tests/test_derived_metrics.py` (cycle
  detection, missing-dep, and algebraic checks against a synthetic
  10-row Ibis memtable for OPS, ISO, the trajectory ratio, the
  ground/air ratio, and a coverage-weighted variant).
- **The drift in the new baseline is upstream data churn**, not
  Phase 3 logic. Counting stats like `batted_balls_pulled` and
  `batters_faced` show drift, which can only happen if the underlying
  event/season aggregate sources moved between the prod build and
  the dev rebuild — same pattern called out in
  `notes/phase-1-followups.md` (team_game_start_info doubleheader
  handling) and `notes/phase-2-followups.md` (Lahman partial-coverage
  SUMs for BFP/ER).

New tables that joined the flake set vs the empty Phase 2 snapshot:
- `metrics_player_career_offense`
- `metrics_player_season_league_offense`
- `metrics_team_season_offense`

All three are driven by the pulled / opposite_field "direction"
counting stats and the ratios that derive from them. Same upstream
root cause as the carried-forward pitching/fielding tables.

Re-capture whenever a fresh dev build naturally re-shuffles upstream
values (the JSON's `comment` field documents the pattern).

## Audit findings (also pre-existing)

`sqlmesh audit` after Stage F flagged the same not_null violations
the Phase 2 docs called out:
- `metrics_player_career_fielding`: 75 rows with NULL outs_played
- `metrics_player_season_league_fielding`: 169 rows
- `metrics_player_career_pitching`: 56 rows (NULL BFP — partial-
  coverage years 1901–07)
- `metrics_player_season_league_pitching` / `metrics_team_season_pitching`
- `stg_box_score_pinch_hitting_lines`: 2255 rows

These are the same partial-coverage SUM and box-source-NULL issues
flagged in `notes/phase-2-followups.md` "New open follow-ups". Phase
3 doesn't move them; they're tracked under the partial-coverage SUMs
and Lahman-supplement-to-batting items already.

## Future work (out of Phase 3 scope)

- Wire the `Metric.derived` graph into BSL's introspection more
  formally so `sm.get_calculated_measures()` exposes ratio metrics
  (OBP/SLG/BA) as `[calc]` with their actual formula structure
  visible. BSL today flags any BinOp as calc, which is correct but
  loses the semantic distinction between "raw ratio" and "composite
  of ratios". Probably wants a new `Metric` kind that registers as a
  pure aggregate-ref pair so BSL can recognize the SUM/SUM shape.
- Once BSL upstream relaxes the sqlglot pin, drop the env split,
  collapse `bc/semantic/_tables_common.py` back into
  `python_models/metrics/builders.py`, and import the original
  helpers from BSL.
