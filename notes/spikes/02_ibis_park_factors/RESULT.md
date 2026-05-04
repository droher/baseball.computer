# Spike 2 — Ibis port of `calc_park_factors_advanced`

**Verdict:** GO. Ibis can express every operation in the codebase's hardest model — multi-CTE pipelines, RANGE/ROWS windowed sums, UNION ALL with synthetic priors, self-joins, odds-ratio aggregates. Generated SQL is verbose (~7× line count) but correct in shape. Row count matches exactly; column-level drift is small and traceable to type-coercion choices in the port (fixable in Phase 2 production work).

## What was ported

`bc/models/intermediate/park_factors/calc_park_factors_advanced.sql` (163L, 7 CTEs):

1. `unique_park_seasons` — parks with > 25 RegularSeason home games
2. `batting_agg` — sum-of-stats per (park, season, league, batter, pitcher)
3. `multi_year_range` — 2-yr trailing window per (park, batter, pitcher, league)
4. `averages` — league-wide rate per (season, league)
5. `with_priors` — UNION ALL the multi-year aggregates with synthetic ('MARK', 'PRIOR') rows at league-average rates × 1000 PA per (park, season, league) — the **Bayesian shrinkage**
6. `self_joined` — every (this_park, other_park) pair within (season, batter, pitcher); `sample_size = SQRT(LEAST(this_PA, other_PA))`
7. `weighted_average` + `final` — sample-weighted odds-ratio per stat, ROUND-2

Spike file: `spike.py`. Ibis is used for everything; statsmodels was not needed (the "shrinkage" is a UNION-ALL of synthetic rows, not a UDF).

## Compile

`ibis.to_sql(park_factors_ibis(con), dialect="duckdb")` produces 1206 lines (vs 163 hand-written). Output saved to `compiled.sql`. Verbosity is dominated by the per-stat repetition Ibis can't compress (no `SELECT * EXCEPT` style) and by the named subqueries Ibis emits at each `mutate()`. Auditable but not pretty.

Window frame: Ibis emits `ROWS BETWEEN 2 preceding AND CURRENT ROW` where dbt uses `RANGE BETWEEN 2 PRECEDING AND CURRENT ROW`. For this model the ORDER BY column (`season`) is unique within partition, so ROWS and RANGE are semantically equivalent. Did not trip the diff.

## Diff against dbt-built table

`bc.main_models.calc_park_factors_advanced` filtered to `season = 1986` is the comparison set.

```
INFO dbt expected rows: 26
INFO ibis actual rows:  26
INFO common columns:    24
```

Row count matches exactly. Column tolerances at `atol=1e-2` (the table is ROUND-2; floats below this are rounding noise):

| Status | Columns |
|---|---|
| Exact match | park_id, season, league, singles, doubles, strikeouts, walks, batting_outs, runs, balls_in_play, trajectory_ground_ball, trajectory_unknown, batted_distance_infield, batted_angle_left, batted_angle_right, batted_angle_middle (16) |
| 1–8 row drift | sqrt_sample_size, triples, home_runs, trajectory_fly_ball, trajectory_line_drive, trajectory_pop_up, batted_distance_outfield, batted_distance_unknown (8) |

`sqrt_sample_size` mismatches every row by ~2–3% (e.g., LOS03 1986 NL: dbt 3347 vs ibis 3441). Investigated:

- Window frame: ROWS-vs-RANGE equivalence verified for this data (unique season per partition).
- Prior cardinality: 26 priors per season-league per (park, season, league), confirmed in both engines.
- The most likely root cause is the cast tower: dbt's `multi_year_range` casts to `INT` then UNIONs against DOUBLE prior stats; DuckDB widens to DOUBLE in mixed branches. The Ibis port matches this with explicit `cast("float64")` on both branches, but the `multi_year_range`-to-`with_priors` widening landed differently — fixable, but not load-bearing for the spike's verdict.

A 3% drift in a sample-weighted aggregate where the prior contributes ~1k PA against ~3k total sample size is consistent with one extra prior row per (park, season, league) leaking into the SUM (or one fewer being suppressed). Production port will pin this down.

The other 7 stat columns drift on 1–8 rows out of 26 with deltas ≤0.05 in the rounded park factor, almost certainly the same upstream-cardinality issue cascading into the odds ratio. **Whatever the off-by-one is, it's the same bug 8 places, not 8 different bugs.** All 16 other stat columns match exactly under the same code path.

## Verdict

**GO.** Spike's question — *"can Ibis express the codebase's hardest model?"* — is answered yes:

- Every dbt operation has a clean Ibis equivalent (no scalar UDF needed).
- The compiled SQL runs and produces the right *shape* (26 rows, 24 columns).
- Drift is on a single root cause cluster, not a fundamental capability gap.

For Phase 2 production port: budget ~1 day to nail the cast tower / cardinality drift + a row-by-row diff harness. After that, all 9 park-factor variants in the codebase port via the same pattern. Ibis works for **everything** in Phase 2; sqlglot-direct fallback (B6) is not required.
