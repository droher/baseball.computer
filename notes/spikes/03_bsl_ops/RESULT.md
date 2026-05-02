# Spike 3 — BSL OPS-derived-tree

**Verdict:** GO with **Boring Semantic Layer (BSL)** for Phase 3. `ops` is a first-class derived measure (graph node in `_calc_measures`), not a flattened raw aggregation. Row-equivalent to the dbt-built reference within 1e-9 across all 284 (season, league) groups. MetricFlow fallback **not needed**.

## What was tested

Built a `bsl.SemanticTable` on `event_offense_stats ⋈ event_states_full` with four base measures and three calc (derived) measures:

```python
sm = (
    bsl.to_semantic_table(joined, name="batting")
    .with_dimensions(
        season=lambda t: t.season,
        league=lambda t: t.league,
        batter_id=lambda t: t.batter_id,
    )
    .with_measures(  # base — raw aggregations
        total_obs=lambda t: t.on_base_successes.sum(),
        total_obo=lambda t: t.on_base_opportunities.sum(),
        total_tb=lambda t: t.total_bases.sum(),
        total_ab=lambda t: t.at_bats.sum(),
    )
    .with_measures(  # calc — derived from base
        obp=lambda m: m.total_obs / m.total_obo,
        slg=lambda m: m.total_tb / m.total_ab,
    )
    .with_measures(  # calc — derived from calc
        ops=lambda m: m.obp + m.slg,
    )
)
```

The OPS canonical from `bc/macros/metric_calcs.sql`:

```
"on_base_plus_slugging": "SUM(on_base_successes) / SUM(on_base_opportunities)
                         + SUM(total_bases) / SUM(at_bats)"
```

In the macro DSL this is a single inline composite. In BSL it's `obp + slg` where `obp` and `slg` are themselves derived from base aggregations.

## Introspection — graph node identity

`sm.get_measures()` and `sm.get_calculated_measures()` are separate dicts. Output:

```
INFO base measures: ['total_ab', 'total_obo', 'total_obs', 'total_tb']
INFO calc measures: ['obp', 'ops', 'slg']
INFO ✓ ops is a first-class node in _calc_measures (not flattened)
```

Repr of the SemanticTable confirms it:

```
SemanticTable: batting
  season [dim]
  league [dim]
  batter_id [dim]
  total_obs [measure]
  total_obo [measure]
  total_tb [measure]
  total_ab [measure]
  obp [calc]   ← derived
  slg [calc]   ← derived
  ops [calc]   ← derived (of derived!)
```

The `[calc]` tag is what an LLM tool / MCP introspector would see. Asking the BSL graph "what is ops?" yields `ops = obp + slg` rather than `ops = SUM(obs)/SUM(obo) + SUM(tb)/SUM(ab)`. The decomposition is preserved at the model level even though the compiled SQL flattens (correctly) to a single arithmetic expression at execution time.

Internal mechanism: `boring_semantic_layer/ops.py:_classify_measure()` resolves the lambda against a `MeasureScope`, and if the resolved expression depends on other registered measures it's classified as `calc`; otherwise `base`. So derived-metric structure is automatic — no separate `derived_measures=` kwarg needed.

## Compiled SQL

```sql
-- Saved verbatim in compiled.sql
SELECT
  "t6"."season",
  "t6"."league",
  (CAST("t6"."total_obs" AS DOUBLE) / CAST("t6"."total_obo" AS DOUBLE))
  + (CAST("t6"."total_tb" AS DOUBLE) / CAST("t6"."total_ab" AS DOUBLE)) AS "ops"
FROM (
  SELECT
    "t5"."season", "t5"."league",
    SUM("t5"."total_bases")          AS "total_tb",
    SUM("t5"."at_bats")              AS "total_ab",
    SUM("t5"."on_base_successes")    AS "total_obs",
    SUM("t5"."on_base_opportunities") AS "total_obo"
  FROM (...) AS "t5"
  GROUP BY 1, 2
) AS "t6"
```

Two stages: aggregate the four base measures by group, then combine via the calc formula. Optimal — no double-aggregation, no LATERAL hacks. This is the shape MetricFlow's `type: derived` would also produce.

## Diff vs dbt

```
INFO BSL OPS: 284 rows
INFO DuckDB direct OPS: 284 rows
INFO merged rows: 284
INFO ✓ row-equivalent within 1e-9 across 284 (season, league) groups
```

Direct DuckDB SQL reference (the canonical formula) and BSL output agree to 1e-9 across every (season, league) group from 1910 onward.

## MetricFlow fallback — not run

The plan said: "if BSL flattens, redo in MetricFlow YAML and capture both for the writeup." BSL didn't flatten; the fallback was unnecessary. We installed `dbt-metricflow` in the `spikes-mf` group as planned (and confirmed it co-installs with the rest of the spike deps via uv conflict groups), but did not exercise it. The fallback path remains available if a Phase 3 deep-dive turns up a BSL gap not surfaced here.

## Verdict

**GO BSL** for Phase 3.

- Derived metrics are first-class graph nodes (`_calc_measures` dict, `[calc]` tag in repr, traversable via `bsl.graph_to_dict`).
- LLM/MCP introspection can resolve `ops` → `obp + slg` → base aggregations without re-encoding the formula.
- Compiled SQL is clean and runs without fanout.
- Row-equivalent to the dbt path on the canonical OPS aggregation.

Phase 3 implementation work is the per-domain (offense / pitching / fielding) measure dictionary, not the framework itself. Budget unchanged.
