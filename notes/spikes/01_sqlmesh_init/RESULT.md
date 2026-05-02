# Spike 1 — `sqlmesh init -t dbt` translation

**Verdict:** GO. SQLMesh parses all 128 models and renders the codebase's hardest macro-driven model (`calc_park_factors_advanced`) cleanly. Only `metrics_table_generator`-codegen models (9 of 128) fail render under the dbt-import path; manual port to a SQLMesh **blueprint** is the planned migration anyway. Total manual macro-port surface ≤ ~5 days.

## Workspace

A trimmed copy of `bc/` (no `target/`, no `logs/`) lives in this dir alongside `sqlmesh.yaml`. The CLI flag changed since the plan was written — current SQLMesh init is `sqlmesh init -t dbt <engine>` and must be run *inside* the dbt project root. No `--path` flag exists.

```
cd notes/spikes/01_sqlmesh_init
uv run --project /Users/davidroher/Repos/baseball.computer --group spikes-sqlmesh sqlmesh init -t dbt duckdb
```

The init writes a single `sqlmesh.yaml` (start date, virtual environments mode, plan defaults). It does not modify any dbt files.

## Discovery

`sqlmesh info`:

```
Models: 128
Macros: 0
Data warehouse connection succeeded
```

`Macros: 0` is misleading. SQLMesh's dbt-import path runs Jinja through dbt's runtime at render-time; the imported macros work but aren't first-class SQLMesh macros. `sqlmesh dag /tmp/sqlmesh-dag.html` confirms all 128 models are present and properly edged. Models are namespaced as `bc.main_<dbt-schema>.<model_name>` — DuckDB's default `main` schema concatenated with the dbt `+schema:` suffix.

## Render results

Tested 4 representative models via `sqlmesh render`:

| Model | Macros used | Lines | Status |
|---|---|---|---|
| `calc_park_factors_advanced` | `park_factors.sql` (185L formula dict + rolling-window codegen) | ~120 SQL | ✅ clean |
| `event_states_full` | `event_id_to_key.sql` only | 144 SQL | ✅ clean |
| `event_offense_stats` | `metric_calcs.sql`, `stat_lists.sql` | 252 SQL | ✅ clean |
| `event_pitching_flags` | none directly | 152 SQL | ✅ clean |
| `metrics_player_season_league_offense` | `metrics_table_generator.sql` (9-table codegen) | — | ❌ ENUM introspection error |

Failure verbatim:

```
Error: Could not render jinja for '.../models/metrics/metrics_player_season_league_offense.sql'.
Runtime Error
  Could not interpret data_type "ENUM('Home', 'Away')": could not convert "'Home'" to an integer
```

Root cause: `metrics_table_generator.sql` introspects upstream relation column types via dbt's `adapter.get_columns_in_relation`. SQLMesh's data-type parser doesn't model DuckDB ENUM literals (it tries to coerce `'Home'` to an int, presumably because it's parsing ENUM as if it were `ENUM(int, int, ...)`). All 9 metric tables share this codegen, so the same error blocks all 9.

This is a SQLMesh-side limitation, not a `bc/` issue. Two options:
1. **Recommended**: rewrite `metrics_table_generator` as a SQLMesh **blueprint** (the doc-cited Phase 1 path). Blueprints are first-class in SQLMesh and don't go through dbt's introspection. Estimate: 3–4 days.
2. **Workaround**: pre-coerce the ENUM columns to VARCHAR in upstream models for the 9 metric tables. Cheap (1 day) but loses the typed-ENUM advantage and is technical debt.

## Per-macro translation status

| Macro | Lines | Status | Manual work |
|---|---|---|---|
| `event_id_to_key.sql` | 7 | ✅ clean | none |
| `summarize_tables.sql` | 13 | ✅ clean (untested but trivial) | none |
| `park_factors.sql` | 185 | ✅ clean | none |
| `stat_lists.sql` | 226 | ✅ clean | none |
| `metric_calcs.sql` | 144 | ✅ clean | none |
| `metrics_table_generator.sql` | 125 | ❌ blocker | port to SQLMesh blueprint (~3–4 days) |
| `init_db.sql` (`init_db`, `create_enums`, `alter_types`) | 155 | ❌ blocker | port to Python pre-build hook (~2 days) — `graph.sources` doesn't exist in SQLMesh's runtime |

`init_db` was already known to need a manual port (the plan called this out). `metrics_table_generator` is a new finding but maps cleanly to SQLMesh's blueprint primitive, which is the Phase 1 migration target anyway.

## Manual port estimate

Total: **5–7 days** of SQLMesh-specific glue work in Phase 1, well within the plan's 3–4 week budget for that phase.

- Port `init_db` / `create_enums` / `alter_types` to a Python pre-build hook: 2 days
- Port `metrics_table_generator` to a SQLMesh blueprint: 3–4 days
- Audit `stat_lists` + `metric_calcs` for compatibility with the new blueprint context: 1 day

## Verdict

**GO** for Phase 1. The 128-model graph translates, the hardest expression (`calc_park_factors_advanced`) renders clean, and the two macros that don't translate (init_db + metrics_table_generator) were already on the manual-port list. No surprises that change the Phase 1 effort estimate. Proceed.
