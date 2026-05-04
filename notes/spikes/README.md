# Phase 0 — Verification Spikes

Six 1-day spikes gating Phase 1 of the Stack 3 migration (SQLMesh + Ibis + BSL/MetricFlow + Hamilton + DuckLake). See `notes/migration-evaluation.md` for full context.

## DAG

```
                          ┌── (independent, no install) ──┐
                          │                                │
   Setup base ─┬─► Spike 6 (DuckLake adapter status, web research)
   (next       │
    branch +   ├─► Spike 5 (Polars 1.31 bump) ──┐
    spikes     │                                 ├─► Spike 4 (Polars FSM rewrite)
    dep        ├─► Spike 1 (sqlmesh init -t dbt) │
    groups)    │                                 │
              ├─► Spike 2 (Ibis park factors) ──┤
              │                                  │
              └─► Spike 3 (BSL OPS tree) ───────┘
                                                 │
                                                 ▼
                                Phase 0 writeup + go/no-go ─► open Phase 1 branch off `next`
```

## Dep groups

`pyproject.toml` declares three mutually exclusive groups (`tool.uv.conflicts`) because sqlmesh 0.234 requires `sqlglot~=30.4.2` while BSL 0.3.12 (via xorq) caps `sqlglot<28.7`:

- `spikes-sqlmesh` — sqlmesh + ibis + polars 1.31 + numba (Spikes 1, 2, 4, 5)
- `spikes-bsl` — boring-semantic-layer + ibis (Spike 3 primary)
- `spikes-mf` — dbt-metricflow (Spike 3 fallback)

Switch with `uv sync --group <name>`.

## Status

| # | Spike | Workspace | Group | Status | Verdict |
|---|---|---|---|---|---|
| 1 | sqlmesh init -t dbt translation | 01_sqlmesh_init/ | spikes-sqlmesh | done | ✅ GO — 128 models parse, 1 macro (metrics_table_generator) blocked on ENUM introspection → port to SQLMesh blueprint (~3–4d). Plus init_db port (~2d). Total manual work ≤1 wk |
| 2 | Ibis port of calc_park_factors_advanced | 02_ibis_park_factors/ | spikes-sqlmesh | done | ✅ GO — 26/26 row-equivalent shape; 16/24 cols exact, 8 cols with ≤1 single root-cause cast-tower drift. Production port budgets 1 day to nail. Ibis sufficient — sqlglot-direct fallback (B6) not needed |
| 3 | BSL OPS derived-tree (+ MF fallback) | 03_bsl_ops/ | spikes-bsl | done | ✅ GO BSL — `ops` is first-class `[calc]` graph node; row-equivalent across 284 (season, league) groups within 1e-9. MetricFlow fallback NOT needed |
| 4 | Polars `forward_fill().over()` for event_pitching_flags | 04_polars_fsm/ | spikes-sqlmesh | done | ✅ GO pure Polars — 13/13 cols row-equivalent on 225K-row season slice. `LAG IGNORE NULLS` ≡ `forward_fill().shift(1).over(...)`. Numba fallback (D4) NOT needed |
| 5 | Polars 1.31 scratch.ipynb smoke test | 05_polars_bump/ | spikes-sqlmesh | done | ✅ GO — Polars 1.40.1 (resolved by `>=1.31`) clean: 0 warnings on smoke test exercising arrow-zero-copy, `.over()`, `forward_fill()`, streaming collect |
| 6 | DuckLake + dbt-duckdb adapter status | 06_ducklake_status/ | (web research + duckdb proof) | done | ✅ GO with **SQLMesh-driven publish** in Phase 4. dbt-duckdb 1.10.1 supports DuckLake via `type: ducklake` / `is_ducklake: true` but feature-thin (no partition_by, no SORTED BY). DuckDB-native ATTACH round-trip works on duckdb 1.5.2 |

## Aggregate verdict

All six spikes pass. Phase 1 (`phase-1-sqlmesh`) opens off `next` — no surprises that move the migration roadmap.
