# Spike 5 — Polars version bump

**Verdict:** GO. Polars 1.40.1 (resolved by `>=1.31` constraint) imports clean and exercises every API the codebase + Spike 4 will use. Zero deprecation warnings.

## What got tested

`scratch.ipynb` (the only repo file that imports polars; gitignored at the repo root) only does `import polars as pl` plus a `CHUNK_SIZE` constant in the saved cells. No live polars expressions. Smoke-testing the notebook itself proves nothing about the upgrade.

Instead `smoke.py` (next to this writeup) exercises the API surface that Spike 4 + axis-D Phase 5 will rely on, against the existing `bc.db`:

1. `pl.from_arrow(duckdb.sql(...).arrow())` — zero-copy arrow → polars
2. `lf.with_columns(pl.col(...).cum_sum().over(...))` — windowed cumsum
3. `forward_fill().over("g", order_by="i")` — the canonical Spike 4 primitive
4. `con.sql(...).fetchone()` for a 26M-row count (sanity)

`warnings.simplefilter("always")` + custom `showwarning` capture deprecation/future warnings.

## Output

```
INFO polars version: 1.40.1
INFO zero-copy from arrow: 50000 rows, 138 cols
INFO .over() lazy collect: 50000 rows
INFO forward_fill().over() OK
INFO event_offense_stats has 26296376 rows
INFO warnings captured: 0
```

## Notes

- The `>=1.31` floor in `spikes-sqlmesh` resolved to **1.40.1** because that's the latest pre-2.x release at spike time. We get more than asked for; nothing in the codebase touches APIs removed/changed in 1.31 → 1.40.
- Production pin was relaxed in the foundation commit from `>=0.20.17,<2` to `>=0.20.17` (drop the upper cap), so any downstream consumer of the prod env can also pull 1.x. The change was needed for uv to resolve the spike groups, but it's also intended to land permanently — Polars 1.x is stable, and the prior `<2` cap was defensive against a 2.x that hasn't appeared.
- The `forward_fill().over(group, order_by=col)` form (with explicit `order_by=`) is the Polars 1.0+ shape; older Polars required pre-sorting. Spike 4 will rely on this exact form. Confirmed working.

## Verdict

**GO.** Unblocks Spike 4 and the axis-D Phase 5 plan.
