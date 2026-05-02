# baseball.computer

Library powering the [baseball.computer](https://baseball.computer) database.

Starts from a set of Retrosheet files dropped off by the
[Rust parser](https://github.com/droher/baseball.computer.rs) and builds a
DuckDB database (`bc.db`) of the box-score, event, and seasonal models
documented at [docs.baseball.computer](https://docs.baseball.computer).

## Build engine

Phase 1.5 (current) uses **SQLMesh-native** as the build engine. Models
live under `bc/models/` as `MODEL(...)` blocks; sources and seeds are
loaded by `before_all` Python `@macro`s in `bc/macros/_init_db.py`. dbt
is no longer a runtime dependency.

```bash
uv sync --group spikes-sqlmesh
cd bc
uv run --group spikes-sqlmesh sqlmesh plan dev --auto-apply   # build dev env
uv run --group spikes-sqlmesh sqlmesh audit                   # run audits
```

Source-table metadata (45 parquet sources) lives in
`bc/external_models.yaml` — single source of truth for SQLMesh's external
loader and `_init_db.py`'s DDL emission. Shared docstrings live in
`bc/models/**/*.md` doc-block files; `@doc('key')` refs in MODEL blocks
resolve at parse time via `bc/macros/_docs.py`.

State lives in `bc/bc_state.db` (auto-created, separate from `bc.db`).

## Documentation

- [docs.baseball.computer](https://docs.baseball.computer) — model + column docs
- `notes/phase-1-plan.md` — Phase 1 (dbt → SQLMesh dbt-import) status
- `notes/phase-1-followups.md` — open items + Phase 1.5 deferred work
- `notes/migration-evaluation.md` — multi-phase migration plan
