# baseball.computer

Library powering the [baseball.computer](https://baseball.computer) database.

Starts from a set of Retrosheet files dropped off by the
[Rust parser](https://github.com/droher/baseball.computer.rs) and builds a
DuckDB database (`bc.db`) of the box-score, event, and seasonal models
documented at [docs.baseball.computer](https://docs.baseball.computer).

## Build engine

SQLMesh-native. Models live under `bc/models/` as `MODEL(...)` blocks;
sources and seeds load via `before_all` Python `@macro`s in
`bc/macros/_init_db.py`.

```bash
uv sync --group build
cd bc
uv run --group build sqlmesh plan dev --auto-apply   # build dev env
uv run --group build sqlmesh audit                   # run audits
```

Source-table metadata (45 parquet sources) lives in
`bc/external_models.yaml` — single source of truth for SQLMesh's external
loader and `_init_db.py`'s DDL emission. Shared docstrings live in
`bc/models/**/*.md` doc-block files; `@doc('key')` refs in MODEL blocks
resolve at parse time via `bc/macros/_docs.py`.

State lives in `bc/bc_state.db` (auto-created, separate from `bc.db`).
Set `BC_DB_PATH` or `BC_STATE_DB_PATH` to point SQLMesh at temporary or
alternate DuckDB files when you need an isolated build.

## Documentation

- [docs.baseball.computer](https://docs.baseball.computer) — model + column docs
- `CLAUDE.md` — present-state guide for AI agents
- `notes/followups.md` — open operational items
- [Modeling evidence contract](docs/modeling-evidence-contract.md) — validation, artifact integrity, publication policy, and the reconstruction benchmark
- [Geometry target correction](docs/geometry-target-correction-2026-09-11.md) — global-side provenance, dataset compatibility, and trajectory development results
- [Corrected geometry refits](docs/geometry-corrected-refits-2026-09-11.md) — isolated materialization, accepted research references, and historical validation priorities
- [Historical geometry stress test](docs/historical-geometry-stress-2026-09-11.md) — backward transfer, whole-scorer exclusion, support limits, and confirmation boundaries
- [Scorer and Statcast findings](docs/geometry-scorer-statcast-findings-2026-09-11.md) — recording selection, separate standardized and historical targets, and the first verified modern match
- [Statcast acquisition mechanics](docs/geometry-statcast-mechanics-results-2026-09-11.md) — stratified modern sample, audited game matching, and the gate before larger acquisition
- [Statcast fitting audit](docs/geometry-statcast-fitting-audit-2026-09-11.md) — 373 paired games, airborne-only targets, missingness, and source-agreement limits
- [Historical side hierarchy](docs/geometry-side-hierarchical-results-2026-09-11.md) — converged full fits that still fail historical calibration
- [Historical fielder-clue provenance](docs/geometry-side-clue-provenance-2026-09-11.md) — shared-source dependencies and why fielder fields do not resolve naturally missing side
- [Modern scorer identification](docs/geometry-modern-observation-identifiability-2026-09-11.md) — conflated source fields and limits on separating scorer and park effects
- [Airborne translation results](docs/geometry-air-development-results-2026-09-11.md) — improved scores but failed season calibration, with a substantial change in recording conventions
- [Scorer provenance contract](docs/scorer-provenance-contract.md) — separate official and administrative source keys, preserving legacy compatibility and the source migration boundary

## Agent skills

`.claude/skills/sqlmesh/` ships a project-agnostic SQLMesh reference
skill (model authoring, plan/apply workflow, audits, CLI). Loaded
automatically when working on SQLMesh code, or invoked explicitly
with `/sqlmesh`.

## Production query storage

The browser site queries a public DuckLake 1.0 catalog and immutable Parquet on R2.
`uv run --group build python scripts/publish_ducklake.py` exports `bc.db` read-only
and checks row-count parity. `uv run --group build python scripts/upload_ducklake.py`
uploads data, schema metadata, and finally the catalog. For an existing Wrangler
OAuth login, use `--wrangler`; a fresh version prefix can use `--skip-purge`.

Attach from DuckDB with:

```sql
INSTALL ducklake;
LOAD ducklake;
ATTACH 'ducklake:https://data.baseball.computer/baseball/v1/baseball.ducklake'
  AS baseball (READ_ONLY);
USE baseball.main_models;
```

See `docs/ducklake-production.md` for publication and verification.
