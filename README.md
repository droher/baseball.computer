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
just --list
just plan
just audit
```

Run these commands from the repository root. `just plan` builds the current
branch's development environment. If production databases already exist,
`just bootstrap-dev` seeds or refreshes the development copies; it overwrites
existing dev databases. See the [build conventions](.claude/rules/sqlmesh.md)
and [performance guidance](.claude/rules/performance.md) before a full build.

Source-table metadata (45 parquet sources) lives in
`bc/external_models.yaml` — single source of truth for SQLMesh's external
loader and `_init_db.py`'s DDL emission. Shared docstrings live in
`bc/models/**/*.md` doc-block files; `@doc('key')` refs in MODEL blocks
resolve at parse time via `bc/macros/_docs.py`.

Development uses `bc_dev.db` and `bc/bc_state_dev.db`; production uses
`bc.db` and `bc/bc_state.db`. The [justfile](justfile) sets the database paths
for development recipes. Production changes use `just promote-prod` or
`just rebuild-prod`, as described in the build conventions.

## Documentation

Start with the [documentation guide](docs/README.md) for the complete map.

- [Model and column reference](https://docs.baseball.computer)
- [Modeling contracts, protocols, and results](docs/modeling/README.md)
- [Full-history PBP imputation completion plan](notes/full-history-imputation-plan.md)
- [PBP imputation outputs and reproducible builds](docs/pbp-imputation.md)
- [Production publication and verification](docs/ducklake-production.md)
- [LLM schema context](docs/llm/README.md)
- [Research notes and implementation plans](notes/README.md)
- [Open follow-ups](notes/followups.md)
- [Agent guide](CLAUDE.md)

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

See [DuckLake production publication](docs/ducklake-production.md) for publication and verification.
