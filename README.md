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

## Agent skills

`.claude/skills/sqlmesh/` ships a project-agnostic SQLMesh reference
skill (model authoring, plan/apply workflow, audits, CLI). Loaded
automatically when working on SQLMesh code, or invoked explicitly
with `/sqlmesh`.
