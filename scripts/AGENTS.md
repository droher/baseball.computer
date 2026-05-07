# AGENTS.md

<!-- claude-primary-sync:managed -->

This file is generated from Claude-native project guidance.
Edit Claude sources, then rerun `claude-primary-sync --scope project --write`.

Directionality: project Claude sources -> project Codex/OpenCode artifacts.
Never edit this file by hand.

Interpretation rules:
- Later, more specific guidance wins over earlier general guidance.
- Conditional rules only apply when the files you touch match the listed globs.
- Mirrored skills and agents are generated from Claude-native sources in this scope chain.
- This compiled view is for work rooted at `scripts`.

## Base Claude Guidance

### CLAUDE.md

Source: `CLAUDE.md`

# baseball.computer

Builds a 40+ GB DuckDB database (`bc.db`) of every documented major-league play-by-play event, plus aggregated season and career metrics. Source data lands in R2 as parquet via the [Rust parser](https://github.com/droher/baseball.computer.rs); this repo turns it into the published model layer.

## Getting around

- `uv sync --group build` once before first use.
- `just --list` — full recipe surface. The four most-used:
  - `just plan` — plan + apply current branch's dev env.
  - `just plan-model main_models.<model>` — plan + apply one model.
  - `just eval main_models.<model>` — run the query, print rows, no materialization.
  - `just promote-prod main_models.<model> [more...]` — restate model(s) in prod (writes `bc.db`).

## Dev / prod DB split

Dev work targets `bc_dev.db` + `bc/bc_state_dev.db`; prod targets `bc.db` + `bc/bc_state.db`. The justfile sets the right env vars per recipe — non-prod recipes can never accidentally write `bc.db`. Bootstrap with `just bootstrap-dev` to seed dev from prod (or refresh after a prod rebuild).

## Where to find more

- `.claude/rules/sqlmesh.md` — SQLMesh project conventions, the DEV_ONLY trap, per-branch envs, source-table layout.
- `.claude/rules/performance.md` — `BC_*` env vars and tuning.
- `bc/python_models/ml/CLAUDE.md` — Keras + MLflow training pipeline conventions.
- `bc/semantic/CLAUDE.md` — BSL `SemanticTable` constraints.
- `scripts/CLAUDE.md` — publish / upload / web-db pipeline.
- `notes/followups.md` — open follow-ups.

### scripts/CLAUDE.md

Source: `scripts/CLAUDE.md`

# Publish pipeline

- `publish_ducklake.py` copies `main_models.*` + `main_seeds.*` out of `bc.db` into the `bc_publish` DuckLake catalog (`bc/bc_publish.ducklake` + `bc/bc_publish_data/`). ENUM columns cast to VARCHAR — DuckLake v1.0 doesn't preserve user-defined types.
- `upload_ducklake.py` ships catalog + data dir to `s3://timeball/baseball/v<DATA_VERSION>/` with long-lived `Cache-Control` and a Cloudflare cache purge.
- `create_web_db.py` publishes `bc_remote.db` + per-table parquet under the `dbt/` R2 prefix as the canonical site artifact. DuckLake site cutover is tracked in `notes/followups.md`.
- `preload_sources.py` is the only ad-hoc script allowed to write `bc.db` directly (only `CREATE TABLE IF NOT EXISTS`). Everything else goes through SQLMesh.

## Always-On Claude Rules

### .claude/rules/performance.md

Source: `.claude/rules/performance.md`

---
---

- DuckDB intra-query threads: `BC_DUCKDB_THREADS` (default 14).
- SQLMesh task pool + DuckDB connection-pool size: `BC_CONCURRENT_TASKS` (default 6, perf mode 1).
- `scripts/preload_sources.py` worker count: `BC_INIT_DB_PARALLELISM` (default 8).
- Run `just preload` before any full `sqlmesh plan`. It parallel-loads the 45 source parquet files, so SQLMesh's `init_db` becomes a chain of `CREATE TABLE IF NOT EXISTS` no-ops. CI / production builds do this automatically.
- `scripts/grid_search.py` sweeps `(threads, workers)` and writes `logs/perf/grid/grid_results.json`.
- Deep dives: `notes/perf-deep-dive.md`, `notes/perf-profile-report.md`.

### .claude/rules/sqlmesh.md

Source: `.claude/rules/sqlmesh.md`

---
---

- Pure SQLMesh: `MODEL (...)` blocks (no jinja in bodies), Python `@macro`s under `bc/macros/_*.py`, Python `@model` decorators in `bc/models/`. `bc/python_models/` holds library code imported by `@model` files — SQLMesh does not auto-load it.
- The SQLMesh CLI must run from `bc/` (it discovers `config.py` there). Every justfile recipe `cd`s for you.
- Source-table metadata for the 45 parquet sources lives in `bc/external_models.yaml`. SQLMesh auto-discovers it for lineage / types / audits, and `bc/macros/_init_db.py` reads it to emit `CREATE TABLE` DDL via `config.before_all`. Seeds load via `@load_seeds()` in `before_all`.
- Don't write to `bc.db` from ad-hoc scripts; go through SQLMesh. Sole exception: `scripts/preload_sources.py` (only `CREATE TABLE IF NOT EXISTS` for source parquet).
- Dev work targets `bc_dev.db` + `bc/bc_state_dev.db`; prod targets `bc.db` + `bc/bc_state.db`. The justfile sets `BC_DB_PATH` and `BC_STATE_DB_PATH` (read by `bc/config.py`) for non-prod recipes and leaves them unset for `promote-prod` / `rebuild-prod` / `preload`.
- Per-branch envs: `just plan` defaults to a slug of the current git branch (`just which-env` previews it). Envs share fingerprinted physical snapshots inside `bc_dev.db`, so a branch only pays for the models it changed. Reserved env names: `prod` (only via `promote-prod` / `rebuild-prod`) and `dev` (legacy shared env, not the default for any target).
- DEV_ONLY trap: `virtual_environment_mode=DEV_ONLY` plus `always_recreate_environment=True` mean a bare `sqlmesh plan` (no env arg) silently advances state without rebuilding the unsuffixed `main_models.*` prod tables. To promote a code change to prod, use `just promote-prod <model> [more...]` (runs `sqlmesh plan --restate-model <each>` and cascades downstream). For a clean wipe, `just rebuild-prod`.
- To use local parquet instead of R2, pass `--vars '{source_roots: {event: file:///...}}'` to whichever `just` recipe forwards `*ARGS`.

## Claude Skills

These skills are mirrored into portable and tool-native skill directories for this scope.

### `sqlmesh`

Source: `.claude/skills/sqlmesh/SKILL.md`
Description: Use when working with SQLMesh — writing or editing MODEL blocks, Python @model decorators, Python @macros, audits, unit tests, external_models.yaml, or seeds; running `sqlmesh plan/apply/audit/render/evaluate/test`; debugging plans, snapshots, virtual environments, or state issues; configuring `config.py`, gateways, or `before_all`/`after_all` hooks; choosing a model kind (FULL, INCREMENTAL_BY_TIME_RANGE, INCREMENTAL_BY_UNIQUE_KEY, VIEW, SEED, EMBEDDED, SCD_TYPE_2, EXTERNAL, MANAGED); migrating a project from dbt; or asking whether SQLMesh is still maintained. Trigger on these terms even when the user does not name the tool. SQLMesh changes quickly and the model's prior knowledge is often wrong — fetch the canonical docs at https://sqlmesh.readthedocs.io/en/stable/ before answering anything non-trivial.
