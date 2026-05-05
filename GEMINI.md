# GEMINI.md

<!-- claude-primary-sync:managed -->

This file is generated from Claude-native project guidance.
Edit Claude sources, then rerun `claude-primary-sync --scope project --write`.

Directionality: project Claude sources -> project Gemini artifacts.
Never edit this file by hand.

Interpretation rules:
- Later, more specific guidance wins over earlier general guidance.
- Conditional rules only apply when the files you touch match the listed globs.
- Mirrored skills and agents are generated from Claude-native sources in this scope chain.
- This compiled view is for work rooted at `repo root`.

## Base Claude Guidance

### CLAUDE.md

Source: `CLAUDE.md`

# Repo guide for AI agents

## What this repo is

`baseball.computer` builds a 40+ GB DuckDB database (`bc.db`) of every
documented MLB / minor / Negro League play-by-play event, plus
aggregated season and career metrics. Source data lands in R2 as
parquet via the [Rust parser](https://github.com/droher/baseball.computer.rs);
this repo turns it into the published model layer.

## Build engine

Pure SQLMesh: `MODEL (...)` blocks, no jinja in bodies. Helper macros
are Python `@macro`s under `bc/macros/_*.py`. Seeds load via
`@load_seeds()` in `config.before_all`. Source-table metadata
(45 parquet sources across 6 schemas) lives in `bc/external_models.yaml`
— SQLMesh auto-discovers it for lineage/types/audits and
`_init_db.py` reads it to emit DDL in `before_all`.

```bash
uv sync --group build
cd bc
uv run --group build sqlmesh info
uv run --group build sqlmesh render main_models.<model>
uv run --group build sqlmesh plan dev --auto-apply
uv run --group build sqlmesh audit dev
```

Dev tables land in `main_models__dev`; prod tables in `main_models`.
State backend is `bc/bc_state.db` (separate file from `bc.db`).

## Repo structure

```
bc/
  config.py        SQLMesh config (gateway, before_all, vars)
  models/          .sql MODEL(...) blocks AND .py @model decorators
  macros/          Python @macros (_*.py)
  python_models/   Library code imported by Python @model files:
                     metrics/        Pydantic Metric registry + Ibis SQL
                     park_factors/   Ibis park-factor builders
                     event_locality/ Polars FSM transforms
                     game_level/     Polars FSM transforms
                     ml/             Keras + MLflow training/scoring
                   (NOT auto-loaded by SQLMesh; imported by .py models)
  seeds/           CSVs loaded into main_seeds
  audits/          relationships, bounded_range, sum_consistency,
                   unique_grain, valid_baseball_season
  tests/           pytest unit tests (uv run --group build pytest)
notes/             present-state notes + open follow-ups
scripts/           publish/upload, perf, web_db, ML training drivers
```

## Conventions

- Use `uv run --group build <cmd>` for any Python or SQLMesh CLI.
  The base uv env doesn't include sqlmesh.
- SQLMesh CLI must run from `bc/` (it discovers `config.py` there).
- Don't write to `bc.db` from ad-hoc scripts; go through SQLMesh.
  Sole exception: `scripts/preload_sources.py` writes only
  `CREATE TABLE IF NOT EXISTS` for source parquet.
- Source data publishes to `https://data.baseball.computer/...`. To
  use local parquet:
  `--vars '{source_roots: {event: file:///...}}'`.

## Performance knobs

- `BC_DUCKDB_THREADS` (default 14) — DuckDB intra-query thread count.
- `BC_CONCURRENT_TASKS` (default 6, perf-mode 1) — SQLMesh task pool
  + DuckDB connection-pool size.
- `BC_INIT_DB_PARALLELISM` (default 8) — `scripts/preload_sources.py`
  worker count.
- `scripts/preload_sources.py` parallel-loads the 45 source parquet
  files before SQLMesh opens, so `init_db` becomes a string of
  `CREATE TABLE IF NOT EXISTS` no-ops. CI / production builds should
  run it before `sqlmesh plan`.
- `scripts/grid_search.py` sweeps `(threads, workers)` and writes
  `logs/perf/grid/grid_results.json`.

Detail in `notes/perf-deep-dive.md` and `notes/perf-profile-report.md`.

## Publish

`scripts/publish_ducklake.py` copies `main_models.*` + `main_seeds.*`
out of `bc.db` into the `bc_publish` DuckLake catalog
(`bc/bc_publish.ducklake` + `bc/bc_publish_data/`). ENUM columns cast
to VARCHAR — DuckLake v1.0 doesn't preserve user-defined types.
`scripts/upload_ducklake.py` ships catalog + data dir to
`s3://timeball/baseball/v<DATA_VERSION>/` with long-lived
`Cache-Control` and a Cloudflare cache purge.

`scripts/create_web_db.py` still publishes `bc_remote.db` + per-table
parquet under the `dbt/` R2 prefix as the canonical site artifact.
DuckLake site cutover is tracked in `notes/followups.md`.

## Machine learning

`bc/python_models/ml/` is a Keras 3 + PyTorch + MLflow pipeline.
`KERAS_BACKEND=torch` is set once in `ml/__init__.py`. `TargetSpec`
(`features.py`) carries `kind ∈ {multiclass, binary, regression}`;
`model_factory._make_outputs_layer` dispatches softmax / sigmoid /
linear heads. Per-target wrappers stay thin (`model_<target>.py`,
`scripts/train_<target>.py`, `predictions_<target>.py`).

Training is offline — run `scripts/train_<name>.py` to produce the
artifact JSON at `bc/python_models/ml/artifacts/<name>.json`. The
`predictions_<target>.py` `@model` gates on `artifact_exists(target)`,
so a fresh prod plan skips untrained targets cleanly. Predictions
stream via DuckDB Arrow `to_batches` (`_BATCH_ROWS=500_000`).

ML deps live in the `ml` uv group (`apache-hamilton`,
`mlflow`, `keras`, `torch`, `scikit-learn`), mutually exclusive with
`bsl`.

## Semantic layer

`bc/semantic/` exposes 6 BSL `SemanticTable` factories
(offense/pitching/fielding × event/season). Runs under
`uv --group bsl` only — its xorq dep pins sqlglot <28 while
SQLMesh needs 30+. The Pydantic `Metric` registry is shared between
build and BSL paths; the import paths are not. `bc/semantic/` must
not import any sqlmesh module.

## Open follow-ups

`notes/followups.md`.

## Claude Skills

These skills are mirrored into portable and tool-native skill directories for this scope.

### `sqlmesh`

Source: `.claude/skills/sqlmesh/SKILL.md`
Description: Use when working with SQLMesh — writing or editing MODEL blocks, Python @model decorators, Python @macros, audits, unit tests, external_models.yaml, or seeds; running `sqlmesh plan/apply/audit/render/evaluate/test`; debugging plans, snapshots, virtual environments, or state issues; configuring `config.py`, gateways, or `before_all`/`after_all` hooks; choosing a model kind (FULL, INCREMENTAL_BY_TIME_RANGE, INCREMENTAL_BY_UNIQUE_KEY, VIEW, SEED, EMBEDDED, SCD_TYPE_2, EXTERNAL, MANAGED); migrating a project from dbt; or asking whether SQLMesh is still maintained. Trigger on these terms even when the user does not name the tool. SQLMesh changes quickly and the model's prior knowledge is often wrong — fetch the canonical docs at https://sqlmesh.readthedocs.io/en/stable/ before answering anything non-trivial.
