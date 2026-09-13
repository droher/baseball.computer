# baseball.computer

Builds a DuckDB database (`bc.db`) of every documented major-league play-by-play event, plus aggregated season and career metrics. Source data lands in R2 as parquet via the [Rust parser](https://github.com/droher/baseball.computer.rs); this repo turns it into the published model layer.

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

- [Documentation guide](docs/README.md) — operations, modeling, schema context, research notes, and handoffs.
- `.claude/rules/sqlmesh.md` — SQLMesh project conventions, the DEV_ONLY trap, per-branch envs, source-table layout.
- `.claude/rules/performance.md` — `BC_*` env vars and tuning.
- `bc/python_models/ml/CLAUDE.md` — Keras + MLflow training pipeline conventions.
- `bc/python_models/mcp_server/` — MCP server exposing schema retrieval + DuckDB execution. `just mcp-stdio` (Claude Desktop / Cursor / claude-cli) or `just mcp-http` (Streamable HTTP, requires `BC_MCP_TOKEN`).
- `bc/semantic/CLAUDE.md` — BSL `SemanticTable` constraints.
- `scripts/CLAUDE.md` — DuckLake publish/upload pipeline; `docs/ducklake-production.md` — production verification.
- `docs/llm/lsf_1_spec.md` — LSF-1 spec (with baseball.computer profile in §17.4); `just gen-llm-context` produces `docs/llm/baseball.lsf` from `supplement.yaml` + SQLMesh metadata + the metric registry.
- `notes/followups.md` — open follow-ups.
