set positional-arguments := true

repo_root := justfile_directory()

_dev_db := repo_root / "bc_dev.db"
_dev_state := repo_root / "bc" / "bc_state_dev.db"
_prod_db := repo_root / "bc.db"
_prod_state := repo_root / "bc" / "bc_state.db"

_dev_env := 'BC_DB_PATH="' + _dev_db + '" BC_STATE_DB_PATH="' + _dev_state + '"'

_branch_slug := `git branch --show-current 2>/dev/null | tr -c 'A-Za-z0-9\n' _ | tr A-Z a-z | sed 's/_*$//'`

# List recipes
default:
    @just --list

# Echo the branch slug `just plan` would target
which-env:
    @echo "{{ _branch_slug }}"

# --- Lifecycle ---

# Run preload_sources.py against PROD (bc.db). Idempotent. Pass --force-reload to wipe + reload.
preload *ARGS:
    uv run --group build python scripts/preload_sources.py "$@"

# Copy prod bc.db + bc_state.db onto dev paths. Refuses if dev files are locked.
bootstrap-dev:
    #!/usr/bin/env bash
    set -euo pipefail
    for f in "{{ _dev_db }}" "{{ _dev_state }}"; do
        if [[ -e "$f" ]] && lsof "$f" >/dev/null 2>&1; then
            echo "ERROR: $f is locked (in use). Stop dev SQLMesh processes first." >&2
            exit 1
        fi
    done
    if [[ ! -f "{{ _prod_db }}" ]]; then
        echo "ERROR: {{ _prod_db }} does not exist." >&2
        exit 1
    fi
    if [[ ! -f "{{ _prod_state }}" ]]; then
        echo "ERROR: {{ _prod_state }} does not exist." >&2
        exit 1
    fi
    echo "Copying {{ _prod_db }} -> {{ _dev_db }}"
    cp "{{ _prod_db }}" "{{ _dev_db }}"
    echo "Copying {{ _prod_state }} -> {{ _dev_state }}"
    cp "{{ _prod_state }}" "{{ _dev_state }}"
    echo "Done."

# sqlmesh info (dev)
info:
    cd bc && {{ _dev_env }} uv run --group build sqlmesh info

# sqlmesh environments (dev)
envs:
    cd bc && {{ _dev_env }} uv run --group build sqlmesh environments

# --- Per-branch dev (default env = current branch slug) ---

# Plan + apply ENV (default: current-branch slug). Writes to bc_dev.db.
plan env=_branch_slug:
    @test -n "{{ env }}" || { echo "ERROR: empty env (detached HEAD?). Pass an env name explicitly." >&2; exit 1; }
    cd bc && {{ _dev_env }} uv run --group build sqlmesh plan {{ env }} --auto-apply --no-prompts

# Plan + apply only MODEL into ENV. Writes to bc_dev.db.
plan-model MODEL env=_branch_slug:
    @test -n "{{ env }}" || { echo "ERROR: empty env (detached HEAD?). Pass an env name explicitly." >&2; exit 1; }
    cd bc && {{ _dev_env }} uv run --group build sqlmesh plan {{ env }} --select-model {{ MODEL }} --auto-apply --no-prompts

# Run audits against the dev DB.
audit *ARGS:
    cd bc && {{ _dev_env }} uv run --group build sqlmesh audit "$@"

# Drop ENV pointer. Physical tables GC by snapshot TTL.
invalidate env=_branch_slug:
    @test -n "{{ env }}" || { echo "ERROR: empty env (detached HEAD?). Pass an env name explicitly." >&2; exit 1; }
    cd bc && {{ _dev_env }} uv run --group build sqlmesh invalidate {{ env }}

# --- Per-model iteration (no env mutation) ---

# Run MODEL's query and print rows. No materialization, no env touched.
eval MODEL *ARGS:
    cd bc && {{ _dev_env }} uv run --group build sqlmesh evaluate "$@"

# Render MODEL's compiled SQL.
render MODEL:
    cd bc && {{ _dev_env }} uv run --group build sqlmesh render {{ MODEL }}

# Show MODEL's lineage.
lineage MODEL:
    cd bc && {{ _dev_env }} uv run --group build sqlmesh lineage {{ MODEL }}

# table_diff for MODEL between SRC and TGT envs. Example: just diff main_models.foo main feature_x
diff MODEL SRC TGT:
    cd bc && {{ _dev_env }} uv run --group build sqlmesh table_diff '{{ SRC }}:{{ TGT }}' --select-model {{ MODEL }}

# --- Data-coverage baseline ---

# Phase 0 baseline: read-only DuckDB snapshot under artifacts/statistical/baseline/.
baseline-data-coverage *ARGS:
    {{ _dev_env }} BC_LEDGER_SCHEMA="main_models__{{ _branch_slug }}" BC_STATS_PUBLISHED_ROOT="{{ repo_root }}/artifacts/statistical/published-{{ _branch_slug }}/" PYTHONPATH="{{ repo_root }}/bc" uv run --group build python scripts/baseline_data_coverage.py "$@"

# Diff two data-coverage baseline JSONs (or the latest against the live DB).
compare-baseline *ARGS:
    {{ _dev_env }} BC_LEDGER_SCHEMA="main_models__{{ _branch_slug }}" BC_STATS_PUBLISHED_ROOT="{{ repo_root }}/artifacts/statistical/published-{{ _branch_slug }}/" PYTHONPATH="{{ repo_root }}/bc" uv run --group build python scripts/compare_baseline.py "$@"

# Prepare a modeling-dataset Parquet snapshot from the per-branch ledger schema.
# Usage: just prepare-dataset model_input_observation_batted_ball <artifact-id> [extra args]
prepare-dataset DATASET ARTIFACT_ID *ARGS:
    {{ _dev_env }} BC_LEDGER_SCHEMA="main_models__{{ _branch_slug }}" BC_STATS_PUBLISHED_ROOT="{{ repo_root }}/artifacts/statistical/published-{{ _branch_slug }}/" PYTHONPATH="{{ repo_root }}/bc" uv run --group build python -m python_models.statistical.cli prepare-dataset --dataset {{ DATASET }} --artifact-id {{ ARTIFACT_ID }} "$@"

# Rollup parity check: existing completeness model vs ledger reproduction.
# All 7 current models carry an accept-gap disposition (see
# notes/data-coverage-implementation/rollup-parity-dispositions.md): the
# heuristic flag rules pre-date the ledgers and the diffs reflect real
# semantic divergence, not bugs. The defaults below let the recipe exit 0
# without `--allow-delta`-style escape hatches; pass extra `--allow-mismatch`
# args to suppress additional models, or override entirely with
# `just rollup-parity-checks --model <name>`.
rollup-parity-checks *ARGS:
    {{ _dev_env }} BC_LEDGER_SCHEMA="main_models__{{ _branch_slug }}" PYTHONPATH="{{ repo_root }}/bc" uv run --group build python scripts/rollup_parity_checks.py \
        --allow-mismatch event_completeness_pitches \
        --allow-mismatch event_completeness_fielding_credit \
        --allow-mismatch event_completeness_batted_balls \
        --allow-mismatch player_game_data_completeness \
        --allow-mismatch player_completeness \
        --allow-mismatch game_data_completeness \
        --allow-mismatch season_team_coverage \
        "$@"

# --- LLM context ---

# Generate the LSF-1 context packet at docs/llm/baseball.lsf. Read-only against bc.db.
gen-llm-context *ARGS:
    uv run --group build python scripts/generate_llm_context.py --validate "$@"

# --- Tests ---

# pytest under bc/tests (dev DB env).
test *ARGS:
    {{ _dev_env }} uv run --group build pytest bc/tests "$@"

# sqlmesh test — runs YAML model fixtures.
unit-tests:
    cd bc && {{ _dev_env }} uv run --group build sqlmesh test

# --- Prod (guarded) ---

# Restate one or more models in PROD. Cascades to downstream. Writes to bc.db.
[confirm("Restate listed model(s) in PROD (bc.db)? Type 'yes' to proceed.")]
promote-prod *MODELS:
    #!/usr/bin/env bash
    set -euo pipefail
    if [[ "$#" -eq 0 ]]; then
        echo "ERROR: pass at least one model. e.g. just promote-prod main_models.event_pitching_stats" >&2
        exit 1
    fi
    args=()
    for m in "$@"; do args+=(--restate-model "$m"); done
    cd bc && uv run --group build sqlmesh plan "${args[@]}" --auto-apply --no-prompts

# Wipe bc.db + bc/bc_state.db, then preload + plan PROD from sources. After: re-run `just bootstrap-dev` to resync dev.
[confirm("Delete bc.db AND bc/bc_state.db and rebuild PROD from sources? Type 'yes' to proceed.")]
rebuild-prod:
    rm -f "{{ _prod_db }}" "{{ _prod_state }}"
    uv run --group build python scripts/preload_sources.py
    cd bc && uv run --group build sqlmesh plan --auto-apply --no-prompts
