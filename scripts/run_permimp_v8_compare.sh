#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "$0")/.."

REPO="$(pwd)"
BRANCH_SLUG="$(git rev-parse --abbrev-ref HEAD | tr -c 'a-zA-Z0-9' '_')"
DATASET="${REPO}/artifacts/statistical/datasets/model_input_geometry/phase3-tune-v4-prep/dataset.parquet"
LOGDIR="${REPO}/logs/permimp_v8"
PUB_ROOT="${REPO}/artifacts/statistical/published-${BRANCH_SLUG}/"

mkdir -p "${LOGDIR}"

run() {
    local label="$1"; shift
    local logfile="${LOGDIR}/${label}.log"
    echo "==> perm-imp ${label} -> ${logfile}"
    if "$@" > "${logfile}" 2>&1; then
        echo "    [ok] ${label}"
    else
        echo "    [FAIL] ${label} (exit $?)"
    fi
}

run baseline env \
    BC_DEEP_DISABLE_PRETRAIN=1 \
    PYTHONPATH="${REPO}/bc" \
    uv run --group ml python scripts/permutation_importance_generic.py \
        --target geometry_trajectory \
        --dataset-parquet "${DATASET}" \
        --time-forward

run v6 env \
    BC_STATS_PUBLISHED_ROOT="${PUB_ROOT}" \
    PYTHONPATH="${REPO}/bc" \
    uv run --group ml python scripts/permutation_importance_generic.py \
        --target geometry_trajectory \
        --dataset-parquet "${DATASET}" \
        --time-forward

run v8 env \
    BC_PRETRAIN_SKIP_DIM_MISMATCH=1 \
    BC_DEEP_FORCE_EMBED_DIM=128 \
    BC_DEEP_PRETRAIN_ARTIFACT_OVERRIDE=event_universe_v8 \
    BC_STATS_PUBLISHED_ROOT="${PUB_ROOT}" \
    PYTHONPATH="${REPO}/bc" \
    uv run --group ml python scripts/permutation_importance_generic.py \
        --target geometry_trajectory \
        --dataset-parquet "${DATASET}" \
        --time-forward

echo "==> all perm-imp runs complete"
