#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "$0")/.."

REPO="$(pwd)"
LOGDIR="${REPO}/logs/permimp"
BRANCH_SLUG="$(git rev-parse --abbrev-ref HEAD | tr -c 'a-zA-Z0-9' '_')"
PUB_ROOT="${REPO}/artifacts/statistical/published-${BRANCH_SLUG}/"
GEOM_DATA="${REPO}/artifacts/statistical/datasets/model_input_geometry/phase3-tune-v4-prep/dataset.parquet"

mkdir -p "${LOGDIR}"

dataset_for() {
    case "$1" in
        geometry_*) echo "${GEOM_DATA}" ;;
        *) echo "UNKNOWN_DATASET_FOR_$1"; return 1 ;;
    esac
}

use_time_forward() {
    case "$1" in
        geometry_*) return 0 ;;
        *) return 1 ;;
    esac
}

run_arm() {
    local target="$1" arm="$2" data="$3"; shift 3
    local logfile="${LOGDIR}/${target}__${arm}.log"
    local tf_flag=""
    if use_time_forward "${target}"; then
        tf_flag="--time-forward"
    fi
    echo "==> ${target} ${arm} -> ${logfile}"
    if env "$@" PYTHONPATH="${REPO}/bc" \
        uv run --group ml python scripts/permutation_importance_generic.py \
            --target "${target}" \
            --dataset-parquet "${data}" \
            ${tf_flag} \
        > "${logfile}" 2>&1; then
        echo "    [ok] ${target} ${arm}"
    else
        echo "    [FAIL] ${target} ${arm} (exit $?)"
    fi
}

TARGETS="geometry_location_side geometry_location_depth geometry_location_edge"

for tgt in ${TARGETS}; do
    data="$(dataset_for "${tgt}")"
    run_arm "${tgt}" baseline "${data}" \
        BC_DEEP_DISABLE_PRETRAIN=1
    run_arm "${tgt}" pretrained "${data}" \
        BC_PRETRAIN_SKIP_DIM_MISMATCH=1 \
        BC_DEEP_FORCE_EMBED_DIM=128 \
        BC_STATS_PUBLISHED_ROOT="${PUB_ROOT}"
done

echo "==> all batted-ball perm-imp runs complete"
