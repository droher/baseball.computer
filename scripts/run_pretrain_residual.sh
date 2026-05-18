#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

REPO="$(pwd)"
STAGE1_ID="${1:-pretrain-stage1}"
STAGE2_ID="${2:-pretrain}"
DATASET_ARTIFACT="${BC_PRETRAIN_DATASET_ARTIFACT:-pretrain-prep}"

STAGE1_EPOCHS="${BC_PRETRAIN_STAGE1_EPOCHS:-6}"
STAGE2_EPOCHS="${BC_PRETRAIN_STAGE2_EPOCHS:-8}"

export BC_PRETRAIN_LOSS="${BC_PRETRAIN_LOSS:-focal}"
export BC_PRETRAIN_FOCAL_GAMMA="${BC_PRETRAIN_FOCAL_GAMMA:-2.0}"
export BC_PRETRAIN_USE_HARD_HEAD_ES="${BC_PRETRAIN_USE_HARD_HEAD_ES:-1}"
export BC_PRETRAIN_HARD_HEADS="${BC_PRETRAIN_HARD_HEADS:-trajectory_remapped,batted_location_general,batted_to_fielder_class}"

DATASET_PARQUET="${REPO}/artifacts/statistical/datasets/model_input_event_universe/${DATASET_ARTIFACT}/dataset.parquet"

mkdir -p logs/pretrain

echo "==> residual-decomposition pretrain orchestrator"
echo "    stage1_id        = ${STAGE1_ID}"
echo "    stage2_id        = ${STAGE2_ID}"
echo "    dataset_artifact = ${DATASET_ARTIFACT}"
echo "    stage1_epochs    = ${STAGE1_EPOCHS}"
echo "    stage2_epochs    = ${STAGE2_EPOCHS}"
echo "    loss             = ${BC_PRETRAIN_LOSS} (gamma=${BC_PRETRAIN_FOCAL_GAMMA})"
echo "    hard_head_es     = ${BC_PRETRAIN_USE_HARD_HEAD_ES} (heads=${BC_PRETRAIN_HARD_HEADS})"

step_prepare() {
    if [ -f "${DATASET_PARQUET}" ]; then
        echo "==> [skip] dataset already prepared at ${DATASET_PARQUET}"
        return 0
    fi
    echo "==> prepare dataset artifact=${DATASET_ARTIFACT}"
    just prepare-dataset model_input_event_universe "${DATASET_ARTIFACT}"
}

step_stage1() {
    local stage1_dir="${REPO}/artifacts/statistical/deep/event_universe_context/${STAGE1_ID}"
    if [ -f "${stage1_dir}/manifest.json" ]; then
        echo "==> [skip] stage-1 artifact already present at ${stage1_dir}"
        return 0
    fi
    echo "==> stage-1 fit id=${STAGE1_ID} epochs=${STAGE1_EPOCHS}"
    BC_PRETRAIN_USE_SPLIT_OPTIMIZER=0 \
    BC_DB_PATH="${REPO}/bc_dev.db" \
        just fit-pretrain "${DATASET_ARTIFACT}" "${STAGE1_ID}" \
            --pretrain-target event_universe_context \
            --epochs "${STAGE1_EPOCHS}" \
        2>&1 | tee "logs/pretrain/${STAGE1_ID}_fit.log"
}

step_emit_offsets() {
    local stage1_dir="${REPO}/artifacts/statistical/deep/event_universe_context/${STAGE1_ID}"
    if [ -d "${stage1_dir}/offsets" ] && [ -f "${stage1_dir}/offsets/trajectory_remapped.parquet" ]; then
        echo "==> [skip] offsets already emitted at ${stage1_dir}/offsets"
        return 0
    fi
    echo "==> emit stage-1 offsets"
    PYTHONPATH="${REPO}/bc" \
    KERAS_BACKEND=torch \
        uv run --group ml python scripts/pretrain_emit_offsets.py \
            "${STAGE1_ID}" \
            --pretrain-target event_universe_context \
            --dataset-artifact "${DATASET_ARTIFACT}" \
        2>&1 | tee "logs/pretrain/${STAGE1_ID}_emit.log"
}

step_stage2() {
    local stage2_dir="${REPO}/artifacts/statistical/deep/event_universe/${STAGE2_ID}"
    if [ -f "${stage2_dir}/manifest.json" ]; then
        echo "==> [skip] stage-2 artifact already present at ${stage2_dir}"
        return 0
    fi
    echo "==> stage-2 fit id=${STAGE2_ID} epochs=${STAGE2_EPOCHS}"
    BC_PRETRAIN_OFFSET_ARTIFACT="${STAGE1_ID}" \
    BC_PRETRAIN_USE_SPLIT_OPTIMIZER=1 \
    BC_DB_PATH="${REPO}/bc_dev.db" \
        just fit-pretrain "${DATASET_ARTIFACT}" "${STAGE2_ID}" \
            --pretrain-target event_universe \
            --epochs "${STAGE2_EPOCHS}" \
        2>&1 | tee "logs/pretrain/${STAGE2_ID}_fit.log"
}

step_publish() {
    if [ "${BC_PRETRAIN_SKIP_PUBLISH:-0}" = "1" ]; then
        echo "==> [skip] BC_PRETRAIN_SKIP_PUBLISH=1; not publishing pointer"
        return 0
    fi
    local branch_slug
    branch_slug="$(git rev-parse --abbrev-ref HEAD | tr -c 'a-zA-Z0-9' '_')"
    local published_root="${REPO}/artifacts/statistical/published-${branch_slug}/"
    echo "==> publish pretrain pointer ${STAGE2_ID} → ${published_root}"
    BC_STATS_PUBLISHED_ROOT="${published_root}" \
    BC_DB_PATH="${REPO}/bc_dev.db" \
    PYTHONPATH="${REPO}/bc" \
        uv run --group build python -m python_models.statistical.cli publish-pretrain \
            --artifact-id "${STAGE2_ID}" \
        2>&1 | tee "logs/pretrain/${STAGE2_ID}_publish.log"
}

step_prepare
step_stage1
step_emit_offsets
step_stage2
step_publish

echo "==> pretrain orchestrator complete"
echo "    stage-1 artifact: artifacts/statistical/deep/event_universe_context/${STAGE1_ID}"
echo "    stage-2 artifact: artifacts/statistical/deep/event_universe/${STAGE2_ID}"
