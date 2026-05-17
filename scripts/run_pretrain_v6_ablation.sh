#!/usr/bin/env bash
# Phase-3 v6 mini ablation matrix.
#
# 4 variants × {fit, sidecar, perm-imp trajectory + fielding_credit_putout}.
# No-pretrain baselines on the two supplements run once up front.
#
# Variants (all share v6 inputs + 11 non-redundant heads + v2-prep dataset
# + split optimizer + slash-line probe + sidecar metrics):
#
#   ab1: single-stage, val_loss ES                  (A8 reproduction)
#   ab2: stage1=3ep freeze + stage2=3ep, val_loss ES
#   ab3: single-stage, hard-head ES
#   ab4: stage1=3ep freeze + stage2=3ep, hard-head ES
#
# Each variant fits at --epochs 6 to keep wall clock manageable.
# Sequential execution.

set -euo pipefail
cd "$(dirname "$0")/.."

REPO="$(pwd)"
DATASET_ARTIFACT="phase3-pretrain-v2-prep"
DATASET_PARQUET="${REPO}/artifacts/statistical/datasets/model_input_event_universe/${DATASET_ARTIFACT}/dataset.parquet"
TRAJECTORY_PARQUET="${REPO}/artifacts/statistical/datasets/model_input_geometry/phase3-tune-v3-prep/dataset.parquet"
FC_PARQUET="${REPO}/artifacts/statistical/datasets/model_input_fielding_credit/phase3-pretrain-v5-fc-prep/dataset.parquet"

BRANCH_SLUG="phase3_rearch_v6"
PUBLISHED_ROOT="${REPO}/artifacts/statistical/published-${BRANCH_SLUG}/"

mkdir -p logs/ablation_v6

run_sidecar() {
    local artifact_id="$1"
    local outlog="logs/ablation_v6/${artifact_id}_sidecar.log"
    if [ -f "${REPO}/artifacts/statistical/deep/event_universe/${artifact_id}/eval/eval_report.json" ]; then
        echo "    [skip sidecar — already present]"
        return 0
    fi
    BC_DB_PATH="${REPO}/bc_dev.db" \
    PYTHONPATH="${REPO}/bc" \
    uv run --group ml python scripts/pretrain_eval_pretrain.py "${artifact_id}" \
        > "${outlog}" 2>&1
}

run_permimp() {
    local target="$1"
    local parquet="$2"
    local tag="$3"
    local fold_flag="$4"   # "--time-forward" or "" for spec default
    local outlog="logs/ablation_v6/${tag}_${target}_permimp.log"
    if [ -f "${outlog}" ] && grep -q "baseline CE" "${outlog}" 2>/dev/null; then
        echo "    [skip permimp ${target} — log present]"
        return 0
    fi
    BC_DB_PATH="${REPO}/bc_dev.db" \
    PYTHONPATH="${REPO}/bc" \
    uv run --group ml python scripts/permutation_importance_generic.py \
        --target "${target}" \
        --dataset-parquet "${parquet}" \
        ${fold_flag} \
        > "${outlog}" 2>&1
}

run_permimp_no_pretrain() {
    local target="$1"
    local parquet="$2"
    local fold_flag="$3"
    local outlog="logs/ablation_v6/no_pretrain_${target}_permimp.log"
    if [ -f "${outlog}" ] && grep -q "baseline CE" "${outlog}" 2>/dev/null; then
        echo "    [skip no-pretrain permimp ${target} — log present]"
        return 0
    fi
    BC_DEEP_DISABLE_PRETRAIN=1 \
    BC_DB_PATH="${REPO}/bc_dev.db" \
    PYTHONPATH="${REPO}/bc" \
    uv run --group ml python scripts/permutation_importance_generic.py \
        --target "${target}" \
        --dataset-parquet "${parquet}" \
        ${fold_flag} \
        > "${outlog}" 2>&1
}

publish_as_event_universe() {
    local artifact_id="$1"
    BC_STATS_PUBLISHED_ROOT="${PUBLISHED_ROOT}" \
    PYTHONPATH="${REPO}/bc" \
    uv run --group build python -m python_models.statistical.cli publish-pretrain \
        --artifact-id "${artifact_id}" \
        > "logs/ablation_v6/${artifact_id}_publish.log" 2>&1
}

fit_variant() {
    local artifact_id="$1"
    local stage1_epochs="$2"
    local hard_head_es="$3"
    local outlog="logs/ablation_v6/${artifact_id}_fit.log"

    if [ -f "${REPO}/artifacts/statistical/deep/event_universe/${artifact_id}/manifest.json" ]; then
        echo "    [skip fit — manifest already present]"
        return 0
    fi

    local extra_env=""
    if [ "${hard_head_es}" = "1" ]; then
        extra_env="BC_PRETRAIN_USE_HARD_HEAD_ES=1"
    fi

    env BC_PRETRAIN_SLASH_PROBE=1 \
        BC_PRETRAIN_USE_SPLIT_OPTIMIZER=1 \
        BC_PRETRAIN_STAGE1_EPOCHS="${stage1_epochs}" \
        BC_DB_PATH="${REPO}/bc_dev.db" \
        ${extra_env} \
        just fit-pretrain "${DATASET_ARTIFACT}" "${artifact_id}" \
            --epochs 6 \
            > "${outlog}" 2>&1
}

echo "==================== no-pretrain baselines ===================="
# Per supplement: (name, parquet, fold_flag)
#   geometry_trajectory uses --time-forward (data spans all seasons)
#   fielding_credit_putout uses primary_fold (eligible_for_allocation rows only
#   span 1910-1996; time-forward VAL=2023 has zero rows)
echo ">>> no-pretrain perm-imp geometry_trajectory (time-forward)"
run_permimp_no_pretrain geometry_trajectory "${TRAJECTORY_PARQUET}" "--time-forward"
echo ">>> no-pretrain perm-imp fielding_credit_putout (primary_fold)"
run_permimp_no_pretrain fielding_credit_putout "${FC_PARQUET}" ""

echo ""
echo "==================== ablation variants ===================="
# Format: artifact_id : stage1_epochs : hard_head_es (0/1)
VARIANTS=(
    "phase3-pretrain-v6-ab1:0:0"
    "phase3-pretrain-v6-ab2:3:0"
    "phase3-pretrain-v6-ab3:0:1"
    "phase3-pretrain-v6-ab4:3:1"
)

for spec in "${VARIANTS[@]}"; do
    IFS=':' read -r artifact stage1 hh <<< "${spec}"
    echo ""
    echo ">>> variant ${artifact} stage1=${stage1} hard_head_es=${hh}"
    echo "  fit"
    fit_variant "${artifact}" "${stage1}" "${hh}"
    echo "  sidecar"
    run_sidecar "${artifact}"
    echo "  publish as event_universe pointer"
    publish_as_event_universe "${artifact}"
    echo "  perm-imp trajectory (time-forward)"
    run_permimp geometry_trajectory "${TRAJECTORY_PARQUET}" "${artifact}" "--time-forward"
    echo "  perm-imp fielding_credit_putout (primary_fold)"
    run_permimp fielding_credit_putout "${FC_PARQUET}" "${artifact}" ""
done

echo ""
echo "==================== summary ===================="
PYTHONPATH="${REPO}/bc" uv run --group ml python scripts/summarize_pretrain_v6_ablation.py \
    > logs/ablation_v6/SUMMARY.md
cat logs/ablation_v6/SUMMARY.md
