#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "$0")/.."
lane="$1"; shift
log_dir="logs/review-refits"
mkdir -p "$log_dir"
for spec in "$@"; do
  read -r model dataset artifact flags <<<"$spec"
  manifest="artifacts/statistical/bayes/$model/$artifact/manifest.json"
  if [ -f "$manifest" ]; then
    echo "[$lane] skip $model/$artifact (manifest exists)"
    continue
  fi
  echo "[$lane] start $model/$artifact $(date -u +%FT%TZ)"
  if just fit-bayes "$model" "$dataset" "$artifact" ${flags:-} > "$log_dir/$model--$artifact.log" 2>&1; then
    echo "[$lane] done  $model/$artifact $(date -u +%FT%TZ)"
  else
    echo "[$lane] FAIL  $model/$artifact $(date -u +%FT%TZ) (see $log_dir/$model--$artifact.log)"
  fi
done
echo "[$lane] lane complete $(date -u +%FT%TZ)"
