# Publish pipeline

- `publish_ducklake.py` copies `main_models.*` + `main_seeds.*` out of `bc.db` into the `bc_publish` DuckLake catalog (`bc/bc_publish.ducklake` + `bc/bc_publish_data/`). ENUM columns cast to VARCHAR — DuckLake v1.0 doesn't preserve user-defined types.
- `upload_ducklake.py` ships catalog + data dir to `s3://timeball/baseball/v<DATA_VERSION>/` with long-lived `Cache-Control` and a Cloudflare cache purge.
- `create_web_db.py` publishes `bc_remote.db` + per-table parquet under the `dbt/` R2 prefix as the canonical site artifact. DuckLake site cutover is tracked in `notes/followups.md`.
- `preload_sources.py` is the only ad-hoc script allowed to write `bc.db` directly (only `CREATE TABLE IF NOT EXISTS`). Everything else goes through SQLMesh.

# Pretrain probes (sidecar)

- `slash_probe_pretrain.py <artifact_id>` — AVG/OBP/SLG against a saved pretrain `model_best.keras`. Always sets `KERAS_BACKEND=torch` at startup. Reads dataset_artifact_id from manifest.
- `fielding_probe_pretrain.py <artifact_id> [--dataset-artifact <id>]` — SS P(out) on GroundBall→SS contexts plus RF P(OutAdvancing) on OF-fly-with-r1 contexts. Needs `BC_DB_PATH` for `stg_people` roster resolution. Pass `--dataset-artifact` explicitly when the pretrain manifest hasn't been written yet (i.e. probe is being run mid-fit against a checkpoint).
- `run_pretrain_v8_residual.sh STAGE1_ID STAGE2_ID` — residual-decomposition pretrain orchestrator (prep dataset → stage-1 fit on context-only spec → emit per-head logit offsets → stage-2 fit on full spec with `BC_PRETRAIN_OFFSET_ARTIFACT` → sidecar (currently gated off by default; see `BC_V8_RUN_SIDECAR`) → publish branch-scoped pointer). Env: `BC_V8_DATASET_ARTIFACT`, `BC_V8_STAGE1_EPOCHS`, `BC_V8_STAGE2_EPOCHS`, `BC_V8_SKIP_PUBLISH`, `BC_PRETRAIN_DATASET_LIMIT` (smoke), plus `BC_PRETRAIN_LOSS=focal`, `BC_PRETRAIN_USE_HARD_HEAD_ES=1`, `BC_PRETRAIN_HARD_HEADS=...` defaulted appropriately. Script filename retains a version label (carried over from iteration); the underlying architecture is described in [[pretrain-architecture]].
- `pretrain_emit_offsets.py <stage1_artifact_id> --dataset-artifact <id>` — load a saved stage-1 pretrain model, build an inference submodel exposing per-head `{head}_logits` Dense layers, stream the dataset through `.predict()`, write float16 per-head parquet (`event_key, logit_0..logit_{K-1}`) under `<stage1_artifact_dir>/offsets/`. Respects `BC_PRETRAIN_DATASET_LIMIT` and the spec's `row_filter_predicate`. Loads model with `compile=False` (loss-config deserialization isn't needed for inference; also avoids registration issues with closure-returned focal loss).
- `pretrain_eval_pretrain.py <artifact_id>` — historical M1–M4 evaluation suite built against a head set that includes `pa_result`. Incompatible with the current 5-head batted-ball pretrain (no `pa_result`). Slated for replacement by a perm-imp regression suite over the live downstream supplements.
- `run_permimp_v8_compare.sh` / `run_permimp_v8_batted_ball.sh` — perm-imp acceptance-gate runners. The first does baseline-vs-pretrain on `geometry_trajectory`; the second sweeps the 3 geometry location targets (`location_side` / `_depth` / `_edge`). Fielding-credit DL is intentionally out of scope. Both invoke `permutation_importance_generic.py` and set the env knobs needed to consume the active pretrain (`BC_DEEP_FORCE_EMBED_DIM=128`, `BC_PRETRAIN_SKIP_DIM_MISMATCH=1`, `BC_DEEP_PRETRAIN_ARTIFACT_OVERRIDE=...`). `PUB_ROOT` is derived from the current git branch so the published-pointer resolution survives merge into other branches.

# Permutation importance

- `permutation_importance_generic.py` — fit a deep target on its FeatureLayout and report per-feature Δ_CE on a held-out slice. Use to derive per-supplement acceptance-gate floors (`BC_DEEP_DISABLE_PRETRAIN=1` for no-pretrain baseline; unset for v6-pretrained model). Flags / env:
  - `--target` / `PERMIMP_TARGET` — deep target name (e.g. `geometry_trajectory`).
  - `--dataset-parquet` / `PERMIMP_DATASET_PARQUET` — fully qualified parquet path.
  - `--time-forward` — eval on `time_forward_fold` instead of the spec's default `primary_fold`. Use for Phase-3 v6 gates so the slice matches production temporal eval.
  - `--val-filter` / `PERMIMP_VAL_FILTER` — extra SQL predicate applied to the VALIDATE rows post-partition.
  - `--full` / `PERMIMP_FULL=1` — disable train/val subsampling.
  - `--epochs` / `PERMIMP_EPOCHS` — override fit epochs.
- `permutation_importance_trajectory.py` — trajectory-only wrapper kept as a smoke-runner. Use the generic script for any new supplement.
