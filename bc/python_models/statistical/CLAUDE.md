# statistical/ — data-coverage statistical pipeline

Owns Phase 0–5 of the data-coverage initiative (`notes/data-coverage-implementation/`). The Phase-3 deep package (`deep/`) is the most recently expanded surface.

## Layout

- `cli.py` — `bc-stats` CLI. Subcommands: `prepare-dataset`, `run-eda`, `check-split-leakage`, `check-publication-gate`, `fit-deep`, `fit-pretrain`, `fit-bayes`, `publish-pretrain`, `publish-manifest`, `validate`. The fit-deep handler lazy-imports keras and the fit-bayes handler lazy-imports pymc inside the function body so `bc-stats --help` stays Torch- and PyMC-free.
- `dataset_registry.py` — per-dataset metadata for the seven `main_models.model_input_*` views. `dataset_version="0.2.0"` after the PR that flipped `dl_p_class` to `DOUBLE[]`.
- `datasets.py` / `eda.py` — Phase-2 snapshot + EDA producers.
- `model_config.py` / `publication.py` — Phase-2 publication gate.
- `calibration.py` — temperature scaling, isotonic regression, ECE, Brier, reliability curves. Numerical primitives shared with `deep/`.
- `validation.py` — probability-normalization / conservation-residual helpers consumed by the EDA runner.
- `validate.py` — cross-phase `validate_artifact()` dispatcher. Reads `manifest.kind` and runs the appropriate check suite. Emits a `ValidationReport`.
- `manifests.py` — artifact-id generation, atomic manifest read / write, published-pointer resolution rooted at `BC_STATS_PUBLISHED_ROOT` (per-branch) with a global fallback under `artifacts/statistical/published/`.
- `schemas.py` — Pydantic v2 schemas for manifests, datasets, diagnostics, validation, EDA report, validation report.
- `splits.py` — `game_hash_fold(game_id, fold_count)` BLAKE2s splits.
- `leakage.py` — split-registry leakage detection.
- `outputs.py` / `diagnostics.py` — Phase-4/5 hooks (mostly stubs).
- `bayes/` — Phase-4 hierarchical-Bayes runtime. `artifacts.py` lays out `artifacts/statistical/bayes/<model_name>/<artifact_id>/{inference,exports,validation,manifest.json}`. `training.py` exposes `run_bayes_model(...)` which builds inputs via `models/_data.prepare_observation_inputs`, samples prior predictive, NUTS posterior (unless `prior_only`), and posterior predictive, then writes `inference/*.nc`, `exports/{posterior_summary,calibration_curve}.parquet`, and `validation/diagnostics.json` atomically. PR1 dispatches `trajectory_observedness` only; PR2 covers the remaining 3 dims and the `gamma_dl_shrunk` flavor.
- `models/observation.py` — PyMC `build_trajectory_observedness_model(inputs)` (non-centered season/scorer/source intercepts, `gamma_dl=0` frozen, `dl_logit` carried as `pm.Data` to match the PR2 shrunk path). Other three observedness builders raise `NotImplementedError` until PR2. `models/_data.ObservationModelInputs` is a frozen Pydantic model carrying numpy index arrays + label coords.
- `pymc_utils.py` — `SamplingConfig` + `SMOKE_CONFIG`/`DEFAULT_CONFIG` + `sample_model`/`prior_predictive`/`posterior_predictive`. Imports PyMC and ArviZ at module top; the module is itself lazy-imported by the CLI handler.

### Bayes smoke loop

- `just fit-bayes <model> <dataset-artifact-id> <fit-artifact-id> --smoke` runs `SMOKE_CONFIG` (50 draws × 50 tune × 2 chains) and applies the default 100k-row subsample. `--prior-only` short-circuits before `pm.sample`. Subsampling uses `df.sample(n=N, seed=20260513)` for reproducibility.
- `BC_STATS_SMOKE_LIMIT=N` overrides the row count (mirrors `BC_PRETRAIN_DATASET_LIMIT`).
- `validate-artifact <id>` dispatches on `manifest.kind == "bayes"` to thresholds in `validate._validate_bayes`: smoke gates (`rhat_max ≤ 1.5`, `ess_bulk_min ≥ 10`, `divergences ≤ 5% of total_draws`, ECE/bucket-dev warn-level only — sized to catch *broken NUTS* on SMOKE_CONFIG's 100-draw budget, not slow mixing) vs default gates (`rhat ≤ 1.05`, `ess ≥ 400`, zero divergences). SMOKE_CONFIG samples sequentially (`cores=1`) on macOS to dodge the multiprocess-fork-after-Accelerate hang.

## Deep package (`deep/`)

Train deep proposal distributions, embeddings, and calibrators on frozen modeling datasets so Phase-4 Bayes models can consume out-of-fold deep outputs as regularized inputs. **Deep outputs never publish as facts.**

- `__init__.py` — sets `KERAS_BACKEND=torch` via `os.environ.setdefault` so any deep import lands on Torch.
- `target_spec.py` — `DeepTargetSpec(BaseModel, frozen=True)`. Required fields: `name`, `dataset_name`, `target_column`, `weight_column`, `kind ∈ {multiclass, binary}`, `proposal_dimension` (drives `published_manifest_name()` and the `dimension` column on the sibling manifest). Optional `filter_predicate` (DuckDB SQL on the dataset frame). Opt-in `pretrained_embeddings_artifact_id` + `freeze_pretrained_embeddings` warm-start the per-target Embedding layers from a `kind="pretrain"` artifact post-`build_model`.
- `feature_layout.py` — re-exports `FeatureLayout` from `ml.features` and provides a per-dataset registry. Each target's first import triggers `register_coverage_layout(...)` as a side effect. Also exports `validate_pre_event(layout)`; every `_register()` calls it before publishing the layout. The deny-list rejects post-event / outcome-correlated columns (`*_end`, `*_on_play`, `pa_result`, `result_family`, `fielder_chain`, `gap_class`, `fielding_evidence_status`, `batted_to_fielder*`, `hit_or_out`, `balls_called`, `strikes_*`, `swings*`, `pitches*`).
- `registry.py` — target name → `DeepTargetSpec` and per-target `SiblingManifestName` mapping (`dl_proposal_manifest | dl_credit_proposal_manifest | dl_advancement_proposal_manifest | dl_embedding_artifact`).
- `io.py` — Parquet loader + `add_kfold_id` (BLAKE2s on game_id) + `assert_game_group_invariant` write-time check.
- `training.py` — fold runner. For each target: `fold_count` OOF fits + 1 full fit. Writes `probabilities.parquet`, `class_labels.json`, `manifest.json` atomically.
- `artifacts.py` — atomic writes under `artifacts/statistical/deep/<target>/<artifact_id>/`.
- `manifest_ingest.py` — `aggregate_proposal_manifest_frames(sibling)` feeds the `dl_*_proposal_manifest` SQLMesh `@model` files. Always yields at least one (possibly empty) frame so the typed schema persists.
- `calibrators.py` — `fit_multiclass_temperature`, `fit_isotonic_per_class`, `fit_platt_per_slice` (with parent-slice fallback).
- `embeddings.py` — extraction + parquet export + `aggregate_embedding_frames` for the `dl_embedding_artifact` @model.
- `pretrain/` — shared entity-embedding pretrain via two-stage residual decomposition. See [[pretrain-architecture]] memory entry for the full architecture. `spec.py` defines `PretrainSpec` + `HeadSpec` (the `PretrainSpec.row_filter_predicate` field carries a polars-SQL predicate applied pre-partition). `heads.py` does per-head encoding with NULL-mask `sample_weight`s.

  **Two-stage fit.** Stage 1 trains the trunk + heads on a context-only layout (no entity IDs, no observed-outcome inputs). `scripts/pretrain_emit_offsets.py` loads the saved stage-1 `model.keras` (with `compile=False`), builds an inference submodel whose outputs are the per-head `{head}_logits` intermediate Dense layers, streams the dataset through `.predict()`, and writes one float16 parquet per head under `artifacts/statistical/deep/<stage1_spec>/<stage1_id>/offsets/<head>.parquet` with columns `event_key, logit_0 .. logit_{K-1}`. Stage 2 runs the full architecture (with batter+pitcher embedding group + observed outcomes as inputs) with `BC_PRETRAIN_OFFSET_ARTIFACT=<stage1_id>` set: `pretrain/training.run_pretrain` joins the per-head offsets onto the dataset by `event_key`, encodes `x[f"offset_{head}"] = float32 (N, num_classes)`, overrides head class labels with stage-1's manifest so cached logits align by index, and passes `offset_inputs={...}` to `build_pretrain_model`. Loss path is `CE(softmax(stage1_logits + Δ_logits), label)` so the embedding only learns the residual / interaction signal not predictable from context alone.

  **Active spec.** `targets.py` registers two spec names: `event_universe` (full layout) and `event_universe_context` (stage-1 context-only layout). Both share the 5-head spec and the batted-ball row filter. Full layout: embedding group `('player', ('batter_id', 'pitcher_id'))` plus ungrouped `park_id` / `scorer`; low_card / numeric intentionally include post-event observed outcomes (`pa_result`, `outs_on_play_capped`, `runs_on_play_capped`, `r1/r2/r3_advancement`). `validate_pre_event` is NOT called on pretrain layouts for this reason. Heads (5, all imputation targets): `trajectory_remapped`, `batted_location_general`, `batted_location_depth`, `batted_location_edge`, `batted_to_fielder_class`. Row filter: `pa_result IN (<11 batted-ball outcomes>)`.

  **Training config.** `training.py` honors split optimizers (trunk Adam 1e-3, embed Adam 5e-3→1.5e-3), Kendall-Gal uncertainty weighting, `BC_PRETRAIN_LOSS=focal` (γ via `BC_PRETRAIN_FOCAL_GAMMA`), and `BC_PRETRAIN_USE_HARD_HEAD_ES=1` with `BC_PRETRAIN_HARD_HEADS=...` (default `trajectory_remapped,batted_location_general,batted_to_fielder_class` for the active 5-head spec). Smoke knob: `BC_PRETRAIN_DATASET_LIMIT=<N>` deterministically subsamples both stages with `df.sample(n=N, seed=0)` so stage-1 / emit / stage-2 agree on rows.

  `artifacts.py` writes one embedding matrix per embedding unit (group or ungrouped col); `vocab.json` keys mirror those units. Pretrain training-time diagnostic probes (the historical `SlashLineProbe` / `FieldingProbe` / `OutfieldArmProbe` / `LinearProbeDownstreamCallback` / `MonteCarloSlashProbe`) were removed when the pretrain head set dropped `pa_result`. Replacement evaluation: batted-ball perm-imp regression suite (`scripts/permutation_importance_generic.py`).

- `leakage_probes.py` — `source_probe_held_out(embeddings, source_labels, held_out_family)` → `ProbeResult` with sklearn LR + AUC tiers (`>=0.75` → diagnostic_only / `<0.65` → full / `[0.65, 0.75)` → manual_review).

- `targets/` — per-target registrations.
  - `geometry.py` — 4 specs (trajectory, location_side, location_depth, location_edge). Pre-event layout: high-card batter / pitcher / park / scorer; numeric ball/strike count, outs_start, score_margin, leverage_index, season, inning_start.
  - `advancement.py` — 3 specs (`advancement_r1/_r2/_r3`), 7-class each, per-baserunner filter. Pre-event layout includes `runner_id` in high-card and `baserunner` / `base_start` in low-card / numeric. Excludes the dataset's own `trajectory_class` / `location_depth_class` / `ball_handler_position_class` / `hit_or_out` / `runs_on_play` / `result_family` (all post-event leak). Not registered on import; the `model_input_advancement` SQL gap closes before `_register()` runs.

  Each target's `_register()` invokes `validate_pre_event(layout)`. Every registered spec MUST set `pretrained_embeddings_artifact_id = "event_universe"`. `_maybe_load_pretrained_embeddings` early-returns on `None` and `BC_DEEP_PRETRAIN_ARTIFACT_OVERRIDE` never fires in that case. `test_every_geometry_spec_declares_pretrain_artifact` guards geometry; add equivalent for new target modules. Override via `BC_DEEP_PRETRAIN_ARTIFACT_OVERRIDE=<spec_name>` when running against a non-canonical pretrain.

  Fielding-credit DL allocation is out of scope. Spatial-allocation task that doesn't benefit from shared player embeddings; Phase-4 Bayes owns it. The `batted_to_fielder_class` pretrain head stays as auxiliary signal for the location heads. Pitch-summary DL imputation (`has_count`, `has_pitch_sequence`, per-stream dims) is also Phase-4+; deferred specs are not currently in tree.

### Pretrain invariants

- **Pre-event inputs only on downstream supplements.** Every supplement's `FeatureLayout` must contain only features observable at inference time. Post-event columns (`fielder_chain`, `pa_result`, `result_family`, `batted_to_fielder*`, `outs_on_play`, `runs_on_play`, `*_end`, cumulative AB pitch/swing counters, `gap_class`, `fielding_evidence_status`, `hit_or_out`) train to a distribution that does not exist at scoring time and let the model shortcut entity priors. `validate_pre_event` enforces a deny-list and is called from every supplement's `_register()`. The pretrain layout is exempt. See [[pretrain-architecture]].
- **Time-forward gate eval.** Permutation-importance and acceptance gates run on `time_forward_fold = 'VALIDATE'` (season = 2023). Use `scripts/permutation_importance_generic.py --time-forward`. The training split stays on `primary_fold` (HASH(game_id)) to maximize data.
- **Pretrain head set.** Heads cover only imputation targets (data genuinely missing in retrosheet). Fully-observed outcomes go in the input set, not the head set. See [[pretrain-architecture]] for the rationale and current head list.
- **Pretrain → supplement gate.** No-pretrain baseline ran per supplement on the stripped layout sets the gate floor; pretrained artifact must hit ≥ ×1.5 batter-side Δ_CE and not regress on park / pitcher. Recorded under `notes/data-coverage-implementation/phase3-acceptance-gates-v6.md` (filename label is incidental).
- **Training labels = directly recorded only.** Modeling-dataset SQL (`model_input_geometry`, `model_input_event_universe`, `model_input_advancement`) uses `observed_status = 'observed'` (not `IN ('observed', 'derived')`) and `class = raw_value` (not `COALESCE(raw_value, deduced_value)`). Heuristic deductions in `calc_batted_ball_type` (HR→Fly, OF-putout→AirBall, infielder-assisted-putout→GroundBall, fielder-position-derived location_side/depth) and `event_observation_geometry` still surface for analyses / published aggregates; they are excluded from the DL training-label path so pretrain heads + downstream supplements train only on what was directly recorded. The `pulled_opposite` dimension is purely derived (zero eligible rows under this rule).
- **Downstream embed-dim env.** Geometry fits need `BC_DEEP_FORCE_EMBED_DIM=128` to match the pretrain's capped 128-D player embedding. Combine with `BC_PRETRAIN_SKIP_DIM_MISMATCH=1` so the ungrouped `park_id` (pretrain dim 17) and `scorer` (pretrain dim 95) layers skip warm-start instead of failing. Tracked as a follow-up in `notes/followups.md`.

## SQLMesh @models (in `bc/models/intermediate/coverage/`)

Four Python `@model` files gate the Phase-3 deep outputs:

- `dl_proposal_manifest.py` — grain `(event_key, dimension)`, joined by `model_input_geometry`, `model_input_observation_batted_ball`, `model_input_pitch_summary`, `model_input_park_factors`, `model_input_run_values`.
- `dl_credit_proposal_manifest.py` — grain `(event_key, player_id, fielding_position, credit_type)`, joined by `model_input_fielding_credit`. Materializes as a zero-row stub (fielding-credit DL out of scope).
- `dl_advancement_proposal_manifest.py` — grain `(event_key, baserunner)`, joined by `model_input_advancement`. Materializes as a zero-row stub until advancement specs register.
- `dl_embedding_artifact.py` — embedding artifact aggregator.

Every `@model` reads its rows via the corresponding `manifest_ingest.aggregate_*` helper. The test suite hits the helper directly because importing `@model` files without a SQLMesh context fails to parse the project dialect (`UINTEGER` is not parsed in stock sqlglot).

## Manifest path discovery

`manifests.find_published_manifest(name)` resolves a model name to its published-pointer path. Resolution order:

1. `BC_STATS_PUBLISHED_ROOT` (per-branch root). Set by the justfile recipes to `artifacts/statistical/published-<branch_slug>/`.
2. `GLOBAL_PUBLISHED_ROOT` = `artifacts/statistical/published/` (cross-branch fallback).

`find_published_pretrain(name)` mirrors this but resolves under the `pretrain/` subdir of each root.

Tests use `monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, ...)` to redirect to a tmp path. `cfg.DEEP_ROOT` / `cfg.BAYES_ROOT` are referenced via attribute access in `validate.py` and CLI handlers so `monkeypatch.setattr(cfg, "DEEP_ROOT", tmp_path)` propagates at call time.

## Tests

Under `bc/tests/statistical/` (CLI, prepare-dataset, run-eda, publication gate) and `bc/tests/statistical/deep/` (deep package). The fold runner test is marked `@pytest.mark.slow` because it imports Keras + Torch.

`pytest -m "not slow"` is the default fast tier. `pytest -m slow` runs the fold-runner tests. Full `pytest bc/tests` runs everything. CI scopes via these markers.

## Conventions

- Pydantic v2 `BaseModel` for any schema with validation or serialization.
- `logging.getLogger(__name__)` with module-level `_log = ...`.
- No `print()` in library code. The CLI's structured-log info messages are the audit trail.
- Atomic writes via `tempfile.mkstemp` + `os.replace` for any file the publication gate reads.
- Lazy `import keras` and `import sklearn` inside function bodies so `bc-stats --help` does not pay the cost.
