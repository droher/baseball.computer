# statistical/ — data-coverage statistical pipeline

Owns Phase 0–5 of the data-coverage initiative
(`notes/data-coverage-implementation/`). The Phase-3 deep package
(`deep/`) is the most recently expanded surface.

## Layout

- `cli.py` — `bc-stats` CLI. Subcommands: `prepare-dataset`,
  `run-eda`, `check-split-leakage`, `check-publication-gate`,
  `fit-deep`, `publish-manifest`, `validate`. The fit-deep handler
  lazy-imports keras inside the function body so a default
  `bc-stats --help` stays Torch-free.
- `dataset_registry.py` — per-dataset metadata for the seven
  `main_models.model_input_*` views. `dataset_version="0.2.0"` after
  Phase-3 PR3 (dl_p_class became DOUBLE[]).
- `datasets.py` / `eda.py` — Phase-2 snapshot + EDA producers.
- `model_config.py` / `publication.py` — Phase-2 publication gate.
- `calibration.py` — temperature scaling, isotonic regression, ECE,
  Brier, reliability curves. Numerical primitives shared with `deep/`.
- `validation.py` — small probability-normalization /
  conservation-residual helpers consumed by the EDA runner.
- `validate.py` — cross-phase `validate_artifact()` dispatcher
  (Phase-3 PR2). Reads `manifest.kind` and runs the appropriate check
  suite; emits a `ValidationReport`.
- `manifests.py` — artifact-id generation, manifest read/write
  (atomic), published-pointer resolution rooted at
  `BC_STATS_PUBLISHED_ROOT` (per-branch) with a global fallback under
  `artifacts/statistical/published/`.
- `schemas.py` — Pydantic v2 schemas for manifests, datasets,
  diagnostics, validation, EDA report, validation report.
- `splits.py` — `game_hash_fold(game_id, fold_count)` BLAKE2s splits.
- `leakage.py` — split-registry leakage detection (Phase-2).
- `outputs.py` / `diagnostics.py` — Phase-4/5 hooks (mostly stubs).

## Deep package (`deep/`)

Phase-3 supplements: train deep proposal distributions, embeddings,
and calibrators on frozen modeling datasets so Phase-4 Bayes models
can consume out-of-fold deep outputs as regularized inputs. **Deep
outputs never publish as facts.**

- `__init__.py` — sets `KERAS_BACKEND=torch` via `os.environ.setdefault`
  so any deep import lands on Torch.
- `target_spec.py` — `DeepTargetSpec(BaseModel, frozen=True)`. Required
  fields include `name`, `dataset_name`, `target_column`,
  `weight_column`, `kind ∈ {multiclass, binary}`, `proposal_dimension`
  (drives `published_manifest_name()` and the dimension column on the
  sibling manifest), and optional `filter_predicate` (DuckDB SQL on the
  dataset frame). Opt-in `pretrained_embeddings_artifact_id` +
  `freeze_pretrained_embeddings` warm-start the per-target Embedding
  layers from a `kind="pretrain"` artifact post-`build_model` (PR8).
- `feature_layout.py` — re-exports `FeatureLayout` from `ml.features`
  and provides a per-dataset registry. Each target's first import
  triggers `register_coverage_layout(...)` as a side effect. Also
  exports `validate_pre_event(layout)` — every `_register()` calls it
  before publishing the layout. The deny-list rejects post-event /
  outcome-correlated columns (`*_end`, `*_on_play`, `pa_result`,
  `result_family`, `fielder_chain`, `gap_class`,
  `fielding_evidence_status`, `batted_to_fielder*`,
  `hit_or_out`, `balls_called`, `strikes_*`, `swings*`, `pitches*`).
  See [pretrain invariants](#pretrain-invariants).
- `registry.py` — target name → `DeepTargetSpec` and per-target
  `SiblingManifestName` mapping (`dl_proposal_manifest |
  dl_credit_proposal_manifest | dl_advancement_proposal_manifest |
  dl_embedding_artifact`).
- `io.py` — Parquet loader + `add_kfold_id` (BLAKE2s on game_id) +
  `assert_game_group_invariant` write-time check.
- `training.py` — fold runner. For each target: 5 OOF fits + 1 full
  fit (`fold_count + 1` total). Writes `probabilities.parquet`,
  `class_labels.json`, `manifest.json` atomically.
- `artifacts.py` — atomic writes under
  `artifacts/statistical/deep/<target>/<artifact_id>/`.
- `manifest_ingest.py` — `aggregate_proposal_manifest_frames(sibling)`
  feeds the `dl_*_proposal_manifest` SQLMesh `@model` files. Always
  yields at least one (possibly empty) frame so the typed schema
  persists.
- `calibrators.py` — `fit_multiclass_temperature`,
  `fit_isotonic_per_class`, `fit_platt_per_slice` (with parent-slice
  fallback).
- `embeddings.py` — extraction + parquet export +
  `aggregate_embedding_frames` for the `dl_embedding_artifact` @model.
- `pretrain/` — shared entity-embedding pretraining via two-stage
  residual decomposition. `spec.py` defines `PretrainSpec` +
  `HeadSpec` (the `PretrainSpec.row_filter_predicate` field carries a
  polars-SQL predicate applied pre-partition). `heads.py` does
  per-head encoding with NULL-mask `sample_weight`s.

  **Two-stage fit.** Stage 1 trains the trunk + heads on a
  context-only layout (no entity IDs, no observed-outcome inputs).
  `scripts/pretrain_emit_offsets.py` loads the saved stage-1
  `model.keras` (with `compile=False`), builds an inference submodel
  whose outputs are the per-head `{head}_logits` intermediate Dense
  layers, streams the dataset through `.predict()`, and writes one
  float16 parquet per head under
  `artifacts/statistical/deep/<stage1_spec>/<stage1_id>/offsets/<head>.parquet`
  with columns `event_key, logit_0 .. logit_{K-1}`. Stage 2 runs the
  full architecture (with batter+pitcher embedding group + observed
  outcomes as inputs) with `BC_PRETRAIN_OFFSET_ARTIFACT=<stage1_id>`
  set: `pretrain/training.run_pretrain` joins the per-head offsets
  onto the dataset by `event_key`, encodes
  `x[f"offset_{head}"] = float32 (N, num_classes)`, overrides head
  class labels with stage-1's manifest so cached logits align by
  index, and passes `offset_inputs={...}` to `build_pretrain_model`.
  Loss path is `CE(softmax(stage1_logits + Δ_logits), label)` so the
  embedding only learns the residual / interaction signal not
  predictable from context alone.

  **Active spec.** `targets.py` registers the live spec
  (currently named `event_universe_v8` — to be renamed; the
  versioned suffix is incidental) with embedding group
  `('player', ('batter_id', 'pitcher_id'))` plus ungrouped
  `park_id` / `scorer`. Layout intentionally includes post-event
  observed outcomes — `pa_result`, `outs_on_play_capped`,
  `runs_on_play_capped`, `r1/r2/r3_advancement` — as inputs (low_card
  / numeric). `validate_pre_event` is NOT called on pretrain layouts
  for this reason. Heads (5, all imputation targets):
  `trajectory_remapped`, `batted_location_general`,
  `batted_location_depth`, `batted_location_edge`,
  `batted_to_fielder_class`. Row filter: `pa_result IN (<11
  batted-ball outcomes>)`. The earlier 11-head + 13-slot
  player-group variant remains in tree under the
  `event_universe` / `event_universe_context` spec names for back-
  compat only — not the active pretrain.

  **Training config.** `training.py` honors split optimizers (trunk
  Adam 1e-3, embed Adam 5e-3→1.5e-3), Kendall-Gal uncertainty
  weighting, `BC_PRETRAIN_LOSS=focal` (γ via
  `BC_PRETRAIN_FOCAL_GAMMA`), and `BC_PRETRAIN_USE_HARD_HEAD_ES=1`
  with `BC_PRETRAIN_HARD_HEADS=...` (defaults to the v6
  `pa_result, trajectory_remapped, batted_to_fielder_class` set — for
  the active 5-head spec, override to
  `trajectory_remapped,batted_location_general,batted_to_fielder_class`).
  Smoke knob: `BC_PRETRAIN_DATASET_LIMIT=<N>` deterministically
  subsamples both stages with `df.sample(n=N, seed=0)` so stage-1 /
  emit / stage-2 agree on rows.

  **Diagnostic probes** (`probes.py`, each gated on its own env var,
  all also need `BC_DB_PATH`): `SlashLineProbe`, `FieldingProbe`,
  `OutfieldArmProbe`, `LinearProbeDownstreamCallback`, and
  `MonteCarloSlashProbe`. These were built against a head set that
  includes `pa_result` and are incompatible with the active 5-head
  spec — sidecar (`scripts/pretrain_eval_pretrain.py`) is gated off
  by default in `scripts/run_pretrain_v8_residual.sh` until a probe
  suite targeting the live heads is written. Replace with batted-ball
  perm-imp regression suite. `artifacts.py` writes one embedding
  matrix per **embedding unit** (group or ungrouped col);
  `vocab.json` keys mirror those units.
- `leakage_probes.py` — `source_probe_held_out(embeddings, source_labels,
  held_out_family)` → `ProbeResult` with sklearn LR + AUC tiers
  (`>=0.75` → diagnostic_only / `<0.65` → full / `[0.65, 0.75)` →
  manual_review).
- `targets/` — per-target registrations.
  - `geometry.py` — 4 specs (trajectory, location_side, location_depth,
    location_edge). Pre-event layout: high-card batter /
    pitcher / park / scorer; numeric ball/strike count, outs_start,
    score_margin, leverage_index, season, inning_start.
  - `pitch_summary.py` — 1 binary spec on `has_count`. Pre-event
    LOW_CARD only; `result_family` excluded as post-event leak.
  - `advancement.py` — 3 specs (`advancement_r1/_r2/_r3`), 7-class
    each, per-baserunner filter. Pre-event layout includes `runner_id`
    in high-card and `baserunner` / `base_start` in low-card / numeric.
    Excludes the dataset's own `trajectory_class` /
    `location_depth_class` / `ball_handler_position_class` / `hit_or_out`
    / `runs_on_play` / `result_family` — all post-event leak.
  Each target's `_register()` invokes `validate_pre_event(layout)`.
  Every registered spec MUST set `pretrained_embeddings_artifact_id =
  "event_universe"` — `_maybe_load_pretrained_embeddings` early-returns
  on `None` and `BC_DEEP_PRETRAIN_ARTIFACT_OVERRIDE` never fires in
  that case. `test_every_geometry_spec_declares_pretrain_artifact`
  guards geometry; add equivalent for new target modules. Override
  via `BC_DEEP_PRETRAIN_ARTIFACT_OVERRIDE=<spec_name>` when running
  against a non-canonical pretrain.
  Fielding-credit DL allocation is intentionally out of scope — it's
  a spatial-allocation task that doesn't benefit from shared player
  embeddings; Phase-4 Bayes owns it. The `batted_to_fielder_class`
  pretrain head stays (auxiliary signal for the location heads).

### Pretrain invariants

- **Pre-event inputs only on downstream supplements.** Every
  supplement's `FeatureLayout` must contain only features observable
  at inference time. Post-event columns (`fielder_chain`,
  `pa_result`, `result_family`, `batted_to_fielder*`, `outs_on_play`,
  `runs_on_play`, `*_end`, cumulative AB pitch/swing counters,
  `gap_class`, `fielding_evidence_status`, `hit_or_out`) train to a
  distribution that does not exist at scoring time and let the model
  shortcut entity priors. `validate_pre_event` enforces a deny-list
  and is called from every supplement's `_register()`. The pretrain
  layout is exempt — see [[pretrain-architecture]].
- **Time-forward gate eval.** Permutation-importance and acceptance
  gates run on `time_forward_fold = 'VALIDATE'` (season = 2023). Use
  `scripts/permutation_importance_generic.py --time-forward`. The
  training split stays on `primary_fold` (HASH(game_id)) to maximize
  data.
- **Pretrain head set.** Heads must cover only imputation targets
  (data genuinely missing in retrosheet). Fully-observed outcomes go
  in the input set, not the head set. See [[pretrain-architecture]]
  for the load-bearing rationale and current head list.
- **Pretrain → supplement gate.** No-pretrain baseline ran per
  supplement on the stripped layout sets the gate floor; pretrained
  artifact must hit ≥ ×1.5 batter-side Δ_CE and not regress on
  park / pitcher. Recorded under
  `notes/data-coverage-implementation/phase3-acceptance-gates-v6.md`
  (filename label is incidental).
- **Training labels = directly recorded only.** Modeling-dataset SQL
  (`model_input_geometry`, `model_input_event_universe`,
  `model_input_advancement`) uses `observed_status = 'observed'`
  (not `IN ('observed', 'derived')`) and `class = raw_value` (not
  `COALESCE(raw_value, deduced_value)`). Heuristic deductions in
  `calc_batted_ball_type` (HR→Fly, OF-putout→AirBall,
  infielder-assisted-putout→GroundBall, fielder-position-derived
  location_side/depth) and `event_observation_geometry` still surface
  for analyses / published aggregates; they are excluded from the
  DL training-label path so pretrain heads + downstream supplements
  train only on what was directly recorded. The `pulled_opposite`
  dimension is purely derived → 0 eligible rows under this rule.

## SQLMesh @models (in `bc/models/intermediate/coverage/`)

Three Python `@model` files now gate the Phase-3 deep outputs:

- `dl_proposal_manifest.py` — grain `(event_key, dimension)`, joined
  by `model_input_geometry`, `model_input_observation_batted_ball`,
  `model_input_pitch_summary`, `model_input_park_factors`,
  `model_input_run_values`.
- `dl_credit_proposal_manifest.py` — grain
  `(event_key, player_id, fielding_position, credit_type)`, joined by
  `model_input_fielding_credit`.
- `dl_advancement_proposal_manifest.py` — grain
  `(event_key, baserunner)`, joined by `model_input_advancement`.
- `dl_embedding_artifact.py` — embedding artifact aggregator (PR6).

Every `@model` reads its rows via the corresponding
`manifest_ingest.aggregate_*` helper; the test suite hits the helper
directly because importing `@model` files without a SQLMesh context
fails to parse the project dialect ("UINTEGER" is not parsed in stock
sqlglot).

## Manifest path discovery

`manifests.find_published_manifest(name)` resolves a model name to its
published-pointer path. Resolution order:

1. `BC_STATS_PUBLISHED_ROOT` (per-branch root) — set by the justfile
   recipes to
   `artifacts/statistical/published-<branch_slug>/`.
2. `GLOBAL_PUBLISHED_ROOT` =
   `artifacts/statistical/published/` (cross-branch fallback).

Tests use `monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, ...)` to redirect
to a tmp path. `cfg.DEEP_ROOT` / `cfg.BAYES_ROOT` etc. are referenced
via attribute access in `validate.py` and CLI handlers so
`monkeypatch.setattr(cfg, "DEEP_ROOT", tmp_path)` propagates at call
time.

## Tests

Under `bc/tests/statistical/` (CLI, prepare-dataset, run-eda,
publication gate) and `bc/tests/statistical/deep/` (deep package). The
fold runner test is marked `@pytest.mark.slow` because it imports
Keras + Torch.

`pytest -m "not slow"` is the default fast tier (excludes 5 fold-runner
tests). `pytest -m slow` runs them; full `pytest bc/tests` runs
everything. CI scopes via these markers.

## Conventions

- Pydantic v2 `BaseModel` for any schema with validation or serialization.
- `logging.getLogger(__name__)` with module-level `_log = ...`.
- No `print()` in library code; the CLI's structured-log info messages
  are the audit trail.
- Atomic writes via `tempfile.mkstemp` + `os.replace` for any file the
  publication gate reads.
- Lazy `import keras` and `import sklearn` inside function bodies so
  `bc-stats --help` does not pay the cost.
