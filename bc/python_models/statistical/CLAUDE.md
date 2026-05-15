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
  dataset frame).
- `feature_layout.py` — re-exports `FeatureLayout` from `ml.features`
  and provides a per-dataset registry. Each target's first import
  triggers `register_coverage_layout(...)` as a side effect.
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
- `leakage_probes.py` — `source_probe_held_out(embeddings, source_labels,
  held_out_family)` → `ProbeResult` with sklearn LR + AUC tiers
  (`>=0.75` → diagnostic_only / `<0.65` → full / `[0.65, 0.75)` →
  manual_review).
- `targets/` — per-target registrations. PR3 Geometry (5 specs), PR4
  pitch_summary (1 spec), PR5 fielding_credit (3 specs).

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
