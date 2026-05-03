# Phase 6 — Open Follow-ups

Branch: `phase-6-hamilton-ml`. Goal: re-enable
`bc/models/intermediate/machine_learning/` (disabled 2026-04-30,
commit `e8ada10` when JAX/Keras stack was dropped) as a Hamilton-shaped
DAG with Keras 3 + PyTorch backend, MLflow file-mode tracking, and
predictions written back into DuckDB via a streaming Python `@model`.

## What landed (first target — `outcome_plate_appearance_cat`)

- `bc/models/intermediate/machine_learning/ml_event_outcomes.sql` —
  `enabled FALSE` removed. Audits added: `not_null`, `unique_grain` on
  `event_key`. 16,313,067 rows in dev.
- `bc/models/intermediate/machine_learning/ml_features.sql` —
  `enabled FALSE` removed. Audits added: `not_null`, `unique_grain`
  on `event_key`, plus `relationships` to `ml_event_outcomes`. Same
  rowcount; train/test split is `HASH(game_id) % 100 BETWEEN 0 AND 97
  → TRAIN, else TEST` (98 / 2 by game).
- `bc/python_models/ml/` — new package. Files:
  - `__init__.py` — sets `KERAS_BACKEND=torch` before any keras import.
  - `data_loaders.py` — `open_bc_db()` context manager and
    `stream_query()` that yields Polars frames per DuckDB Arrow
    record batch.
  - `features.py` — feature schema source of truth: 6 high-card
    categorical columns (batter, pitcher, park, three runner slots),
    3 low-card categorical, 7 numeric, plus the target / weight /
    split / grain column names. `Vocabulary` dataclass reserves
    index 0 for OOV; `build_vocabulary` skips nulls and sorts.
  - `model_plate_appearance_cat.py` — `build_model(vocab_sizes,
    numeric_means, numeric_variances, num_classes)` returns a
    Keras 3 model: per-categorical embeddings (dim ≈ √vocab + 1, capped
    at 64) for high-card, `CategoryEncoding(one_hot)` for low-card,
    stateless `Normalization` for numerics, two dense ReLU blocks,
    softmax head.
  - `training_plate_appearance_cat.py` — vocab + numeric stats pass,
    streaming generator with mini-batch sub-chunking, `model.fit`,
    MLflow logging, vocabulary parquet writes, pin JSON.
  - `prediction.py` — `Scorer.score(features) → pl.DataFrame` plus
    `load_scorer(target)` that reads the pin JSON and rebuilds the
    scorer from MLflow + saved vocabularies.
  - `artifacts/plate_appearance_cat.json` — checked-in pin file with
    `{run_id, tracking_uri, vocab_paths, class_labels, numeric_means,
    numeric_variances}`.
- `bc/models/intermediate/machine_learning/predictions_plate_appearance_cat.py`
  — SQLMesh Python `@model` (FULL, grain=[event_key]), 4 audits.
  Materializes `ml_features` once via DuckDB Arrow `fetch_arrow_table`,
  iterates `to_batches(max_chunksize=500_000)`, scores each, yields
  pandas. 16,313,067 prediction rows in dev, 4 audits passed.
- `scripts/train_plate_appearance_cat.py` — CLI entry point. Flags:
  `--db`, `--schema` (defaults to `main_models__dev`), `--epochs`,
  `--rows-per-batch`, `--log-level`.
- `bc/tests/test_ml_features.py` — 7 unit tests on feature schema and
  vocabulary helpers.
- `bc/tests/test_plate_appearance_cat_model.py` — 4 unit tests on
  model factory + scorer invariants (no fitted weights asserted —
  invariants only).
- `bc/python_models/ml/hamilton_dag.py` — Hamilton DAG: 8 nodes
  (`feature_stats`, `num_classes`, `class_index`, `vocab_sizes`,
  `model`, `mlflow_setup`, `fitted_run`, `saved_vocab_paths`,
  `pin_written`). `train()` in `training_plate_appearance_cat.py`
  builds a `hamilton.driver.Driver`, requests the `pin_written` leaf,
  and returns the run id. Adding the second target reuses the same
  `feature_stats` and `mlflow_setup` nodes — only `model` and
  `fitted_run` are target-specific.
- `notes/phase-6-dag.png` — Hamilton DAG visualization, regenerable
  via `export_dag_diagram(Path('notes/phase-6-dag.png'))`.
- `pyproject.toml` — new `migration-ml` dep group:
  `apache-hamilton[visualization]>=1.90` (canonical Apache name as of
  2025; `sf-hamilton` is now a redirect package), `mlflow>=2.18`,
  `scikit-learn>=1.5`, `keras>=3.7`, `torch>=2.5`. Mutually exclusive
  with `spikes-bsl` (same sqlglot pin reason as `migration`). Also
  added `[tool.pyright] extraPaths = ["bc"]` so basedpyright resolves
  the `python_models.*` package layout.
- `.gitignore` — adds `bc/mlruns/`, `bc/hamilton_artifacts/`.

## Decisions / why-not log

- **Keras 3 + PyTorch backend, not XGBoost.** High-cardinality player
  IDs map naturally onto embedding layers; XGBoost would force target
  encoding or one-hot blowup. PyTorch backend chosen over TF backend
  because torch core ships MPS support — no `tensorflow-metal` plugin
  needed.
- **Streaming via separate-cursor + `to_arrow_reader`.** Adopts the
  pattern from dbt-duckdb PR #269 ("Enable batch processing on
  Python models"). SQLMesh's `engine_adapter.cursor` is a
  `DuckDBPyConnection` shared with the writer; calling `.cursor()` on
  it returns a fresh cursor whose Arrow scan survives concurrent
  INSERTs from the writer cursor. RAM peak is one
  `_BATCH_ROWS=500_000`-row record batch, not the full table. Earlier
  attempts that failed:
  - `fetch_record_batch` on the shared cursor — invalidated after the
    writer's first INSERT (StopIteration).
  - Second `duckdb.connect(path)` — DuckDB rejects two handles per
    file per process.
  - `fetch_arrow_table().to_batches()` — works but loads the whole
    result into Arrow first; not actually streaming.
- **Argmax + max-prob output, not full per-class probability vector.**
  Narrow schema chosen for now. If a downstream wants full probs, add
  wide columns then — no premature width.
- **Training is offline.** The prediction `@model` only loads the
  pinned MLflow artifact; it never re-trains. Otherwise every
  `sqlmesh plan dev` would re-train, which is non-deterministic and
  slow.
- **No R2 publish.** Predictions stay internal to `bc.db` until a
  consumer asks. No `download_parquet` URL on the predictions model.
- **MLflow tracking via SQLite (`bc/mlflow.db`), artifacts under
  `bc/mlruns/`.** MLflow 3.x deprecated the filesystem-backed
  `./mlruns` tracking store. We use `sqlite:///bc/mlflow.db` for run
  metadata and `file:bc/mlruns` as the experiment's artifact location.
  Both gitignored. Model signature is inferred from a sample test
  batch (`mlflow.models.signature.infer_signature`) so MLflow logs
  the model with full input/output schema rather than a warning.

## What landed (second-wave first target — `outcome_is_in_play_bin`)

- `bc/python_models/ml/features.py` — added `TargetSpec` (Pydantic v2,
  frozen) plus `PLATE_APPEARANCE_CAT` and `IS_IN_PLAY_BIN` instances
  and an `ALL_TARGETS` tuple. `target_by_name` looks up by name.
  Legacy `TARGET_COLUMN` / `SAMPLE_WEIGHT_COLUMN` aliases remain
  bound to the plate-appearance spec for back-compat.
- `bc/python_models/ml/model_factory.py` — new shared builder.
  `_make_outputs_layer(trunk, target_spec, num_classes)` dispatches:
  `multiclass` → `Dense(num_classes, softmax)` + sparse-categorical
  CE; `binary` → `Dense(1, sigmoid)` + binary CE. The trunk
  (per-categorical embeddings, one-hot for low-card, normalized
  numerics, two dense ReLU blocks) is identical for both targets.
- `bc/python_models/ml/model_plate_appearance_cat.py`,
  `model_is_in_play_bin.py` — thin wrappers binding their `TargetSpec`
  to `model_factory.build_model`. The PA shim preserves the existing
  test surface and keeps the saved Keras layer/model names stable.
- `bc/python_models/ml/training.py` — replaces
  `training_plate_appearance_cat.py`. Generic over `TargetSpec`:
  `_select`, `collect_feature_stats`, `_encode_batch`,
  `make_batch_generator`, `run_fit_and_log`, `write_pin`, and `train`
  all take a spec. Binary `_encode_batch` casts the target to
  `Float32` and skips the class-index round-trip (`class_index` is
  ignored by the binary path). `_select` filters
  `WHERE <weight_column> > 0` so zero-weight rows don't waste batches
  — important for binary targets like `in_play_sample_weight` where
  most events outside batting have weight 0.
- `bc/python_models/ml/hamilton_dag.py` — same DAG, now consumes
  `target_spec` as an input. `num_classes` returns 1 for `binary`
  kind, `len(class_labels)` otherwise. Adding a third target requires
  no DAG changes.
- `bc/python_models/ml/prediction.py` — `Scorer` gained a `kind`
  field and a per-kind output schema. Multiclass schema is unchanged
  (`predicted_class`, `predicted_class_proba`). Binary emits
  `predicted_class_bin` (UInt8, threshold 0.5) and `predicted_proba`
  (Float64, raw sigmoid). The pin JSON now carries `kind` and
  `target_column`/`target_name`, which `load_scorer` reads to choose
  the right output path.
- `bc/models/intermediate/machine_learning/predictions_is_in_play_bin.py`
  — SQLMesh Python `@model` mirroring `predictions_plate_appearance_cat`
  but with the binary-schema columns and matching audits
  (`bounded_range` 0/1 on `predicted_class_bin`, 0.0/1.0 on
  `predicted_proba`).
- `scripts/train_is_in_play_bin.py` — CLI parallel to
  `train_plate_appearance_cat.py`.
- `bc/tests/test_is_in_play_bin_model.py` — 5 tests on the binary
  build_model + Scorer schema (output shape, shim parity, schema,
  empty-input, OOV handling).

## Decisions / why-not log (second wave)

- **TargetSpec, not a base class hierarchy.** A flat Pydantic model
  with a `kind` enum is enough to express the differences between
  multiclass and binary targets at the four touch points (model head,
  loss/metric, `_encode_batch`, Scorer output schema). A class
  hierarchy would have spread the dispatch across more files without
  reducing the total branch count.
- **Shared trunk, per-target heads — but trained independently.** The
  followups previously suggested a multi-task model with one shared
  trunk and many heads. Skipping that for now: each target trains
  separately and ships its own MLflow run. Multi-task training is a
  larger optimization choice (target weighting, loss balancing,
  joint validation) that we should make once we have ≥3 targets and
  a clear use case.
- **Filter zero-weight rows in `_select`, not in `_encode_batch`.**
  For binary targets like `is_in_play_bin`, ~half of events have
  zero weight. Filtering at SQL time keeps DuckDB from streaming
  rows we'd discard immediately, and keeps the train/test row counts
  honest.
- **Binary head emits a single sigmoid scalar, not 2-class softmax.**
  Both are equivalent in expressiveness, but the scalar form gives a
  cleaner output schema (`predicted_proba` directly) and a smaller
  model. `predicted_class_bin` is just `proba >= 0.5`.

## Open third-wave targets

`ml_event_outcomes` provides 6 more targets, each with its own sample
weight column. Order roughly by feature reuse and reviewability:

| Target column | Type | Sample weight | Notes |
|---|---|---|---|
| `outcome_batted_trajectory_cat` | multiclass | `trajectory_sample_weight` | Conditional on in-play. Either nest under `is_in_play_bin` or accept a "not in play" class. |
| `outcome_batted_location_cat` | multiclass | `location_sample_weight` | Same conditional structure as trajectory. |
| `outcome_baserunning_cat` | multiclass | `baserunning_play_sample_weight` | Different feature subset (runners on base matter most). |
| `outcome_runs_following_num` | regression | `generic_sample_weight` | Needs a `regression` branch in `_make_outputs_layer` (linear head + MSE). |
| `outcome_is_win_bin` | binary | `win_sample_weight` | Win probability; check meta_train_test_split partitions correctly across game state. |
| `outcome_has_batting_bin` | binary | `generic_sample_weight` | Lowest priority — already largely deterministic from event type. |

For the multiclass + binary targets above, adding a new target is now
purely additive: declare a `TargetSpec` in `features.py`, add a
`scripts/train_<name>.py` CLI and a `predictions_<name>.py` `@model`,
and reuse the existing factory + DAG. Regression needs a small extension
to `_make_outputs_layer` and `_encode_batch`.

## Other follow-ups

- **MLflow → R2 artifact upload.** Deferred until multi-target. When
  multiple models need to be loaded by the prediction `@model`, push
  fitted-model artifacts to R2 and have `load_scorer` fetch by run_id.
- **Calibration pass.** Single-epoch baseline produces argmax
  probabilities clustered around the majority-class prior (avg p ≈
  0.48 for `InPlayOut`). After more epochs, validate calibration with
  a reliability diagram before reporting accuracy.
- **Sklearn pipeline option.** A regression baseline (logistic
  regression with one-hot + target-encoded categoricals) would be a
  cheap sanity check against the Keras model — useful when a target
  has too few examples to justify deep embeddings.
- **`run_id` propagation into the audit.** `model_run_id` is uniform
  per scoring run (one column value across the whole table). Adding
  an audit that asserts this uniformity would catch accidental
  multi-run mixing if the scorer is ever called more than once per
  `execute()`.
- **Predictions parquet snapshot.** If a downstream model wants
  predictions, add `download_parquet` to the predictions @model and
  re-run publish. No code changes to consumers.
- **Hamilton dependency upgrade path.** `apache-hamilton` 1.90
  resolves alongside SQLMesh 0.234 cleanly. Watch for sqlglot pin
  conflicts on future Hamilton upgrades — the same constraint story
  as `boring-semantic-layer` could appear.
