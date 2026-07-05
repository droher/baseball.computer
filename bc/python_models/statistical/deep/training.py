"""Deep fold runner: out-of-fold + full-fit predictions per DeepTargetSpec.

Total fits per target = ``fold_count + 1``.

For each ``DeepTargetSpec``:
  1. Load the modeling-dataset Parquet snapshot.
  2. Partition into TRAIN / VALIDATE / TEST via ``layout.split_column``.
  3. Assign ``kfold_id = blake2s(game_id) % fold_count`` per TRAIN row.
  4. Group-leakage write-time check (every game_id maps to one fold).
  5. For ``k in range(fold_count)``: build feature stats on ``kfold_id != k``,
     build a Keras model via ``model_factory.build_model(layout=...)``, fit on
     train-fold, predict on out-of-fold rows.
  6. Train one full-fit model on all of TRAIN; predict VALIDATE + TEST with it.
  7. Stack OOF + VALIDATE + TEST predictions → ``probabilities.parquet``.
  8. Write ``class_labels.json`` + ``manifest.json``.

PR1 ships the fold runner against synthetic Parquet fixtures. Real
targets register themselves in PR3+.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
from numpy.typing import NDArray

from python_models.ml.features import (
    FeatureLayout,
    TargetSpec,
    Vocabulary,
)
from python_models.ml.model_factory import (
    build_model,
    set_pretrained_embeddings,
)
from python_models.statistical.config import DEEP_ROOT
from python_models.statistical.deep.artifacts import (
    deep_artifact_dir,
    deep_exports_dir,
    write_class_labels_json,
    write_probabilities_parquet,
)
from python_models.statistical.deep.io import (
    FOLD_ID_COLUMN,
    KFOLD_COLUMN,
    add_kfold_id,
    assert_export_partition_invariants,
    assert_game_group_invariant,
    load_dataset_parquet,
    partition_by_split,
)
from python_models.statistical.deep.target_spec import DeepTargetSpec
from python_models.statistical.manifests import (
    package_versions,
    query_hash,
    write_manifest,
)
from python_models.statistical.schemas import ArtifactManifest

_log = logging.getLogger(__name__)

DEFAULT_EPOCHS: int = 20
DEFAULT_KERAS_BATCH_SIZE: int = 4096
DEFAULT_PREDICT_BATCH_SIZE: int = 8192
DEFAULT_EARLY_STOPPING_PATIENCE: int = 3


@dataclass(frozen=True)
class PolarsFeatureStats:
    vocabularies: dict[str, Vocabulary]
    numeric_means: dict[str, float]
    numeric_variances: dict[str, float]
    class_labels: tuple[str, ...]


@dataclass(frozen=True)
class FoldRunResult:
    artifact_dir: Path
    probabilities_path: Path
    class_labels_path: Path
    manifest_path: Path
    train_rows: int
    validate_rows: int
    test_rows: int


def _collect_polars_stats(
    train_df: pl.DataFrame,
    *,
    layout: FeatureLayout,
    target_column: str,
    class_universe: tuple[str, ...] | None,
) -> PolarsFeatureStats:
    vocabularies: dict[str, Vocabulary] = {}
    for col in layout.categorical_columns:
        values = (
            train_df[col].cast(pl.Utf8).drop_nulls().unique().sort().to_list()
        )
        vocabularies[col] = Vocabulary(column=col, values=tuple(str(v) for v in values))

    numeric_means: dict[str, float] = {}
    numeric_variances: dict[str, float] = {}
    for col in layout.numeric_columns:
        arr = (
            train_df[col]
            .cast(pl.Float64)
            .fill_null(0.0)
            .to_numpy()
            .astype(np.float64)
        )
        numeric_means[col] = float(arr.mean()) if arr.size > 0 else 0.0
        numeric_variances[col] = (
            max(float(arr.var()), 1e-6) if arr.size > 0 else 1e-6
        )

    if class_universe is None:
        labels_raw = (
            train_df[target_column].cast(pl.Utf8).drop_nulls().unique().sort().to_list()
        )
        class_labels = tuple(str(v) for v in labels_raw)
    else:
        class_labels = class_universe

    return PolarsFeatureStats(
        vocabularies=vocabularies,
        numeric_means=numeric_means,
        numeric_variances=numeric_variances,
        class_labels=class_labels,
    )


def _encode_inputs(
    df: pl.DataFrame, *, layout: FeatureLayout, stats: PolarsFeatureStats
) -> dict[str, NDArray[Any]]:
    inputs: dict[str, NDArray[Any]] = {}
    for col in layout.categorical_columns:
        encoded = stats.vocabularies[col].encode(df[col]).to_numpy()
        inputs[col] = encoded.astype(np.int64).reshape(-1, 1)
    for col in layout.numeric_columns:
        inputs[col] = (
            df[col].cast(pl.Float32).fill_null(0.0).to_numpy().reshape(-1, 1)
        )
    return inputs


def _encode_targets(
    df: pl.DataFrame, *, target_column: str, class_index: dict[str, int]
) -> tuple[NDArray[np.int64], NDArray[np.bool_]]:
    codes = (
        df[target_column]
        .cast(pl.Utf8)
        .replace_strict(class_index, default=-1)
        .cast(pl.Int64)
        .to_numpy()
    )
    valid = codes >= 0
    return codes.astype(np.int64), valid


def _legacy_target_spec(spec: DeepTargetSpec) -> TargetSpec:
    return TargetSpec(
        name=spec.name,
        target_column=spec.target_column,
        weight_column=spec.weight_column,
        kind=spec.kind,
    )


def _vocab_sizes(stats: PolarsFeatureStats) -> dict[str, int]:
    return {col: v.size for col, v in stats.vocabularies.items()}


@dataclass(frozen=True)
class FitOutcome:
    model: Any
    best_epoch: int
    epochs_ran: int
    final_train_loss: float
    final_val_loss: float | None


def _make_per_class_reporter(
    *,
    val_x: dict[str, NDArray[Any]],
    val_y: NDArray[np.int64],
    val_w: NDArray[np.float32],
    class_labels: tuple[str, ...],
    log_tag: str,
    max_rows: int = 200_000,
    seed: int = 0,
) -> Any:
    import keras

    n = int(val_y.shape[0])
    if n > max_rows:
        rng = np.random.default_rng(seed)
        idx = rng.choice(n, size=max_rows, replace=False)
        sample_x = {k: v[idx] for k, v in val_x.items()}
        sample_y = val_y[idx]
        sample_w = val_w[idx]
    else:
        sample_x = val_x
        sample_y = val_y
        sample_w = val_w

    class _PerClassEpochReporter(keras.callbacks.Callback):
        def on_epoch_end(self, epoch: int, logs: dict[str, Any] | None = None) -> None:
            logs = logs or {}
            train_loss = logs.get("loss")
            val_loss = logs.get("val_loss")
            probs = np.asarray(
                self.model.predict(
                    sample_x,
                    batch_size=DEFAULT_PREDICT_BATCH_SIZE,
                    verbose=0,
                ),
                dtype=np.float64,
            )
            w = sample_w.astype(np.float64)
            eligible = w > 0
            if not bool(eligible.any()):
                _log.info(
                    "%s epoch %d train_loss=%.4f val_loss=%s (no loss-eligible val rows)",
                    log_tag,
                    epoch + 1,
                    float(train_loss) if train_loss is not None else float("nan"),
                    f"{val_loss:.4f}" if val_loss is not None else "n/a",
                )
                return
            p_e = probs[eligible]
            y_e = sample_y[eligible]
            w_e = w[eligible]
            pred = np.argmax(p_e, axis=1)
            eps = 1e-12
            rows: list[tuple[str, float, float, float, float]] = []
            f1s: list[float] = []
            for c in range(probs.shape[1]):
                label = (
                    class_labels[c] if c < len(class_labels) else f"class_{c}"
                )
                true_mask = y_e == c
                pred_mask = pred == c
                tp = float(w_e[true_mask & pred_mask].sum())
                fp = float(w_e[~true_mask & pred_mask].sum())
                fn = float(w_e[true_mask & ~pred_mask].sum())
                precision = tp / (tp + fp) if (tp + fp) > 0 else float("nan")
                recall = tp / (tp + fn) if (tp + fn) > 0 else float("nan")
                if (
                    not np.isnan(precision)
                    and not np.isnan(recall)
                    and (precision + recall) > 0
                ):
                    f1 = 2 * precision * recall / (precision + recall)
                else:
                    f1 = float("nan")
                if tp + fn > 0:
                    p_true = np.clip(p_e[true_mask, c], eps, 1.0)
                    nll = float(
                        -np.average(np.log(p_true), weights=w_e[true_mask])
                    )
                else:
                    nll = float("nan")
                rows.append((label, precision, recall, f1, nll))
                if not np.isnan(f1):
                    f1s.append(f1)
            macro_f1 = float(np.mean(f1s)) if f1s else float("nan")
            breakdown = " ".join(
                f"{lbl}[P={pr:.3f}/R={rc:.3f}/F1={f1:.3f}/ll={ll:.3f}]"
                for (lbl, pr, rc, f1, ll) in rows
            )
            _log.info(
                "%s epoch %d train_loss=%.4f val_loss=%s macro_f1=%.3f | %s",
                log_tag,
                epoch + 1,
                float(train_loss) if train_loss is not None else float("nan"),
                f"{val_loss:.4f}" if val_loss is not None else "n/a",
                macro_f1,
                breakdown,
            )

    return _PerClassEpochReporter()


def _prep_multiclass_arrays(
    df: pl.DataFrame,
    *,
    layout: FeatureLayout,
    stats: PolarsFeatureStats,
    target_column: str,
    weight_column: str,
    class_index: dict[str, int],
) -> tuple[dict[str, NDArray[Any]], NDArray[np.int64], NDArray[np.float32]]:
    x = _encode_inputs(df, layout=layout, stats=stats)
    y, valid = _encode_targets(
        df, target_column=target_column, class_index=class_index
    )
    if not bool(valid.all()):
        x = {k: v[valid] for k, v in x.items()}
        y = y[valid]
    weights = (
        df[weight_column].cast(pl.Float32).fill_null(0.0).to_numpy()[valid]
    )
    return x, y, weights.astype(np.float32)


def _resolve_pretrain_artifact_dir(artifact_id_or_name: str) -> Path:
    from python_models.statistical.manifests import (
        find_published_pretrain,
        read_published_pointer,
    )

    pointer_path = find_published_pretrain(artifact_id_or_name)
    if pointer_path is not None:
        pointer = read_published_pointer(pointer_path)
        manifest_path = Path(pointer.manifest_path)
        if manifest_path.exists():
            return manifest_path.parent
    for candidate in DEEP_ROOT.rglob(f"{artifact_id_or_name}/manifest.json"):
        return candidate.parent
    raise FileNotFoundError(
        f"pretrain artifact {artifact_id_or_name!r} not found via published pointer "
        f"or under {DEEP_ROOT}"
    )


def _maybe_load_pretrained_embeddings(
    model: Any,
    *,
    spec: DeepTargetSpec,
    layout: FeatureLayout,
    stats: PolarsFeatureStats,
    steps_per_epoch: int,
    total_epochs: int,
    num_classes: int,
    legacy_spec: TargetSpec,
) -> None:
    if spec.pretrained_embeddings_artifact_id is None:
        return
    if os.environ.get("BC_DEEP_DISABLE_PRETRAIN", "0") in ("1", "true", "TRUE"):
        _log.info("pretrain disabled via BC_DEEP_DISABLE_PRETRAIN")
        return
    import keras

    artifact_name_override = os.environ.get("BC_DEEP_PRETRAIN_ARTIFACT_OVERRIDE")
    effective_artifact_name = (
        artifact_name_override if artifact_name_override else spec.pretrained_embeddings_artifact_id
    )
    if artifact_name_override:
        _log.info(
            "pretrain artifact overridden via BC_DEEP_PRETRAIN_ARTIFACT_OVERRIDE: %s -> %s",
            spec.pretrained_embeddings_artifact_id,
            artifact_name_override,
        )
    artifact_dir = _resolve_pretrain_artifact_dir(effective_artifact_name)
    target_vocab_values = {
        col: stats.vocabularies[col].values
        for col in layout.high_card_columns
        if col in stats.vocabularies
    }
    alignment_stats = set_pretrained_embeddings(
        model,
        pretrain_artifact_dir=artifact_dir,
        high_card_columns=layout.high_card_columns,
        target_vocab_values=target_vocab_values,
        embedding_groups=layout.embedding_groups,
    )
    _log.info(
        "loaded pretrained embeddings target=%s pretrain_artifact=%s alignment=%s",
        spec.name,
        effective_artifact_name,
        alignment_stats,
    )

    force_freeze = os.environ.get("BC_DEEP_FORCE_FREEZE_PRETRAIN", "0") in (
        "1", "true", "TRUE"
    )
    if not spec.freeze_pretrained_embeddings and not force_freeze:
        return
    if force_freeze and not spec.freeze_pretrained_embeddings:
        _log.info("freeze forced via BC_DEEP_FORCE_FREEZE_PRETRAIN")
    from python_models.ml.model_factory import (
        INITIAL_LEARNING_RATE,
        sparse_categorical_focal_loss,
    )

    for col in layout.high_card_columns:
        try:
            layer = model.get_layer(f"embed_{col}")
        except ValueError:
            continue
        layer.trainable = False

    if steps_per_epoch > 0 and total_epochs > 0:
        schedule = keras.optimizers.schedules.CosineDecay(
            initial_learning_rate=INITIAL_LEARNING_RATE,
            decay_steps=steps_per_epoch * total_epochs,
            alpha=0.01,
        )
        optimizer = keras.optimizers.Adam(learning_rate=schedule)
    else:
        optimizer = keras.optimizers.Adam(learning_rate=INITIAL_LEARNING_RATE)

    if spec.kind == "multiclass":
        if spec.loss_type == "focal":
            loss = sparse_categorical_focal_loss(gamma=spec.focal_gamma)
        else:
            loss = keras.losses.SparseCategoricalCrossentropy()
        metrics = [keras.metrics.SparseCategoricalAccuracy()]
    elif spec.kind == "binary":
        loss = keras.losses.BinaryCrossentropy()
        metrics = [keras.metrics.BinaryAccuracy()]
    else:
        raise ValueError(f"unsupported spec.kind {spec.kind!r}")
    _ = num_classes
    _ = legacy_spec
    model.compile(optimizer=optimizer, loss=loss, weighted_metrics=metrics)


def _assert_binary_target_values(
    fit_df: pl.DataFrame, *, target_column: str
) -> None:
    distinct = fit_df.get_column(target_column).drop_nulls().unique().to_list()
    invalid = sorted(v for v in distinct if v not in (0, 1))
    if invalid:
        raise ValueError(
            f"binary target {target_column!r} carries non-binary label values "
            f"{invalid}; a binary DeepTargetSpec expects labels in {{0, 1}} — "
            f"use kind='multiclass' for a multiclass label space"
        )


def _fit_keras(
    *,
    spec: DeepTargetSpec,
    layout: FeatureLayout,
    stats: PolarsFeatureStats,
    fit_df: pl.DataFrame,
    class_index: dict[str, int],
    epochs: int,
    keras_batch_size: int,
    validation_df: pl.DataFrame | None = None,
    early_stopping_patience: int = DEFAULT_EARLY_STOPPING_PATIENCE,
    log_tag: str = "fit",
) -> FitOutcome:
    import keras

    legacy_spec = _legacy_target_spec(spec)
    num_classes = len(stats.class_labels) if spec.kind == "multiclass" else 1
    fit_rows_estimate = max(int(fit_df.height), 1)
    steps_per_epoch = max(1, -(-fit_rows_estimate // keras_batch_size))
    model = build_model(
        target_spec=legacy_spec,
        layout=layout,
        vocab_sizes=_vocab_sizes(stats),
        numeric_means=stats.numeric_means,
        numeric_variances=stats.numeric_variances,
        num_classes=num_classes,
        steps_per_epoch=steps_per_epoch,
        total_epochs=epochs,
        loss_type=spec.loss_type,
        focal_gamma=spec.focal_gamma,
    )
    _maybe_load_pretrained_embeddings(
        model,
        spec=spec,
        layout=layout,
        stats=stats,
        steps_per_epoch=steps_per_epoch,
        total_epochs=epochs,
        num_classes=num_classes,
        legacy_spec=legacy_spec,
    )

    if spec.kind == "multiclass":
        x, y, weights = _prep_multiclass_arrays(
            fit_df,
            layout=layout,
            stats=stats,
            target_column=spec.target_column,
            weight_column=spec.weight_column,
            class_index=class_index,
        )
        validation_data: tuple[Any, ...] | None
        if validation_df is not None and validation_df.height > 0:
            v_x, v_y, v_w = _prep_multiclass_arrays(
                validation_df,
                layout=layout,
                stats=stats,
                target_column=spec.target_column,
                weight_column=spec.weight_column,
                class_index=class_index,
            )
            validation_data = (v_x, v_y, v_w) if v_y.size > 0 else None
        else:
            validation_data = None
        callbacks: list[Any] = []
        if validation_data is not None:
            callbacks.append(
                keras.callbacks.EarlyStopping(
                    monitor="val_loss",
                    patience=early_stopping_patience,
                    restore_best_weights=True,
                )
            )
            callbacks.append(
                _make_per_class_reporter(
                    val_x=validation_data[0],
                    val_y=validation_data[1],
                    val_w=validation_data[2],
                    class_labels=stats.class_labels,
                    log_tag=log_tag,
                )
            )
        history = model.fit(
            x,
            y,
            sample_weight=weights,
            epochs=epochs,
            batch_size=keras_batch_size,
            validation_data=validation_data,
            callbacks=callbacks,
            verbose="0",
        )
    else:
        _assert_binary_target_values(fit_df, target_column=spec.target_column)
        if validation_df is not None and validation_df.height > 0:
            _assert_binary_target_values(
                validation_df, target_column=spec.target_column
            )
        y_raw = (
            fit_df[spec.target_column].cast(pl.Float32).fill_null(0.0).to_numpy()
        )
        weights = (
            fit_df[spec.weight_column].cast(pl.Float32).fill_null(0.0).to_numpy()
        )
        x = _encode_inputs(fit_df, layout=layout, stats=stats)
        validation_data = None
        if validation_df is not None and validation_df.height > 0:
            v_x = _encode_inputs(validation_df, layout=layout, stats=stats)
            v_y = (
                validation_df[spec.target_column]
                .cast(pl.Float32)
                .fill_null(0.0)
                .to_numpy()
                .reshape(-1, 1)
            )
            v_w = (
                validation_df[spec.weight_column]
                .cast(pl.Float32)
                .fill_null(0.0)
                .to_numpy()
                .astype(np.float32)
            )
            validation_data = (v_x, v_y, v_w)
        callbacks = []
        if validation_data is not None:
            callbacks.append(
                keras.callbacks.EarlyStopping(
                    monitor="val_loss",
                    patience=early_stopping_patience,
                    restore_best_weights=True,
                )
            )
        history = model.fit(
            x,
            y_raw.reshape(-1, 1),
            sample_weight=weights.astype(np.float32),
            epochs=epochs,
            batch_size=keras_batch_size,
            validation_data=validation_data,
            callbacks=callbacks,
            verbose="0",
        )

    train_losses = history.history.get("loss", [])
    val_losses = history.history.get("val_loss", [])
    epochs_ran = len(train_losses)
    if val_losses:
        best_epoch = int(np.argmin(val_losses))
        final_val_loss: float | None = float(val_losses[best_epoch])
    else:
        best_epoch = epochs_ran - 1 if epochs_ran > 0 else 0
        final_val_loss = None
    final_train_loss = (
        float(train_losses[best_epoch]) if train_losses else float("nan")
    )
    return FitOutcome(
        model=model,
        best_epoch=best_epoch,
        epochs_ran=epochs_ran,
        final_train_loss=final_train_loss,
        final_val_loss=final_val_loss,
    )


def _predict_probabilities(
    model: Any,
    df: pl.DataFrame,
    *,
    layout: FeatureLayout,
    stats: PolarsFeatureStats,
    batch_size: int = DEFAULT_PREDICT_BATCH_SIZE,
) -> NDArray[np.float64]:
    x = _encode_inputs(df, layout=layout, stats=stats)
    raw = np.asarray(
        model.predict(x, batch_size=batch_size, verbose=0)
    ).astype(np.float64)
    return raw


def _prep_eval_arrays(
    df: pl.DataFrame,
    *,
    spec: DeepTargetSpec,
    layout: FeatureLayout,
    stats: PolarsFeatureStats,
    class_index: dict[str, int],
) -> tuple[Any, NDArray[Any], NDArray[np.float32]]:
    if spec.kind == "multiclass":
        return _prep_multiclass_arrays(
            df,
            layout=layout,
            stats=stats,
            target_column=spec.target_column,
            weight_column=spec.weight_column,
            class_index=class_index,
        )
    x = _encode_inputs(df, layout=layout, stats=stats)
    y = (
        df[spec.target_column]
        .cast(pl.Float32)
        .fill_null(0.0)
        .to_numpy()
        .reshape(-1, 1)
        .astype(np.float32)
    )
    w = (
        df[spec.weight_column]
        .cast(pl.Float32)
        .fill_null(0.0)
        .to_numpy()
        .astype(np.float32)
    )
    return x, y, w


def _evaluate_partition_loss(
    model: Any,
    df: pl.DataFrame,
    *,
    spec: DeepTargetSpec,
    layout: FeatureLayout,
    stats: PolarsFeatureStats,
    class_index: dict[str, int],
    batch_size: int = DEFAULT_PREDICT_BATCH_SIZE,
) -> float:
    x, y, w = _prep_eval_arrays(
        df, spec=spec, layout=layout, stats=stats, class_index=class_index
    )
    result = model.evaluate(
        x,
        y,
        sample_weight=w,
        batch_size=batch_size,
        verbose=0,
        return_dict=True,
    )
    return float(result["loss"])


def _partition_label_for_rows(n: int, label: str) -> list[str]:
    return [label] * n


def run_target(
    spec: DeepTargetSpec,
    *,
    dataset_parquet: Path,
    artifact_id: str,
    layout: FeatureLayout,
    source_snapshot_id: str,
    dataset_artifact_id: str | None = None,
    artifact_root: Path = DEEP_ROOT,
    epochs: int = DEFAULT_EPOCHS,
    keras_batch_size: int = DEFAULT_KERAS_BATCH_SIZE,
) -> FoldRunResult:
    artifact_dir = deep_artifact_dir(spec.name, artifact_id, root=artifact_root)
    manifest_path = artifact_dir / "manifest.json"
    if manifest_path.exists():
        _log.info(
            "deep artifact %s already present; returning idempotent result",
            manifest_path,
        )
        existing_probs = deep_exports_dir(spec.name, artifact_id, root=artifact_root) / "probabilities.parquet"
        existing_labels = deep_exports_dir(spec.name, artifact_id, root=artifact_root) / "class_labels.json"
        existing = pl.read_parquet(existing_probs)
        n_train = int(
            existing.filter(pl.col("partition") == "OOF").height
        )
        n_val = int(
            existing.filter(pl.col("partition") == "VALIDATE").height
        )
        n_test = int(
            existing.filter(pl.col("partition") == "TEST").height
        )
        return FoldRunResult(
            artifact_dir=artifact_dir,
            probabilities_path=existing_probs,
            class_labels_path=existing_labels,
            manifest_path=manifest_path,
            train_rows=n_train,
            validate_rows=n_val,
            test_rows=n_test,
        )

    df = load_dataset_parquet(str(dataset_parquet))
    if spec.filter_predicate is not None:
        df = df.sql(f"SELECT * FROM self WHERE {spec.filter_predicate}")
    remap = spec.remap_dict()
    if remap:
        df = df.with_columns(
            pl.col(spec.target_column)
            .cast(pl.Utf8)
            .replace(remap)
            .alias(spec.target_column)
        )
        _log.info(
            "applied target_remap: %s",
            {k: v for k, v in sorted(remap.items())},
        )
    if spec.loss_mask_predicate is not None:
        ctx = pl.SQLContext({"self": df.lazy()})
        mask_series = (
            ctx.execute(
                f"SELECT ({spec.loss_mask_predicate}) AS _loss_mask FROM self"
            )
            .collect()
            .to_series()
        )
        df = df.with_columns(
            pl.when(mask_series)
            .then(pl.col(spec.weight_column).cast(pl.Float64))
            .otherwise(0.0)
            .alias(spec.weight_column)
        )
        masked_in = int(mask_series.sum() or 0)
        _log.info(
            "applied loss_mask_predicate: %s (loss-eligible rows=%d of %d)",
            spec.loss_mask_predicate,
            masked_in,
            df.height,
        )

    train_df, validate_df, test_df = partition_by_split(
        df,
        split_column=spec.split_column,
        train_label=spec.train_label,
        validate_label=spec.validate_label,
        test_label=spec.test_label,
    )
    if train_df.height == 0:
        raise ValueError("TRAIN partition is empty after filtering")

    train_df = add_kfold_id(
        train_df, game_id_column=spec.game_id_column, fold_count=spec.fold_count
    )
    assert_game_group_invariant(
        train_df, game_id_column=spec.game_id_column, kfold_column=KFOLD_COLUMN
    )

    if spec.class_universe_source == "configured":
        class_universe: tuple[str, ...] | None = spec.configured_class_labels
    else:
        class_universe = None
    full_stats = _collect_polars_stats(
        train_df,
        layout=layout,
        target_column=spec.target_column,
        class_universe=class_universe,
    )
    class_labels = full_stats.class_labels
    if spec.kind == "multiclass" and len(class_labels) == 0:
        raise ValueError("class_labels is empty for multiclass target")
    class_index: dict[str, int] = {label: i for i, label in enumerate(class_labels)}

    fit_diagnostics: list[dict[str, float | int | str]] = []
    oof_records: list[pl.DataFrame] = []
    validation_pool: pl.DataFrame | None = validate_df if validate_df.height > 0 else None
    fold_iter = range(spec.fold_count)
    for k in fold_iter:
        fit_subset = train_df.filter(pl.col(KFOLD_COLUMN) != k)
        oof_subset = train_df.filter(pl.col(KFOLD_COLUMN) == k)
        if oof_subset.height == 0:
            continue
        fold_stats = _collect_polars_stats(
            fit_subset,
            layout=layout,
            target_column=spec.target_column,
            class_universe=class_labels,
        )
        fold_start = time.perf_counter()
        fold_outcome = _fit_keras(
            spec=spec,
            layout=layout,
            stats=fold_stats,
            fit_df=fit_subset,
            class_index=class_index,
            epochs=epochs,
            keras_batch_size=keras_batch_size,
            validation_df=validation_pool,
            log_tag=f"fold_{k}",
        )
        fold_elapsed = time.perf_counter() - fold_start
        fit_diagnostics.append(
            {
                "fit": f"fold_{k}",
                "fit_rows": int(fit_subset.height),
                "best_epoch": fold_outcome.best_epoch,
                "epochs_ran": fold_outcome.epochs_ran,
                "train_loss": fold_outcome.final_train_loss,
                "val_loss": (
                    fold_outcome.final_val_loss
                    if fold_outcome.final_val_loss is not None
                    else float("nan")
                ),
                "elapsed_sec": fold_elapsed,
            }
        )
        _log.info(
            "fold %d/%d fit done rows=%d best_epoch=%d epochs_ran=%d val_loss=%s elapsed=%.1fs",
            k + 1,
            spec.fold_count,
            fit_subset.height,
            fold_outcome.best_epoch,
            fold_outcome.epochs_ran,
            f"{fold_outcome.final_val_loss:.4f}" if fold_outcome.final_val_loss is not None else "n/a",
            fold_elapsed,
        )
        probs = _predict_probabilities(
            fold_outcome.model, oof_subset, layout=layout, stats=fold_stats
        )
        oof_records.append(
            _build_partition_frame(
                oof_subset,
                probs=probs,
                partition_label="OOF",
                grain_column=layout.grain_column,
                spec=spec,
            )
        )

    full_start = time.perf_counter()
    full_outcome = _fit_keras(
        spec=spec,
        layout=layout,
        stats=full_stats,
        fit_df=train_df,
        class_index=class_index,
        epochs=epochs,
        keras_batch_size=keras_batch_size,
        validation_df=validation_pool,
        log_tag="full",
    )
    full_elapsed = time.perf_counter() - full_start
    fit_diagnostics.append(
        {
            "fit": "full",
            "fit_rows": int(train_df.height),
            "best_epoch": full_outcome.best_epoch,
            "epochs_ran": full_outcome.epochs_ran,
            "train_loss": full_outcome.final_train_loss,
            "val_loss": (
                full_outcome.final_val_loss
                if full_outcome.final_val_loss is not None
                else float("nan")
            ),
            "elapsed_sec": full_elapsed,
        }
    )
    _log.info(
        "full fit done rows=%d best_epoch=%d epochs_ran=%d val_loss=%s elapsed=%.1fs",
        train_df.height,
        full_outcome.best_epoch,
        full_outcome.epochs_ran,
        f"{full_outcome.final_val_loss:.4f}" if full_outcome.final_val_loss is not None else "n/a",
        full_elapsed,
    )
    full_model = full_outcome.model

    if not oof_records:
        raise RuntimeError(
            "no per-fold OOF predictions were produced; OOF export requires "
            "per-fold predictions and full-fit predictions must never be "
            "exported under the OOF partition"
        )

    validate_records = (
        [
            _build_partition_frame(
                validate_df,
                probs=_predict_probabilities(
                    full_model, validate_df, layout=layout, stats=full_stats
                ),
                partition_label="VALIDATE",
                grain_column=layout.grain_column,
                spec=spec,
            )
        ]
        if validate_df.height > 0
        else []
    )
    test_records = (
        [
            _build_partition_frame(
                test_df,
                probs=_predict_probabilities(
                    full_model, test_df, layout=layout, stats=full_stats
                ),
                partition_label="TEST",
                grain_column=layout.grain_column,
                spec=spec,
            )
        ]
        if test_df.height > 0
        else []
    )

    selection_partition = spec.validate_label if validation_pool is not None else None
    held_out_test_loss: float | None = None
    if test_df.height > 0:
        held_out_test_loss = _evaluate_partition_loss(
            full_model,
            test_df,
            spec=spec,
            layout=layout,
            stats=full_stats,
            class_index=class_index,
        )
        _log.info(
            "held-out %s loss=%.4f rows=%d (never used for model selection or "
            "early stopping); reported val_loss diagnostics are on the %s "
            "selection partition and are optimistic",
            spec.test_label,
            held_out_test_loss,
            test_df.height,
            selection_partition or "training",
        )
    else:
        _log.warning(
            "no held-out %s partition; all reported val_loss diagnostics are on "
            "the %s selection partition (used for early stopping) and are "
            "optimistic, not a held-out estimate",
            spec.test_label,
            selection_partition or "training",
        )

    all_frames = [*oof_records, *validate_records, *test_records]
    if not all_frames:
        raise ValueError("no prediction rows produced")
    probabilities = pl.concat(all_frames, how="vertical_relaxed")
    assert_export_partition_invariants(
        probabilities,
        train_df=train_df,
        grain_column=layout.grain_column,
        game_id_column=spec.game_id_column,
        fold_count=spec.fold_count,
    )

    probabilities_path = write_probabilities_parquet(
        spec.name, artifact_id, df=probabilities, root=artifact_root
    )
    class_labels_path = write_class_labels_json(
        spec.name, artifact_id, class_labels=class_labels, root=artifact_root
    )
    manifest = _build_manifest(
        spec=spec,
        artifact_id=artifact_id,
        artifact_dir=artifact_dir,
        probabilities_path=probabilities_path,
        class_labels_path=class_labels_path,
        source_snapshot_id=source_snapshot_id,
        dataset_artifact_id=dataset_artifact_id,
        train_rows=train_df.height,
        validate_rows=validate_df.height,
        test_rows=test_df.height,
        class_labels=class_labels,
        fit_diagnostics=fit_diagnostics,
        selection_partition=selection_partition,
        held_out_test_loss=held_out_test_loss,
    )
    write_manifest(manifest, manifest_path)

    return FoldRunResult(
        artifact_dir=artifact_dir,
        probabilities_path=probabilities_path,
        class_labels_path=class_labels_path,
        manifest_path=manifest_path,
        train_rows=train_df.height,
        validate_rows=validate_df.height,
        test_rows=test_df.height,
    )


def _build_partition_frame(
    subset: pl.DataFrame,
    *,
    probs: NDArray[np.float64],
    partition_label: str,
    grain_column: str,
    spec: DeepTargetSpec,
) -> pl.DataFrame:
    if spec.kind == "binary":
        flat = probs.reshape(-1).astype(np.float64)
        p_class = [[1.0 - p, p] for p in flat.tolist()]
    else:
        p_class = [list(row) for row in probs.astype(np.float64).tolist()]
    if partition_label == "OOF":
        fold_id = subset[KFOLD_COLUMN].cast(pl.Int32).rename(FOLD_ID_COLUMN)
    else:
        fold_id = pl.Series(
            FOLD_ID_COLUMN, [None] * subset.height, dtype=pl.Int32
        )
    columns = {
        grain_column: subset[grain_column],
        "partition": pl.Series(
            "partition",
            _partition_label_for_rows(subset.height, partition_label),
            dtype=pl.Utf8,
        ),
        FOLD_ID_COLUMN: fold_id,
        "dl_p_class": pl.Series(
            "dl_p_class",
            p_class,
            dtype=pl.List(pl.Float64),
        ),
    }
    return pl.DataFrame(columns)


def _build_manifest(
    *,
    spec: DeepTargetSpec,
    artifact_id: str,
    artifact_dir: Path,
    probabilities_path: Path,
    class_labels_path: Path,
    source_snapshot_id: str,
    dataset_artifact_id: str | None,
    train_rows: int,
    validate_rows: int,
    test_rows: int,
    class_labels: tuple[str, ...],
    fit_diagnostics: list[dict[str, float | int | str]] | None = None,
    selection_partition: str | None = None,
    held_out_test_loss: float | None = None,
) -> ArtifactManifest:
    metadata: dict[str, str | int | float | bool] = {
        "fold_count": spec.fold_count,
        "kind": spec.kind,
        "calibration_method": "none",
        "train_rows": train_rows,
        "validate_rows": validate_rows,
        "test_rows": test_rows,
        "num_classes": len(class_labels),
        "reported_val_loss_partition": selection_partition or "none",
        "held_out_test_available": held_out_test_loss is not None,
    }
    if held_out_test_loss is not None:
        metadata["held_out_test_loss"] = held_out_test_loss
    if fit_diagnostics:
        metadata["fit_diagnostics_json"] = json.dumps(fit_diagnostics)
        elapsed_total = sum(
            float(d.get("elapsed_sec", 0.0) or 0.0) for d in fit_diagnostics
        )
        metadata["total_fit_elapsed_sec"] = elapsed_total
    return ArtifactManifest(
        artifact_id=artifact_id,
        kind="deep",
        name=spec.name,
        version="0.1.0",
        created_at=datetime.now(tz=timezone.utc),
        source_snapshot_id=source_snapshot_id,
        query_hash=query_hash(
            f"{spec.name}|{spec.target_column}|{spec.weight_column}|fold={spec.fold_count}"
        ),
        dataset_artifact_id=dataset_artifact_id,
        output_paths={
            "artifact_dir": artifact_dir,
            "probabilities": probabilities_path,
            "class_labels": class_labels_path,
        },
        package_versions=package_versions(),
        metadata=metadata,
    )
