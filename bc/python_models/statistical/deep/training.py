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

import logging
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
from python_models.ml.model_factory import build_model
from python_models.statistical.config import DEEP_ROOT
from python_models.statistical.deep.artifacts import (
    deep_artifact_dir,
    deep_exports_dir,
    write_class_labels_json,
    write_probabilities_parquet,
)
from python_models.statistical.deep.io import (
    KFOLD_COLUMN,
    add_kfold_id,
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

DEFAULT_EPOCHS: int = 3
DEFAULT_KERAS_BATCH_SIZE: int = 1024


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


def _fit_keras(
    *,
    spec: DeepTargetSpec,
    layout: FeatureLayout,
    stats: PolarsFeatureStats,
    fit_df: pl.DataFrame,
    class_index: dict[str, int],
    epochs: int,
    keras_batch_size: int,
):
    import keras  # noqa: F401  # lazy to avoid Keras import at module load

    legacy_spec = _legacy_target_spec(spec)
    num_classes = len(stats.class_labels) if spec.kind == "multiclass" else 1
    model = build_model(
        target_spec=legacy_spec,
        layout=layout,
        vocab_sizes=_vocab_sizes(stats),
        numeric_means=stats.numeric_means,
        numeric_variances=stats.numeric_variances,
        num_classes=num_classes,
    )
    x = _encode_inputs(fit_df, layout=layout, stats=stats)
    if spec.kind == "multiclass":
        y, valid = _encode_targets(
            fit_df, target_column=spec.target_column, class_index=class_index
        )
        if not bool(valid.all()):
            x = {k: v[valid] for k, v in x.items()}
            y = y[valid]
        weights = (
            fit_df[spec.weight_column]
            .cast(pl.Float32)
            .fill_null(0.0)
            .to_numpy()[valid]
        )
        _ = model.fit(
            x,
            y,
            sample_weight=weights.astype(np.float32),
            epochs=epochs,
            batch_size=keras_batch_size,
            verbose="0",
        )
    else:
        y_raw = (
            fit_df[spec.target_column].cast(pl.Float32).fill_null(0.0).to_numpy()
        )
        weights = (
            fit_df[spec.weight_column].cast(pl.Float32).fill_null(0.0).to_numpy()
        )
        _ = model.fit(
            x,
            y_raw.reshape(-1, 1),
            sample_weight=weights.astype(np.float32),
            epochs=epochs,
            batch_size=keras_batch_size,
            verbose="0",
        )
    return model


def _predict_probabilities(
    model: Any, df: pl.DataFrame, *, layout: FeatureLayout, stats: PolarsFeatureStats
) -> NDArray[np.float64]:
    x = _encode_inputs(df, layout=layout, stats=stats)
    raw = np.asarray(model.predict(x, verbose=0)).astype(np.float64)
    return raw


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

    oof_records: list[pl.DataFrame] = []
    for k in range(spec.fold_count):
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
        fold_model = _fit_keras(
            spec=spec,
            layout=layout,
            stats=fold_stats,
            fit_df=fit_subset,
            class_index=class_index,
            epochs=epochs,
            keras_batch_size=keras_batch_size,
        )
        probs = _predict_probabilities(
            fold_model, oof_subset, layout=layout, stats=fold_stats
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

    full_model = _fit_keras(
        spec=spec,
        layout=layout,
        stats=full_stats,
        fit_df=train_df,
        class_index=class_index,
        epochs=epochs,
        keras_batch_size=keras_batch_size,
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

    all_frames = [*oof_records, *validate_records, *test_records]
    if not all_frames:
        raise ValueError("no prediction rows produced")
    probabilities = pl.concat(all_frames, how="vertical_relaxed")

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
    columns = {
        grain_column: subset[grain_column],
        "partition": pl.Series(
            "partition",
            _partition_label_for_rows(subset.height, partition_label),
            dtype=pl.Utf8,
        ),
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
) -> ArtifactManifest:
    metadata: dict[str, str | int | float | bool] = {
        "fold_count": spec.fold_count,
        "kind": spec.kind,
        "calibration_method": spec.calibration_method,
        "train_rows": train_rows,
        "validate_rows": validate_rows,
        "test_rows": test_rows,
        "num_classes": len(class_labels),
    }
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
