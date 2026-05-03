"""Training entry point for the plate-appearance-cat model.

One pass over the TRAIN partition collects vocabularies + numeric
statistics; a Keras model is constructed from those; a streaming
generator over `ml_features` feeds `model.fit`. The fitted artifact is
logged to MLflow (file backend) and the run id is pinned so the
prediction `@model` can find it.
"""

from __future__ import annotations

import python_models.ml  # noqa: F401  # set KERAS_BACKEND before keras import

import json
import logging
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import mlflow
import numpy as np
import polars as pl
from numpy.typing import NDArray

from python_models.ml.data_loaders import open_bc_db, stream_query
from python_models.ml.features import (
    ALL_FEATURE_COLUMNS,
    GRAIN_COLUMN,
    HIGH_CARD_CATEGORICAL,
    LOW_CARD_CATEGORICAL,
    NUMERIC,
    SAMPLE_WEIGHT_COLUMN,
    SPLIT_COLUMN,
    TARGET_COLUMN,
    Vocabulary,
    build_vocabulary,
)
from python_models.ml.model_plate_appearance_cat import build_model

_log = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[3]
ARTIFACT_DIR = REPO_ROOT / "bc" / "hamilton_artifacts"
VOCAB_DIR = ARTIFACT_DIR / "vocabs" / "plate_appearance_cat"
MLRUNS_DIR = REPO_ROOT / "bc" / "mlruns"
MLFLOW_TRACKING_DB = REPO_ROOT / "bc" / "mlflow.db"
PIN_PATH = REPO_ROOT / "bc" / "python_models" / "ml" / "artifacts" / "plate_appearance_cat.json"
DEFAULT_BATCH_ROWS = 250_000
DEFAULT_EPOCHS = 3
DEFAULT_DB = str(REPO_ROOT / "bc.db")
DEFAULT_SCHEMA = "main_models__dev"


@dataclass(frozen=True)
class FeatureStats:
    vocabularies: dict[str, Vocabulary]
    numeric_means: dict[str, float]
    numeric_variances: dict[str, float]
    class_labels: tuple[str, ...]
    train_row_count: int
    test_row_count: int


_VALID_SPLITS: frozenset[str] = frozenset({"TRAIN", "TEST"})
_VALID_SCHEMAS: frozenset[str] = frozenset({"main_models", "main_models__dev"})


def _select(split: str, schema: str) -> str:
    if split not in _VALID_SPLITS:
        raise ValueError(f"split {split!r} not in {sorted(_VALID_SPLITS)}")
    if schema not in _VALID_SCHEMAS:
        raise ValueError(f"schema {schema!r} not in {sorted(_VALID_SCHEMAS)}")
    cols = ", ".join((GRAIN_COLUMN, *ALL_FEATURE_COLUMNS, TARGET_COLUMN, SAMPLE_WEIGHT_COLUMN))
    return (
        f"SELECT {cols} FROM {schema}.ml_features "
        f"WHERE {SPLIT_COLUMN} = '{split}' AND {TARGET_COLUMN} IS NOT NULL"
    )


def _load_cached_vocab(col: str, vocab_dir: Path) -> Vocabulary | None:
    path = vocab_dir / f"{col}.parquet"
    if not path.exists():
        return None
    df = pl.read_parquet(path)
    values: list[str] = df["value"].cast(pl.Utf8).to_list()
    return Vocabulary(column=col, values=tuple(values))


def collect_feature_stats(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    *,
    vocab_dir: Path = VOCAB_DIR,
    rebuild_vocabs: bool = False,
) -> FeatureStats:
    """One pass over the TRAIN partition to capture vocabularies + numeric stats.

    Vocabularies are cached as parquet under `vocab_dir`. They depend
    only on the upstream `ml_features` distinct values; rebuild only
    when the upstream changes (or pass `rebuild_vocabs=True`). Numeric
    statistics and class labels are recomputed every run.
    """
    train_query = _select("TRAIN", schema)

    vocabularies: dict[str, Vocabulary] = {}
    if not rebuild_vocabs:
        for col in (*HIGH_CARD_CATEGORICAL, *LOW_CARD_CATEGORICAL):
            cached = _load_cached_vocab(col, vocab_dir)
            if cached is None:
                _log.info("vocab cache miss for %s; will rebuild all", col)
                vocabularies = {}
                rebuild_vocabs = True
                break
            vocabularies[col] = cached
        if vocabularies:
            for col, v in vocabularies.items():
                _log.info("vocab %s: %d entries (cached)", col, v.size)

    if rebuild_vocabs or not vocabularies:
        _log.info("collecting categorical vocabularies")
        vocabularies = {}
        for col in (*HIGH_CARD_CATEGORICAL, *LOW_CARD_CATEGORICAL):
            rows = con.execute(
                f"SELECT DISTINCT {col} AS v FROM ({train_query}) WHERE {col} IS NOT NULL ORDER BY v"
            ).fetchall()
            series = pl.Series(col, [r[0] for r in rows], dtype=pl.Utf8)
            vocabularies[col] = build_vocabulary(series, col)
            _log.info("vocab %s: %d entries", col, vocabularies[col].size)

    _log.info("collecting numeric mean/variance")
    numeric_means: dict[str, float] = {}
    numeric_variances: dict[str, float] = {}
    agg_select = ", ".join(
        f"AVG({c})::DOUBLE AS {c}_mean, VAR_POP({c})::DOUBLE AS {c}_var"
        for c in NUMERIC
    )
    row = con.execute(
        f"SELECT {agg_select} FROM ({train_query})"
    ).fetchone()
    assert row is not None
    for i, c in enumerate(NUMERIC):
        numeric_means[c] = float(row[2 * i])
        numeric_variances[c] = max(float(row[2 * i + 1]), 1e-6)

    _log.info("collecting target class labels")
    class_rows = con.execute(
        f"SELECT DISTINCT {TARGET_COLUMN} AS v FROM ({train_query}) WHERE {TARGET_COLUMN} IS NOT NULL ORDER BY v"
    ).fetchall()
    class_labels = tuple(r[0] for r in class_rows)

    train_count = con.execute(f"SELECT COUNT(*) FROM ({train_query})").fetchone()
    test_count = con.execute(
        f"SELECT COUNT(*) FROM ({_select('TEST', schema)})"
    ).fetchone()
    assert train_count is not None and test_count is not None

    return FeatureStats(
        vocabularies=vocabularies,
        numeric_means=numeric_means,
        numeric_variances=numeric_variances,
        class_labels=class_labels,
        train_row_count=int(train_count[0]),
        test_row_count=int(test_count[0]),
    )


def _encode_batch(
    df: pl.DataFrame,
    stats: FeatureStats,
    class_index: dict[str, int],
) -> tuple[dict[str, NDArray[np.int64] | NDArray[np.float32]], NDArray[np.int64], NDArray[np.float32]]:
    inputs: dict[str, NDArray[np.int64] | NDArray[np.float32]] = {}
    for col in (*HIGH_CARD_CATEGORICAL, *LOW_CARD_CATEGORICAL):
        encoded = stats.vocabularies[col].encode(df[col]).to_numpy()
        inputs[col] = encoded.astype(np.int64).reshape(-1, 1)
    for col in NUMERIC:
        inputs[col] = df[col].cast(pl.Float32).fill_null(0.0).to_numpy().reshape(-1, 1)

    target_codes = (
        df[TARGET_COLUMN]
        .cast(pl.Utf8)
        .replace_strict(class_index, default=-1)
        .cast(pl.Int64)
        .to_numpy()
    )
    valid = target_codes >= 0
    if not bool(valid.all()):
        _log.warning("dropping %d rows with unknown target class", int((~valid).sum()))
        for k, v in inputs.items():
            inputs[k] = v[valid]
        target_codes = target_codes[valid]

    weights = df[SAMPLE_WEIGHT_COLUMN].cast(pl.Float32).fill_null(0.0).to_numpy()
    if not bool(valid.all()):
        weights = weights[valid]
    return inputs, target_codes.astype(np.int64), weights.astype(np.float32)


def make_batch_generator(
    db_path: str,
    split: str,
    schema: str,
    stats: FeatureStats,
    class_index: dict[str, int],
    rows_per_fetch: int,
    keras_batch_size: int,
) -> Iterator[tuple[dict[str, NDArray[np.int64] | NDArray[np.float32]], NDArray[np.int64], NDArray[np.float32]]]:
    """Yield Keras-sized mini-batches.

    DuckDB streams `rows_per_fetch`-row chunks from `ml_features`; each
    chunk is encoded once and then sliced into `keras_batch_size`
    mini-batches before yielding. Mirrors the inner `batches_to_grab`
    factor from the deleted `keras_sandbox.ipynb`. The connection is
    re-opened per pass so Keras can re-iterate per epoch.
    """
    while True:
        with open_bc_db(db_path, read_only=True) as con:
            for chunk in stream_query(
                con, _select(split, schema), rows_per_batch=rows_per_fetch
            ):
                if chunk.height == 0:
                    continue
                inputs, targets, weights = _encode_batch(chunk, stats, class_index)
                n = targets.shape[0]
                for start in range(0, n, keras_batch_size):
                    end = min(start + keras_batch_size, n)
                    if end - start < keras_batch_size:
                        # Skip ragged tail — Keras prefers fixed-size batches and
                        # the next fetch refills the buffer.
                        continue
                    sliced_inputs = {k: v[start:end] for k, v in inputs.items()}
                    yield sliced_inputs, targets[start:end], weights[start:end]


def save_vocabularies(stats: FeatureStats, vocab_dir: Path) -> dict[str, str]:
    vocab_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, str] = {}
    for col, vocab in stats.vocabularies.items():
        out = vocab_dir / f"{col}.parquet"
        pl.DataFrame({"value": list(vocab.values)}).write_parquet(out)
        paths[col] = str(out)
    return paths


def write_pin(
    pin_path: Path,
    *,
    run_id: str,
    tracking_uri: str,
    vocab_paths: dict[str, str],
    class_labels: tuple[str, ...],
    numeric_means: dict[str, float],
    numeric_variances: dict[str, float],
) -> None:
    pin_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "run_id": run_id,
        "tracking_uri": tracking_uri,
        "vocab_paths": vocab_paths,
        "class_labels": list(class_labels),
        "numeric_means": numeric_means,
        "numeric_variances": numeric_variances,
    }
    pin_path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    _log.info("wrote pin %s", pin_path)


def run_fit_and_log(
    *,
    model: Any,
    stats: FeatureStats,
    class_index: dict[str, int],
    db_path: str,
    schema: str,
    epochs: int,
    rows_per_batch: int,
    keras_batch_size: int,
) -> dict[str, str]:
    """Drive `model.fit` over streaming generators and log to MLflow.

    Hamilton's `fitted_run` node calls into this. Kept as a plain
    function so it stays directly callable for ad-hoc training too.
    """
    train_gen = make_batch_generator(
        db_path, "TRAIN", schema, stats, class_index, rows_per_batch, keras_batch_size
    )
    test_gen = make_batch_generator(
        db_path, "TEST", schema, stats, class_index, rows_per_batch, keras_batch_size
    )

    steps_per_epoch = max(1, stats.train_row_count // keras_batch_size)
    validation_steps = max(1, stats.test_row_count // keras_batch_size)

    _log.info(
        "training set: %d rows; test set: %d rows; %d classes",
        stats.train_row_count,
        stats.test_row_count,
        len(stats.class_labels),
    )

    result: dict[str, str] = {}
    with mlflow.start_run() as run:
        _ = mlflow.log_params(
            {
                "epochs": epochs,
                "rows_per_batch": rows_per_batch,
                "keras_batch_size": keras_batch_size,
                "num_classes": len(stats.class_labels),
                "train_rows": stats.train_row_count,
                "test_rows": stats.test_row_count,
            }
        )
        history = model.fit(
            train_gen,
            steps_per_epoch=steps_per_epoch,
            validation_data=test_gen,
            validation_steps=validation_steps,
            epochs=epochs,
            verbose=2,
        )
        for metric_name, values in history.history.items():
            for epoch_idx, v in enumerate(values):
                _ = mlflow.log_metric(metric_name, float(v), step=epoch_idx)

        # Build a model signature from a sample batch so MLflow stops
        # warning about unknown input/output schema.
        from mlflow.models.signature import infer_signature

        sample_inputs, _, _ = next(
            make_batch_generator(
                db_path,
                "TEST",
                schema,
                stats,
                class_index,
                rows_per_batch,
                keras_batch_size,
            )
        )
        sample_predictions = model.predict(sample_inputs, verbose=0)
        signature = infer_signature(sample_inputs, sample_predictions)
        _ = mlflow.keras.log_model(model, name="model", signature=signature)
        result = {
            "run_id": str(run.info.run_id),
            "tracking_uri": mlflow.get_tracking_uri(),
        }
    return result


def train(
    *,
    db_path: str = DEFAULT_DB,
    schema: str = DEFAULT_SCHEMA,
    epochs: int = DEFAULT_EPOCHS,
    rows_per_batch: int = DEFAULT_BATCH_ROWS,
    keras_batch_size: int = 4096,
    rebuild_vocabs: bool = False,
) -> str:
    """Compose the Hamilton DAG and materialize the pinned run id.

    The `pin_written` node is the leaf of the DAG; requesting it pulls
    feature_stats → model → fitted_run → vocab persistence → pin write
    in dependency order.
    """
    from hamilton import driver

    from python_models.ml import hamilton_dag

    dr = driver.Builder().with_modules(hamilton_dag).build()
    inputs: dict[str, object] = {
        "db_path": db_path,
        "schema": schema,
        "epochs": epochs,
        "rows_per_batch": rows_per_batch,
        "keras_batch_size": keras_batch_size,
        "rebuild_vocabs": rebuild_vocabs,
        "vocab_dir": VOCAB_DIR,
        "mlflow_tracking_db": MLFLOW_TRACKING_DB,
        "mlruns_dir": MLRUNS_DIR,
        "experiment_name": "plate_appearance_cat",
        "pin_path": PIN_PATH,
    }
    result = dr.execute(["pin_written"], inputs=inputs)
    return str(result["pin_written"])


def export_dag_diagram(out_path: Path) -> Path:
    """Emit the Hamilton training DAG as a PNG/dot file for the followups."""
    from hamilton import driver

    from python_models.ml import hamilton_dag

    dr = driver.Builder().with_modules(hamilton_dag).build()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fmt = out_path.suffix.lstrip(".") or "png"
    _ = dr.display_all_functions(
        output_file_path=str(out_path),
        render_kwargs={"format": fmt},
    )
    return out_path
