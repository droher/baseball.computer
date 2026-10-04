"""Hamilton DAG for the ML training pipeline.

Each top-level function is a Hamilton node; parameter names declare
dependencies. Compose a `hamilton.driver.Driver` over this module and
materialize a leaf node (`pin_written`) to run.

The DAG is target-agnostic: it consumes a `TargetSpec` via the
`target_spec` input, and the model factory + training driver dispatch
on `target_spec.kind`. To add a new target, declare its spec in
`features.py`, hand it to `training.train(...)`, and the same DAG
runs end-to-end.
"""

from __future__ import annotations

import python_models.ml  # noqa: F401  # set KERAS_BACKEND before keras import

import logging
from pathlib import Path
from typing import Any

import keras
import mlflow

from python_models.ml.data_loaders import open_bc_db
from python_models.ml.features import TargetSpec, Vocabulary
from python_models.ml.model_factory import build_model

_log = logging.getLogger(__name__)


def feature_stats(
    target_spec: TargetSpec,
    db_path: str,
    schema: str,
    rebuild_vocabs: bool,
    vocab_dir: Path,
) -> Any:
    from python_models.ml.training import collect_feature_stats

    with open_bc_db(db_path, read_only=True) as con:
        return collect_feature_stats(
            con,
            target_spec,
            schema,
            vocab_dir=vocab_dir,
            rebuild_vocabs=rebuild_vocabs,
        )


def num_classes(target_spec: TargetSpec, feature_stats: Any) -> int:
    # Binary and regression heads emit one scalar per row; class_labels
    # is unused at fit time for both.
    if target_spec.kind in {"binary", "regression"}:
        return 1
    return len(feature_stats.class_labels)


def class_index(feature_stats: Any) -> dict[str, int]:
    return {label: i for i, label in enumerate(feature_stats.class_labels)}


def vocab_sizes(feature_stats: Any) -> dict[str, int]:
    vs: dict[str, int] = {}
    for col, vocab in feature_stats.vocabularies.items():
        assert isinstance(vocab, Vocabulary)
        vs[col] = vocab.size
    return vs


def model(
    target_spec: TargetSpec,
    feature_stats: Any,
    vocab_sizes: dict[str, int],
    num_classes: int,
) -> keras.Model:
    return build_model(
        target_spec=target_spec,
        vocab_sizes=vocab_sizes,
        numeric_means=feature_stats.numeric_means,
        numeric_variances=feature_stats.numeric_variances,
        num_classes=num_classes,
    )


def mlflow_setup(
    mlflow_tracking_db: Path,
    mlruns_dir: Path,
    experiment_name: str,
) -> str:
    mlruns_dir.mkdir(parents=True, exist_ok=True)
    mlflow.set_tracking_uri(f"sqlite:///{mlflow_tracking_db}")
    if mlflow.get_experiment_by_name(experiment_name) is None:
        _ = mlflow.create_experiment(
            experiment_name, artifact_location=f"file:{mlruns_dir}"
        )
    _ = mlflow.set_experiment(experiment_name)
    return mlflow.get_tracking_uri()


def fitted_run(
    target_spec: TargetSpec,
    model: keras.Model,
    feature_stats: Any,
    class_index: dict[str, int],
    db_path: str,
    schema: str,
    epochs: int,
    rows_per_batch: int,
    keras_batch_size: int,
    mlflow_setup: str,
) -> dict[str, str]:
    del mlflow_setup  # consumed for ordering
    from python_models.ml.training import run_fit_and_log

    return run_fit_and_log(
        target_spec=target_spec,
        model=model,
        stats=feature_stats,
        class_index=class_index,
        db_path=db_path,
        schema=schema,
        epochs=epochs,
        rows_per_batch=rows_per_batch,
        keras_batch_size=keras_batch_size,
    )


def saved_vocab_paths(feature_stats: Any, vocab_dir: Path) -> dict[str, str]:
    from python_models.ml.training import save_vocabularies

    return save_vocabularies(feature_stats, vocab_dir)


def pin_written(
    target_spec: TargetSpec,
    fitted_run: dict[str, str],
    saved_vocab_paths: dict[str, str],
    feature_stats: Any,
    pin_path: Path,
) -> str:
    from python_models.ml.training import write_pin

    write_pin(
        pin_path,
        target_spec=target_spec,
        run_id=fitted_run["run_id"],
        tracking_uri=fitted_run["tracking_uri"],
        vocab_paths=saved_vocab_paths,
        class_labels=feature_stats.class_labels,
        numeric_means=feature_stats.numeric_means,
        numeric_variances=feature_stats.numeric_variances,
    )
    return fitted_run["run_id"]
