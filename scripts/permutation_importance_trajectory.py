from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

_ = os.environ.setdefault("KERAS_BACKEND", "torch")

import numpy as np
import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "bc"))

from python_models.statistical.deep import targets as _targets  # noqa: F401
from python_models.statistical.deep.io import load_dataset_parquet, partition_by_split
from python_models.statistical.deep.registry import get_target
from python_models.statistical.deep.feature_layout import coverage_layout_for
from python_models.statistical.deep.training import (  # noqa: PLC2701
    _collect_polars_stats,
    _encode_inputs,
    _encode_targets,
    _fit_keras,
    DEFAULT_PREDICT_BATCH_SIZE,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
log = logging.getLogger("perm_imp")

_NONE = os.environ.get("PERMIMP_FULL") == "1"
TRAIN_SUBSAMPLE = None if _NONE else 500_000
VAL_SUBSAMPLE = None if _NONE else 100_000
EPOCHS = int(os.environ.get("PERMIMP_EPOCHS", "20" if _NONE else "8"))
BATCH = 4096
RNG_SEED = 0
VAL_FILTER = os.environ.get("PERMIMP_VAL_FILTER")
DATASET_PARQUET = (
    REPO_ROOT
    / "artifacts/statistical/datasets/model_input_geometry/phase3-tune-v3-prep/dataset.parquet"
)


def ce(
    probs: np.ndarray[Any, Any],
    y: np.ndarray[Any, Any],
    w: np.ndarray[Any, Any],
) -> float:
    eps = 1e-12
    pt = np.clip(probs[np.arange(len(y)), y], eps, 1.0)
    return float(-np.average(np.log(pt), weights=w))


def main() -> None:
    spec = get_target("geometry_trajectory")
    layout = coverage_layout_for(spec.dataset_name)
    log.info("loading dataset %s", DATASET_PARQUET)
    df = load_dataset_parquet(str(DATASET_PARQUET))
    log.info("filter_predicate=%s", spec.filter_predicate)
    df = df.sql(f"SELECT * FROM self WHERE {spec.filter_predicate}")
    remap = spec.remap_dict()
    if remap:
        df = df.with_columns(
            pl.col(spec.target_column).cast(pl.Utf8).replace(remap).alias(spec.target_column)
        )
    if spec.loss_mask_predicate:
        ctx = pl.SQLContext({"self": df.lazy()})
        mask = (
            ctx.execute(
                f"SELECT ({spec.loss_mask_predicate}) AS _m FROM self"
            ).collect().to_series()
        )
        df = df.with_columns(
            pl.when(mask)
            .then(pl.col(spec.weight_column).cast(pl.Float64))
            .otherwise(0.0)
            .alias(spec.weight_column)
        )
    train_df, validate_df, _ = partition_by_split(
        df,
        split_column=spec.split_column,
        train_label=spec.train_label,
        validate_label=spec.validate_label,
        test_label=spec.test_label,
    )
    log.info(
        "rows: train=%d validate=%d", train_df.height, validate_df.height
    )
    if VAL_FILTER:
        validate_df = validate_df.sql(f"SELECT * FROM self WHERE {VAL_FILTER}")
        log.info("val filter %r → validate=%d rows", VAL_FILTER, validate_df.height)

    class_universe = (
        spec.configured_class_labels
        if spec.class_universe_source == "configured"
        else None
    )
    stats = _collect_polars_stats(
        train_df,
        layout=layout,
        target_column=spec.target_column,
        class_universe=class_universe,
    )

    train_sample = (
        train_df
        if TRAIN_SUBSAMPLE is None
        else train_df.sample(n=min(TRAIN_SUBSAMPLE, train_df.height), seed=RNG_SEED)
    )
    val_sample = (
        validate_df
        if VAL_SUBSAMPLE is None
        else validate_df.sample(n=min(VAL_SUBSAMPLE, validate_df.height), seed=RNG_SEED)
    )
    log.info("subsamples: train=%d val=%d", train_sample.height, val_sample.height)
    class_index = {label: i for i, label in enumerate(stats.class_labels)}
    log.info("class_labels=%s", stats.class_labels)

    log.info("fitting (epochs=%d batch=%d)", EPOCHS, BATCH)
    t0 = time.perf_counter()
    outcome = _fit_keras(
        spec=spec,
        layout=layout,
        stats=stats,
        fit_df=train_sample,
        class_index=class_index,
        epochs=EPOCHS,
        keras_batch_size=BATCH,
        validation_df=val_sample,
        log_tag="perm_imp_fit",
    )
    log.info(
        "fit done best_epoch=%d epochs_ran=%d val_loss=%.4f elapsed=%.1fs",
        outcome.best_epoch,
        outcome.epochs_ran,
        outcome.final_val_loss if outcome.final_val_loss is not None else float("nan"),
        time.perf_counter() - t0,
    )
    model = outcome.model

    val_x = _encode_inputs(val_sample, layout=layout, stats=stats)
    val_y_raw, val_valid = _encode_targets(
        val_sample, target_column=spec.target_column, class_index=class_index
    )
    val_w_raw = val_sample[spec.weight_column].cast(pl.Float32).fill_null(0.0).to_numpy()

    keep = val_valid & (val_w_raw > 0)
    val_x_eff = {k: v[keep] for k, v in val_x.items()}
    val_y = val_y_raw[keep]
    val_w = val_w_raw[keep].astype(np.float64)
    log.info("val eligible rows: %d / %d", int(keep.sum()), len(val_valid))

    baseline_probs = np.asarray(
        model.predict(val_x_eff, batch_size=DEFAULT_PREDICT_BATCH_SIZE, verbose=0),
        dtype=np.float64,
    )
    baseline_ce = ce(baseline_probs, val_y, val_w)
    log.info("baseline CE=%.4f", baseline_ce)

    feature_columns = list(layout.categorical_columns) + list(layout.numeric_columns)
    rng = np.random.default_rng(RNG_SEED)
    results: list[tuple[str, float, float]] = []
    n_eff = val_y.shape[0]

    for fcol in feature_columns:
        x_perm = {k: v.copy() for k, v in val_x_eff.items()}
        perm_idx = rng.permutation(n_eff)
        x_perm[fcol] = x_perm[fcol][perm_idx]
        probs = np.asarray(
            model.predict(x_perm, batch_size=DEFAULT_PREDICT_BATCH_SIZE, verbose=0),
            dtype=np.float64,
        )
        new_ce = ce(probs, val_y, val_w)
        delta = new_ce - baseline_ce
        results.append((fcol, new_ce, delta))
        log.info("perm %-30s CE=%.4f Δ=%+.4f", fcol, new_ce, delta)

    results.sort(key=lambda r: -r[2])
    print("\n=== Permutation Importance (sorted by Δ CE descending) ===")
    print(f"{'feature':<32}{'CE_after':>10}{'Δ_CE':>10}{'kind':>8}")
    cat = set(layout.categorical_columns)
    num = set(layout.numeric_columns)
    for f, c, d in results:
        kind = "cat" if f in cat else ("num" if f in num else "?")
        print(f"{f:<32}{c:>10.4f}{d:>+10.4f}{kind:>8}")
    print(f"\nbaseline CE = {baseline_ce:.4f}")


if __name__ == "__main__":
    main()
