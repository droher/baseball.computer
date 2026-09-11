from __future__ import annotations

import json
import logging
import math
from pathlib import Path
from typing import NamedTuple

import arviz as az
import numpy as np
import numpy.typing as npt
import polars as pl
import pymc as pm
from pymc.math import softmax

from python_models.statistical.pymc_utils import SamplingConfig, sample_model

_log = logging.getLogger(__name__)

FEATURE_COLUMNS: tuple[str, ...] = (
    "era",
    "result_family",
    "base_state_start",
    "outs_start",
    "alignment_regime",
    "batter_hand",
)
TARGET_COLUMN = "target_class"
PREDICTION_CHUNK_SIZE = 256

FloatArray = npt.NDArray[np.float64]
IntArray = npt.NDArray[np.int64]


class ReferenceTrainingData(NamedTuple):
    counts: IntArray
    feature_codes: dict[str, IntArray]
    feature_levels: dict[str, tuple[str, ...]]
    class_labels: tuple[str, ...]
    feature_columns: tuple[str, ...]


def _require_columns(frame: pl.DataFrame, columns: tuple[str, ...]) -> None:
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise ValueError(f"frame is missing required columns: {missing}")
    for column in columns:
        if frame.get_column(column).null_count() > 0:
            raise ValueError(f"column {column!r} contains null values")
        if frame.schema[column] != pl.String:
            raise TypeError(
                f"column {column!r} must be String, got {frame.schema[column]}"
            )


def _prepare_training_data(
    train: pl.DataFrame, *, feature_columns: tuple[str, ...] = FEATURE_COLUMNS
) -> ReferenceTrainingData:
    if not feature_columns or len(set(feature_columns)) != len(feature_columns):
        raise ValueError("feature_columns must be non-empty and unique")
    if TARGET_COLUMN in feature_columns:
        raise ValueError(f"{TARGET_COLUMN!r} cannot be a feature column")
    _require_columns(train, (*feature_columns, TARGET_COLUMN))
    if train.height == 0:
        raise ValueError("training frame is empty")

    class_labels = tuple(
        sorted(
            str(value) for value in train.get_column(TARGET_COLUMN).unique().to_list()
        )
    )
    if len(class_labels) < 2:
        raise ValueError("training frame must contain at least two target classes")
    feature_levels = {
        column: tuple(
            sorted(str(value) for value in train.get_column(column).unique().to_list())
        )
        for column in feature_columns
    }

    grouped = (
        train.group_by([*feature_columns, TARGET_COLUMN], maintain_order=False)
        .len(name="count")
        .sort([*feature_columns, TARGET_COLUMN])
    )
    cell_rows = grouped.select(feature_columns).unique(maintain_order=True).rows()
    cell_index = {
        tuple(str(value) for value in row): i for i, row in enumerate(cell_rows)
    }
    class_index = {label: i for i, label in enumerate(class_labels)}
    counts = np.zeros((len(cell_rows), len(class_labels)), dtype=np.int64)
    for row in grouped.iter_rows(named=True):
        key = tuple(str(row[column]) for column in feature_columns)
        counts[cell_index[key], class_index[str(row[TARGET_COLUMN])]] = int(
            row["count"]
        )
    if int(counts.sum()) != train.height or np.any(counts.sum(axis=1) <= 0):
        raise AssertionError("aggregated counts do not conserve training rows")

    feature_codes: dict[str, IntArray] = {}
    for position, column in enumerate(feature_columns):
        mapping = {level: i for i, level in enumerate(feature_levels[column])}
        feature_codes[column] = np.asarray(
            [mapping[str(row[position])] for row in cell_rows], dtype=np.int64
        )
    return ReferenceTrainingData(
        counts=counts,
        feature_codes=feature_codes,
        feature_levels=feature_levels,
        class_labels=class_labels,
        feature_columns=feature_columns,
    )


def _build_reference_model(
    data: ReferenceTrainingData, *, include_cell_prob: bool = False
) -> pm.Model:
    coords: dict[str, list[str]] = {
        "cell": [str(i) for i in range(data.counts.shape[0])],
        "class": list(data.class_labels),
    }
    for column, levels in data.feature_levels.items():
        coords[f"{column}_level"] = list(levels)

    with pm.Model(coords=coords) as model:
        alpha = pm.ZeroSumNormal("alpha_class", sigma=1.5, dims="class")
        eta = alpha[None, :]
        for column in data.feature_columns:
            levels = data.feature_levels[column]
            if len(levels) <= 1:
                continue
            codes = pm.Data(
                f"{column}_codes",
                data.feature_codes[column],
                dims="cell",
            )
            delta = pm.ZeroSumNormal(
                f"delta_{column}",
                sigma=0.5,
                n_zerosum_axes=2,
                dims=(f"{column}_level", "class"),
            )
            eta = eta + delta[codes]
        probabilities = softmax(eta, axis=1)
        if include_cell_prob:
            probabilities = pm.Deterministic(
                "cell_prob", probabilities, dims=("cell", "class")
            )
        pm.Multinomial(
            "observed_counts",
            n=data.counts.sum(axis=1),
            p=probabilities,
            observed=data.counts,
            dims=("cell", "class"),
        )
    return model


def _sampling_config(*, smoke: bool, seed: int) -> SamplingConfig:
    if smoke:
        return SamplingConfig(
            draws=50,
            tune=50,
            chains=2,
            target_accept=0.95,
            random_seed=seed,
            cores=1,
            max_treedepth=12,
            backend="nutpie",
        )
    return SamplingConfig(
        draws=1000,
        tune=1000,
        chains=4,
        target_accept=0.95,
        random_seed=seed,
        cores=1,
        max_treedepth=12,
        backend="nutpie",
    )


def _prior_check(
    model: pm.Model, *, seed: int
) -> tuple[az.InferenceData, dict[str, object]]:
    with model:
        prior = pm.sample_prior_predictive(
            draws=100,
            random_seed=seed,
            var_names=["cell_prob"],
        )
    prior_group = getattr(prior, "prior")
    probabilities = np.asarray(prior_group["cell_prob"].values, dtype=np.float64)
    finite = bool(np.isfinite(probabilities).all())
    normalized = bool(
        np.allclose(probabilities.sum(axis=-1), 1.0, atol=1e-10, rtol=0.0)
    )
    positive = bool(np.all(probabilities > 0.0))
    entropy = -np.sum(probabilities * np.log(probabilities), axis=-1)
    class_shares = probabilities.mean(axis=(0, 1, 2))
    entropy_min = float(entropy.min())
    entropy_max = float(entropy.max())
    entropy_bound = math.log(probabilities.shape[-1])
    entropy_valid = entropy_min >= -1e-10 and entropy_max <= entropy_bound + 1e-10
    payload: dict[str, object] = {
        "draws": 100,
        "finite": finite,
        "normalized": normalized,
        "strictly_positive": positive,
        "prior_class_shares": [float(value) for value in class_shares],
        "entropy_min": entropy_min,
        "entropy_max": entropy_max,
        "entropy_lower_bound": 0.0,
        "entropy_upper_bound": entropy_bound,
        "passed": finite and normalized and positive and entropy_valid,
    }
    if not bool(payload["passed"]):
        raise ValueError(f"prior predictive check failed: {payload}")
    return prior, payload


def fit_reference(
    train: pl.DataFrame,
    *,
    output_dir: Path,
    smoke: bool,
    seed: int = 20260911,
    feature_columns: tuple[str, ...] = FEATURE_COLUMNS,
    sampling_config: SamplingConfig | None = None,
) -> tuple[az.InferenceData, dict[str, tuple[str, ...]], tuple[str, ...]]:
    data = _prepare_training_data(train, feature_columns=feature_columns)
    output_dir.mkdir(parents=True, exist_ok=True)
    posterior_path = output_dir / "posterior.nc"
    prior_check_path = output_dir / "prior_check.json"
    fit_config_path = output_dir / "fit_config.json"
    if posterior_path.exists() or prior_check_path.exists() or fit_config_path.exists():
        raise FileExistsError(f"reference fit output already exists under {output_dir}")

    prior_model = _build_reference_model(data, include_cell_prob=True)
    _, prior_payload = _prior_check(prior_model, seed=seed)
    prior_check_path.write_text(
        json.dumps(prior_payload, indent=2, sort_keys=True), encoding="utf-8"
    )
    sampler = (
        _sampling_config(smoke=smoke, seed=seed)
        if sampling_config is None
        else sampling_config
    )
    fit_config_path.write_text(
        json.dumps(
            {
                "class_labels": list(data.class_labels),
                "feature_columns": list(data.feature_columns),
                "feature_levels": {
                    column: list(levels)
                    for column, levels in data.feature_levels.items()
                },
                "training_rows": train.height,
                "aggregated_cells": int(data.counts.shape[0]),
                "sampler": {
                    "backend": sampler.backend,
                    "chains": sampler.chains,
                    "cores": sampler.cores,
                    "draws": sampler.draws,
                    "max_treedepth": sampler.max_treedepth,
                    "random_seed": sampler.random_seed,
                    "target_accept": sampler.target_accept,
                    "tune": sampler.tune,
                },
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    model = _build_reference_model(data)
    _log.info(
        "fit geometry reference rows=%d cells=%d classes=%d smoke=%s",
        train.height,
        data.counts.shape[0],
        len(data.class_labels),
        smoke,
    )
    idata = sample_model(
        model,
        sampler,
        output_path=posterior_path,
        progress_log_path=output_dir / "sampling.log",
    )
    return idata, data.feature_levels, data.class_labels


def predict_reference(
    idata: az.InferenceData,
    frame: pl.DataFrame,
    feature_levels: dict[str, tuple[str, ...]],
    class_labels: tuple[str, ...],
    *,
    feature_columns: tuple[str, ...] | None = None,
) -> FloatArray:
    selected_columns = (
        tuple(feature_levels) if feature_columns is None else feature_columns
    )
    if not selected_columns or len(set(selected_columns)) != len(selected_columns):
        raise ValueError("feature_columns must be non-empty and unique")
    _require_columns(frame, selected_columns)
    if tuple(feature_levels) != selected_columns:
        raise ValueError(f"feature_levels keys must equal {selected_columns}")
    if not class_labels:
        raise ValueError("class_labels must not be empty")
    if frame.height == 0:
        return np.zeros((0, len(class_labels)), dtype=np.float64)

    rows = [
        tuple(str(value) for value in row)
        for row in frame.select(selected_columns).rows()
    ]
    unique_rows = tuple(dict.fromkeys(rows))
    unique_index = {row: i for i, row in enumerate(unique_rows)}
    inverse = np.asarray([unique_index[row] for row in rows], dtype=np.int64)
    mappings = {
        column: {level: i for i, level in enumerate(feature_levels[column])}
        for column in selected_columns
    }
    codes = {
        column: np.asarray(
            [mappings[column].get(row[position], -1) for row in unique_rows],
            dtype=np.int64,
        )
        for position, column in enumerate(selected_columns)
    }

    posterior = getattr(idata, "posterior")
    alpha = np.asarray(posterior["alpha_class"].values, dtype=np.float64)
    if alpha.shape[-1] != len(class_labels):
        raise ValueError("posterior class dimension does not match class_labels")
    sample_count = alpha.shape[0] * alpha.shape[1]
    alpha_samples = alpha.reshape(sample_count, len(class_labels))
    deltas: dict[str, FloatArray] = {}
    for column in selected_columns:
        variable = f"delta_{column}"
        if variable in posterior.data_vars:
            raw = np.asarray(posterior[variable].values, dtype=np.float64)
            deltas[column] = raw.reshape(sample_count, raw.shape[-2], raw.shape[-1])

    unique_probabilities = np.empty(
        (len(unique_rows), len(class_labels)), dtype=np.float64
    )
    for start in range(0, len(unique_rows), PREDICTION_CHUNK_SIZE):
        stop = min(start + PREDICTION_CHUNK_SIZE, len(unique_rows))
        eta = np.broadcast_to(
            alpha_samples[:, None, :],
            (sample_count, stop - start, len(class_labels)),
        ).copy()
        for column, delta in deltas.items():
            chunk_codes = codes[column][start:stop]
            seen = chunk_codes >= 0
            safe_codes = np.where(seen, chunk_codes, 0)
            eta += delta[:, safe_codes, :] * seen[None, :, None]
        eta -= eta.max(axis=-1, keepdims=True)
        probabilities = np.exp(eta)
        probabilities /= probabilities.sum(axis=-1, keepdims=True)
        unique_probabilities[start:stop] = probabilities.mean(axis=0)
    result = unique_probabilities[inverse]
    if not np.isfinite(result).all() or not np.allclose(
        result.sum(axis=1), 1.0, atol=1e-10, rtol=0.0
    ):
        raise ValueError("posterior predictions are non-finite or not normalized")
    return result.astype(np.float64, copy=False)
