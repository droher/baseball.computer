from __future__ import annotations

import hashlib
import json
import logging
import math
from collections.abc import Iterator
from pathlib import Path
from typing import Literal, NamedTuple, Self

import arviz as az
import numpy as np
import numpy.typing as npt
import polars as pl
import pymc as pm
from pydantic import BaseModel, ConfigDict, model_validator
from pymc.math import softmax

from python_models.statistical.pymc_utils import SamplingConfig, sample_model

LOGGER = logging.getLogger(__name__)
TARGET_COLUMN = "target_class"
ERA_COLUMN = "era"
RESULT_COLUMN = "result_family"
CONTEXT_COLUMNS = (
    "base_state_start",
    "outs_start",
    "alignment_regime",
    "batter_hand",
)
FEATURE_COLUMNS = (ERA_COLUMN, RESULT_COLUMN, *CONTEXT_COLUMNS)
PREDICTION_CHUNK_SIZE = 256

FloatArray = npt.NDArray[np.float64]
IntArray = npt.NDArray[np.int64]
BoolArray = npt.NDArray[np.bool_]


class HierarchicalEncoding(BaseModel):
    model_config = ConfigDict(frozen=True)

    model_type: Literal["exchangeable_era_result_v1"] = "exchangeable_era_result_v1"
    class_labels: tuple[str, ...]
    observed_class_labels: tuple[str, ...]
    result_domain: tuple[str, ...]
    observed_result_levels: tuple[str, ...]
    era_levels: tuple[str, ...]
    context_levels: dict[str, tuple[str, ...]]
    observed_era_result_pairs: tuple[tuple[str, str], ...]
    training_rows: int
    unseen_era_policy: str = "posterior_population_draw"
    unseen_result_policy: str = "error"
    unseen_context_policy: str = "prior_population_draw"

    @model_validator(mode="after")
    def validate_encoding(self) -> Self:
        _validate_domain(self.class_labels, "class_labels")
        _validate_domain(self.result_domain, "result_domain")
        if (
            not self.era_levels
            or tuple(sorted(set(self.era_levels))) != self.era_levels
        ):
            raise ValueError("era_levels must be non-empty, sorted, and unique")
        if set(self.observed_class_labels) - set(self.class_labels):
            raise ValueError("observed_class_labels must belong to class_labels")
        if set(self.observed_result_levels) - set(self.result_domain):
            raise ValueError("observed_result_levels must belong to result_domain")
        if set(self.context_levels) != set(CONTEXT_COLUMNS):
            raise ValueError("context_levels keys must equal CONTEXT_COLUMNS")
        for column, levels in self.context_levels.items():
            if not levels or tuple(sorted(set(levels))) != levels:
                raise ValueError(
                    f"context levels for {column} must be sorted and unique"
                )
        if any(
            era not in self.era_levels or result not in self.result_domain
            for era, result in self.observed_era_result_pairs
        ):
            raise ValueError("observed era-result pair lies outside encoding levels")
        if self.training_rows <= 0:
            raise ValueError("training_rows must be positive")
        return self


class HierarchicalTrainingData(NamedTuple):
    counts: IntArray
    era_codes: IntArray
    result_codes: IntArray
    context_codes: dict[str, IntArray]
    encoding: HierarchicalEncoding


class ProbabilityDrawChunk(NamedTuple):
    cell_indices: IntArray
    feature_rows: tuple[tuple[str, ...], ...]
    probabilities: FloatArray


class PredictiveResult(NamedTuple):
    probabilities: FloatArray
    event_cell_index: IntArray
    known_era: BoolArray
    observed_era_result_pair: BoolArray
    unseen_context_count: IntArray


def _require_string_columns(frame: pl.DataFrame, columns: tuple[str, ...]) -> None:
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise ValueError(f"frame is missing required columns: {missing}")
    for column in columns:
        series = frame.get_column(column)
        if series.null_count():
            raise ValueError(f"column {column!r} contains null values")
        if series.dtype != pl.String:
            raise TypeError(f"column {column!r} must be String, got {series.dtype}")


def _validate_domain(values: tuple[str, ...], name: str) -> None:
    if len(values) < 2 or len(set(values)) != len(values):
        raise ValueError(f"{name} must contain at least two unique values")
    if tuple(sorted(values)) != values:
        raise ValueError(f"{name} must be sorted")


def prepare_hierarchical_data(
    train: pl.DataFrame,
    *,
    class_labels: tuple[str, ...],
    result_domain: tuple[str, ...],
) -> HierarchicalTrainingData:
    _validate_domain(class_labels, "class_labels")
    _validate_domain(result_domain, "result_domain")
    _require_string_columns(train, (*FEATURE_COLUMNS, TARGET_COLUMN))
    if train.is_empty():
        raise ValueError("training frame is empty")
    observed_classes = tuple(sorted(set(train[TARGET_COLUMN].to_list())))
    unknown_classes = sorted(set(observed_classes) - set(class_labels))
    if unknown_classes:
        raise ValueError(f"TRAIN classes outside fixed class_labels: {unknown_classes}")
    observed_results = tuple(sorted(set(train[RESULT_COLUMN].to_list())))
    unknown_results = sorted(set(observed_results) - set(result_domain))
    if unknown_results:
        raise ValueError(
            f"TRAIN results outside fixed result_domain: {unknown_results}"
        )
    era_levels = tuple(sorted(set(train[ERA_COLUMN].to_list())))
    context_levels = {
        column: tuple(sorted(set(train[column].to_list())))
        for column in CONTEXT_COLUMNS
    }
    grouped = (
        train.group_by([*FEATURE_COLUMNS, TARGET_COLUMN])
        .len(name="count")
        .sort([*FEATURE_COLUMNS, TARGET_COLUMN])
    )
    cell_rows = grouped.select(FEATURE_COLUMNS).unique(maintain_order=True).rows()
    cell_lookup = {
        tuple(str(value) for value in row): index for index, row in enumerate(cell_rows)
    }
    class_lookup = {label: index for index, label in enumerate(class_labels)}
    counts = np.zeros((len(cell_rows), len(class_labels)), dtype=np.int64)
    for row in grouped.iter_rows(named=True):
        feature_key = tuple(str(row[column]) for column in FEATURE_COLUMNS)
        counts[cell_lookup[feature_key], class_lookup[str(row[TARGET_COLUMN])]] = int(
            row["count"]
        )
    if int(counts.sum()) != train.height or np.any(counts.sum(axis=1) <= 0):
        raise AssertionError("aggregated counts do not conserve training rows")
    era_lookup = {level: index for index, level in enumerate(era_levels)}
    result_lookup = {level: index for index, level in enumerate(result_domain)}
    context_lookup = {
        column: {level: index for index, level in enumerate(levels)}
        for column, levels in context_levels.items()
    }
    era_codes = np.asarray(
        [era_lookup[str(row[0])] for row in cell_rows], dtype=np.int64
    )
    result_codes = np.asarray(
        [result_lookup[str(row[1])] for row in cell_rows], dtype=np.int64
    )
    context_codes = {
        column: np.asarray(
            [context_lookup[column][str(row[position])] for row in cell_rows],
            dtype=np.int64,
        )
        for position, column in enumerate(FEATURE_COLUMNS[2:], start=2)
    }
    encoding = HierarchicalEncoding(
        class_labels=class_labels,
        observed_class_labels=observed_classes,
        result_domain=result_domain,
        observed_result_levels=observed_results,
        era_levels=era_levels,
        context_levels=context_levels,
        observed_era_result_pairs=tuple(
            sorted(
                set(
                    (str(row[0]), str(row[1]))
                    for row in train.select(ERA_COLUMN, RESULT_COLUMN).rows()
                )
            )
        ),
        training_rows=train.height,
    )
    return HierarchicalTrainingData(
        counts=counts,
        era_codes=era_codes,
        result_codes=result_codes,
        context_codes=context_codes,
        encoding=encoding,
    )


def build_hierarchical_model(
    data: HierarchicalTrainingData, *, include_cell_prob: bool = False
) -> pm.Model:
    encoding = data.encoding
    coords: dict[str, list[str]] = {
        "cell": [str(index) for index in range(data.counts.shape[0])],
        "class": list(encoding.class_labels),
        "era_level": list(encoding.era_levels),
        "result_level": list(encoding.result_domain),
    }
    for column, levels in encoding.context_levels.items():
        coords[f"{column}_level"] = list(levels)
    with pm.Model(coords=coords) as model:
        alpha = pm.ZeroSumNormal("alpha_class", sigma=1.5, dims="class")
        beta_result = pm.ZeroSumNormal(
            "beta_result",
            sigma=0.5,
            n_zerosum_axes=2,
            dims=("result_level", "class"),
        )
        sigma_era = pm.HalfNormal("sigma_era", sigma=0.5)
        z_era = pm.ZeroSumNormal(
            "z_era", sigma=1.0, n_zerosum_axes=1, dims=("era_level", "class")
        )
        delta_era = pm.Deterministic(
            "delta_era", sigma_era * z_era, dims=("era_level", "class")
        )
        sigma_era_result = pm.HalfNormal("sigma_era_result", sigma=0.5)
        z_era_result = pm.ZeroSumNormal(
            "z_era_result",
            sigma=1.0,
            n_zerosum_axes=2,
            dims=("era_level", "result_level", "class"),
        )
        delta_era_result = pm.Deterministic(
            "delta_era_result",
            sigma_era_result * z_era_result,
            dims=("era_level", "result_level", "class"),
        )
        era_codes = pm.Data("era_codes", data.era_codes, dims="cell")
        result_codes = pm.Data("result_codes", data.result_codes, dims="cell")
        eta = (
            alpha[None, :]
            + beta_result[result_codes]
            + delta_era[era_codes]
            + delta_era_result[era_codes, result_codes]
        )
        for column in CONTEXT_COLUMNS:
            codes = pm.Data(f"{column}_codes", data.context_codes[column], dims="cell")
            delta = pm.ZeroSumNormal(
                f"delta_{column}",
                sigma=0.5,
                n_zerosum_axes=1,
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
    return SamplingConfig(
        draws=50 if smoke else 1000,
        tune=50 if smoke else 1000,
        chains=2 if smoke else 4,
        target_accept=0.95,
        random_seed=seed,
        cores=1,
        max_treedepth=12,
        backend="nutpie",
    )


def _seed(seed: int, component: str, level: str) -> int:
    digest = hashlib.sha256(f"{seed}:{component}:{level}".encode()).digest()
    return int.from_bytes(digest[:8], "big")


def _class_zero_draws(sample_count: int, class_count: int, *, seed: int) -> FloatArray:
    raw = np.random.default_rng(seed).normal(size=(sample_count, class_count))
    return raw - raw.mean(axis=1, keepdims=True)


def _result_class_zero_draws(
    sample_count: int, result_count: int, class_count: int, *, seed: int
) -> FloatArray:
    raw = np.random.default_rng(seed).normal(
        size=(sample_count, result_count, class_count)
    )
    return (
        raw
        - raw.mean(axis=1, keepdims=True)
        - raw.mean(axis=2, keepdims=True)
        + raw.mean(axis=(1, 2), keepdims=True)
    )


def _softmax(values: FloatArray) -> FloatArray:
    shifted = values - values.max(axis=-1, keepdims=True)
    exponentiated = np.exp(shifted)
    return exponentiated / exponentiated.sum(axis=-1, keepdims=True)


def _posterior_samples(idata: az.InferenceData, name: str) -> FloatArray:
    posterior = getattr(idata, "posterior")
    values = np.asarray(posterior[name].values, dtype=np.float64)
    return values.reshape(values.shape[0] * values.shape[1], *values.shape[2:])


def _validate_posterior_contract(
    idata: az.InferenceData, encoding: HierarchicalEncoding
) -> None:
    posterior = getattr(idata, "posterior")
    expected_coords = {
        "class": encoding.class_labels,
        "era_level": encoding.era_levels,
        "result_level": encoding.result_domain,
        **{
            f"{column}_level": levels
            for column, levels in encoding.context_levels.items()
        },
    }
    for name, expected in expected_coords.items():
        if name not in posterior.coords:
            raise ValueError(f"posterior is missing coordinate {name}")
        actual = tuple(str(value) for value in posterior.coords[name].values.tolist())
        if actual != expected:
            raise ValueError(f"posterior coordinate {name} differs from encoding")
    expected_dims = {
        "alpha_class": ("chain", "draw", "class"),
        "beta_result": ("chain", "draw", "result_level", "class"),
        "sigma_era": ("chain", "draw"),
        "delta_era": ("chain", "draw", "era_level", "class"),
        "sigma_era_result": ("chain", "draw"),
        "delta_era_result": (
            "chain",
            "draw",
            "era_level",
            "result_level",
            "class",
        ),
        **{
            f"delta_{column}": (
                "chain",
                "draw",
                f"{column}_level",
                "class",
            )
            for column in CONTEXT_COLUMNS
        },
    }
    for name, expected in expected_dims.items():
        if name not in posterior.data_vars:
            raise ValueError(f"posterior is missing variable {name}")
        if tuple(posterior[name].dims) != expected:
            raise ValueError(
                f"posterior variable {name} dimensions differ from encoding"
            )


def _prediction_cells(
    frame: pl.DataFrame,
) -> tuple[tuple[tuple[str, ...], ...], IntArray]:
    rows = tuple(
        tuple(str(value) for value in row)
        for row in frame.select(FEATURE_COLUMNS).rows()
    )
    unique_rows = tuple(dict.fromkeys(rows))
    lookup = {row: index for index, row in enumerate(unique_rows)}
    inverse = np.asarray([lookup[row] for row in rows], dtype=np.int64)
    return unique_rows, inverse


def iter_hierarchical_probability_draws(
    idata: az.InferenceData,
    frame: pl.DataFrame,
    encoding: HierarchicalEncoding,
    *,
    seed: int,
    chunk_size: int = PREDICTION_CHUNK_SIZE,
) -> Iterator[ProbabilityDrawChunk]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    _require_string_columns(frame, FEATURE_COLUMNS)
    unknown_results = sorted(
        set(frame[RESULT_COLUMN].to_list()) - set(encoding.result_domain)
    )
    if unknown_results:
        raise ValueError(
            f"prediction results outside fixed result_domain: {unknown_results}"
        )
    _validate_posterior_contract(idata, encoding)
    cells, _ = _prediction_cells(frame)
    class_count = len(encoding.class_labels)
    result_count = len(encoding.result_domain)
    alpha = _posterior_samples(idata, "alpha_class")
    beta_result = _posterior_samples(idata, "beta_result")
    delta_era = _posterior_samples(idata, "delta_era")
    delta_era_result = _posterior_samples(idata, "delta_era_result")
    sigma_era = _posterior_samples(idata, "sigma_era").reshape(-1, 1)
    sigma_era_result = _posterior_samples(idata, "sigma_era_result").reshape(-1, 1, 1)
    sample_count = alpha.shape[0]
    expected_shapes = {
        "alpha_class": (sample_count, class_count),
        "beta_result": (sample_count, result_count, class_count),
        "delta_era": (sample_count, len(encoding.era_levels), class_count),
        "delta_era_result": (
            sample_count,
            len(encoding.era_levels),
            result_count,
            class_count,
        ),
        "sigma_era": (sample_count, 1),
        "sigma_era_result": (sample_count, 1, 1),
    }
    actual = {
        "alpha_class": alpha.shape,
        "beta_result": beta_result.shape,
        "delta_era": delta_era.shape,
        "delta_era_result": delta_era_result.shape,
        "sigma_era": sigma_era.shape,
        "sigma_era_result": sigma_era_result.shape,
    }
    for name, shape in expected_shapes.items():
        if actual[name] != shape:
            raise ValueError(f"posterior {name} shape {actual[name]} != {shape}")
    era_lookup = {level: index for index, level in enumerate(encoding.era_levels)}
    result_lookup = {level: index for index, level in enumerate(encoding.result_domain)}
    context_lookup = {
        column: {level: index for index, level in enumerate(levels)}
        for column, levels in encoding.context_levels.items()
    }
    context_samples = {
        column: _posterior_samples(idata, f"delta_{column}")
        for column in CONTEXT_COLUMNS
    }
    for column, samples in context_samples.items():
        expected = (sample_count, len(encoding.context_levels[column]), class_count)
        if samples.shape != expected:
            raise ValueError(
                f"posterior delta_{column} shape {samples.shape} != {expected}"
            )
    for name, samples in {
        "alpha_class": alpha,
        "beta_result": beta_result,
        "delta_era": delta_era,
        "delta_era_result": delta_era_result,
        "sigma_era": sigma_era,
        "sigma_era_result": sigma_era_result,
        **{f"delta_{column}": value for column, value in context_samples.items()},
    }.items():
        if not np.isfinite(samples).all():
            raise ValueError(f"posterior {name} contains non-finite values")
    if np.any(sigma_era < 0.0) or np.any(sigma_era_result < 0.0):
        raise ValueError("posterior hierarchical scales must be non-negative")
    unseen_era: dict[str, tuple[FloatArray, FloatArray]] = {}
    unseen_context: dict[tuple[str, str], FloatArray] = {}
    for start in range(0, len(cells), chunk_size):
        stop = min(start + chunk_size, len(cells))
        eta = np.broadcast_to(
            alpha[:, None, :], (sample_count, stop - start, class_count)
        ).copy()
        for local_index, cell in enumerate(cells[start:stop]):
            era, result = cell[0], cell[1]
            result_code = result_lookup[result]
            eta[:, local_index, :] += beta_result[:, result_code, :]
            if era in era_lookup:
                era_code = era_lookup[era]
                eta[:, local_index, :] += delta_era[:, era_code, :]
                eta[:, local_index, :] += delta_era_result[:, era_code, result_code, :]
            else:
                if era not in unseen_era:
                    era_main = (
                        _class_zero_draws(
                            sample_count,
                            class_count,
                            seed=_seed(seed, "era", era),
                        )
                        * sigma_era
                    )
                    interaction = (
                        _result_class_zero_draws(
                            sample_count,
                            result_count,
                            class_count,
                            seed=_seed(seed, "era_result", era),
                        )
                        * sigma_era_result
                    )
                    unseen_era[era] = era_main, interaction
                era_main, interaction = unseen_era[era]
                eta[:, local_index, :] += era_main
                eta[:, local_index, :] += interaction[:, result_code, :]
            for position, column in enumerate(CONTEXT_COLUMNS, start=2):
                level = cell[position]
                if level in context_lookup[column]:
                    eta[:, local_index, :] += context_samples[column][
                        :, context_lookup[column][level], :
                    ]
                else:
                    key = column, level
                    if key not in unseen_context:
                        unseen_context[key] = 0.5 * _class_zero_draws(
                            sample_count,
                            class_count,
                            seed=_seed(seed, column, level),
                        )
                    eta[:, local_index, :] += unseen_context[key]
        probabilities = _softmax(eta)
        if not np.isfinite(probabilities).all() or not np.allclose(
            probabilities.sum(axis=-1), 1.0, atol=1e-12, rtol=0.0
        ):
            raise ValueError("streamed posterior probabilities are invalid")
        yield ProbabilityDrawChunk(
            cell_indices=np.arange(start, stop, dtype=np.int64),
            feature_rows=cells[start:stop],
            probabilities=probabilities,
        )


def predict_hierarchical(
    idata: az.InferenceData,
    frame: pl.DataFrame,
    encoding: HierarchicalEncoding,
    *,
    seed: int,
) -> PredictiveResult:
    _require_string_columns(frame, FEATURE_COLUMNS)
    if frame.is_empty():
        empty_float = np.zeros((0, len(encoding.class_labels)), dtype=np.float64)
        empty_int = np.zeros(0, dtype=np.int64)
        empty_bool = np.zeros(0, dtype=np.bool_)
        return PredictiveResult(
            empty_float, empty_int, empty_bool, empty_bool.copy(), empty_int.copy()
        )
    cells, inverse = _prediction_cells(frame)
    cell_means = np.empty((len(cells), len(encoding.class_labels)), dtype=np.float64)
    for chunk in iter_hierarchical_probability_draws(idata, frame, encoding, seed=seed):
        cell_means[chunk.cell_indices] = chunk.probabilities.mean(axis=0)
    era_set = set(encoding.era_levels)
    pair_set = set(encoding.observed_era_result_pairs)
    context_sets = {
        column: set(levels) for column, levels in encoding.context_levels.items()
    }
    known_era_cells = np.asarray([cell[0] in era_set for cell in cells], dtype=np.bool_)
    observed_pair_cells = np.asarray(
        [(cell[0], cell[1]) in pair_set for cell in cells], dtype=np.bool_
    )
    unseen_context_cells = np.asarray(
        [
            sum(
                cell[position] not in context_sets[column]
                for position, column in enumerate(CONTEXT_COLUMNS, start=2)
            )
            for cell in cells
        ],
        dtype=np.int64,
    )
    return PredictiveResult(
        probabilities=cell_means[inverse],
        event_cell_index=inverse,
        known_era=known_era_cells[inverse],
        observed_era_result_pair=observed_pair_cells[inverse],
        unseen_context_count=unseen_context_cells[inverse],
    )


def _probability_summary(probabilities: FloatArray) -> dict[str, object]:
    entropy = -np.sum(probabilities * np.log(probabilities), axis=-1)
    class_shares = probabilities.mean(axis=tuple(range(probabilities.ndim - 1)))
    return {
        "finite": bool(np.isfinite(probabilities).all()),
        "normalized": bool(
            np.allclose(probabilities.sum(axis=-1), 1.0, atol=1e-10, rtol=0.0)
        ),
        "strictly_positive": bool(np.all(probabilities > 0.0)),
        "class_shares": [float(value) for value in class_shares],
        "entropy_min": float(entropy.min()),
        "entropy_mean": float(entropy.mean()),
        "entropy_max": float(entropy.max()),
        "entropy_upper_bound": math.log(probabilities.shape[-1]),
    }


def _prior_check(data: HierarchicalTrainingData, *, seed: int) -> dict[str, object]:
    model = build_hierarchical_model(data, include_cell_prob=True)
    names = [
        "cell_prob",
        "alpha_class",
        "beta_result",
        "sigma_era",
        "sigma_era_result",
        *[f"delta_{column}" for column in CONTEXT_COLUMNS],
    ]
    with model:
        prior = pm.sample_prior_predictive(draws=100, random_seed=seed, var_names=names)
    group = getattr(prior, "prior")
    seen = np.asarray(group["cell_prob"].values, dtype=np.float64)
    alpha = np.asarray(group["alpha_class"].values, dtype=np.float64).reshape(
        100, len(data.encoding.class_labels)
    )
    beta = np.asarray(group["beta_result"].values, dtype=np.float64).reshape(
        100, len(data.encoding.result_domain), len(data.encoding.class_labels)
    )
    sigma_era = np.asarray(group["sigma_era"].values, dtype=np.float64).reshape(100, 1)
    sigma_interaction = np.asarray(
        group["sigma_era_result"].values, dtype=np.float64
    ).reshape(100, 1, 1)
    new_main = (
        _class_zero_draws(
            100, len(data.encoding.class_labels), seed=_seed(seed, "prior", "era")
        )
        * sigma_era
    )
    new_interaction = (
        _result_class_zero_draws(
            100,
            len(data.encoding.result_domain),
            len(data.encoding.class_labels),
            seed=_seed(seed, "prior", "era_result"),
        )
        * sigma_interaction
    )
    unseen_eta = (
        alpha[:, None, :]
        + beta[:, data.result_codes, :]
        + new_main[:, None, :]
        + new_interaction[:, data.result_codes, :]
    )
    for column in CONTEXT_COLUMNS:
        context = np.asarray(group[f"delta_{column}"].values, dtype=np.float64)
        context = context.reshape(
            100,
            len(data.encoding.context_levels[column]),
            len(data.encoding.class_labels),
        )
        unseen_eta += context[:, data.context_codes[column], :]
    unseen = _softmax(unseen_eta)
    seen_summary = _probability_summary(seen)
    unseen_summary = _probability_summary(unseen)
    passed = all(
        bool(summary[key])
        for summary in (seen_summary, unseen_summary)
        for key in ("finite", "normalized", "strictly_positive")
    )
    if not passed:
        raise ValueError("hierarchical prior predictive check failed")
    return {
        "draws": 100,
        "seen_training_cells": seen_summary,
        "unseen_era_population": unseen_summary,
        "passed": True,
    }


def fit_hierarchical(
    train: pl.DataFrame,
    *,
    output_dir: Path,
    class_labels: tuple[str, ...],
    result_domain: tuple[str, ...],
    smoke: bool,
    seed: int,
    sampling_config: SamplingConfig | None = None,
) -> tuple[az.InferenceData, HierarchicalEncoding]:
    data = prepare_hierarchical_data(
        train, class_labels=class_labels, result_domain=result_domain
    )
    if output_dir.exists() and any(output_dir.iterdir()):
        raise FileExistsError(
            f"hierarchical fit output already exists under {output_dir}"
        )
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "posterior": output_dir / "posterior.nc",
        "prior": output_dir / "prior_check.json",
        "config": output_dir / "fit_config.json",
    }
    prior = _prior_check(data, seed=seed)
    paths["prior"].write_text(
        json.dumps(prior, indent=2, sort_keys=True), encoding="utf-8"
    )
    sampler = sampling_config or _sampling_config(smoke=smoke, seed=seed)
    config = {
        "seed": seed,
        "unseen_prediction_seed_default": seed,
        "encoding": data.encoding.model_dump(mode="json"),
        "aggregated_cells": int(data.counts.shape[0]),
        "component_identification": "mean_zero_hierarchical_priors",
        "priors": {
            "alpha_class_sigma": 1.5,
            "beta_result_sigma": 0.5,
            "sigma_era_half_normal_sigma": 0.5,
            "sigma_era_result_half_normal_sigma": 0.5,
            "context_effect_sigma": 0.5,
        },
        "constraints": {
            "alpha_class": ["class"],
            "beta_result": ["result", "class"],
            "z_era": ["class_within_era"],
            "z_era_result": ["result_and_class_within_era"],
            "context_effects": ["class_within_level"],
        },
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
    }
    paths["config"].write_text(
        json.dumps(config, indent=2, sort_keys=True), encoding="utf-8"
    )
    model = build_hierarchical_model(data)
    LOGGER.info(
        "fit hierarchical geometry rows=%d cells=%d eras=%d results=%d classes=%d",
        train.height,
        data.counts.shape[0],
        len(data.encoding.era_levels),
        len(data.encoding.result_domain),
        len(data.encoding.class_labels),
    )
    idata = sample_model(
        model,
        sampler,
        output_path=paths["posterior"],
        progress_log_path=output_dir / "sampling.log",
    )
    return idata, data.encoding
