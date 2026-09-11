from __future__ import annotations

from typing import cast

import numpy as np
import numpy.typing as npt
import polars as pl

FloatArray = npt.NDArray[np.float64]
IntArray = npt.NDArray[np.int64]
BoolArray = npt.NDArray[np.bool_]
REQUIRED_COLUMNS = (
    "event_key",
    "game_id",
    "season",
    "scorer",
    "mask_pattern",
    "target_class",
    "model_probability",
    "baseline_probability",
)


def _probability_matrix(
    frame: pl.DataFrame, column: str, *, class_count: int
) -> FloatArray:
    try:
        matrix = np.asarray(frame.get_column(column).to_list(), dtype=np.float64)
    except ValueError as exc:
        raise ValueError(
            f"{column} must contain equal-length probability vectors"
        ) from exc
    if matrix.shape != (frame.height, class_count):
        raise ValueError(
            f"{column} shape {matrix.shape} does not match "
            f"({frame.height}, {class_count})"
        )
    if not np.isfinite(matrix).all():
        raise ValueError(f"{column} contains non-finite probabilities")
    if (matrix < 0.0).any():
        raise ValueError(f"{column} contains negative probabilities")
    sums = matrix.sum(axis=1)
    if not np.allclose(sums, 1.0, rtol=0.0, atol=1e-6):
        raise ValueError(f"{column} probability vectors are not normalized")
    return matrix


def _validate_frame(
    frame: pl.DataFrame, labels: tuple[str, ...]
) -> tuple[pl.DataFrame, IntArray, dict[str, FloatArray]]:
    missing = sorted(set(REQUIRED_COLUMNS) - set(frame.columns))
    if missing:
        raise ValueError(f"stress evaluation frame missing columns: {missing}")
    if frame.is_empty():
        raise ValueError("stress evaluation frame is empty")
    if not labels or len(set(labels)) != len(labels):
        raise ValueError("labels must be non-empty and unique")
    for column in REQUIRED_COLUMNS[:-2]:
        if frame.get_column(column).null_count() > 0:
            raise ValueError(f"{column} contains null values")
    if frame.get_column("event_key").n_unique() != frame.height:
        raise ValueError("event_key must be unique")
    ordered = frame.sort("event_key")
    label_index = {label: index for index, label in enumerate(labels)}
    raw_labels = [str(value) for value in ordered.get_column("target_class")]
    unknown = sorted(set(raw_labels) - set(label_index))
    if unknown:
        raise ValueError(f"target_class contains labels outside labels: {unknown}")
    encoded = np.asarray([label_index[label] for label in raw_labels], dtype=np.int64)
    probabilities = {
        "model": _probability_matrix(
            ordered, "model_probability", class_count=len(labels)
        ),
        "baseline": _probability_matrix(
            ordered, "baseline_probability", class_count=len(labels)
        ),
    }
    return ordered, encoded, probabilities


def _event_losses(
    probabilities: FloatArray, labels: IntArray
) -> tuple[FloatArray, FloatArray]:
    rows = np.arange(labels.size)
    log_loss = -np.log(np.clip(probabilities[rows, labels], 1e-15, 1.0))
    outcomes = np.zeros_like(probabilities)
    outcomes[rows, labels] = 1.0
    return log_loss, np.square(probabilities - outcomes).sum(axis=1)


def _reliability(
    probabilities: FloatArray,
    encoded_labels: IntArray,
    *,
    predictor: str,
    slice_name: str,
    labels: tuple[str, ...],
) -> tuple[float, list[dict[str, object]]]:
    rows: list[dict[str, object]] = []
    total_gap = 0.0
    denominator = probabilities.shape[0] * probabilities.shape[1]
    for class_index, label in enumerate(labels):
        predicted = probabilities[:, class_index]
        observed = (encoded_labels == class_index).astype(np.float64)
        bins = np.minimum((predicted * 15).astype(np.int64), 14)
        counts = np.bincount(bins, minlength=15)
        predicted_sums = np.bincount(bins, weights=predicted, minlength=15)
        observed_sums = np.bincount(bins, weights=observed, minlength=15)
        for bin_index in np.flatnonzero(counts):
            count = int(counts[bin_index])
            mean_probability = float(predicted_sums[bin_index] / count)
            observed_rate = float(observed_sums[bin_index] / count)
            absolute_gap = abs(mean_probability - observed_rate)
            total_gap += count * absolute_gap
            rows.append(
                {
                    "slice": slice_name,
                    "predictor": predictor,
                    "class_label": label,
                    "bin": int(bin_index),
                    "count": count,
                    "mean_probability": mean_probability,
                    "observed_rate": observed_rate,
                    "absolute_gap": absolute_gap,
                }
            )
    return total_gap / denominator, rows


def _predictor_metrics(
    probabilities: FloatArray,
    encoded_labels: IntArray,
    *,
    predictor: str,
    slice_name: str,
    labels: tuple[str, ...],
) -> tuple[dict[str, object], list[dict[str, object]]]:
    log_loss, brier = _event_losses(probabilities, encoded_labels)
    ece, reliability = _reliability(
        probabilities,
        encoded_labels,
        predictor=predictor,
        slice_name=slice_name,
        labels=labels,
    )
    observed_counts = np.bincount(encoded_labels, minlength=len(labels)).astype(
        np.int64
    )
    expected_counts = probabilities.sum(axis=0)
    row_count = encoded_labels.size
    expected_shares = expected_counts / row_count
    observed_shares = observed_counts / row_count
    share_bias = expected_shares - observed_shares
    return (
        {
            "log_loss": float(log_loss.mean()),
            "brier": float(brier.mean()),
            "classwise_ece_15_bin": ece,
            "expected_class_counts": {
                label: float(expected_counts[index])
                for index, label in enumerate(labels)
            },
            "observed_class_counts": {
                label: int(observed_counts[index]) for index, label in enumerate(labels)
            },
            "expected_class_shares": {
                label: float(expected_shares[index])
                for index, label in enumerate(labels)
            },
            "observed_class_shares": {
                label: float(observed_shares[index])
                for index, label in enumerate(labels)
            },
            "class_share_bias": {
                label: float(share_bias[index]) for index, label in enumerate(labels)
            },
            "max_absolute_class_share_bias": float(np.max(np.abs(share_bias))),
            "total_expected_count": float(expected_counts.sum()),
            "total_observed_count": int(observed_counts.sum()),
        },
        reliability,
    )


def _cluster_bootstrap(
    games: npt.NDArray[np.str_],
    encoded_labels: IntArray,
    probabilities: dict[str, FloatArray],
    *,
    labels: tuple[str, ...],
    repetitions: int,
    rng: np.random.Generator,
) -> dict[str, object]:
    unique_games, inverse = np.unique(games, return_inverse=True)
    game_count = unique_games.size
    rows_per_game = np.bincount(inverse).astype(np.float64)
    losses = {
        predictor: _event_losses(matrix, encoded_labels)
        for predictor, matrix in probabilities.items()
    }
    game_losses = {
        predictor: (
            np.bincount(inverse, weights=loss[0]),
            np.bincount(inverse, weights=loss[1]),
        )
        for predictor, loss in losses.items()
    }
    outcomes = np.zeros_like(probabilities["model"])
    outcomes[np.arange(encoded_labels.size), encoded_labels] = 1.0
    game_bias = {
        predictor: np.stack(
            [
                np.bincount(
                    inverse,
                    weights=matrix[:, class_index] - outcomes[:, class_index],
                    minlength=game_count,
                )
                for class_index in range(len(labels))
            ],
            axis=1,
        )
        for predictor, matrix in probabilities.items()
    }
    log_gains = np.empty(repetitions, dtype=np.float64)
    brier_gains = np.empty(repetitions, dtype=np.float64)
    bias_draws = {
        predictor: np.empty((repetitions, len(labels)), dtype=np.float64)
        for predictor in probabilities
    }
    model_log, model_brier = game_losses["model"]
    baseline_log, baseline_brier = game_losses["baseline"]
    for repetition in range(repetitions):
        sampled = rng.integers(0, game_count, size=game_count)
        denominator = rows_per_game[sampled].sum()
        log_gains[repetition] = (
            baseline_log[sampled].sum() - model_log[sampled].sum()
        ) / denominator
        brier_gains[repetition] = (
            baseline_brier[sampled].sum() - model_brier[sampled].sum()
        ) / denominator
        for predictor, aggregated in game_bias.items():
            bias_draws[predictor][repetition] = aggregated[sampled].sum(axis=0) / (
                denominator
            )
    return {
        "cluster_unit": "game_id",
        "game_count": int(game_count),
        "repetitions": repetitions,
        "fits_during_bootstrap": "fixed",
        "positive_gain_favors_model": True,
        "log_loss_gain_ci95": [
            float(np.quantile(log_gains, 0.025)),
            float(np.quantile(log_gains, 0.975)),
        ],
        "brier_gain_ci95": [
            float(np.quantile(brier_gains, 0.025)),
            float(np.quantile(brier_gains, 0.975)),
        ],
        "class_share_bias_ci95": {
            predictor: {
                label: [
                    float(np.quantile(draws[:, index], 0.025)),
                    float(np.quantile(draws[:, index], 0.975)),
                ]
                for index, label in enumerate(labels)
            }
            for predictor, draws in bias_draws.items()
        },
    }


def _slice_result(
    mask: BoolArray,
    ordered: pl.DataFrame,
    encoded_labels: IntArray,
    probabilities: dict[str, FloatArray],
    *,
    slice_name: str,
    labels: tuple[str, ...],
    repetitions: int,
    rng: np.random.Generator,
    thresholded: bool,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    row_count = int(mask.sum())
    games = np.asarray(
        ordered.get_column("game_id").cast(pl.String).to_list(), dtype=np.str_
    )[mask]
    game_count = int(np.unique(games).size)
    supported = row_count > 0 and (
        not thresholded or (row_count >= 500 and game_count >= 50)
    )
    if not supported:
        reason = (
            "empty"
            if row_count == 0
            else "slice requires at least 500 events and 50 games"
        )
        return (
            {
                "row_count": row_count,
                "game_count": game_count,
                "supported": False,
                "unsupported_reason": reason,
                "predictors": None,
                "paired_game_bootstrap": None,
            },
            [],
        )
    selected_labels = encoded_labels[mask]
    selected_probabilities = {
        predictor: matrix[mask] for predictor, matrix in probabilities.items()
    }
    predictor_metrics: dict[str, object] = {}
    reliability: list[dict[str, object]] = []
    for predictor, matrix in selected_probabilities.items():
        metrics, bins = _predictor_metrics(
            matrix,
            selected_labels,
            predictor=predictor,
            slice_name=slice_name,
            labels=labels,
        )
        predictor_metrics[predictor] = metrics
        reliability.extend(bins)
    return (
        {
            "row_count": row_count,
            "game_count": game_count,
            "supported": True,
            "unsupported_reason": None,
            "predictors": predictor_metrics,
            "paired_game_bootstrap": _cluster_bootstrap(
                games,
                selected_labels,
                selected_probabilities,
                labels=labels,
                repetitions=repetitions,
                rng=rng,
            ),
        },
        reliability,
    )


def evaluate_stress(
    frame: pl.DataFrame,
    labels: tuple[str, ...],
    *,
    repetitions: int = 500,
    seed: int = 20260911,
) -> dict[str, object]:
    if repetitions <= 0:
        raise ValueError("repetitions must be positive")
    ordered, encoded_labels, probabilities = _validate_frame(frame, labels)
    seasons = ordered.get_column("season").cast(pl.Int64).to_numpy()
    masks: dict[str, tuple[BoolArray, bool]] = {
        "overall": (np.ones(ordered.height, dtype=np.bool_), False),
        "pre_1988": (seasons < 1988, False),
    }
    for decade in sorted(set((seasons // 10 * 10).tolist())):
        masks[f"decade_{decade}"] = (seasons // 10 * 10 == decade, True)
    patterns = np.asarray(
        ordered.get_column("mask_pattern").cast(pl.String).to_list(), dtype=np.str_
    )
    for pattern in sorted(set(patterns.tolist())):
        masks[f"mask_pattern_{pattern}"] = (patterns == pattern, True)
    if "model_context_supported" in ordered.columns:
        support_column = ordered.get_column("model_context_supported")
        if support_column.null_count() or support_column.dtype != pl.Boolean:
            raise ValueError("model_context_supported must be non-null Boolean")
        supported_context = support_column.to_numpy()
        masks["context_supported"] = (supported_context, True)
        masks["context_unsupported"] = (~supported_context, True)
    rng = np.random.default_rng(seed)
    slices: dict[str, object] = {}
    reliability_bins: list[dict[str, object]] = []
    for slice_name, (mask, thresholded) in masks.items():
        result, bins = _slice_result(
            mask,
            ordered,
            encoded_labels,
            probabilities,
            slice_name=slice_name,
            labels=labels,
            repetitions=repetitions,
            rng=rng,
            thresholded=thresholded,
        )
        slices[slice_name] = result
        reliability_bins.extend(bins)
    overall = cast(dict[str, object], slices["overall"])
    predictors = cast(dict[str, dict[str, object]], overall["predictors"])
    bootstrap = cast(dict[str, object], overall["paired_game_bootstrap"])
    model = predictors["model"]
    log_ci = cast(list[float], bootstrap["log_loss_gain_ci95"])
    brier_ci = cast(list[float], bootstrap["brier_gain_ci95"])
    model_ece = cast(float, model["classwise_ece_15_bin"])
    model_share_bias = cast(float, model["max_absolute_class_share_bias"])
    flags = {
        "overall_log_loss_gain_ci_lower_above_zero": float(log_ci[0]) > 0.0,
        "overall_brier_gain_ci_lower_above_zero": float(brier_ci[0]) > 0.0,
        "overall_model_ece_at_most_0_05": model_ece <= 0.05,
        "overall_model_max_class_share_bias_at_most_0_02": (model_share_bias <= 0.02),
    }
    return {
        "status": "historical_stress_evaluation",
        "class_labels": list(labels),
        "row_count": ordered.height,
        "game_count": ordered.get_column("game_id").n_unique(),
        "scorer_count": ordered.get_column("scorer").n_unique(),
        "mask_pattern_counts": {
            str(pattern): int(count)
            for pattern, count in ordered.group_by("mask_pattern")
            .len(name="count")
            .sort("mask_pattern")
            .iter_rows()
        },
        "seed": seed,
        "bootstrap_repetitions": repetitions,
        "decade_support_threshold": {"games": 50, "events": 500},
        "slices": slices,
        "reliability_bins": reliability_bins,
        "decision_flags": {**flags, "passed": all(flags.values())},
    }
