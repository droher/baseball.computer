from __future__ import annotations

from typing import cast

import numpy as np
import polars as pl
from numpy.typing import NDArray


FloatArray = NDArray[np.float64]
IntArray = NDArray[np.int64]
BoolArray = NDArray[np.bool_]
PREDICTOR_COLUMNS: dict[str, str] = {
    "model": "model_probability",
    "budget_marginal": "budget_marginal_probability",
    "budget_contextual": "budget_contextual_probability",
    "full_contextual": "full_contextual_probability",
}
REQUIRED_COLUMNS = (
    "event_key",
    "game_id",
    "season",
    "target_class",
    *PREDICTOR_COLUMNS.values(),
)


def _probability_matrix(
    frame: pl.DataFrame, column: str, *, class_count: int
) -> FloatArray:
    values = frame.get_column(column).to_list()
    try:
        matrix = np.asarray(values, dtype=np.float64)
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
        deviation = float(np.max(np.abs(sums - 1.0)))
        raise ValueError(
            f"{column} probability vectors are not normalized; "
            f"maximum deviation={deviation}"
        )
    return matrix


def _validate_frame(
    frame: pl.DataFrame, class_labels: tuple[str, ...]
) -> tuple[IntArray, dict[str, FloatArray]]:
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"evaluation frame missing required columns: {missing}")
    if frame.is_empty():
        raise ValueError("evaluation frame is empty")
    if not class_labels or len(set(class_labels)) != len(class_labels):
        raise ValueError("class_labels must be non-empty and unique")
    for column in ("event_key", "game_id", "season", "target_class"):
        if frame.get_column(column).null_count() != 0:
            raise ValueError(f"{column} contains null values")
    event_keys = frame.get_column("event_key")
    if event_keys.n_unique() != frame.height:
        raise ValueError("event_key must be unique")
    class_index = {label: index for index, label in enumerate(class_labels)}
    labels = frame.get_column("target_class").cast(pl.String).to_list()
    unknown = sorted({label for label in labels if label not in class_index})
    if unknown:
        raise ValueError(
            f"target_class contains labels outside class_labels: {unknown}"
        )
    label_index = np.asarray([class_index[label] for label in labels], dtype=np.int64)
    probabilities = {
        name: _probability_matrix(frame, column, class_count=len(class_labels))
        for name, column in PREDICTOR_COLUMNS.items()
    }
    return label_index, probabilities


def _event_scores(
    probabilities: FloatArray, labels: IntArray
) -> tuple[FloatArray, FloatArray]:
    rows = np.arange(labels.size)
    log_loss = -np.log(np.clip(probabilities[rows, labels], 1e-15, 1.0))
    outcomes = np.zeros_like(probabilities)
    outcomes[rows, labels] = 1.0
    brier = np.square(probabilities - outcomes).sum(axis=1)
    return log_loss, brier


def _reliability(
    probabilities: FloatArray,
    labels: IntArray,
    *,
    predictor: str,
    slice_name: str,
    class_labels: tuple[str, ...],
) -> tuple[float, list[dict[str, object]]]:
    rows: list[dict[str, object]] = []
    weighted_gap = 0.0
    total = probabilities.shape[0] * probabilities.shape[1]
    for class_index, class_label in enumerate(class_labels):
        predicted = probabilities[:, class_index]
        observed = (labels == class_index).astype(np.float64)
        bins = np.minimum((predicted * 15).astype(np.int64), 14)
        counts = np.bincount(bins, minlength=15)
        probability_sums = np.bincount(bins, weights=predicted, minlength=15)
        observed_sums = np.bincount(bins, weights=observed, minlength=15)
        for bin_index in np.flatnonzero(counts):
            count = int(counts[bin_index])
            mean_probability = float(probability_sums[bin_index] / count)
            observed_rate = float(observed_sums[bin_index] / count)
            gap = abs(mean_probability - observed_rate)
            weighted_gap += count * gap
            rows.append(
                {
                    "predictor": predictor,
                    "slice": slice_name,
                    "class_label": class_label,
                    "bin": int(bin_index),
                    "count": count,
                    "mean_probability": mean_probability,
                    "observed_rate": observed_rate,
                    "absolute_gap": gap,
                }
            )
    return weighted_gap / total, rows


def _slice_metrics(
    mask: BoolArray,
    labels: IntArray,
    probabilities: dict[str, FloatArray],
    *,
    slice_name: str,
    class_labels: tuple[str, ...],
) -> tuple[dict[str, dict[str, float | int | None]], list[dict[str, object]]]:
    count = int(mask.sum())
    metrics: dict[str, dict[str, float | int | None]] = {}
    reliability_rows: list[dict[str, object]] = []
    for predictor, matrix in probabilities.items():
        if count == 0:
            metrics[predictor] = {
                "row_count": 0,
                "log_loss": None,
                "brier": None,
                "classwise_ece_15_bin": None,
            }
            continue
        selected_labels = labels[mask]
        selected_probabilities = matrix[mask]
        log_loss, brier = _event_scores(selected_probabilities, selected_labels)
        ece, rows = _reliability(
            selected_probabilities,
            selected_labels,
            predictor=predictor,
            slice_name=slice_name,
            class_labels=class_labels,
        )
        metrics[predictor] = {
            "row_count": count,
            "log_loss": float(log_loss.mean()),
            "brier": float(brier.mean()),
            "classwise_ece_15_bin": ece,
        }
        reliability_rows.extend(rows)
    return metrics, reliability_rows


def _cluster_bootstrap(
    mask: BoolArray,
    games: NDArray[np.str_],
    event_scores: dict[str, tuple[FloatArray, FloatArray]],
    *,
    slice_name: str,
    repetitions: int,
    rng: np.random.Generator,
) -> list[dict[str, object]]:
    selected_games = games[mask]
    if selected_games.size == 0:
        return []
    _, game_index = np.unique(selected_games, return_inverse=True)
    game_index = game_index.astype(np.int64, copy=False)
    game_count = int(game_index.max()) + 1
    game_rows = np.bincount(game_index, minlength=game_count).astype(np.float64)
    aggregated: dict[str, tuple[FloatArray, FloatArray]] = {}
    for predictor, (log_loss, brier) in event_scores.items():
        aggregated[predictor] = (
            np.bincount(
                game_index, weights=log_loss[mask], minlength=game_count
            ).astype(np.float64),
            np.bincount(game_index, weights=brier[mask], minlength=game_count).astype(
                np.float64
            ),
        )
    output: list[dict[str, object]] = []
    model_log, model_brier = aggregated["model"]
    for comparator in (
        "budget_marginal",
        "budget_contextual",
        "full_contextual",
    ):
        comparator_log, comparator_brier = aggregated[comparator]
        log_gains = np.empty(repetitions, dtype=np.float64)
        brier_gains = np.empty(repetitions, dtype=np.float64)
        for repetition in range(repetitions):
            sampled_games = rng.integers(0, game_count, size=game_count)
            denominator = game_rows[sampled_games].sum()
            log_gains[repetition] = (
                comparator_log[sampled_games].sum() - model_log[sampled_games].sum()
            ) / denominator
            brier_gains[repetition] = (
                comparator_brier[sampled_games].sum() - model_brier[sampled_games].sum()
            ) / denominator
        output.append(
            {
                "slice": slice_name,
                "model": "model",
                "comparator": comparator,
                "cluster_unit": "game_id",
                "game_count": game_count,
                "repetitions": repetitions,
                "train_fit_during_bootstrap": "fixed",
                "log_loss_gain_ci95": [
                    float(np.quantile(log_gains, 0.025)),
                    float(np.quantile(log_gains, 0.975)),
                ],
                "brier_gain_ci95": [
                    float(np.quantile(brier_gains, 0.025)),
                    float(np.quantile(brier_gains, 0.975)),
                ],
            }
        )
    return output


def _metric_value(
    metrics: dict[str, dict[str, dict[str, float | int | None]]],
    slice_name: str,
    predictor: str,
    metric: str,
) -> float | None:
    value = metrics[slice_name][predictor][metric]
    return float(value) if value is not None else None


def evaluate_reference(
    frame: pl.DataFrame,
    class_labels: tuple[str, ...],
    *,
    repetitions: int = 500,
    seed: int = 20260911,
) -> dict[str, object]:
    if repetitions <= 0:
        raise ValueError("repetitions must be positive")
    labels, probabilities = _validate_frame(frame, class_labels)
    seasons = frame.get_column("season").cast(pl.Int64).to_numpy()
    games = np.asarray(
        frame.get_column("game_id").cast(pl.String).to_list(), dtype=np.str_
    )
    masks: dict[str, BoolArray] = {
        "overall": np.ones(frame.height, dtype=np.bool_),
        "pre_1988_observed_only": seasons < 1988,
    }
    scores = {
        predictor: _event_scores(matrix, labels)
        for predictor, matrix in probabilities.items()
    }
    metrics: dict[str, dict[str, dict[str, float | int | None]]] = {}
    reliability_rows: list[dict[str, object]] = []
    bootstrap_rows: list[dict[str, object]] = []
    rng = np.random.default_rng(seed)
    for slice_name, mask in masks.items():
        slice_result, slice_reliability = _slice_metrics(
            mask,
            labels,
            probabilities,
            slice_name=slice_name,
            class_labels=class_labels,
        )
        metrics[slice_name] = slice_result
        reliability_rows.extend(slice_reliability)
        bootstrap_rows.extend(
            _cluster_bootstrap(
                mask,
                games,
                scores,
                slice_name=slice_name,
                repetitions=repetitions,
                rng=rng,
            )
        )
    contextual_overall = next(
        row
        for row in bootstrap_rows
        if row["slice"] == "overall" and row["comparator"] == "budget_contextual"
    )
    log_ci = contextual_overall["log_loss_gain_ci95"]
    brier_ci = contextual_overall["brier_gain_ci95"]
    if not isinstance(log_ci, list) or not isinstance(brier_ci, list):
        raise RuntimeError("bootstrap intervals have an invalid shape")
    model_ece = _metric_value(metrics, "overall", "model", "classwise_ece_15_bin")
    model_pre_log = _metric_value(
        metrics, "pre_1988_observed_only", "model", "log_loss"
    )
    contextual_pre_log = _metric_value(
        metrics, "pre_1988_observed_only", "budget_contextual", "log_loss"
    )
    flags = {
        "log_loss_improvement_ci_lower_above_zero_vs_budget_contextual": (
            float(cast(float, log_ci[0])) > 0.0
        ),
        "brier_improvement_ci_lower_above_zero_vs_budget_contextual": (
            float(cast(float, brier_ci[0])) > 0.0
        ),
        "model_classwise_ece_at_most_0_05": (
            model_ece is not None and model_ece <= 0.05
        ),
        "no_pre_1988_observed_only_log_loss_regression": (
            model_pre_log is not None
            and contextual_pre_log is not None
            and model_pre_log <= contextual_pre_log
        ),
    }
    return {
        "status": "development_evidence",
        "class_labels": list(class_labels),
        "row_count": frame.height,
        "game_count": frame.get_column("game_id").n_unique(),
        "seed": seed,
        "bootstrap_repetitions": repetitions,
        "bootstrap_policy": "resample held-out games with replacement while all TRAIN fits remain fixed",
        "historical_slice_policy": "pre-1988 directly observed labels only; no historical transport or MNAR identification claim",
        "metrics": metrics,
        "paired_game_bootstrap": bootstrap_rows,
        "reliability_bins": reliability_rows,
        "decision_flags": {**flags, "passed": all(flags.values())},
    }
