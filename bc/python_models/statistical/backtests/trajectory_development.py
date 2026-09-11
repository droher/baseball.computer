from __future__ import annotations

import argparse
import hashlib
import json
import logging
import platform
from collections.abc import Sequence
from importlib.metadata import version
from pathlib import Path
from typing import NamedTuple, cast

import numpy as np
import numpy.typing as npt
import polars as pl
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import OneHotEncoder

_log = logging.getLogger(__name__)

FEATURE_COLUMNS: tuple[str, ...] = (
    "era",
    "result_family",
    "base_state_start",
    "outs_start",
    "alignment_regime",
    "batter_hand",
)
OTHER_CONTEXT_COLUMNS: tuple[str, ...] = (
    "base_state_start",
    "outs_start",
    "alignment_regime",
    "batter_hand",
)
TARGET_COLUMN = "target_class"
SPLIT_SEED = 20260911
SPLIT_SALT = "trajectory-development-v1"
INNER_TRAIN_PERCENT = 80
REGULARIZATION_C = 1.0
MAX_ITERATIONS = 2000
TOLERANCE = 1e-6

FloatArray = npt.NDArray[np.float64]
IntArray = npt.NDArray[np.int64]


class FittedLogit(NamedTuple):
    encoder: OneHotEncoder
    model: LogisticRegression
    columns: tuple[str, ...]
    levels: dict[str, tuple[str, ...]]


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _game_fold(game_id: str, *, seed: int) -> int:
    payload = f"{game_id}:{SPLIT_SALT}:{seed}".encode()
    return int.from_bytes(hashlib.sha256(payload).digest()[:8], "big") % 100


def split_games(
    frame: pl.DataFrame, *, seed: int = SPLIT_SEED
) -> tuple[pl.DataFrame, pl.DataFrame]:
    if "game_id" not in frame.columns or frame.get_column("game_id").null_count() > 0:
        raise ValueError("game_id must be present and non-null")
    assignments = {
        str(game_id): _game_fold(str(game_id), seed=seed) < INNER_TRAIN_PERCENT
        for game_id in frame.get_column("game_id").unique().to_list()
    }
    is_train = pl.col("game_id").replace_strict(assignments, return_dtype=pl.Boolean)
    inner_train = frame.filter(is_train)
    development = frame.filter(~is_train)
    if inner_train.is_empty() or development.is_empty():
        raise ValueError("internal game split produced an empty partition")
    overlap = set(inner_train.get_column("game_id")) & set(
        development.get_column("game_id")
    )
    if overlap:
        raise AssertionError("internal training and development games overlap")
    return inner_train, development


def _feature_frame(frame: pl.DataFrame, columns: tuple[str, ...]) -> np.ndarray:
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise ValueError(f"missing feature columns: {missing}")
    for column in columns:
        if frame.get_column(column).null_count() > 0:
            raise ValueError(f"feature column {column!r} contains null values")
    return frame.select(columns).to_numpy().astype(str)


def fit_logit(
    train: pl.DataFrame,
    *,
    columns: tuple[str, ...],
    seed: int = SPLIT_SEED,
) -> FittedLogit:
    x = _feature_frame(train, columns)
    y = train.get_column(TARGET_COLUMN).cast(pl.String).to_numpy()
    levels = {
        column: tuple(
            sorted(str(value) for value in train.get_column(column).unique().to_list())
        )
        for column in columns
    }
    encoder = OneHotEncoder(
        handle_unknown="ignore",
        sparse_output=True,
        dtype=np.float64,
    )
    encoded = encoder.fit_transform(x)
    model = LogisticRegression(
        C=REGULARIZATION_C,
        solver="lbfgs",
        max_iter=MAX_ITERATIONS,
        tol=TOLERANCE,
        random_state=seed,
    )
    model.fit(encoded, y)
    if int(np.max(model.n_iter_)) >= MAX_ITERATIONS:
        raise RuntimeError("logistic regression reached its iteration limit")
    return FittedLogit(encoder=encoder, model=model, columns=columns, levels=levels)


def predict_logit(
    fitted: FittedLogit, frame: pl.DataFrame, class_labels: tuple[str, ...]
) -> FloatArray:
    encoded = fitted.encoder.transform(_feature_frame(frame, fitted.columns))
    raw = np.asarray(fitted.model.predict_proba(encoded), dtype=np.float64)
    model_labels = tuple(str(value) for value in fitted.model.classes_.tolist())
    if set(model_labels) != set(class_labels):
        raise ValueError("fitted model classes do not match inner-training vocabulary")
    order = [model_labels.index(label) for label in class_labels]
    return raw[:, order]


def baseline_probabilities(
    train: pl.DataFrame,
    frame: pl.DataFrame,
    class_labels: tuple[str, ...],
    *,
    conditional: bool,
) -> FloatArray:
    label_index = {label: index for index, label in enumerate(class_labels)}
    counts: dict[tuple[str, str], FloatArray] = {}
    grouped = train.group_by("era", "result_family", TARGET_COLUMN).len(name="n")
    for era, result, label, count in grouped.iter_rows():
        key = (str(era), str(result)) if conditional else ("", "")
        if key not in counts:
            counts[key] = np.ones(len(class_labels), dtype=np.float64)
        counts[key][label_index[str(label)]] += int(count)
    probabilities = {key: value / value.sum() for key, value in counts.items()}
    uniform = np.full(len(class_labels), 1.0 / len(class_labels), dtype=np.float64)
    if not conditional:
        return np.tile(probabilities[("", "")], (frame.height, 1))
    return np.asarray(
        [
            probabilities.get((str(era), str(result)), uniform)
            for era, result in frame.select("era", "result_family").iter_rows()
        ],
        dtype=np.float64,
    )


def _classwise_ece(probabilities: FloatArray, truth: IntArray) -> float:
    weighted_gap = 0.0
    total = probabilities.shape[0] * probabilities.shape[1]
    for class_index in range(probabilities.shape[1]):
        predicted = probabilities[:, class_index]
        observed = (truth == class_index).astype(np.float64)
        bins = np.minimum((predicted * 15).astype(np.int64), 14)
        counts = np.bincount(bins, minlength=15)
        predicted_sums = np.bincount(bins, weights=predicted, minlength=15)
        observed_sums = np.bincount(bins, weights=observed, minlength=15)
        populated = counts > 0
        weighted_gap += float(
            np.sum(
                np.abs(
                    predicted_sums[populated] / counts[populated]
                    - observed_sums[populated] / counts[populated]
                )
                * counts[populated]
            )
        )
    return weighted_gap / total


def score_predictions(probabilities: FloatArray, truth: IntArray) -> dict[str, float]:
    if probabilities.shape[0] != truth.shape[0] or probabilities.ndim != 2:
        raise ValueError("probabilities and truth do not align")
    if not np.isfinite(probabilities).all() or not np.allclose(
        probabilities.sum(axis=1), 1.0, atol=1e-10, rtol=0.0
    ):
        raise ValueError("probabilities are invalid")
    selected = np.clip(probabilities[np.arange(truth.size), truth], 1e-15, 1.0)
    outcomes = np.zeros_like(probabilities)
    outcomes[np.arange(truth.size), truth] = 1.0
    return {
        "log_loss": float(-np.log(selected).mean()),
        "brier": float(np.square(probabilities - outcomes).sum(axis=1).mean()),
        "classwise_ece_15_bin": _classwise_ece(probabilities, truth),
    }


def _slice_rows(
    development: pl.DataFrame,
    truth: IntArray,
    predictions: dict[str, FloatArray],
    *,
    column: str,
) -> list[dict[str, object]]:
    values = development.get_column(column).cast(pl.String).to_numpy()
    output: list[dict[str, object]] = []
    for value in sorted(str(item) for item in np.unique(values).tolist()):
        mask = values == value
        metrics = {
            name: score_predictions(probabilities[mask], truth[mask])
            for name, probabilities in predictions.items()
        }
        output.append(
            {
                "slice": column,
                "value": value,
                "rows": int(mask.sum()),
                "metrics": metrics,
                "interaction_log_loss_gain_over_additive": metrics["additive"][
                    "log_loss"
                ]
                - metrics["era_result_interaction"]["log_loss"],
                "interaction_brier_gain_over_additive": metrics["additive"]["brier"]
                - metrics["era_result_interaction"]["brier"],
            }
        )
    return output


def _game_digest(frame: pl.DataFrame) -> str:
    digest = hashlib.sha256()
    for game_id in sorted(str(value) for value in frame.get_column("game_id").unique()):
        encoded = game_id.encode()
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def run_development(
    source_path: Path,
    *,
    output_json: Path,
    output_markdown: Path,
    seed: int = SPLIT_SEED,
) -> dict[str, object]:
    frame = pl.read_parquet(source_path)
    inner_train, development = split_games(frame, seed=seed)
    class_labels = tuple(
        sorted(
            str(value)
            for value in inner_train.get_column(TARGET_COLUMN).unique().to_list()
        )
    )
    unseen_truth = set(development.get_column(TARGET_COLUMN)) - set(class_labels)
    if unseen_truth:
        raise ValueError(f"development contains unseen target classes: {unseen_truth}")
    config = {
        "status": "configured_before_scoring",
        "source_path": str(source_path),
        "source_sha256": _file_digest(source_path),
        "seed": seed,
        "split_salt": SPLIT_SALT,
        "inner_train_percent": INNER_TRAIN_PERCENT,
        "regularization_c": REGULARIZATION_C,
        "solver": "lbfgs",
        "max_iterations": MAX_ITERATIONS,
        "optimizer_repair": "Raised max_iter from 500 after the additive fit reached the iteration limit before development scoring; objective and all data-dependent choices unchanged.",
        "tolerance": TOLERANCE,
        "feature_columns": FEATURE_COLUMNS,
        "interaction_columns": ("era_result", *OTHER_CONTEXT_COLUMNS),
        "inner_train_rows": inner_train.height,
        "development_rows": development.height,
        "inner_train_games_sha256": _game_digest(inner_train),
        "development_games_sha256": _game_digest(development),
        "class_labels": class_labels,
    }
    config_path = output_json.with_name(
        f"{output_json.stem}-config-maxiter{MAX_ITERATIONS}.json"
    )
    config_path.write_text(
        json.dumps(config, indent=2, sort_keys=True), encoding="utf-8"
    )
    _log.info("froze trajectory development config at %s", config_path)

    interaction_column = "era_result"
    inner_train = inner_train.with_columns(
        (pl.col("era") + pl.lit("|") + pl.col("result_family")).alias(
            interaction_column
        )
    )
    development = development.with_columns(
        (pl.col("era") + pl.lit("|") + pl.col("result_family")).alias(
            interaction_column
        )
    )
    additive = fit_logit(inner_train, columns=FEATURE_COLUMNS, seed=seed)
    interaction_columns = (interaction_column, *OTHER_CONTEXT_COLUMNS)
    interaction = fit_logit(inner_train, columns=interaction_columns, seed=seed)
    predictions = {
        "marginal": baseline_probabilities(
            inner_train, development, class_labels, conditional=False
        ),
        "decade_result": baseline_probabilities(
            inner_train, development, class_labels, conditional=True
        ),
        "additive": predict_logit(additive, development, class_labels),
        "era_result_interaction": predict_logit(interaction, development, class_labels),
    }
    label_index = {label: index for index, label in enumerate(class_labels)}
    truth = np.asarray(
        [label_index[str(value)] for value in development.get_column(TARGET_COLUMN)],
        dtype=np.int64,
    )
    overall = {
        name: score_predictions(probabilities, truth)
        for name, probabilities in predictions.items()
    }
    historical_mask = development.get_column("season").to_numpy() < 1988
    pre_1988 = {
        name: score_predictions(probabilities[historical_mask], truth[historical_mask])
        for name, probabilities in predictions.items()
    }
    slice_rows = [
        *_slice_rows(development, truth, predictions, column=TARGET_COLUMN),
        *_slice_rows(development, truth, predictions, column="result_family"),
        *_slice_rows(development, truth, predictions, column="era"),
    ]
    result: dict[str, object] = {
        "status": "research_only_development_evidence",
        "claim_boundary": "Uses only a new internal split of the frozen selected primary TRAIN artifact; no primary TEST or VALIDATE labels are read.",
        "configuration": config,
        "runtime": {
            "python": platform.python_version(),
            "numpy": version("numpy"),
            "polars": version("polars"),
            "scikit_learn": version("scikit-learn"),
        },
        "encoder_levels": {
            "additive": additive.levels,
            "era_result_interaction": interaction.levels,
        },
        "iterations": {
            "additive": [int(value) for value in additive.model.n_iter_],
            "era_result_interaction": [
                int(value) for value in interaction.model.n_iter_
            ],
        },
        "metrics": {"all_development": overall, "pre_1988": pre_1988},
        "slices": slice_rows,
    }
    output_json.write_text(
        json.dumps(result, indent=2, sort_keys=True, allow_nan=False), encoding="utf-8"
    )
    additive_gain = (
        overall["additive"]["log_loss"] - overall["era_result_interaction"]["log_loss"]
    )
    historical_gain = (
        pre_1988["additive"]["log_loss"]
        - pre_1988["era_result_interaction"]["log_loss"]
    )
    markdown = """# Trajectory internal development diagnosis

This research-only check used an 80/20 game-disjoint split inside the frozen 100,000-row primary TRAIN selection. It did not read primary TEST or VALIDATE labels.

| Model | All log loss | All Brier | All ECE | Pre-1988 log loss |
|---|---:|---:|---:|---:|
"""
    for name, metrics in overall.items():
        markdown += (
            f"| {name} | {metrics['log_loss']:.6f} | {metrics['brier']:.6f} | "
            f"{metrics['classwise_ece_15_bin']:.6f} | {pre_1988[name]['log_loss']:.6f} |\n"
        )
    markdown += f"""
The era-by-result interaction changed log loss relative to the additive logit by {additive_gain:+.6f} overall and {historical_gain:+.6f} before 1988. This isolates one structural hypothesis under deterministic MAP-style regularized logistic fits. It does not diagnose Bayesian sampling, prove that an interaction caused the primary TEST result, or justify tuning on primary TEST.
"""
    output_markdown.write_text(markdown, encoding="utf-8")
    return result


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-markdown", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=SPLIT_SEED)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    run_development(
        cast(Path, args.source),
        output_json=cast(Path, args.output_json),
        output_markdown=cast(Path, args.output_markdown),
        seed=int(args.seed),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
