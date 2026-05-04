"""Unit tests for the Keras model factory + scoring contract."""

from __future__ import annotations

import os

os.environ.setdefault("KERAS_BACKEND", "torch")

import numpy as np
import polars as pl

from python_models.ml.features import (
    GRAIN_COLUMN,
    HIGH_CARD_CATEGORICAL,
    LOW_CARD_CATEGORICAL,
    NUMERIC,
    Vocabulary,
)
from python_models.ml.model_plate_appearance_cat import build_model
from python_models.ml.prediction import Scorer


def _vocab_sizes() -> dict[str, int]:
    sizes: dict[str, int] = {}
    for col in HIGH_CARD_CATEGORICAL:
        sizes[col] = 100
    for col in LOW_CARD_CATEGORICAL:
        sizes[col] = 8
    return sizes


def _numeric_stats() -> tuple[dict[str, float], dict[str, float]]:
    means = {c: 0.0 for c in NUMERIC}
    variances = {c: 1.0 for c in NUMERIC}
    return means, variances


def test_build_model_input_shape_matches_features() -> None:
    means, variances = _numeric_stats()
    model = build_model(
        vocab_sizes=_vocab_sizes(),
        numeric_means=means,
        numeric_variances=variances,
        num_classes=3,
    )
    expected = set(HIGH_CARD_CATEGORICAL) | set(LOW_CARD_CATEGORICAL) | set(NUMERIC)
    assert {inp.name for inp in model.inputs} == expected


def test_build_model_output_dim_equals_num_classes() -> None:
    means, variances = _numeric_stats()
    for n in (3, 7, 13):
        model = build_model(
            vocab_sizes=_vocab_sizes(),
            numeric_means=means,
            numeric_variances=variances,
            num_classes=n,
        )
        assert model.output_shape == (None, n)


def _synthetic_features(n_rows: int) -> pl.DataFrame:
    rng = np.random.default_rng(42)
    data: dict[str, list[object]] = {GRAIN_COLUMN: list(range(n_rows))}
    for col in HIGH_CARD_CATEGORICAL:
        data[col] = [f"id_{i}" for i in rng.integers(0, 50, size=n_rows)]
    for col in LOW_CARD_CATEGORICAL:
        data[col] = [f"v{i}" for i in rng.integers(0, 4, size=n_rows)]
    for col in NUMERIC:
        data[col] = rng.normal(size=n_rows).tolist()
    return pl.DataFrame(data)


def test_scorer_invariants() -> None:
    means, variances = _numeric_stats()
    num_classes = 3
    model = build_model(
        vocab_sizes=_vocab_sizes(),
        numeric_means=means,
        numeric_variances=variances,
        num_classes=num_classes,
    )
    vocabularies: dict[str, Vocabulary] = {}
    for col in HIGH_CARD_CATEGORICAL:
        vocabularies[col] = Vocabulary(
            column=col, values=tuple(f"id_{i}" for i in range(50))
        )
    for col in LOW_CARD_CATEGORICAL:
        vocabularies[col] = Vocabulary(
            column=col, values=tuple(f"v{i}" for i in range(4))
        )
    scorer = Scorer(
        run_id="test_run",
        model=model,
        vocabularies=vocabularies,
        class_labels=("ClassA", "ClassB", "ClassC"),
    )

    n_rows = 64
    features = _synthetic_features(n_rows)
    scored = scorer.score(features)

    assert scored.height == n_rows
    assert scored.columns == [
        GRAIN_COLUMN,
        "predicted_class",
        "predicted_class_proba",
        "model_run_id",
    ]
    assert scored[GRAIN_COLUMN].to_list() == features[GRAIN_COLUMN].to_list()
    probas = scored["predicted_class_proba"].to_numpy()
    assert (probas >= 0.0).all() and (probas <= 1.0).all()
    # multiclass softmax: argmax probability must beat 1/num_classes by construction
    assert (probas >= 1.0 / num_classes - 1e-6).all()
    assert set(scored["predicted_class"].unique().to_list()) <= {
        "ClassA",
        "ClassB",
        "ClassC",
    }
    assert set(scored["model_run_id"].unique().to_list()) == {"test_run"}


def test_scorer_encodes_oov_to_zero() -> None:
    """Unknown categorical values must encode to index 0 (OOV slot) and
    still produce a valid prediction — not crash with an index-out-of-bounds.
    """
    means, variances = _numeric_stats()
    num_classes = 3
    model = build_model(
        vocab_sizes=_vocab_sizes(),
        numeric_means=means,
        numeric_variances=variances,
        num_classes=num_classes,
    )
    vocabularies: dict[str, Vocabulary] = {}
    for col in HIGH_CARD_CATEGORICAL:
        vocabularies[col] = Vocabulary(
            column=col, values=tuple(f"id_{i}" for i in range(50))
        )
    for col in LOW_CARD_CATEGORICAL:
        vocabularies[col] = Vocabulary(
            column=col, values=tuple(f"v{i}" for i in range(4))
        )
    scorer = Scorer(
        run_id="test_run",
        model=model,
        vocabularies=vocabularies,
        class_labels=("ClassA", "ClassB", "ClassC"),
    )

    n_rows = 8
    data: dict[str, list[object]] = {GRAIN_COLUMN: list(range(n_rows))}
    for col in HIGH_CARD_CATEGORICAL:
        data[col] = ["never_seen_id"] * n_rows
    for col in LOW_CARD_CATEGORICAL:
        data[col] = ["never_seen_v"] * n_rows
    for col in NUMERIC:
        data[col] = [0.0] * n_rows
    features = pl.DataFrame(data)

    scored = scorer.score(features)
    assert scored.height == n_rows
    probas = scored["predicted_class_proba"].to_numpy()
    assert (probas >= 0.0).all() and (probas <= 1.0).all()


def test_scorer_handles_empty_input() -> None:
    means, variances = _numeric_stats()
    model = build_model(
        vocab_sizes=_vocab_sizes(),
        numeric_means=means,
        numeric_variances=variances,
        num_classes=3,
    )
    scorer = Scorer(
        run_id="test_run",
        model=model,
        vocabularies={
            col: Vocabulary(column=col, values=())
            for col in (*HIGH_CARD_CATEGORICAL, *LOW_CARD_CATEGORICAL)
        },
        class_labels=("ClassA", "ClassB", "ClassC"),
    )
    empty = pl.DataFrame({GRAIN_COLUMN: pl.Series([], dtype=pl.UInt32)})
    out = scorer.score(empty)
    assert out.height == 0
    assert out.columns == [
        GRAIN_COLUMN,
        "predicted_class",
        "predicted_class_proba",
        "model_run_id",
    ]
