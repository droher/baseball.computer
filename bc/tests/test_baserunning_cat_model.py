"""Unit tests for the baserunning multiclass model factory + Scorer contract."""

from __future__ import annotations

import os

os.environ.setdefault("KERAS_BACKEND", "torch")

import numpy as np
import polars as pl

from python_models.ml.features import (
    BASERUNNING_CAT,
    GRAIN_COLUMN,
    HIGH_CARD_CATEGORICAL,
    LOW_CARD_CATEGORICAL,
    NUMERIC,
    Vocabulary,
)
from python_models.ml.model_baserunning_cat import build_model as build_shim_model
from python_models.ml.model_factory import build_model
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


def test_build_model_softmax_output_shape() -> None:
    means, variances = _numeric_stats()
    for n in (3, 5, 8):
        model = build_model(
            target_spec=BASERUNNING_CAT,
            vocab_sizes=_vocab_sizes(),
            numeric_means=means,
            numeric_variances=variances,
            num_classes=n,
        )
        assert model.output_shape == (None, n)
        assert model.name == BASERUNNING_CAT.name


def test_shim_matches_factory() -> None:
    means, variances = _numeric_stats()
    direct = build_model(
        target_spec=BASERUNNING_CAT,
        vocab_sizes=_vocab_sizes(),
        numeric_means=means,
        numeric_variances=variances,
        num_classes=4,
    )
    via_shim = build_shim_model(
        vocab_sizes=_vocab_sizes(),
        numeric_means=means,
        numeric_variances=variances,
        num_classes=4,
    )
    assert direct.name == via_shim.name
    assert direct.output_shape == via_shim.output_shape
    assert {inp.name for inp in direct.inputs} == {inp.name for inp in via_shim.inputs}


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


def _make_scorer(class_labels: tuple[str, ...]) -> Scorer:
    means, variances = _numeric_stats()
    model = build_model(
        target_spec=BASERUNNING_CAT,
        vocab_sizes=_vocab_sizes(),
        numeric_means=means,
        numeric_variances=variances,
        num_classes=len(class_labels),
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
    return Scorer(
        run_id="test_run",
        model=model,
        vocabularies=vocabularies,
        class_labels=class_labels,
        kind="multiclass",
    )


def test_scorer_emits_multiclass_schema() -> None:
    labels = ("CaughtStealing", "Other", "Pickoff", "Steal")
    scorer = _make_scorer(labels)
    n_rows = 64
    scored = scorer.score(_synthetic_features(n_rows))

    assert scored.height == n_rows
    assert scored.columns == [
        GRAIN_COLUMN,
        "predicted_class",
        "predicted_class_proba",
        "model_run_id",
    ]
    proba = scored["predicted_class_proba"].to_numpy()
    assert (proba >= 0.0).all() and (proba <= 1.0).all()
    assert set(scored["predicted_class"].unique().to_list()) <= set(labels)


def test_scorer_handles_empty_input() -> None:
    scorer = _make_scorer(("A", "B", "C"))
    empty = pl.DataFrame({GRAIN_COLUMN: pl.Series([], dtype=pl.UInt32)})
    out = scorer.score(empty)
    assert out.height == 0
    assert out.columns == [
        GRAIN_COLUMN,
        "predicted_class",
        "predicted_class_proba",
        "model_run_id",
    ]


def test_scorer_encodes_oov_to_zero() -> None:
    labels = ("A", "B", "C")
    scorer = _make_scorer(labels)
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
    proba = scored["predicted_class_proba"].to_numpy()
    assert (proba >= 0.0).all() and (proba <= 1.0).all()
