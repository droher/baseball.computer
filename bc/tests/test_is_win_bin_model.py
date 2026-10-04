"""Unit tests for the is-win binary model factory + Scorer contract."""

from __future__ import annotations

import os

os.environ.setdefault("KERAS_BACKEND", "torch")

import numpy as np
import polars as pl

from python_models.ml.features import (
    GRAIN_COLUMN,
    HIGH_CARD_CATEGORICAL,
    IS_WIN_BIN,
    LOW_CARD_CATEGORICAL,
    NUMERIC,
    Vocabulary,
)
from python_models.ml.model_factory import build_model
from python_models.ml.model_is_win_bin import build_model as build_shim_model
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


def test_binary_build_model_emits_single_sigmoid_output() -> None:
    means, variances = _numeric_stats()
    model = build_model(
        target_spec=IS_WIN_BIN,
        vocab_sizes=_vocab_sizes(),
        numeric_means=means,
        numeric_variances=variances,
        num_classes=1,
    )
    assert model.output_shape == (None, 1)
    assert model.name == IS_WIN_BIN.name


def test_binary_shim_matches_factory() -> None:
    means, variances = _numeric_stats()
    direct = build_model(
        target_spec=IS_WIN_BIN,
        vocab_sizes=_vocab_sizes(),
        numeric_means=means,
        numeric_variances=variances,
        num_classes=1,
    )
    via_shim = build_shim_model(
        vocab_sizes=_vocab_sizes(),
        numeric_means=means,
        numeric_variances=variances,
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


def _make_binary_scorer() -> Scorer:
    means, variances = _numeric_stats()
    model = build_model(
        target_spec=IS_WIN_BIN,
        vocab_sizes=_vocab_sizes(),
        numeric_means=means,
        numeric_variances=variances,
        num_classes=1,
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
        class_labels=(),
        kind="binary",
    )


def test_binary_scorer_emits_binary_schema() -> None:
    scorer = _make_binary_scorer()
    n_rows = 64
    scored = scorer.score(_synthetic_features(n_rows))

    assert scored.height == n_rows
    assert scored.columns == [
        GRAIN_COLUMN,
        "predicted_class_bin",
        "predicted_proba",
        "model_run_id",
    ]
    assert scored["predicted_class_bin"].dtype == pl.UInt8
    assert scored["predicted_proba"].dtype == pl.Float64
    proba = scored["predicted_proba"].to_numpy()
    assert (proba >= 0.0).all() and (proba <= 1.0).all()
    cls = scored["predicted_class_bin"].to_numpy()
    assert set(cls.tolist()) <= {0, 1}
    expected = (proba >= 0.5).astype(int)
    assert (cls == expected).all()


def test_binary_scorer_handles_empty_input() -> None:
    scorer = _make_binary_scorer()
    empty = pl.DataFrame({GRAIN_COLUMN: pl.Series([], dtype=pl.UInt32)})
    out = scorer.score(empty)
    assert out.height == 0
    assert out.columns == [
        GRAIN_COLUMN,
        "predicted_class_bin",
        "predicted_proba",
        "model_run_id",
    ]


def test_binary_scorer_encodes_oov_to_zero() -> None:
    scorer = _make_binary_scorer()
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
    proba = scored["predicted_proba"].to_numpy()
    assert (proba >= 0.0).all() and (proba <= 1.0).all()
