from __future__ import annotations

import numpy as np
import pytest

from python_models.statistical.backtests.geometry_air_pipeline_data import CLASSES
from python_models.statistical.backtests.geometry_air_translation_estimate import (
    RESULT_FAMILIES,
    cells_frame,
    rake,
    raked_translation,
    summarize,
)


def test_raked_translation_matches_both_margins_with_column_factors() -> None:
    seed = np.array([[0.8, 0.1, 0.1], [0.05, 0.95, 0.0], [0.1, 0.0, 0.9]])
    shares = np.array([0.46, 0.38, 0.16])
    mix = np.array([0.412, 0.425, 0.163])
    translation, factors = raked_translation(seed, shares, mix)
    assert np.allclose(translation.sum(axis=1), 1.0)
    assert np.allclose(shares @ translation, mix, atol=1e-8)
    scaled = seed * factors[None, :]
    assert np.allclose(scaled / scaled.sum(axis=1, keepdims=True), translation)
    assert translation[1, 2] == 0.0 and translation[2, 1] == 0.0
    identity, unit = raked_translation(seed, shares, shares @ seed)
    assert np.allclose(identity, seed) and np.allclose(unit, 1.0)


def test_rake_rejects_bad_seeds_and_margins() -> None:
    rows = np.array([0.5, 0.5])
    with pytest.raises(ValueError, match="nonnegative"):
        rake(np.array([[0.5, -0.1], [0.3, 0.3]]), rows, rows)
    with pytest.raises(ValueError, match="positive mass"):
        rake(np.array([[0.5, 0.0], [0.5, 0.0]]), rows, rows)
    with pytest.raises(ValueError, match="same total"):
        rake(np.ones((2, 2)), rows, np.array([0.5, 0.6]))


def test_cells_frame_and_summary_cover_every_class() -> None:
    frame = cells_frame()
    assert frame.height == len(CLASSES) * len(RESULT_FAMILIES)
    assert frame.unique().height == frame.height
    draws = np.random.default_rng(1).dirichlet(np.array([5.0, 3.0, 1.0]), size=2000)
    summary = summarize(draws)
    assert list(summary) == list(CLASSES)
    for label in CLASSES:
        entry = summary[label]
        assert entry["lower_95"] <= entry["mean"] <= entry["upper_95"]
    assert abs(sum(summary[label]["mean"] for label in CLASSES) - 1.0) < 1e-9
