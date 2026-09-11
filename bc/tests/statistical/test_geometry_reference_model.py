from __future__ import annotations

import arviz as az
import numpy as np
import polars as pl
import pytest

from python_models.statistical.backtests.geometry_reference_model import (
    FEATURE_COLUMNS,
    _build_reference_model,
    _prepare_training_data,
    _sampling_config,
    predict_reference,
)


def _training_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "era": ["1990", "1990", "2000", "2000", "2000"],
            "result_family": ["hit", "hit", "out", "out", "out"],
            "base_state_start": ["000", "000", "100", "100", "100"],
            "outs_start": ["0", "0", "1", "1", "1"],
            "alignment_regime": ["a", "a", "b", "b", "b"],
            "batter_hand": ["L", "L", "R", "R", "R"],
            "target_class": ["Left", "Right", "Left", "Left", "Right"],
        },
        schema={
            **{column: pl.String for column in FEATURE_COLUMNS},
            "target_class": pl.String,
        },
    )


def test_training_vocabulary_and_counts_come_only_from_passed_train_frame() -> None:
    data = _prepare_training_data(_training_frame())
    assert data.class_labels == ("Left", "Right")
    assert data.feature_levels["era"] == ("1990", "2000")
    assert data.feature_levels["result_family"] == ("hit", "out")
    assert data.counts.shape == (2, 2)
    assert int(data.counts.sum()) == 5
    assert sorted(data.counts.sum(axis=1).tolist()) == [2, 3]


def test_reference_model_aggregates_multinomial_and_constrains_both_effect_axes() -> (
    None
):
    data = _prepare_training_data(_training_frame())
    model = _build_reference_model(data)
    assert tuple(model.named_vars_to_dims["observed_counts"]) == ("cell", "class")
    assert "cell_prob" not in model.named_vars
    draw = np.asarray(model.compile_fn(model["delta_era"])({}), dtype=np.float64)
    np.testing.assert_allclose(draw.sum(axis=0), 0.0, atol=1e-8)
    np.testing.assert_allclose(draw.sum(axis=1), 0.0, atol=1e-8)


def test_prior_model_exposes_cell_prob_without_changing_likelihood() -> None:
    data = _prepare_training_data(_training_frame())
    model = _build_reference_model(data, include_cell_prob=True)
    assert "cell_prob" in model.named_vars
    assert tuple(model.named_vars_to_dims["cell_prob"]) == ("cell", "class")


def test_sampling_budgets_match_frozen_protocol() -> None:
    smoke = _sampling_config(smoke=True, seed=17)
    full = _sampling_config(smoke=False, seed=19)
    assert (smoke.chains, smoke.tune, smoke.draws) == (2, 50, 50)
    assert (full.chains, full.tune, full.draws) == (4, 1000, 1000)
    assert smoke.backend == full.backend == "nutpie"
    assert smoke.target_accept == full.target_accept == 0.95
    assert smoke.max_treedepth == full.max_treedepth == 12
    assert smoke.random_seed == 17
    assert full.random_seed == 19


def test_predict_preserves_order_reuses_vectors_and_drops_unseen_effect() -> None:
    posterior = az.from_dict(
        posterior={
            "alpha_class": np.array([[[0.2, -0.2], [0.4, -0.4]]]),
            "delta_era": np.array(
                [[[[0.5, -0.5], [-0.5, 0.5]], [[0.3, -0.3], [-0.3, 0.3]]]]
            ),
        }
    )
    feature_levels: dict[str, tuple[str, ...]] = {
        column: ("known",) for column in FEATURE_COLUMNS
    }
    feature_levels["era"] = ("known", "other")
    frame = pl.DataFrame(
        {
            column: ["known", "unseen" if column == "era" else "known", "known"]
            for column in FEATURE_COLUMNS
        },
        schema={column: pl.String for column in FEATURE_COLUMNS},
    )
    result = predict_reference(posterior, frame, feature_levels, ("Left", "Right"))
    assert result.dtype == np.float64
    assert result.shape == (3, 2)
    np.testing.assert_allclose(result[0], result[2], atol=1e-12)
    np.testing.assert_allclose(result.sum(axis=1), 1.0, atol=1e-12)

    alpha = np.array([[0.2, -0.2], [0.4, -0.4]])
    expected_unseen = np.exp(alpha - alpha.max(axis=1, keepdims=True))
    expected_unseen /= expected_unseen.sum(axis=1, keepdims=True)
    np.testing.assert_allclose(result[1], expected_unseen.mean(axis=0), atol=1e-12)


def test_invalid_frames_fail_before_fit_or_prediction() -> None:
    invalid = _training_frame().with_columns(pl.lit(None).alias("era"))
    with pytest.raises(ValueError, match="contains null"):
        _prepare_training_data(invalid)
    with pytest.raises(ValueError, match="missing required"):
        predict_reference(
            az.from_dict(posterior={"alpha_class": np.zeros((1, 1, 2))}),
            pl.DataFrame({"era": ["1990"]}),
            {column: ("known",) for column in FEATURE_COLUMNS},
            ("Left", "Right"),
        )
