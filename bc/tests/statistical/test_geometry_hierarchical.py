from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Protocol, cast

import arviz as az
import numpy as np
import polars as pl
import pytest
import xarray as xr

from python_models.statistical.backtests.geometry_hierarchical import (
    CONTEXT_COLUMNS,
    HierarchicalEncoding,
    build_hierarchical_model,
    iter_hierarchical_probability_draws,
    predict_hierarchical,
    prepare_hierarchical_data,
)


class FromDict(Protocol):
    def __call__(
        self,
        *,
        posterior: dict[str, np.ndarray],
        coords: dict[str, list[str]],
        dims: dict[str, list[str]],
    ) -> az.InferenceData: ...


def _train() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "era": ["1990", "1990", "2000", "2000"],
            "result_family": ["hit", "hit", "out", "out"],
            "base_state_start": ["0", "0", "1", "1"],
            "outs_start": ["0", "0", "1", "1"],
            "alignment_regime": ["a", "a", "b", "b"],
            "batter_hand": ["L", "L", "R", "R"],
            "target_class": ["Fly", "GroundBall", "Fly", "Fly"],
        },
        schema={
            "era": pl.String,
            "result_family": pl.String,
            "base_state_start": pl.String,
            "outs_start": pl.String,
            "alignment_regime": pl.String,
            "batter_hand": pl.String,
            "target_class": pl.String,
        },
    )


def _encoding() -> HierarchicalEncoding:
    return HierarchicalEncoding(
        class_labels=("A", "B"),
        observed_class_labels=("A", "B"),
        result_domain=("hit", "out"),
        observed_result_levels=("hit",),
        era_levels=("known",),
        context_levels={column: ("known",) for column in CONTEXT_COLUMNS},
        observed_era_result_pairs=(("known", "hit"),),
        training_rows=2,
    )


def _posterior(
    *,
    beta: np.ndarray | None = None,
    interaction: np.ndarray | None = None,
    sigma_era: float = 0.0,
    sigma_interaction: float = 0.0,
) -> az.InferenceData:
    draws = 3
    beta_value = np.asarray([[2.0, -2.0], [-2.0, 2.0]]) if beta is None else beta
    interaction_value = (
        np.zeros((1, 2, 2), dtype=np.float64) if interaction is None else interaction
    )
    posterior: dict[str, np.ndarray] = {
        "alpha_class": np.zeros((1, draws, 2)),
        "beta_result": np.broadcast_to(beta_value, (1, draws, 2, 2)).copy(),
        "delta_era": np.zeros((1, draws, 1, 2)),
        "delta_era_result": np.broadcast_to(
            interaction_value, (1, draws, 1, 2, 2)
        ).copy(),
        "sigma_era": np.full((1, draws), sigma_era),
        "sigma_era_result": np.full((1, draws), sigma_interaction),
    }
    for column in CONTEXT_COLUMNS:
        posterior[f"delta_{column}"] = np.zeros((1, draws, 1, 2))
    from_dict = cast(FromDict, getattr(az, "from_dict"))
    return from_dict(
        posterior=posterior,
        coords={
            "class": ["A", "B"],
            "era_level": ["known"],
            "result_level": ["hit", "out"],
            **{f"{column}_level": ["known"] for column in CONTEXT_COLUMNS},
        },
        dims={
            "alpha_class": ["class"],
            "beta_result": ["result_level", "class"],
            "delta_era": ["era_level", "class"],
            "delta_era_result": ["era_level", "result_level", "class"],
            **{
                f"delta_{column}": [f"{column}_level", "class"]
                for column in CONTEXT_COLUMNS
            },
        },
    )


def _frame(eras: list[str], results: list[str]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "era": eras,
            "result_family": results,
            **{column: ["known"] * len(eras) for column in CONTEXT_COLUMNS},
        },
        schema={
            "era": pl.String,
            "result_family": pl.String,
            **{column: pl.String for column in CONTEXT_COLUMNS},
        },
    )


def test_training_uses_fixed_domains_and_retains_absent_classes() -> None:
    data = prepare_hierarchical_data(
        _train(),
        class_labels=("Bunt", "Fly", "GroundBall"),
        result_domain=("choice", "hit", "out"),
    )
    assert data.encoding.observed_class_labels == ("Fly", "GroundBall")
    assert data.encoding.observed_result_levels == ("hit", "out")
    assert data.counts.shape[1] == 3
    assert not data.counts[:, 0].any()
    assert data.encoding.observed_era_result_pairs == (
        ("1990", "hit"),
        ("2000", "out"),
    )
    model = build_hierarchical_model(data)
    coords = cast(Mapping[str, tuple[str, ...] | None], getattr(model, "coords"))
    named_dims = cast(
        Mapping[str, tuple[str, ...]], getattr(model, "named_vars_to_dims")
    )
    assert coords["result_level"] == ("choice", "hit", "out")
    assert named_dims["delta_era_result"] == (
        "era_level",
        "result_level",
        "class",
    )


def test_prior_effects_have_the_declared_within_group_constraints() -> None:
    data = prepare_hierarchical_data(
        _train(),
        class_labels=("Bunt", "Fly", "GroundBall"),
        result_domain=("choice", "hit", "out"),
    )
    model = build_hierarchical_model(data)
    compile_fn = cast(
        Callable[[object], Callable[[dict[str, object]], object]],
        getattr(model, "compile_fn"),
    )
    named_vars = cast(Mapping[str, object], getattr(model, "named_vars"))
    z_era = np.asarray(compile_fn(named_vars["z_era"])({}), dtype=np.float64)
    z_interaction = np.asarray(
        compile_fn(named_vars["z_era_result"])({}), dtype=np.float64
    )
    context = np.asarray(
        compile_fn(named_vars["delta_base_state_start"])({}), dtype=np.float64
    )
    np.testing.assert_allclose(z_era.sum(axis=-1), 0.0, atol=1e-8)
    np.testing.assert_allclose(z_interaction.sum(axis=-1), 0.0, atol=1e-8)
    np.testing.assert_allclose(z_interaction.sum(axis=-2), 0.0, atol=1e-8)
    np.testing.assert_allclose(context.sum(axis=-1), 0.0, atol=1e-8)


def test_unseen_era_preserves_result_main_effect_and_is_stable() -> None:
    frame = _frame(["new", "new"], ["hit", "out"])
    idata = _posterior()
    first = predict_hierarchical(idata, frame, _encoding(), seed=17)
    second = predict_hierarchical(idata, frame.reverse(), _encoding(), seed=17)
    assert first.probabilities[0, 0] > 0.95
    assert first.probabilities[1, 1] > 0.95
    assert not first.known_era.any()
    np.testing.assert_allclose(first.probabilities, second.probabilities[::-1])
    np.testing.assert_allclose(first.probabilities.sum(axis=1), 1.0, atol=1e-12)


def test_unseen_era_interaction_draw_is_shared_across_results_and_chunks() -> None:
    frame = _frame(["new", "new"], ["hit", "out"])
    zero_beta = np.zeros((2, 2), dtype=np.float64)
    idata = _posterior(beta=zero_beta, sigma_era=0.0, sigma_interaction=1.0)
    together = list(
        iter_hierarchical_probability_draws(
            idata, frame, _encoding(), seed=23, chunk_size=2
        )
    )[0].probabilities
    separate = np.concatenate(
        [
            chunk.probabilities
            for chunk in iter_hierarchical_probability_draws(
                idata, frame, _encoding(), seed=23, chunk_size=1
            )
        ],
        axis=1,
    )
    np.testing.assert_allclose(together, separate, atol=1e-12)
    np.testing.assert_allclose(together[:, 0, 0], together[:, 1, 1], atol=1e-12)


def test_known_era_unobserved_pair_uses_cartesian_latent_effect() -> None:
    interaction = np.asarray([[[1.5, -1.5], [-1.5, 1.5]]])
    idata = _posterior(beta=np.zeros((2, 2)), interaction=interaction)
    result = predict_hierarchical(
        idata, _frame(["known"], ["out"]), _encoding(), seed=31
    )
    assert result.known_era.item()
    assert not result.observed_era_result_pair.item()
    assert result.probabilities[0, 1] > 0.95


def test_unseen_context_is_prior_drawn_and_flagged_per_event() -> None:
    frame = _frame(["known", "known"], ["hit", "hit"]).with_columns(
        pl.Series("batter_hand", ["new", "known"])
    )
    idata = _posterior(beta=np.zeros((2, 2)))
    first = predict_hierarchical(idata, frame, _encoding(), seed=41)
    second = predict_hierarchical(idata, frame, _encoding(), seed=41)
    assert first.unseen_context_count.tolist() == [1, 0]
    np.testing.assert_allclose(first.probabilities, second.probabilities, atol=1e-12)
    assert not np.allclose(first.probabilities[0], first.probabilities[1])


def test_unknown_result_and_invalid_training_values_fail_closed() -> None:
    with pytest.raises(ValueError, match="outside fixed result_domain"):
        predict_hierarchical(
            _posterior(), _frame(["known"], ["other"]), _encoding(), seed=5
        )
    with pytest.raises(ValueError, match="TRAIN classes outside"):
        prepare_hierarchical_data(
            _train().with_columns(pl.lit("Other").alias("target_class")),
            class_labels=("Fly", "GroundBall"),
            result_domain=("hit", "out"),
        )
    with pytest.raises(ValueError, match="must be sorted"):
        prepare_hierarchical_data(
            _train(),
            class_labels=("GroundBall", "Fly"),
            result_domain=("hit", "out"),
        )


def test_encoding_and_posterior_coordinates_are_bound_by_label() -> None:
    payload = _encoding().model_dump()
    payload["context_levels"] = {"base_state_start": ("known",)}
    with pytest.raises(ValueError, match="context_levels keys"):
        HierarchicalEncoding.model_validate(payload)
    idata = _posterior()
    posterior = cast(xr.Dataset, getattr(idata, "posterior"))
    posterior.coords["result_level"] = ["out", "hit"]
    with pytest.raises(ValueError, match="coordinate result_level"):
        predict_hierarchical(idata, _frame(["known"], ["hit"]), _encoding(), seed=7)


def test_stream_rejects_non_finite_posterior_values() -> None:
    idata = _posterior()
    posterior = cast(xr.Dataset, getattr(idata, "posterior"))
    posterior["alpha_class"].values[0, 0, 0] = np.nan
    with pytest.raises(ValueError, match="alpha_class contains non-finite"):
        list(
            iter_hierarchical_probability_draws(
                idata,
                _frame(["known"], ["hit"]),
                _encoding(),
                seed=11,
            )
        )
