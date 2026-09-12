from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from python_models.statistical.backtests.geometry_air_pipeline_data import CLASSES
from python_models.statistical.backtests.geometry_air_dirichlet import (
    posterior_grid,
)
from python_models.statistical.backtests.geometry_air_pipeline_model import (
    KAPPA_GRID,
    PipelineFit,
    cached_grid,
    fit_pipeline,
    log_marginal_likelihood,
    predict_pipeline,
    sample_cell,
    season_counts,
)

RESULTS = ("hit", "out_in_play")


def simulated_frame(
    *,
    kappa: float,
    seasons: tuple[int, ...],
    events: int,
    seed: int,
    recorded: tuple[str, ...] = CLASSES,
) -> pl.DataFrame:
    generator = np.random.default_rng(seed)
    rows: list[dict[str, object]] = []
    key = 0
    for label in recorded:
        for result in RESULTS:
            q = generator.dirichlet(np.ones(len(CLASSES)))
            for season in seasons:
                p = generator.dirichlet(kappa * q)
                targets = generator.choice(len(CLASSES), size=events, p=p)
                for game_offset, target in enumerate(targets):
                    rows.append(
                        {
                            "event_key": key,
                            "game_id": f"{label[:3].upper()}{season}{game_offset % 40:04d}",
                            "season": season,
                            "recorded_air_subtype": label,
                            "result_family": result,
                            "target_class": CLASSES[int(target)],
                        }
                    )
                    key += 1
    return pl.DataFrame(rows)


def test_season_counts_conserve_events_and_follow_the_arm() -> None:
    frame = simulated_frame(kappa=30.0, seasons=(2016, 2017), events=50, seed=1)
    fine = season_counts(frame, arm="recorded_result", seasons=(2016, 2017))
    coarse = season_counts(frame, arm="recorded", seasons=(2016, 2017))
    assert sum(int(v.sum()) for v in fine.values()) == frame.height
    assert sum(int(v.sum()) for v in coarse.values()) == frame.height
    assert len(fine) == len(CLASSES) * len(RESULTS)
    assert len(coarse) == len(CLASSES)
    for label in CLASSES:
        pooled = sum(fine[(label, result)] for result in RESULTS)
        assert np.array_equal(pooled, coarse[(label,)])


def test_log_marginal_is_invariant_to_season_and_class_permutations() -> None:
    counts = np.array([[30, 10, 5], [25, 12, 8], [40, 3, 2]], dtype=np.int64)
    base = log_marginal_likelihood(counts, 30.0, 128)
    seasons = log_marginal_likelihood(counts[[2, 0, 1]], 30.0, 128)
    classes = log_marginal_likelihood(counts[:, [1, 2, 0]], 30.0, 128)
    assert abs(base - seasons) < 1e-6
    assert abs(base - classes) < 1e-6


def test_identical_seasons_prefer_larger_concentration() -> None:
    same = np.array([[300, 100, 50]] * 4, dtype=np.int64)
    scattered = np.array(
        [[300, 100, 50], [100, 300, 50], [50, 100, 300], [150, 150, 150]],
        dtype=np.int64,
    )
    for counts, expected_sign in ((same, 1), (scattered, -1)):
        low = log_marginal_likelihood(counts, 3.0, 128)
        high = log_marginal_likelihood(counts, 1000.0, 256)
        assert np.sign(high - low) == expected_sign


def test_fit_recovers_the_simulated_concentration() -> None:
    frame = simulated_frame(
        kappa=30.0, seasons=tuple(range(2010, 2018)), events=1500, seed=2
    )
    fit = fit_pipeline(
        frame,
        pipeline="B",
        arm="recorded_result",
        seasons=tuple(range(2010, 2018)),
        fit_id="recovery",
        draws=256,
    )
    posterior = dict(zip(KAPPA_GRID, fit.kappa_posterior, strict=True))
    mass_near_truth = sum(p for kappa, p in posterior.items() if 10.0 <= kappa <= 100.0)
    assert mass_near_truth > 0.8
    assert fit.lower_boundary_mass < 0.01 and fit.upper_boundary_mass < 0.01
    assert fit.training_events == frame.height


def test_new_season_draws_have_the_concentration_dispersion() -> None:
    seasons = tuple(range(2010, 2018))
    frame = simulated_frame(kappa=30.0, seasons=seasons, events=1500, seed=3)
    fit = fit_pipeline(
        frame,
        pipeline="B",
        arm="recorded",
        seasons=seasons,
        fit_id="dispersion",
        draws=4096,
    )
    cell = fit.cells[0]
    counts = np.array(cell.counts, dtype=np.int64)
    draws = sample_cell(fit, counts, cell.orders, season=None, seed=7)
    assert draws.shape == (4096, len(CLASSES))
    assert np.allclose(draws.sum(axis=1), 1.0)
    q = counts.sum(axis=0) / counts.sum()
    kappa = fit.kappa_posterior_mean
    expected_sd = np.sqrt(q * (1 - q) / (kappa + 1))
    observed_sd = draws.std(axis=0)
    assert np.all(observed_sd > 0.5 * expected_sd)
    assert np.all(observed_sd < 2.0 * expected_sd)
    referenced = sample_cell(fit, counts, cell.orders, season=seasons[0], seed=7)
    assert np.all(referenced.std(axis=0) < observed_sd)


def test_predictions_share_cell_draws_replay_and_flag_unseen_cells() -> None:
    seasons = (2016, 2017, 2018)
    frame = simulated_frame(kappa=30.0, seasons=seasons, events=200, seed=4)
    fit = fit_pipeline(
        frame,
        pipeline="B",
        arm="recorded_result",
        seasons=seasons,
        fit_id="predict",
        draws=512,
    )
    held = pl.concat(
        [
            frame.head(20),
            frame.head(1).with_columns(pl.lit("sacrifice").alias("result_family")),
        ]
    )
    first = predict_pipeline(fit, held, season=None)
    second = predict_pipeline(fit, held, season=None)
    assert np.array_equal(first.probabilities, second.probabilities)
    assert np.allclose(first.probabilities.sum(axis=1), 1.0)
    statuses = {cell.values: cell.status for cell in first.cells}
    assert (
        statuses[(str(held["recorded_air_subtype"][0]), "sacrifice")]
        == "prior_only_cell"
    )
    assert all(
        status == "posterior_cell"
        for values, status in statuses.items()
        if values[1] != "sacrifice"
    )
    same_cell = np.flatnonzero(first.cell_index == first.cell_index[0])
    assert np.all(first.probabilities[same_cell] == first.probabilities[0])
    with pytest.raises(ValueError, match="fitted season"):
        predict_pipeline(fit, held, season=1999)


def test_fit_validation_rejects_inconsistent_counts() -> None:
    seasons = (2016, 2017)
    frame = simulated_frame(kappa=30.0, seasons=seasons, events=30, seed=5)
    fit = fit_pipeline(
        frame,
        pipeline="B",
        arm="recorded",
        seasons=seasons,
        fit_id="validate",
        draws=16,
    )
    payload = fit.model_dump()
    payload["training_events"] += 1
    with pytest.raises(ValueError, match="conserve"):
        PipelineFit.model_validate(payload)


def test_cached_grids_are_shared_and_leave_the_marginal_unchanged() -> None:
    counts = np.array([[30, 10, 5], [25, 12, 8]], dtype=np.int64)
    first = cached_grid(counts, 20.0, 64)
    second = cached_grid(counts.copy(), 20.0, 64)
    assert first is second
    assert cached_grid(counts, 20.0, 128) is not first
    direct = posterior_grid(counts, 20.0, 64)
    assert np.array_equal(direct.nodes, first.nodes)
    assert direct.log_normalizer == first.log_normalizer
    assert log_marginal_likelihood(counts, 20.0, 64) == log_marginal_likelihood(
        counts, 20.0, 64
    )
