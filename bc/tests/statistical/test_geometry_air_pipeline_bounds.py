from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from python_models.statistical.backtests.geometry_air_pipeline_bounds import (
    SLACK_FLOOR,
    VARIABLES,
    band_given_recorded,
    clue_slack,
    clue_table,
    feasible,
    identified_set,
    sample_identified_set,
    season_margins,
    validate_bounds,
)
from python_models.statistical.backtests.geometry_air_pipeline_data import (
    CLASSES,
    CLUE_LEVELS,
)


def simulated_world(seed: int) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    generator = np.random.default_rng(seed)
    translation = np.array([[0.75, 0.20, 0.05], [0.25, 0.65, 0.10], [0.15, 0.10, 0.75]])
    label_shares = generator.dirichlet(np.full(len(CLASSES), 4.0))
    clue_given_band = generator.dirichlet(np.ones(len(CLUE_LEVELS)), size=len(CLASSES))
    return translation, label_shares, clue_given_band


def counts_frame(
    translation: np.ndarray,
    label_shares: np.ndarray,
    clue_given_band: np.ndarray,
    events: int,
    seed: int,
) -> pl.DataFrame:
    generator = np.random.default_rng(seed)
    recorded = generator.choice(len(CLASSES), size=events, p=label_shares)
    rows: dict[tuple[str, str], int] = {}
    for label in recorded:
        band = generator.choice(len(CLASSES), p=translation[label])
        clue = generator.choice(len(CLUE_LEVELS), p=clue_given_band[band])
        key = (CLASSES[int(label)], CLUE_LEVELS[int(clue)])
        rows[key] = rows.get(key, 0) + 1
    return pl.DataFrame(
        {
            "season": [1995] * len(rows),
            "recorded_air_subtype": [key[0] for key in rows],
            "clue": [key[1] for key in rows],
            "n": list(rows.values()),
        }
    )


def test_identified_set_contains_the_truth_and_draws_respect_constraints() -> None:
    translation, label_shares, clue_given_band = simulated_world(1)
    counts = counts_frame(translation, label_shares, clue_given_band, 40000, 2)
    shares, margin, total = season_margins(counts)
    assert total == 40000
    assert abs(float(shares.sum()) - 1.0) < 1e-12
    assert abs(float(margin.sum()) - 1.0) < 1e-12
    slack = np.full(len(CLUE_LEVELS), 0.02)
    identified = identified_set(shares, margin, clue_given_band, slack)
    assert identified.feasible
    validation = validate_bounds(identified, translation)
    assert validation.inside.all(), validation.max_violation
    lower, upper = identified.translation_bounds()
    assert np.all(lower <= upper + 1e-9)
    mix_lower, mix_upper = identified.band_mix_bounds()
    true_mix = label_shares @ translation
    assert np.all(true_mix >= mix_lower - 1e-9) and np.all(true_mix <= mix_upper + 1e-9)
    draws = sample_identified_set(identified, draws=300, seed=3, burn_in=200, thin=2)
    assert draws.shape == (300, VARIABLES)
    assert np.allclose(
        identified.equality @ draws.T, identified.equality_rhs[:, None], atol=1e-8
    )
    assert np.all(
        identified.inequality @ draws.T <= identified.inequality_rhs[:, None] + 1e-8
    )
    assert np.all(draws >= -1e-9) and np.all(draws <= 1 + 1e-9)
    assert np.all(draws.min(axis=0) >= identified.lower - 1e-8)
    assert np.all(draws.max(axis=0) <= identified.upper + 1e-8)


def test_wider_slack_gives_wider_bounds() -> None:
    translation, label_shares, clue_given_band = simulated_world(4)
    counts = counts_frame(translation, label_shares, clue_given_band, 20000, 5)
    shares, margin, _ = season_margins(counts)
    tight = identified_set(
        shares, margin, clue_given_band, np.full(len(CLUE_LEVELS), 0.01)
    )
    loose = identified_set(
        shares, margin, clue_given_band, np.full(len(CLUE_LEVELS), 0.05)
    )
    assert np.all(loose.upper - loose.lower >= tight.upper - tight.lower - 1e-9)


def test_infeasible_set_is_reported_and_cannot_be_sampled() -> None:
    translation, label_shares, clue_given_band = simulated_world(6)
    counts = counts_frame(translation, label_shares, clue_given_band, 20000, 7)
    shares, margin, _ = season_margins(counts)
    wrong_table = np.roll(clue_given_band, 5, axis=1)
    identified = identified_set(shares, margin, wrong_table, np.zeros(len(CLUE_LEVELS)))
    assert not identified.feasible
    assert np.isnan(identified.lower).any()
    with pytest.raises(ValueError, match="infeasible"):
        sample_identified_set(identified, draws=10, seed=1)


def test_clue_slack_uses_the_floor_and_the_largest_band_difference() -> None:
    first = np.full((len(CLASSES), len(CLUE_LEVELS)), 1.0 / len(CLUE_LEVELS))
    second = first.copy()
    second[0, 3] += 0.04
    second[0, 4] -= 0.04
    slack = clue_slack(first, second)
    assert abs(float(slack[3]) - 0.08) < 1e-12 and abs(float(slack[4]) - 0.08) < 1e-12
    assert np.all(np.delete(slack, [3, 4]) == SLACK_FLOOR)


def test_tables_are_row_distributions_over_the_declared_levels() -> None:
    frame = pl.DataFrame(
        {
            "recorded_air_subtype": ["Fly", "Fly", "LineDrive", "PopUp", "PopUp"],
            "target_class": ["Fly", "LineDrive", "LineDrive", "PopUp", "Fly"],
            "clue": [
                CLUE_LEVELS[0],
                CLUE_LEVELS[1],
                CLUE_LEVELS[1],
                CLUE_LEVELS[2],
                CLUE_LEVELS[0],
            ],
        }
    )
    clues = clue_table(frame)
    bands = band_given_recorded(frame)
    assert clues.shape == (len(CLASSES), len(CLUE_LEVELS))
    assert np.allclose(clues.sum(axis=1), 1.0)
    assert np.all(clues > 0)
    assert np.allclose(bands.sum(axis=1), 1.0)
    assert bands[CLASSES.index("LineDrive"), CLASSES.index("LineDrive")] == 1.0
    with pytest.raises(ValueError, match="reference events"):
        band_given_recorded(frame.filter(pl.col("recorded_air_subtype") != "PopUp"))


def test_feasibility_check_agrees_with_the_bounds() -> None:
    translation, label_shares, clue_given_band = simulated_world(8)
    counts = counts_frame(translation, label_shares, clue_given_band, 20000, 9)
    shares, margin, _ = season_margins(counts)
    slack = np.full(len(CLUE_LEVELS), 0.02)
    assert feasible(shares, margin, clue_given_band, slack)
    assert identified_set(shares, margin, clue_given_band, slack).feasible
    wrong = np.roll(clue_given_band, 5, axis=1)
    assert not feasible(shares, margin, wrong, np.zeros(len(CLUE_LEVELS)))
    assert not identified_set(
        shares, margin, wrong, np.zeros(len(CLUE_LEVELS))
    ).feasible
