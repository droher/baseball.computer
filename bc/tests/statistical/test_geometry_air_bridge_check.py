from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import polars as pl
import pytest

from python_models.statistical.backtests.geometry_air_bridge_check import (
    CLASSES,
    batter_true_profiles,
    bridge_cohort,
    cohort_arrays,
    estimate_translation,
    evaluate_window,
    fit_translation,
    load_recorded_counts,
    recorded_profiles,
    standardize_paired,
    translation_matrix,
    transport_gap,
)
from python_models.statistical.backtests.geometry_reliability_data import SCHEMA


def simulate(
    translation: np.ndarray, *, batters: int, events: int, seed: int
) -> tuple[np.ndarray, np.ndarray]:
    generator = np.random.default_rng(seed)
    true_shares = generator.dirichlet(np.array([4.0, 4.0, 2.0]), size=batters)
    true_counts = np.array([generator.multinomial(events, row) for row in true_shares])
    recorded_counts = np.array(
        [generator.multinomial(events, row @ translation) for row in true_shares]
    )
    return true_counts.astype(float), recorded_counts.astype(float)


def test_transport_gap_is_near_zero_under_the_same_translation() -> None:
    translation = np.array([[0.8, 0.1, 0.1], [0.05, 0.95, 0.0], [0.1, 0.0, 0.9]])
    true_counts, recorded_counts = simulate(
        translation, batters=300, events=200, seed=1
    )
    result = transport_gap(
        true_counts, recorded_counts, translation, repetitions=200, seed=2
    )
    assert result.max_abs_gap < 0.02
    for name in CLASSES:
        lower, upper = result.gap_ci95[name]
        assert lower <= 0.0 <= upper


def test_transport_gap_detects_a_changed_translation() -> None:
    reference = np.array([[0.8, 0.1, 0.1], [0.05, 0.95, 0.0], [0.1, 0.0, 0.9]])
    folded = np.array([[0.9, 0.1, 0.0], [0.05, 0.95, 0.0], [0.9, 0.0, 0.1]])
    true_counts, recorded_counts = simulate(folded, batters=300, events=200, seed=3)
    result = transport_gap(
        true_counts, recorded_counts, reference, repetitions=200, seed=4
    )
    assert result.gap["PopUp"] < -0.05
    assert result.gap_ci95["PopUp"][1] < 0.0


def paired_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "batter_id": ["a", "a", "b", "b", "b", "c"],
            "season": [2016, 2016, 2017, 2017, 2018, 2018],
            "recorded_class": [
                "Fly",
                "LineDrive",
                "PopUp",
                "GroundBall",
                "Fly",
                "Fly",
            ],
            "statcast_bb_type": [
                "Fly",
                "LineDrive",
                "PopUp",
                "GroundBall",
                "Fly",
                "Fly",
            ],
            "launch_angle": [30.0, 20.0, 60.0, 5.0, 8.0, None],
            "game_pairing_complete": [True, True, True, True, True, True],
        }
    )


def test_standardization_keeps_only_airborne_standardized_events() -> None:
    frame = standardize_paired(paired_frame())
    assert frame["eligible"].to_list() == [True, True, True, False, False, False]
    assert frame["target_class"].to_list() == [
        "Fly",
        "LineDrive",
        "PopUp",
        "GroundBall",
        None,
        None,
    ]
    assert frame["recorded_air_subtype"].to_list() == [
        "Fly",
        "LineDrive",
        "PopUp",
        None,
        "Fly",
        "Fly",
    ]


def test_translation_rows_are_distributions_and_profiles_conserve_events() -> None:
    frame = standardize_paired(paired_frame())
    translation = translation_matrix(frame)
    assert translation.shape == (3, 3)
    assert np.allclose(translation.sum(axis=1), 1.0)
    profiles = batter_true_profiles(frame)
    assert profiles["true_events"].sum() == frame.filter(pl.col("eligible")).height
    counts = pl.DataFrame(
        {
            "season": [2013, 2013, 2014],
            "batter_id": ["a", "a", "b"],
            "recorded_air_subtype": ["Fly", "PopUp", "Fly"],
            "n": [5, 1, 7],
        }
    )
    recorded = recorded_profiles(counts, [2013, 2014])
    assert recorded.sort("batter_id")["recorded_events"].to_list() == [6, 7]
    cohort = bridge_cohort(profiles, recorded, minimum_events=1)
    true_counts, recorded_counts = cohort_arrays(cohort)
    assert true_counts.shape == recorded_counts.shape == (2, 3)
    assert bridge_cohort(profiles, recorded, minimum_events=2).height == 1


def test_translation_fit_recovers_the_simulated_table() -> None:
    translation = np.array([[0.8, 0.1, 0.1], [0.05, 0.9, 0.05], [0.3, 0.05, 0.65]])
    true_counts, recorded_counts = simulate(
        translation, batters=400, events=300, seed=5
    )
    fitted = fit_translation(true_counts, recorded_counts)
    assert np.allclose(fitted.sum(axis=1), 1.0)
    assert np.abs(fitted - translation).max() < 0.05
    same = estimate_translation(
        true_counts, recorded_counts, translation, repetitions=40, seed=6
    )
    for row in same.difference_ci95.values():
        for low, high in row.values():
            assert low <= 0.0 <= high
    folded = np.array([[0.8, 0.1, 0.1], [0.05, 0.9, 0.05], [0.9, 0.05, 0.05]])
    estimate = estimate_translation(
        true_counts, recorded_counts, folded, repetitions=40, seed=6
    )
    assert estimate.difference_ci95["PopUp"]["Fly"][1] < 0.0
    assert estimate.max_abs_difference > 0.4


def test_empty_bridging_cohort_is_rejected_before_any_bootstrap() -> None:
    profiles = pl.DataFrame(
        {"batter_id": ["a"], "Fly": [5], "LineDrive": [5], "PopUp": [0]}
    ).with_columns(pl.sum_horizontal(CLASSES).alias("true_events"))
    recorded = pl.DataFrame(
        {
            "batter_id": ["a"],
            "recorded_Fly": [3],
            "recorded_LineDrive": [3],
            "recorded_PopUp": [0],
            "recorded_events": [6],
        }
    )
    with pytest.raises(ValueError, match="no hitter"):
        evaluate_window(
            (1995, 1998),
            "transport",
            profiles,
            recorded,
            np.eye(len(CLASSES)),
            minimum_events=100,
            repetitions=5,
            seed=1,
        )


def test_recorded_counts_use_the_same_vocabulary_as_the_reference(
    tmp_path: Path,
) -> None:
    rows = [
        ("keep", 2010, "TRAIN", "observed", "Fly", "RegularSeason"),
        ("keep", 2010, "TRAIN", "observed", "Fly", "RegularSeason"),
        ("keep", 2010, "TRAIN", "observed", "PopUp", "RegularSeason"),
        ("keep", 2010, "TRAIN", "observed", "PopUpBunt", "RegularSeason"),
        ("keep", 2010, "TRAIN", "observed", "LineDriveBunt", "RegularSeason"),
        ("keep", 2010, "TRAIN", "unknown", "LineDrive", "RegularSeason"),
        ("keep", 2010, "TRAIN", "observed", "LineDrive", "WorldSeries"),
        ("keep", 2010, "TEST", "observed", "LineDrive", "RegularSeason"),
        ("keep", 2013, "TRAIN", "observed", "LineDrive", "RegularSeason"),
        ("reserve", 2010, "TRAIN", "observed", "LineDrive", "RegularSeason"),
    ]
    frame = pl.DataFrame(
        rows,
        schema=[
            "game_id",
            "season",
            "primary_fold",
            "observed_status",
            "raw_value",
            "game_type",
        ],
        orient="row",
    ).with_columns(
        pl.lit("batter").alias("batter_id"),
        pl.lit("trajectory").alias("geometry_dimension"),
    )
    database = tmp_path / "bc.db"
    with duckdb.connect(str(database)) as connection:
        _ = connection.register("frame", frame)
        _ = connection.execute(f"CREATE SCHEMA {SCHEMA}")
        _ = connection.execute(
            f"CREATE TABLE {SCHEMA}.model_input_geometry AS SELECT * FROM frame"
        )
    reserve = tmp_path / "reserve.parquet"
    pl.DataFrame({"game_id": ["reserve"]}).write_parquet(reserve)
    counts = load_recorded_counts((2009, 2011), database=database, reserve=reserve)
    table = {
        row["recorded_air_subtype"]: row["n"]
        for row in counts.filter(pl.col("season") == 2010).to_dicts()
    }
    assert table == {"Fly": 2, "PopUp": 1}
    assert set(counts["recorded_air_subtype"].to_list()) <= set(CLASSES)
    assert counts["season"].to_list() == [2010] * counts.height
