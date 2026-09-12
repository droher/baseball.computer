from __future__ import annotations

from pathlib import Path
from typing import cast

import numpy as np
import polars as pl
import pytest

from python_models.statistical.backtests.geometry_air_pipeline_bounds import (
    identified_set,
    season_margins,
)
from python_models.statistical.backtests.geometry_air_pipeline_data import (
    CLASSES,
    CLUE_LEVELS,
    PIPELINES,
    REFERENCE_SEASONS,
)
from python_models.statistical.backtests.geometry_air_pipeline_development import (
    class_calibration,
    game_count_coverage,
    load_or_fit,
    minimal_slack_multiplier,
    paired_game_gain,
    share_coverage,
    smoke_subsample,
    unreferenced_seasons,
)
from python_models.statistical.backtests.geometry_air_pipeline_model import (
    CellPrediction,
    PipelinePrediction,
)
from python_models.statistical.splits import game_hash_fold
from tests.statistical.test_geometry_air_pipeline_model import simulated_frame


def synthetic_prediction(
    *, events: int, games: int, draws: int, seed: int
) -> tuple[PipelinePrediction, pl.DataFrame, np.ndarray]:
    generator = np.random.default_rng(seed)
    cells: list[CellPrediction] = []
    truths: list[np.ndarray] = []
    for label in CLASSES:
        q = generator.dirichlet(np.ones(len(CLASSES)) * 3)
        cell_draws = generator.dirichlet(q * 60, size=draws)
        truths.append(generator.dirichlet(q * 60))
        cells.append(
            CellPrediction(values=(label,), status="posterior_cell", draws=cell_draws)
        )
    cell_index = generator.integers(0, len(CLASSES), size=events)
    labels = np.array(
        [generator.choice(len(CLASSES), p=truths[int(c)]) for c in cell_index]
    )
    held = pl.DataFrame(
        {
            "game_id": [f"G{generator.integers(0, games):04d}" for _ in range(events)],
            "recorded_air_subtype": [CLASSES[int(c)] for c in cell_index],
            "target_class": [CLASSES[int(t)] for t in labels],
        }
    )
    probabilities = np.stack([cell.draws.mean(axis=0) for cell in cells])[cell_index]
    return (
        PipelinePrediction(
            probabilities=probabilities, cell_index=cell_index, cells=tuple(cells)
        ),
        held,
        labels,
    )


def test_calibration_of_the_generating_probabilities_is_small() -> None:
    generator = np.random.default_rng(1)
    probabilities = generator.dirichlet(np.ones(len(CLASSES)), size=20000)
    labels = np.array([generator.choice(len(CLASSES), p=p) for p in probabilities])
    rows = class_calibration(probabilities, labels)
    assert [row["class_label"] for row in rows] == list(CLASSES)
    assert all(float(cast(float, row["ece"])) < 0.02 for row in rows)
    assert all(abs(float(cast(float, row["class_share_bias"]))) < 0.02 for row in rows)
    shifted = np.clip(probabilities + np.array([0.2, -0.1, -0.1]), 0.0, 1.0)
    shifted /= shifted.sum(axis=1, keepdims=True)
    assert float(cast(float, class_calibration(shifted, labels)[0]["ece"])) > 0.05


def test_share_and_game_coverage_report_every_slice_and_class() -> None:
    prediction, held, labels = synthetic_prediction(
        events=3000, games=40, draws=500, seed=2
    )
    rows = share_coverage(prediction, held, labels, fit_id="test")
    slices = {str(row["slice"]) for row in rows}
    assert slices == {"all", *CLASSES}
    assert len(rows) == len(slices) * len(CLASSES)
    for row in rows:
        assert float(cast(float, row["lower_95"])) <= float(
            cast(float, row["predicted_share"])
        )
        assert float(cast(float, row["predicted_share"])) <= float(
            cast(float, row["upper_95"])
        )
        assert float(cast(float, row["lower_95"])) <= float(
            cast(float, row["lower_90"])
        )
        assert float(cast(float, row["upper_90"])) <= float(
            cast(float, row["upper_95"])
        )
    covered = [bool(row["covered_95"]) for row in rows]
    assert sum(covered) >= len(covered) - 2
    coverage, table = game_count_coverage(prediction, held, labels, fit_id="test")
    assert table.height == held["game_id"].n_unique()
    for label in CLASSES:
        assert 0.8 <= coverage[label] <= 1.0
        totals = table.select(pl.col(f"actual_{label}").sum()).item()
        assert totals == int((labels == CLASSES.index(label)).sum())
    replay, _ = game_count_coverage(prediction, held, labels, fit_id="test")
    assert replay == coverage


def test_paired_gain_is_zero_for_identical_arms_and_positive_when_better() -> None:
    generator = np.random.default_rng(3)
    games = [f"G{i:03d}" for i in range(50) for _ in range(20)]
    zero = pl.DataFrame(
        {
            "game_id": games,
            "events": [1] * len(games),
            "gain_log_loss": [0.0] * len(games),
            "gain_brier": [0.0] * len(games),
        }
    )
    result = paired_game_gain(zero, repetitions=100, seed=1)
    assert result["log_loss"]["mean"] == 0.0 and result["brier"]["upper_95"] == 0.0
    better = zero.with_columns(
        pl.Series("gain_log_loss", generator.normal(0.05, 0.02, len(games))),
        pl.Series("gain_brier", generator.normal(0.03, 0.02, len(games))),
    )
    result = paired_game_gain(better, repetitions=200, seed=1)
    assert result["log_loss"]["lower_95"] > 0 and result["brier"]["lower_95"] > 0
    assert result["log_loss"]["lower_95"] <= result["log_loss"]["mean"]
    assert result["log_loss"]["mean"] <= result["log_loss"]["upper_95"]
    again = paired_game_gain(better, repetitions=200, seed=1)
    assert again == result


def test_smoke_subsample_keeps_whole_games_deterministically() -> None:
    frame = pl.DataFrame(
        {
            "game_id": [f"G{i:04d}" for i in range(500) for _ in range(3)],
            "event_key": list(range(1500)),
        }
    )
    first = smoke_subsample(frame, fold_count=5)
    second = smoke_subsample(frame, fold_count=5)
    assert first.equals(second)
    assert 0.1 < first["game_id"].n_unique() / 500 < 0.3
    assert first.group_by("game_id").len()["len"].min() == 3
    assert all(
        game_hash_fold(str(game), fold_count=5) == 0
        for game in first["game_id"].unique().to_list()
    )


def test_unreferenced_seasons_cover_each_pipeline_without_references() -> None:
    for pipeline, seasons in REFERENCE_SEASONS.items():
        first, last = PIPELINES[pipeline]
        gaps = unreferenced_seasons(pipeline)
        assert not set(gaps) & set(seasons)
        assert all(first <= season <= min(last, 2025) for season in gaps)
        assert len(gaps) + len(seasons) == min(last, 2025) - first + 1


def test_minimal_slack_multiplier_is_one_when_feasible_and_grows_otherwise() -> None:
    generator = np.random.default_rng(4)
    translation = np.array([[0.8, 0.15, 0.05], [0.2, 0.7, 0.1], [0.1, 0.1, 0.8]])
    clue_given_band = generator.dirichlet(np.ones(len(CLUE_LEVELS)), size=len(CLASSES))
    label_shares = np.array([0.5, 0.35, 0.15])
    clue_margin = (label_shares @ translation) @ clue_given_band
    counts = pl.DataFrame(
        {
            "season": [1995] * (len(CLASSES) * len(CLUE_LEVELS)),
            "recorded_air_subtype": [label for label in CLASSES for _ in CLUE_LEVELS],
            "clue": [clue for _ in CLASSES for clue in CLUE_LEVELS],
            "n": [
                int(
                    round(
                        10000
                        * label_shares[i]
                        * (translation[i] @ clue_given_band[:, j])
                    )
                )
                for i in range(len(CLASSES))
                for j in range(len(CLUE_LEVELS))
            ],
        }
    )
    shares, margin, _ = season_margins(counts)
    slack = np.full(len(CLUE_LEVELS), 0.01)
    assert minimal_slack_multiplier(shares, margin, clue_given_band, slack) == 1.0
    wrong = np.roll(clue_given_band, 7, axis=1)
    assert not identified_set(shares, margin, wrong, slack).feasible
    multiplier = minimal_slack_multiplier(shares, margin, wrong, slack)
    assert multiplier is not None and multiplier > 1.0
    assert identified_set(shares, margin, wrong, slack * multiplier).feasible
    assert not identified_set(shares, margin, wrong, slack * multiplier / 1.05).feasible
    del clue_margin


def test_checkpoints_are_reused_only_for_the_same_training_rows(
    tmp_path: Path,
) -> None:
    frame = simulated_frame(kappa=30.0, seasons=(2016, 2017), events=40, seed=8)
    first = load_or_fit(
        tmp_path,
        frame,
        pipeline="B",
        arm="recorded",
        seasons=(2016, 2017),
        fit_id="reuse",
        draws=16,
    )
    assert (tmp_path / "reuse.json").is_file()
    assert (tmp_path / "reuse.training.json").is_file()
    again = load_or_fit(
        tmp_path,
        frame,
        pipeline="B",
        arm="recorded",
        seasons=(2016, 2017),
        fit_id="reuse",
        draws=16,
    )
    assert again == first
    shifted = frame.with_columns(pl.col("event_key") + 1)
    with pytest.raises(ValueError, match="does not match"):
        load_or_fit(
            tmp_path,
            shifted,
            pipeline="B",
            arm="recorded",
            seasons=(2016, 2017),
            fit_id="reuse",
            draws=16,
        )
    with pytest.raises(ValueError, match="does not match"):
        load_or_fit(
            tmp_path,
            frame,
            pipeline="B",
            arm="recorded",
            seasons=(2016, 2017),
            fit_id="reuse",
            draws=32,
        )
