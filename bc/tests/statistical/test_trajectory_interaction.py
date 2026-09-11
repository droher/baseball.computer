from __future__ import annotations

import numpy as np
import polars as pl
import pytest
from pathlib import Path
from typing import cast

from python_models.statistical.backtests.trajectory_interaction import (
    BOUND_ADDITIVE_FILES,
    INTERACTION_COLUMN,
    add_interaction,
    paired_additive_comparison,
    repair_sampling_config,
    select_smoke_train,
    validate_alignment,
    verify_report_artifacts,
)
from python_models.statistical.evidence_binding import file_digest


def test_interaction_combines_era_and_result_without_changing_rows() -> None:
    frame = pl.DataFrame(
        {"era": ["1980", "1990"], "result_family": ["hit", "out_in_play"]}
    )
    result = add_interaction(frame)
    assert result.height == frame.height
    assert result.get_column(INTERACTION_COLUMN).to_list() == [
        "1980|hit",
        "1990|out_in_play",
    ]


def test_smoke_selection_is_deterministic_and_unique() -> None:
    frame = pl.DataFrame(
        {"event_key": range(4000), "value": [str(index) for index in range(4000)]}
    )
    first = select_smoke_train(frame, seed=17)
    second = select_smoke_train(frame.reverse(), seed=17)
    assert first.height == 3000
    assert first.get_column("event_key").n_unique() == 3000
    assert set(first.get_column("event_key")) == set(second.get_column("event_key"))


def test_repair_changes_only_saved_draw_budget() -> None:
    config = repair_sampling_config(17)
    assert config.draws == 4000
    assert config.tune == 1000
    assert config.chains == 4
    assert config.target_accept == 0.95
    assert config.max_treedepth == 12
    assert config.backend == "nutpie"
    assert config.random_seed == 17


def test_alignment_rejects_reordered_predictions() -> None:
    test = pl.DataFrame(
        {
            "event_key": [1, 2],
            "game_id": ["a", "b"],
            "season": [1980, 1990],
            "target_class": ["Fly", "GroundBall"],
        }
    )
    validate_alignment(test, test.clone())
    with pytest.raises(ValueError, match="event_key"):
        validate_alignment(test, test.reverse())


def test_report_artifact_binding_rejects_tampering(tmp_path: Path) -> None:
    artifact_files: dict[str, str] = {}
    for relative in BOUND_ADDITIVE_FILES:
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(relative, encoding="utf-8")
        artifact_files[relative] = file_digest(path)
    report: dict[str, object] = {"artifact_files": artifact_files}
    assert verify_report_artifacts(tmp_path, report) == artifact_files
    (tmp_path / "posterior.nc").write_text("tampered", encoding="utf-8")
    with pytest.raises(ValueError, match="posterior.nc"):
        verify_report_artifacts(tmp_path, report)


def test_paired_comparison_reports_positive_gain_for_better_interaction() -> None:
    frame = pl.DataFrame(
        {
            "target_class": ["Fly", "GroundBall", "Fly", "GroundBall"],
            "game_id": ["a", "a", "b", "b"],
            "season": [1980, 1980, 2000, 2000],
            "interaction_probability": [
                [0.9, 0.1],
                [0.1, 0.9],
                [0.8, 0.2],
                [0.2, 0.8],
            ],
            "additive_probability": [[0.6, 0.4], [0.4, 0.6]] * 2,
        }
    )
    result = paired_additive_comparison(
        frame, ("Fly", "GroundBall"), repetitions=20, seed=19
    )
    slices = cast(dict[str, dict[str, object]], result["slices"])
    for slice_result in slices.values():
        log_gain = slice_result["log_loss_gain"]
        brier_gain = slice_result["brier_gain"]
        interaction_log_loss = slice_result["interaction_log_loss"]
        assert isinstance(log_gain, float)
        assert isinstance(brier_gain, float)
        assert isinstance(interaction_log_loss, float)
        assert log_gain > 0.0
        assert brier_gain > 0.0
        assert np.isfinite(interaction_log_loss)


def test_paired_comparison_handles_empty_historical_slice() -> None:
    frame = pl.DataFrame(
        {
            "target_class": ["Fly"],
            "game_id": ["a"],
            "season": [2000],
            "interaction_probability": [[0.8, 0.2]],
            "additive_probability": [[0.6, 0.4]],
        }
    )
    result = paired_additive_comparison(
        frame, ("Fly", "GroundBall"), repetitions=2, seed=19
    )
    slices = cast(dict[str, dict[str, object]], result["slices"])
    historical = slices["pre_1988_observed_only"]
    assert historical["row_count"] == 0
    assert historical["log_loss_gain"] is None
    assert historical["log_loss_gain_ci95"] is None
