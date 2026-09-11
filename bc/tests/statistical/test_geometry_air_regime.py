from __future__ import annotations

import numpy as np
import polars as pl
import pytest
from pydantic import ValidationError

from python_models.statistical.backtests.geometry_air_regime import (
    AIR_CLASSES,
    ARMS,
    AirRegimeFit,
    fit_air_regime,
    predict_air_regime,
)


def training_frame() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    event_key = 0
    for season in (2015, 2019, 2023, 2025):
        for recorded_index, recorded in enumerate(AIR_CLASSES):
            for result in ("out", "hit"):
                for repeat in range(2):
                    rows.append(
                        {
                            "event_key": event_key,
                            "season": season,
                            "recorded_broad_type": "Air",
                            "recorded_air_subtype": recorded,
                            "result_family": result,
                            "target_class": AIR_CLASSES[
                                (recorded_index + repeat) % len(AIR_CLASSES)
                            ],
                            "known_air_evaluation_eligible": True,
                        }
                    )
                    event_key += 1
    return pl.DataFrame(rows)


def test_fit_serialization_conservation_and_permutation() -> None:
    frame = training_frame()
    for arm in ARMS:
        fit = fit_air_regime(frame, predictor=arm, fit_id="fit", draws=32)
        assert AirRegimeFit.model_validate_json(fit.model_dump_json()) == fit
        assert (
            fit_air_regime(frame.reverse(), predictor=arm, fit_id="fit", draws=32)
            == fit
        )
        assert fit.training_events == frame.height
        assert (
            sum(sum(sum(regime) for regime in cell.counts) for cell in fit.cells)
            == frame.height
        )
        assert fit.represented_regimes == ("early", "late")
        assert all(cell.selected_order in {128, 256, 512} for cell in fit.cells)
        assert all(cell.convergence[-1].passed for cell in fit.cells)


def test_prediction_replay_permutation_shared_cells_and_target_noninterference() -> (
    None
):
    frame = training_frame()
    fit = fit_air_regime(frame, predictor="recorded_result", fit_id="shared", draws=64)
    inputs = frame.with_columns(
        pl.lit("secret").alias("target_class"),
        pl.lit(False).alias("known_air_evaluation_eligible"),
    )
    first = predict_air_regime(fit, inputs)
    replay = predict_air_regime(fit, inputs.reverse())
    chunk = predict_air_regime(fit, inputs.head(5))
    assert first.events["event_key"].equals(inputs["event_key"])
    assert replay.events["event_key"].equals(inputs.reverse()["event_key"])
    assert first.events.sort("event_key").equals(replay.events.sort("event_key"))
    assert len(first.cell_draws) < frame.height
    assert {(item.cell_id, item.regime) for item in first.cell_draws} == {
        (item.cell_id, item.regime) for item in replay.cell_draws
    }
    replay_draws = {(item.cell_id, item.regime): item for item in replay.cell_draws}
    for item in first.cell_draws:
        other = replay_draws[(item.cell_id, item.regime)]
        np.testing.assert_array_equal(item.probabilities, other.probabilities)
        assert item.probabilities.shape == (64, 3)
        assert np.isfinite(item.probabilities).all()
        np.testing.assert_allclose(item.probabilities.sum(axis=1), 1.0)
        assert not item.probabilities.flags.writeable
    full_draws = {(item.cell_id, item.regime): item for item in first.cell_draws}
    for item in chunk.cell_draws:
        np.testing.assert_array_equal(
            item.probabilities,
            full_draws[(item.cell_id, item.regime)].probabilities,
        )


def test_ground_support_masks_and_prior_only_cells_do_not_fallback() -> None:
    frame = training_frame()
    fit = fit_air_regime(frame, predictor="recorded_result", fit_id="support", draws=32)
    inputs = pl.DataFrame(
        {
            "event_key": [1, 2, 3, 4, 5, 6],
            "season": [1900, 1900, 2025, 2025, 2025, 2025],
            "recorded_broad_type": ["Ground", "Air", None, "Air", "Air", "Air"],
            "recorded_air_subtype": [None, "Fly", None, None, "Unknown", "Fly"],
            "result_family": [None, "out", "out", "out", "out", "unseen"],
        }
    )
    prediction = predict_air_regime(fit, inputs)
    assert prediction.events["prediction_status"].to_list() == [
        "ground_preserved",
        "regime_unsupported",
        "broad_unknown_unsupported",
        "fine_label_missing_unsupported",
        "fine_label_unknown_unsupported",
        "prior_only_cell",
    ]
    assert prediction.events["probabilities"].to_list()[0] == [1.0, 0.0, 0.0, 0.0]
    assert prediction.events["probabilities"].null_count() == 4
    prior = prediction.cell_draws[0]
    assert prior.status == "prior_only_cell"
    np.testing.assert_allclose(
        prediction.events["probabilities"].to_list()[-1],
        [0.0, 1 / 3, 1 / 3, 1 / 3],
        atol=1e-12,
    )


def test_result_arm_supports_masked_fine_label_without_using_hidden_columns() -> None:
    frame = training_frame()
    fit = fit_air_regime(frame, predictor="result", fit_id="result", draws=32)
    masked = frame.with_columns(
        pl.lit(None, dtype=pl.String).alias("recorded_air_subtype"),
        pl.lit("forbidden_target").alias("target_class"),
    )
    prediction = predict_air_regime(fit, masked)
    assert set(prediction.events["prediction_status"]) == {"posterior_cell"}
    assert prediction.events["probabilities"].null_count() == 0


def test_absent_class_and_unrepresented_regime_are_explicit() -> None:
    frame = (
        training_frame()
        .filter(pl.col("season") == 2015)
        .with_columns(pl.lit("Fly").alias("target_class"))
    )
    fit = fit_air_regime(frame, predictor="marginal", fit_id="early", draws=32)
    assert fit.represented_regimes == ("early",)
    assert all(cell.counts[0][1:] == (0, 0) for cell in fit.cells)
    future = frame.head(1).with_columns(pl.lit(2025).alias("season"))
    prediction = predict_air_regime(fit, future)
    assert prediction.events["prediction_status"].to_list() == ["regime_unsupported"]
    assert not prediction.cell_draws


def test_training_contract_rejects_invalid_frames_and_serialized_counts() -> None:
    frame = training_frame()
    invalid_frames = (
        pl.concat([frame, frame]),
        frame.with_columns(pl.lit(False).alias("known_air_evaluation_eligible")),
        frame.with_columns(pl.lit("Ground").alias("recorded_broad_type")),
        frame.with_columns(pl.lit(1988).alias("season")),
        frame.with_columns(pl.lit("GroundBall").alias("target_class")),
        frame.with_columns(pl.lit(None).alias("recorded_air_subtype")),
        frame.with_columns(pl.lit("").alias("result_family")),
    )
    for invalid in invalid_frames:
        with pytest.raises(ValueError):
            fit_air_regime(invalid, predictor="marginal", fit_id="invalid", draws=8)
    fit = fit_air_regime(frame, predictor="marginal", fit_id="valid", draws=8)
    payload = fit.model_dump()
    payload["training_events"] += 1
    with pytest.raises(ValidationError):
        AirRegimeFit.model_validate(payload)

    invalid_schedule = fit.model_dump()
    invalid_schedule["cells"][0]["convergence"][0]["tolerance"] = 1e-4
    with pytest.raises(ValidationError):
        AirRegimeFit.model_validate(invalid_schedule)

    invalid_maximum = fit.model_dump()
    invalid_maximum["cells"][0]["convergence"][0]["maximum_error"] += 1e-6
    with pytest.raises(ValidationError):
        AirRegimeFit.model_validate(invalid_maximum)

    recorded_fit = fit_air_regime(
        frame, predictor="recorded", fit_id="recorded", draws=8
    )
    invalid_key = recorded_fit.model_dump()
    invalid_key["cells"][0]["values"] = ["Unknown"]
    with pytest.raises(ValidationError):
        AirRegimeFit.model_validate(invalid_key)


def test_cell_identity_separates_concentration_sensitivities() -> None:
    frame = training_frame()
    low = fit_air_regime(
        frame, predictor="marginal", fit_id="sensitivity", concentration=3, draws=8
    )
    high = fit_air_regime(
        frame, predictor="marginal", fit_id="sensitivity", concentration=300, draws=8
    )
    low_id = predict_air_regime(low, frame.head(1)).events["cell_id"][0]
    high_id = predict_air_regime(high, frame.head(1)).events["cell_id"][0]
    assert low_id != high_id


def test_prediction_rejects_duplicate_events_and_incomplete_inputs() -> None:
    frame = training_frame()
    fit = fit_air_regime(frame, predictor="marginal", fit_id="predict", draws=8)
    with pytest.raises(ValueError):
        predict_air_regime(fit, pl.concat([frame.head(1), frame.head(1)]))
    with pytest.raises(ValueError):
        predict_air_regime(fit, frame.drop("season"))
