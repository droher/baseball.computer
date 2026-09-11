from __future__ import annotations

import numpy as np
import polars as pl
import pytest
from hypothesis import given, strategies as st
from pydantic import ValidationError

from python_models.statistical.backtests.geometry_air_translation import (
    AIR_CLASSES,
    AirTranslationFit,
    fit_air_translation,
    predict_air_translation,
    predict_preserving_broad_type,
)


def training_frame(rows: list[tuple[str, str, str]]) -> pl.DataFrame:
    return (
        pl.DataFrame(
            rows,
            schema=["recorded_air_subtype", "result_family", "target_class"],
            orient="row",
        )
        .with_row_index("event_key")
        .with_columns(
            pl.lit(True).alias("known_air_evaluation_eligible"),
            pl.lit("Air").alias("recorded_broad_type"),
        )
    )


ROWS = st.lists(
    st.tuples(
        st.sampled_from(AIR_CLASSES),
        st.sampled_from(("hit", "out", "home_run")),
        st.sampled_from(AIR_CLASSES),
    ),
    min_size=1,
    max_size=80,
)


@given(rows=ROWS, strength=st.floats(min_value=0.1, max_value=1000))
def test_count_conservation_simplexes_and_permutation_equivariance(
    rows: list[tuple[str, str, str]], strength: float
) -> None:
    frame = training_frame(rows)
    fit = fit_air_translation(frame, prior_strength=strength)
    restored = AirTranslationFit.model_validate_json(fit.model_dump_json())
    assert restored == fit
    assert sum(sum(cell.counts) for cell in fit.cells) == frame.height
    assert fit_air_translation(frame.reverse(), prior_strength=strength) == fit
    for predictor in ("marginal", "result", "recorded", "recorded_result"):
        probabilities = predict_air_translation(fit, frame, predictor=predictor)
        assert probabilities.shape == (frame.height, len(AIR_CLASSES))
        assert np.isfinite(probabilities).all()
        assert (probabilities > 0).all()
        np.testing.assert_allclose(probabilities.sum(axis=1), 1)
        np.testing.assert_array_equal(
            probabilities[::-1],
            predict_air_translation(fit, frame.reverse(), predictor=predictor),
        )


@given(rows=ROWS)
def test_masked_recorded_subtype_uses_only_result_information(
    rows: list[tuple[str, str, str]],
) -> None:
    frame = training_frame(rows)
    fit = fit_air_translation(frame)
    masked = frame.with_columns(
        pl.lit(None, dtype=pl.String).alias("recorded_air_subtype"),
        pl.lit("secret_ground_target").alias("target_class"),
        pl.lit(999.0).alias("launch_angle"),
        pl.lit("secret_original_subtype").alias("recorded_class"),
    )
    np.testing.assert_array_equal(
        predict_air_translation(fit, masked),
        predict_air_translation(fit, frame, predictor="result"),
    )
    unknown_results = frame.with_columns(pl.lit("unseen").alias("result_family"))
    np.testing.assert_array_equal(
        predict_air_translation(fit, unknown_results),
        predict_air_translation(fit, frame, predictor="recorded"),
    )


def test_prediction_preserves_ground_air_and_abstains_without_broad_type() -> None:
    frame = training_frame([(label, "out", label) for label in AIR_CLASSES])
    fit = fit_air_translation(frame)
    inputs = frame.with_columns(
        pl.Series("recorded_broad_type", ["Ground", "Air", None]),
        pl.Series("recorded_air_subtype", [None, "LineDrive", None], dtype=pl.String),
    )
    output = predict_preserving_broad_type(fit, inputs)
    assert output["event_key"].equals(frame["event_key"])
    assert output["probabilities"].to_list()[0] == [1.0, 0.0, 0.0, 0.0]
    assert output["probabilities"].to_list()[1][0] == 0
    assert output["probabilities"].to_list()[2] is None
    source_block = inputs.select("event_key").with_columns(
        pl.lit(None, dtype=pl.String).alias("recorded_broad_type")
    )
    assert (
        predict_preserving_broad_type(fit, source_block)["probabilities"].null_count()
        == frame.height
    )
    ground_only = source_block.with_columns(
        pl.lit("Ground").alias("recorded_broad_type")
    )
    assert set(
        predict_preserving_broad_type(fit, ground_only)["prediction_status"]
    ) == {"ground_preserved"}


@pytest.mark.parametrize("strength", [0.0, -1.0, float("nan"), float("inf")])
def test_invalid_smoothing_is_rejected(strength: float) -> None:
    frame = training_frame([("Fly", "out", "PopUp")])
    with pytest.raises(ValidationError):
        fit_air_translation(frame, prior_strength=strength)


def test_training_rejects_unresolved_unknown_and_duplicate_events() -> None:
    frame = training_frame([("Fly", "out", "PopUp")])
    for invalid in (
        pl.concat([frame, frame]),
        frame.with_columns(pl.lit(False).alias("known_air_evaluation_eligible")),
        frame.with_columns(pl.lit("GroundBall").alias("target_class")),
        frame.with_columns(pl.lit(None).alias("target_class")),
        frame.with_columns(pl.lit("").alias("result_family")),
    ):
        with pytest.raises(ValueError):
            fit_air_translation(invalid)


def test_corrupt_serialized_counts_cannot_be_used_as_a_fit() -> None:
    fit = fit_air_translation(training_frame([("Fly", "out", "PopUp")]))
    values = fit.model_dump()
    values["training_events"] = fit.training_events + 1
    with pytest.raises(ValidationError):
        AirTranslationFit.model_validate(values)
