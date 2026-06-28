"""validate_pre_event deny-list behavior."""

from __future__ import annotations

import pytest

from python_models.ml.features import FeatureLayout
from python_models.statistical.deep.feature_layout import (
    PreEventLayoutError,
    validate_pre_event,
)


def _layout(**columns: tuple[str, ...]) -> FeatureLayout:
    return FeatureLayout(
        high_card_columns=columns.get("high_card", ("batter_id", "pitcher_id")),
        low_card_columns=columns.get("low_card", ("league", "frame_start")),
        numeric_columns=columns.get("numeric", ("season",)),
        grain_column="event_key",
        split_column="primary_fold",
    )


def test_clean_layout_passes() -> None:
    validate_pre_event(_layout())


@pytest.mark.parametrize(
    "leak_column",
    [
        "pa_result",
        "result_family",
        "fielder_chain",
        "gap_class",
        "fielding_evidence_status",
        "outs_end",
        "balls_called",
        "hit_or_out",
        "outs_on_play",
        "runs_on_play",
        "score_end",
        "batted_to_fielder_class",
        "batted_to_fielder",
        "strikes_swinging",
        "swings_in_pa",
        "pitches_in_pa",
    ],
)
def test_leak_column_rejected(leak_column: str) -> None:
    layout = _layout(low_card=("league", leak_column))
    with pytest.raises(PreEventLayoutError) as exc_info:
        validate_pre_event(layout)
    assert leak_column in str(exc_info.value)


def test_high_card_leak_detected() -> None:
    layout = _layout(high_card=("batter_id", "pa_result"))
    with pytest.raises(PreEventLayoutError):
        validate_pre_event(layout)


def test_numeric_leak_detected() -> None:
    layout = _layout(numeric=("season", "runs_on_play"))
    with pytest.raises(PreEventLayoutError):
        validate_pre_event(layout)


def test_count_strikes_is_not_leak() -> None:
    layout = _layout(numeric=("season", "count_strikes", "count_balls"))
    validate_pre_event(layout)


def test_inning_start_is_not_leak() -> None:
    layout = _layout(numeric=("season", "inning_start"))
    validate_pre_event(layout)
