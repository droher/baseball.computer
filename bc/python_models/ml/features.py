"""Feature schema for the ML models.

One source of truth for which `ml_features` columns are model inputs,
how they're typed, and the encoding strategy. Targets are described as
`TargetSpec` instances; training, vocabulary construction, and
inference all import from here so they cannot drift.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, Literal

import polars as pl
from pydantic import BaseModel, ConfigDict

# High-cardinality identifier columns get learned embeddings.
HIGH_CARD_CATEGORICAL: tuple[str, ...] = (
    "batter_player",
    "pitcher_player",
    "park_cat",
    "runner_first_player",
    "runner_second_player",
    "runner_third_player",
)

# Low-cardinality categoricals get IntegerLookup → one-hot.
LOW_CARD_CATEGORICAL: tuple[str, ...] = (
    "game_type_cat",
    "league_cat",
    "base_state_cat",
)

NUMERIC: tuple[str, ...] = (
    "season_num",
    "day_of_year_num",
    "inning_num",
    "frame_num",
    "is_night_game_num",
    "score_batting_team_num",
    "score_fielding_team_num",
)

SPLIT_COLUMN = "meta_train_test_split"
GRAIN_COLUMN = "event_key"

ALL_FEATURE_COLUMNS: tuple[str, ...] = (
    *HIGH_CARD_CATEGORICAL,
    *LOW_CARD_CATEGORICAL,
    *NUMERIC,
)

OOV_TOKEN = "<oov>"

TargetKind = Literal["multiclass", "binary", "regression"]


class TargetSpec(BaseModel):
    """Describes one trainable target on `ml_features`.

    `name` is used as the experiment name, the artifact filename stem,
    the Keras model `name`, and the output layer `name`. Anything that
    needs a per-target identifier should derive it from here.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    name: str
    target_column: str
    weight_column: str
    kind: TargetKind
    # When True, training drops rows with weight_column == 0 even for
    # multiclass targets. Binary and regression targets always drop them.
    filter_zero_weight: bool = False


PLATE_APPEARANCE_CAT = TargetSpec(
    name="plate_appearance_cat",
    target_column="outcome_plate_appearance_cat",
    weight_column="plate_appearance_sample_weight",
    kind="multiclass",
)

IS_IN_PLAY_BIN = TargetSpec(
    name="is_in_play_bin",
    target_column="outcome_is_in_play_bin",
    weight_column="in_play_sample_weight",
    kind="binary",
)

BATTED_TRAJECTORY_CAT = TargetSpec(
    name="batted_trajectory_cat",
    target_column="outcome_batted_trajectory_cat",
    weight_column="trajectory_sample_weight",
    kind="multiclass",
    filter_zero_weight=True,
)

BATTED_LOCATION_CAT = TargetSpec(
    name="batted_location_cat",
    target_column="outcome_batted_location_cat",
    weight_column="location_sample_weight",
    kind="multiclass",
    filter_zero_weight=True,
)

BASERUNNING_CAT = TargetSpec(
    name="baserunning_cat",
    target_column="outcome_baserunning_cat",
    weight_column="baserunning_play_sample_weight",
    kind="multiclass",
)

RUNS_FOLLOWING_NUM = TargetSpec(
    name="runs_following_num",
    target_column="outcome_runs_following_num",
    weight_column="generic_sample_weight",
    kind="regression",
)

IS_WIN_BIN = TargetSpec(
    name="is_win_bin",
    target_column="outcome_is_win_bin",
    weight_column="win_sample_weight",
    kind="binary",
)

HAS_BATTING_BIN = TargetSpec(
    name="has_batting_bin",
    target_column="outcome_has_batting_bin",
    weight_column="generic_sample_weight",
    kind="binary",
)

ALL_TARGETS: tuple[TargetSpec, ...] = (
    PLATE_APPEARANCE_CAT,
    IS_IN_PLAY_BIN,
    BATTED_TRAJECTORY_CAT,
    BATTED_LOCATION_CAT,
    BASERUNNING_CAT,
    RUNS_FOLLOWING_NUM,
    IS_WIN_BIN,
    HAS_BATTING_BIN,
)


def target_by_name(name: str) -> TargetSpec:
    for spec in ALL_TARGETS:
        if spec.name == name:
            return spec
    raise KeyError(f"unknown target {name!r}; known: {[s.name for s in ALL_TARGETS]}")


@dataclass(frozen=True)
class Vocabulary:
    """Stable string-to-int mapping for a categorical feature.

    Index 0 is reserved for unseen values at inference time. The list
    in `values` is the lookup table; `values[i]` is the label for
    integer code `i + 1` (so index 0 → OOV).
    """

    column: str
    values: tuple[str, ...]

    @property
    def size(self) -> int:
        return len(self.values) + 1  # +1 for OOV slot at index 0

    def encode(self, series: pl.Series) -> pl.Series:
        cast = series.cast(pl.Utf8)
        index_map = {v: i + 1 for i, v in enumerate(self.values)}
        return cast.replace_strict(index_map, default=0).cast(pl.Int64)


def build_vocabulary(series: pl.Series, column: str) -> Vocabulary:
    distinct = (
        series.cast(pl.Utf8)
        .drop_nulls()
        .unique()
        .sort()
        .to_list()
    )
    return Vocabulary(column=column, values=tuple(distinct))


def target_class_labels(series: pl.Series) -> tuple[str, ...]:
    raw: list[str] = series.cast(pl.Utf8).drop_nulls().unique().to_list()
    return tuple(sorted(raw))
