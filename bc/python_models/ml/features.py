"""Feature schema for the Phase 6 plate-appearance-cat model.

One source of truth for which `ml_features` columns are model inputs,
how they're typed, and the encoding strategy. Training, vocabulary
construction, and inference all import from here so they cannot drift.
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

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

TARGET_COLUMN = "outcome_plate_appearance_cat"
SAMPLE_WEIGHT_COLUMN = "plate_appearance_sample_weight"
SPLIT_COLUMN = "meta_train_test_split"
GRAIN_COLUMN = "event_key"

ALL_FEATURE_COLUMNS: tuple[str, ...] = (
    *HIGH_CARD_CATEGORICAL,
    *LOW_CARD_CATEGORICAL,
    *NUMERIC,
)

OOV_TOKEN = "<oov>"


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
