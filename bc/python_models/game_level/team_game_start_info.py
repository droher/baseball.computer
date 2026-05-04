"""Polars FSM for `team_game_start_info`.

Mirrors the SQL `add_series_start_flag` → `assign_series_id` → `final`
stages. The series-id forward-fill is the inclusive form
`COALESCE(LAG(series_id IGNORE NULLS) OVER ..., series_id)` — Polars'
`forward_fill().over(...)` already covers the inclusive case.
"""

from __future__ import annotations

import polars as pl

TEAM_GAME_START_INFO_OUTPUT_COLUMNS: tuple[str, ...] = (
    "series_id",
    "season_game_number",
    "series_game_number",
    "days_since_last_game",
)


def compute_team_game_start_info(rows: pl.DataFrame) -> pl.DataFrame:
    """Add series-id, season/series game number, and rest-day columns.

    Input is the unioned per-team rowset from `game_start_info` (Home and
    Away rows, joined with franchise/league info). Must contain at least:
    `season`, `team_id`, `game_type`, `opponent_id`, `date`,
    `doubleheader_status`, `game_id`.

    Output preserves all input columns and appends the four columns in
    `TEAM_GAME_START_INFO_OUTPUT_COLUMNS`.
    """
    series_partition = ["season", "team_id", "game_type", "opponent_id"]
    series_order = ["date", "doubleheader_status"]
    season_partition = ["season", "team_id", "game_type"]
    season_order = ["date", "doubleheader_status"]

    df = rows.sort([*series_partition, *series_order])

    prev_opponent = (
        pl.col("opponent_id")
        .cast(pl.String)
        .shift(1)
        .over(series_partition, order_by=series_order)
        .fill_null("N/A")
    )

    df = df.with_columns(
        pl.when(prev_opponent != pl.col("opponent_id").cast(pl.String))
        .then(pl.col("game_id"))
        .otherwise(None)
        .alias("_series_id_start"),
    )

    df = df.with_columns(
        pl.col("_series_id_start")
        .forward_fill()
        .over(series_partition, order_by=series_order)
        .alias("series_id"),
    ).drop("_series_id_start")

    series_count_partition = ["team_id", "series_id"]

    df = df.with_columns(
        pl.col("game_id")
        .cum_count()
        .over(season_partition, order_by=season_order)
        .cast(pl.Int64)
        .alias("season_game_number"),
        pl.col("game_id")
        .cum_count()
        .over(series_count_partition, order_by=series_order)
        .cast(pl.Int64)
        .alias("series_game_number"),
        (
            pl.col("date")
            - pl.col("date").shift(1).over(season_partition, order_by=season_order)
        )
        .dt.total_days()
        .cast(pl.Int64)
        .alias("days_since_last_game"),
    )

    return df
