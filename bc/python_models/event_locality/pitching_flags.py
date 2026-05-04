"""Polars FSM for `event_pitching_flags`.

Mirrors the original SQL stacked-CTE pipeline (init_flags → save_flags →
final) using forward-filled shifts for the four `LAG IGNORE NULLS`
windows in save_flags. See sibling `__init__.py` for the lag mapping.
"""

from __future__ import annotations

import polars as pl

PITCHING_FLAGS_INPUT_COLUMNS: tuple[str, ...] = (
    "game_id",
    "event_key",
    "event_id",
    "batting_side",
    "pitcher_id",
    "pitching_team_starting_pitcher_id",
    "batting_team_margin_start",
    "batting_team_margin_end",
    "inning_in_outs_start",
    "runners_count_start",
)

PITCHING_FLAGS_OUTPUT_COLUMNS: tuple[str, ...] = (
    "game_id",
    "event_key",
    "event_id",
    "previous_pitcher_id",
    "pitcher_id",
    "starting_pitcher_flag",
    "bequeathed_runners",
    "inherited_runners",
    "new_relief_pitcher_flag",
    "pitcher_exit_flag",
    "pitcher_finish_flag",
    "starting_pitcher_exit_flag",
    "starting_pitcher_early_exit_flag",
    "save_situation_start_flag",
    "hold_flag",
    "save_flag",
    "blown_save_flag",
    "blown_long_save_flag",
)


def compute_pitching_flags(events: pl.DataFrame) -> pl.DataFrame:
    """Compute the 18-column `event_pitching_flags` table from event states.

    Input must contain at least the columns in
    `PITCHING_FLAGS_INPUT_COLUMNS`. Output schema matches the SQLMesh
    model: columns in declaration order, integer flags packed back into
    `UInt8` for the runner counts and Booleans elsewhere.
    """
    game_side = ["game_id", "batting_side"]
    appearance = ["game_id", "batting_side", "pitcher_id"]
    order = "event_id"

    df = events.sort([*game_side, order]).with_columns(
        pl.col("pitcher_id")
        .shift(1)
        .over(game_side, order_by=order)
        .alias("previous_pitcher_id"),
        pl.col("pitcher_id")
        .shift(-1)
        .over(game_side, order_by=order)
        .alias("next_pitcher_id"),
        (pl.col("pitching_team_starting_pitcher_id") == pl.col("pitcher_id")).alias(
            "starting_pitcher_flag"
        ),
    )

    df = df.with_columns(
        pl.when(pl.col("previous_pitcher_id").is_null())
        .then(True)
        .otherwise(pl.col("previous_pitcher_id") != pl.col("pitcher_id"))
        .alias("new_pitcher_flag"),
        pl.when(pl.col("next_pitcher_id").is_null())
        .then(False)
        .otherwise(pl.col("next_pitcher_id") != pl.col("pitcher_id"))
        .alias("pitcher_exit_flag"),
        pl.col("next_pitcher_id").is_null().alias("pitcher_finish_flag"),
    )

    df = df.with_columns(
        pl.when(pl.col("new_pitcher_flag"))
        .then(pl.col("runners_count_start"))
        .otherwise(0)
        .alias("inherited_runners"),
        pl.when(pl.col("pitcher_exit_flag"))
        .then(pl.col("runners_count_start").shift(-1).over(game_side, order_by=order))
        .otherwise(0)
        .alias("bequeathed_runners"),
        (
            pl.col("new_pitcher_flag")
            & (pl.col("pitching_team_starting_pitcher_id") != pl.col("pitcher_id"))
        ).alias("new_relief_pitcher_flag"),
        (
            pl.col("new_pitcher_flag")
            & (
                pl.col("previous_pitcher_id")
                == pl.col("pitching_team_starting_pitcher_id")
            )
        )
        .fill_null(False)
        .alias("starting_pitcher_exit_flag"),
    )

    df = df.with_columns(
        (
            pl.col("starting_pitcher_exit_flag") & (pl.col("inning_in_outs_start") < 15)
        ).alias("starting_pitcher_early_exit_flag"),
    )

    df = df.with_columns(
        (
            pl.col("new_relief_pitcher_flag")
            & ~pl.col("starting_pitcher_early_exit_flag")
            & (pl.col("batting_team_margin_start") < 0)
        ).alias("save_situation_base"),
    )

    df = df.with_columns(
        (
            pl.col("save_situation_base")
            & (pl.col("batting_team_margin_start") >= -3)
            & (
                (pl.col("inning_in_outs_start") <= 24)
                | (pl.col("inning_in_outs_start") % 3 == 0)
            )
        ).alias("save_situation_1_flag"),
        (
            pl.col("save_situation_base")
            & (pl.col("batting_team_margin_start") >= -5)
            & (
                (
                    pl.col("batting_team_margin_start")
                    + pl.col("runners_count_start")
                    + 2
                )
                >= 0
            )
        ).alias("save_situation_2_flag"),
        pl.when(pl.col("save_situation_base"))
        .then(pl.col("inning_in_outs_start") <= 18)
        .otherwise(None)
        .alias("long_save_eligible_start_flag"),
    )

    df = df.with_columns(
        pl.when(pl.col("new_pitcher_flag"))
        .then(pl.col("save_situation_1_flag") | pl.col("save_situation_2_flag"))
        .otherwise(None)
        .alias("save_situation_start_flag"),
        pl.when(pl.col("new_pitcher_flag"))
        .then(
            pl.col("save_situation_1_flag")
            | pl.col("save_situation_2_flag")
            | pl.col("long_save_eligible_start_flag")
        )
        .otherwise(None)
        .alias("save_eligible_start_flag"),
        (pl.col("pitcher_exit_flag") & (pl.col("batting_team_margin_end") < 0)).alias(
            "conditional_hold_flag"
        ),
        (pl.col("pitcher_finish_flag") & (pl.col("batting_team_margin_end") < 0)).alias(
            "conditional_save_flag"
        ),
        (pl.col("batting_team_margin_end") >= 0).alias("conditional_blown_save_flag"),
    )

    df = df.sort([*appearance, order]).with_columns(
        pl.col("save_situation_start_flag")
        .forward_fill()
        .over(appearance, order_by=order)
        .shift(1)
        .over(appearance, order_by=order)
        .alias("lag_save_situation_start_flag"),
        pl.col("save_eligible_start_flag")
        .forward_fill()
        .over(appearance, order_by=order)
        .shift(1)
        .over(appearance, order_by=order)
        .alias("lag_save_eligible_start_flag"),
        pl.col("long_save_eligible_start_flag")
        .forward_fill()
        .over(appearance, order_by=order)
        .shift(1)
        .over(appearance, order_by=order)
        .alias("lag_long_save_eligible_start_flag"),
        pl.col("conditional_blown_save_flag")
        .shift(1)
        .over(appearance, order_by=order)
        .alias("lag_conditional_blown_save_flag"),
    )

    df = df.with_columns(
        pl.when(pl.col("lag_save_situation_start_flag"))
        .then(pl.col("conditional_hold_flag"))
        .otherwise(False)
        .alias("hold_flag"),
        pl.when(pl.col("lag_save_eligible_start_flag"))
        .then(pl.col("conditional_save_flag"))
        .otherwise(False)
        .alias("save_flag"),
        pl.when(
            pl.col("lag_save_eligible_start_flag")
            & ~pl.col("lag_conditional_blown_save_flag").fill_null(False)
        )
        .then(pl.col("conditional_blown_save_flag"))
        .otherwise(False)
        .alias("blown_save_flag"),
        pl.when(
            pl.col("lag_long_save_eligible_start_flag")
            & ~pl.col("lag_save_situation_start_flag").fill_null(False)
            & ~pl.col("lag_conditional_blown_save_flag").fill_null(False)
        )
        .then(pl.col("conditional_blown_save_flag"))
        .otherwise(False)
        .alias("blown_long_save_flag"),
    )

    return df.select(
        pl.col("game_id"),
        pl.col("event_key"),
        pl.col("event_id"),
        pl.col("previous_pitcher_id"),
        pl.col("pitcher_id"),
        pl.col("starting_pitcher_flag"),
        pl.col("bequeathed_runners").cast(pl.UInt8),
        pl.col("inherited_runners").cast(pl.UInt8),
        pl.col("new_relief_pitcher_flag").fill_null(False),
        pl.col("pitcher_exit_flag").fill_null(False),
        pl.col("pitcher_finish_flag").fill_null(False),
        pl.col("starting_pitcher_exit_flag").fill_null(False),
        pl.col("starting_pitcher_early_exit_flag").fill_null(False),
        pl.col("save_situation_start_flag").fill_null(False),
        pl.col("hold_flag").fill_null(False),
        pl.col("save_flag").fill_null(False),
        pl.col("blown_save_flag").fill_null(False),
        pl.col("blown_long_save_flag").fill_null(False),
    )
