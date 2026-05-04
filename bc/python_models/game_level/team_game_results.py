"""Polars FSM for `team_game_results`.

Mirrors the original SQL pipeline (`enriched` → `streak_calc_continued` →
`final`) using forward-fill plus cumulative count for the win/loss streak
columns. See `python_models/event_locality/__init__.py` for the lag
mapping convention.
"""

from __future__ import annotations

import polars as pl

TEAM_GAME_RESULTS_INPUT_COLUMNS: tuple[str, ...] = (
    "season",
    "game_id",
    "game_finish_date",
    "team_id",
    "game_type",
    "team_side",
    "league",
    "division",
    "opponent_league",
    "opponent_division",
    "season_game_number",
    "is_interleague",
    "wins",
    "losses",
    "runs_scored",
    "runs_allowed",
    "hits",
    "errors",
    "left_on_base",
    "at_bats",
    "doubles",
    "triples",
    "home_runs",
    "runs_batted_in",
    "sacrifice_hits",
    "sacrifice_flies",
    "hit_by_pitches",
    "walks",
    "intentional_walks",
    "strikeouts",
    "stolen_bases",
    "caught_stealing",
    "grounded_into_double_plays",
    "reached_on_interferences",
    "innings_pitched",
    "individual_earned_runs_allowed",
    "earned_runs_allowed",
    "wild_pitches",
    "balks",
    "putouts",
    "assists",
    "passed_balls",
    "double_plays_turned",
    "triple_plays_turned",
    "opponent_team_id",
    "opponent_runs",
    "opponent_hits",
    "opponent_errors",
    "opponent_left_on_base",
    "opponent_at_bats",
    "opponent_doubles",
    "opponent_triples",
    "opponent_home_runs",
    "opponent_runs_batted_in",
    "opponent_sacrifice_hits",
    "opponent_sacrifice_flies",
    "opponent_hit_by_pitches",
    "opponent_walks",
    "opponent_intentional_walks",
    "opponent_strikeouts",
    "opponent_stolen_bases",
    "opponent_caught_stealing",
    "opponent_grounded_into_double_plays",
    "opponent_reached_on_interferences",
    "opponent_innings_pitched",
    "opponent_individual_earned_runs_allowed",
    "opponent_earned_runs_allowed",
    "opponent_wild_pitches",
    "opponent_balks",
    "opponent_putouts",
    "opponent_assists",
    "opponent_passed_balls",
    "opponent_double_plays",
    "opponent_triple_plays",
)

TEAM_GAME_RESULTS_OUTPUT_COLUMNS: tuple[str, ...] = (
    *TEAM_GAME_RESULTS_INPUT_COLUMNS,
    "home_wins",
    "home_losses",
    "away_wins",
    "away_losses",
    "interleague_wins",
    "interleague_losses",
    "east_wins",
    "east_losses",
    "central_wins",
    "central_losses",
    "west_wins",
    "west_losses",
    "one_run_wins",
    "one_run_losses",
    "win_streak_id",
    "loss_streak_id",
    "win_streak_length",
    "loss_streak_length",
)


def compute_team_game_results(games: pl.DataFrame) -> pl.DataFrame:
    """Compute the `team_game_results` table from the joined upstream rowset.

    `games` must contain at least `TEAM_GAME_RESULTS_INPUT_COLUMNS`.
    Returns the table with the four win/loss-streak columns plus the
    twelve home/away/interleague/division/one-run split counts.
    """
    partition = ["season", "team_id", "game_type"]
    order = ["game_finish_date", "season_game_number"]

    df = games.sort([*partition, *order])

    df = df.with_columns(
        pl.when(pl.col("team_side") == "Home")
        .then(pl.col("wins"))
        .otherwise(0)
        .alias("home_wins"),
        pl.when(pl.col("team_side") == "Home")
        .then(pl.col("losses"))
        .otherwise(0)
        .alias("home_losses"),
        pl.when(pl.col("team_side") == "Away")
        .then(pl.col("wins"))
        .otherwise(0)
        .alias("away_wins"),
        pl.when(pl.col("team_side") == "Away")
        .then(pl.col("losses"))
        .otherwise(0)
        .alias("away_losses"),
        pl.when(pl.col("is_interleague"))
        .then(pl.col("wins"))
        .otherwise(0)
        .alias("interleague_wins"),
        pl.when(pl.col("is_interleague"))
        .then(pl.col("losses"))
        .otherwise(0)
        .alias("interleague_losses"),
        pl.when(~pl.col("is_interleague") & (pl.col("opponent_division") == "E"))
        .then(pl.col("wins"))
        .otherwise(0)
        .alias("east_wins"),
        pl.when(~pl.col("is_interleague") & (pl.col("opponent_division") == "E"))
        .then(pl.col("losses"))
        .otherwise(0)
        .alias("east_losses"),
        pl.when(~pl.col("is_interleague") & (pl.col("opponent_division") == "C"))
        .then(pl.col("wins"))
        .otherwise(0)
        .alias("central_wins"),
        pl.when(~pl.col("is_interleague") & (pl.col("opponent_division") == "C"))
        .then(pl.col("losses"))
        .otherwise(0)
        .alias("central_losses"),
        pl.when(~pl.col("is_interleague") & (pl.col("opponent_division") == "W"))
        .then(pl.col("wins"))
        .otherwise(0)
        .alias("west_wins"),
        pl.when(~pl.col("is_interleague") & (pl.col("opponent_division") == "W"))
        .then(pl.col("losses"))
        .otherwise(0)
        .alias("west_losses"),
        (
            (
                pl.col("runs_scored").cast(pl.Int32)
                - pl.col("runs_allowed").cast(pl.Int32)
            ).abs()
            == 1
        ).alias("_one_run_game"),
    )

    df = df.with_columns(
        pl.when(pl.col("_one_run_game"))
        .then(pl.col("wins"))
        .otherwise(0)
        .alias("one_run_wins"),
        pl.when(pl.col("_one_run_game"))
        .then(pl.col("losses"))
        .otherwise(0)
        .alias("one_run_losses"),
    ).drop("_one_run_game")

    is_win = pl.col("wins") == 1
    is_loss = pl.col("losses") == 1
    prev_win = is_win.shift(1).over(partition, order_by=order).fill_null(False)
    prev_loss = is_loss.shift(1).over(partition, order_by=order).fill_null(False)

    df = df.with_columns(
        pl.when(is_win & ~prev_win)
        .then(pl.col("season_game_number"))
        .otherwise(None)
        .alias("_win_streak_id_start"),
        pl.when(is_loss & ~prev_loss)
        .then(pl.col("season_game_number"))
        .otherwise(None)
        .alias("_loss_streak_id_start"),
    )

    df = df.with_columns(
        pl.when(is_win)
        .then(
            pl.col("_win_streak_id_start")
            .forward_fill()
            .over(partition, order_by=order)
        )
        .otherwise(None)
        .alias("win_streak_id"),
        pl.when(is_loss)
        .then(
            pl.col("_loss_streak_id_start")
            .forward_fill()
            .over(partition, order_by=order)
        )
        .otherwise(None)
        .alias("loss_streak_id"),
    ).drop("_win_streak_id_start", "_loss_streak_id_start")

    win_partition = [*partition, "win_streak_id"]
    loss_partition = [*partition, "loss_streak_id"]

    df = df.with_columns(
        pl.when(is_win)
        .then(
            pl.col("season_game_number").cum_count().over(win_partition, order_by=order)
        )
        .otherwise(0)
        .alias("win_streak_length"),
        pl.when(is_loss)
        .then(
            pl.col("season_game_number")
            .cum_count()
            .over(loss_partition, order_by=order)
        )
        .otherwise(0)
        .alias("loss_streak_length"),
    )

    return df.select(
        pl.col("season").cast(pl.Int16),
        pl.col("game_id"),
        pl.col("game_finish_date"),
        pl.col("team_id"),
        pl.col("game_type"),
        pl.col("team_side"),
        pl.col("league"),
        pl.col("division"),
        pl.col("opponent_league"),
        pl.col("opponent_division"),
        pl.col("season_game_number").cast(pl.Int64),
        pl.col("is_interleague"),
        pl.col("wins").cast(pl.Int32),
        pl.col("losses").cast(pl.Int32),
        pl.col("runs_scored").cast(pl.UInt8),
        pl.col("runs_allowed").cast(pl.UInt8),
        pl.col("hits").cast(pl.UInt16),
        pl.col("errors").cast(pl.UInt8),
        pl.col("left_on_base").cast(pl.UInt16),
        pl.col("at_bats").cast(pl.UInt16),
        pl.col("doubles").cast(pl.UInt16),
        pl.col("triples").cast(pl.UInt16),
        pl.col("home_runs").cast(pl.UInt16),
        pl.col("runs_batted_in").cast(pl.UInt16),
        pl.col("sacrifice_hits").cast(pl.UInt16),
        pl.col("sacrifice_flies").cast(pl.UInt16),
        pl.col("hit_by_pitches").cast(pl.UInt16),
        pl.col("walks").cast(pl.UInt16),
        pl.col("intentional_walks").cast(pl.UInt16),
        pl.col("strikeouts").cast(pl.UInt16),
        pl.col("stolen_bases").cast(pl.UInt16),
        pl.col("caught_stealing").cast(pl.UInt16),
        pl.col("grounded_into_double_plays").cast(pl.UInt16),
        pl.col("reached_on_interferences").cast(pl.UInt16),
        pl.col("innings_pitched"),
        pl.col("individual_earned_runs_allowed").cast(pl.UInt16),
        pl.col("earned_runs_allowed").cast(pl.UInt8),
        pl.col("wild_pitches").cast(pl.UInt16),
        pl.col("balks").cast(pl.UInt16),
        pl.col("putouts").cast(pl.UInt8),
        pl.col("assists").cast(pl.UInt8),
        pl.col("passed_balls").cast(pl.UInt8),
        pl.col("double_plays_turned").cast(pl.UInt8),
        pl.col("triple_plays_turned").cast(pl.UInt8),
        pl.col("opponent_team_id"),
        pl.col("opponent_runs").cast(pl.UInt16),
        pl.col("opponent_hits").cast(pl.UInt16),
        pl.col("opponent_errors").cast(pl.UInt8),
        pl.col("opponent_left_on_base").cast(pl.UInt16),
        pl.col("opponent_at_bats").cast(pl.UInt16),
        pl.col("opponent_doubles").cast(pl.UInt16),
        pl.col("opponent_triples").cast(pl.UInt16),
        pl.col("opponent_home_runs").cast(pl.UInt16),
        pl.col("opponent_runs_batted_in").cast(pl.UInt16),
        pl.col("opponent_sacrifice_hits").cast(pl.UInt16),
        pl.col("opponent_sacrifice_flies").cast(pl.UInt16),
        pl.col("opponent_hit_by_pitches").cast(pl.UInt16),
        pl.col("opponent_walks").cast(pl.UInt16),
        pl.col("opponent_intentional_walks").cast(pl.UInt16),
        pl.col("opponent_strikeouts").cast(pl.UInt16),
        pl.col("opponent_stolen_bases").cast(pl.UInt16),
        pl.col("opponent_caught_stealing").cast(pl.UInt16),
        pl.col("opponent_grounded_into_double_plays").cast(pl.UInt16),
        pl.col("opponent_reached_on_interferences").cast(pl.UInt16),
        pl.col("opponent_innings_pitched"),
        pl.col("opponent_individual_earned_runs_allowed").cast(pl.UInt16),
        pl.col("opponent_earned_runs_allowed").cast(pl.UInt8),
        pl.col("opponent_wild_pitches").cast(pl.UInt16),
        pl.col("opponent_balks").cast(pl.UInt16),
        pl.col("opponent_putouts").cast(pl.UInt8),
        pl.col("opponent_assists").cast(pl.UInt8),
        pl.col("opponent_passed_balls").cast(pl.UInt8),
        pl.col("opponent_double_plays").cast(pl.UInt8),
        pl.col("opponent_triple_plays").cast(pl.UInt8),
        pl.col("home_wins").cast(pl.Int32),
        pl.col("home_losses").cast(pl.Int32),
        pl.col("away_wins").cast(pl.Int32),
        pl.col("away_losses").cast(pl.Int32),
        pl.col("interleague_wins").cast(pl.Int32),
        pl.col("interleague_losses").cast(pl.Int32),
        pl.col("east_wins").cast(pl.Int32),
        pl.col("east_losses").cast(pl.Int32),
        pl.col("central_wins").cast(pl.Int32),
        pl.col("central_losses").cast(pl.Int32),
        pl.col("west_wins").cast(pl.Int32),
        pl.col("west_losses").cast(pl.Int32),
        pl.col("one_run_wins").cast(pl.Int32),
        pl.col("one_run_losses").cast(pl.Int32),
        pl.col("win_streak_id").cast(pl.Int64),
        pl.col("loss_streak_id").cast(pl.Int64),
        pl.col("win_streak_length").cast(pl.Int64),
        pl.col("loss_streak_length").cast(pl.Int64),
    )
