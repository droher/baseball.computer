"""Spike 4 — Polars rewrite of `event_pitching_flags`.

The dbt model leans on DuckDB's `LAG(... IGNORE NULLS) OVER (PARTITION BY game,
side, pitcher ORDER BY event_id)` to forward-propagate save-eligibility flags
through a pitcher's appearance. The save/hold/blown-save/blown-long-save FSM
is the spike target.

Polars mapping:
  LAG(X IGNORE NULLS) over partition/order
    = X.forward_fill().over(group, order_by=event_id).shift(1).over(group, order_by=event_id)
  LAG(X) (without IGNORE NULLS)
    = X.shift(1).over(group, order_by=event_id)
  LEAD(X) IS NULL
    = X.shift(-1).over(group, order_by=event_id).is_null()

Strategy:
  1. Load `event_states_full` for one season from bc.db via duckdb -> arrow -> polars.
  2. Compute init_flags + save_flags purely with Polars expressions (no map_groups).
  3. Compare against bc.db's `main_models.event_pitching_flags` for the same
     season, joined by `event_key`.
  4. Diff the 4 FSM-derived columns: hold_flag, save_flag, blown_save_flag,
     blown_long_save_flag.
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import duckdb
import polars as pl

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("polars_fsm")

DB_PATH = Path("/Users/davidroher/Repos/baseball.computer/bc.db")


def fsm_polars(states: pl.DataFrame) -> pl.DataFrame:
    """Re-implement event_pitching_flags using only columnar Polars expressions."""
    game_side = ["game_id", "batting_side"]
    appearance = ["game_id", "batting_side", "pitcher_id"]
    order = "event_id"

    # ---- init_flags ----
    df = states.sort([*game_side, "event_id"]).with_columns(
        pl.col("pitcher_id").shift(1).over(game_side, order_by=order).alias("previous_pitcher_id"),
        pl.col("pitcher_id").shift(-1).over(game_side, order_by=order).alias("next_pitcher_id"),
        (pl.col("pitching_team_starting_pitcher_id") == pl.col("pitcher_id")).alias("starting_pitcher_flag"),
    )
    df = df.with_columns(
        # COALESCE(prev != cur, TRUE) — when prev is null (first row of game-side), result is TRUE.
        pl.when(pl.col("previous_pitcher_id").is_null())
        .then(True)
        .otherwise(pl.col("previous_pitcher_id") != pl.col("pitcher_id"))
        .alias("new_pitcher_flag"),
        # COALESCE(next != cur, FALSE) — last row of game-side has next null → FALSE.
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
    )

    df = df.with_columns(
        (
            pl.col("new_pitcher_flag")
            & (pl.col("pitching_team_starting_pitcher_id") != pl.col("pitcher_id"))
        ).alias("new_relief_pitcher_flag"),
        # COALESCE(new_pitcher_flag AND prev = starter, FALSE) → null becomes FALSE.
        (
            pl.col("new_pitcher_flag")
            & (pl.col("previous_pitcher_id") == pl.col("pitching_team_starting_pitcher_id"))
        ).fill_null(False).alias("starting_pitcher_exit_flag"),
    )

    df = df.with_columns(
        (pl.col("starting_pitcher_exit_flag") & (pl.col("inning_in_outs_start") < 15))
        .alias("starting_pitcher_early_exit_flag"),
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
            & ((pl.col("inning_in_outs_start") <= 24) | (pl.col("inning_in_outs_start") % 3 == 0))
        ).alias("save_situation_1_flag"),
        (
            pl.col("save_situation_base")
            & (pl.col("batting_team_margin_start") >= -5)
            & ((pl.col("batting_team_margin_start") + pl.col("runners_count_start") + 2) >= 0)
        ).alias("save_situation_2_flag"),
        # CASE WHEN save_situation_base THEN inning_in_outs_start <= 18 (else NULL).
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
        (pl.col("pitcher_exit_flag") & (pl.col("batting_team_margin_end") < 0))
        .alias("conditional_hold_flag"),
        (pl.col("pitcher_finish_flag") & (pl.col("batting_team_margin_end") < 0))
        .alias("conditional_save_flag"),
        (pl.col("batting_team_margin_end") >= 0).alias("conditional_blown_save_flag"),
    )

    # ---- save_flags ----
    # LAG(X IGNORE NULLS) = forward_fill (over appearance) then shift(1).
    df = df.sort([*appearance, "event_id"]).with_columns(
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

    return df


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--season", type=int, default=2019)
    args = p.parse_args()

    log.info("polars version: %s", pl.__version__)

    con = duckdb.connect(str(DB_PATH), read_only=True)
    states_arrow = con.sql(
        f"""
        SELECT *
        FROM main_models.event_states_full
        WHERE season = {args.season}
        ORDER BY game_id, batting_side, event_id
        """
    ).arrow()
    states = pl.from_arrow(states_arrow)
    log.info("loaded event_states_full season=%s rows=%s", args.season, states.height)

    actual = fsm_polars(states).select(
        "event_key",
        "hold_flag",
        "save_flag",
        "blown_save_flag",
        "blown_long_save_flag",
        "pitcher_exit_flag",
        "pitcher_finish_flag",
        "new_relief_pitcher_flag",
        "save_situation_start_flag",
        "starting_pitcher_flag",
        "starting_pitcher_exit_flag",
        "starting_pitcher_early_exit_flag",
        "inherited_runners",
        "bequeathed_runners",
    ).with_columns(
        pl.col("save_situation_start_flag").fill_null(False),
        pl.col("inherited_runners").cast(pl.UInt8),
        pl.col("bequeathed_runners").cast(pl.UInt8),
    )

    expected_arrow = con.sql(
        f"""
        SELECT
            f.event_key,
            f.hold_flag,
            f.save_flag,
            f.blown_save_flag,
            f.blown_long_save_flag,
            f.pitcher_exit_flag,
            f.pitcher_finish_flag,
            f.new_relief_pitcher_flag,
            f.save_situation_start_flag,
            f.starting_pitcher_flag,
            f.starting_pitcher_exit_flag,
            f.starting_pitcher_early_exit_flag,
            f.inherited_runners,
            f.bequeathed_runners
        FROM main_models.event_pitching_flags f
        JOIN main_models.event_states_full s USING (event_key)
        WHERE s.season = {args.season}
        """
    ).arrow()
    expected = pl.from_arrow(expected_arrow)
    log.info("expected rows=%s, actual rows=%s", expected.height, actual.height)

    expected = expected.sort("event_key")
    actual = actual.sort("event_key")
    assert expected["event_key"].equals(actual["event_key"]), "key alignment broken"

    cols = [c for c in expected.columns if c != "event_key"]
    for c in cols:
        e = expected[c]
        a = actual[c]
        n_diff = (e != a).fill_null(True).sum()
        if n_diff:
            log.warning("col %s: %s mismatched", c, n_diff)
        else:
            log.info("col %s: ok", c)


if __name__ == "__main__":
    main()
