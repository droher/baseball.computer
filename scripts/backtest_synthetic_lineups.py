"""Backtest the synthetic-lineup optimizer against real Retrosheet box-score lineups.

Reruns the same MILP optimizer that powers
``synthetic_box_score.lineup_assignments`` on box-score-era games where
real starting lineups exist, using only the gamelog-only feature set
(Lahman/Databank season inputs + gamelog starting pitcher + DH flag).

Measurable seasons: 1871, 1872, 1874, 1898-1910.

Reads ``bc.db`` read-only. Writes:
    logs/synthetic_lineup_backtest/comparison.parquet
    notes/synthetic-lineup-backtest-1871-1910.md

Usage::

    uv run --group build python scripts/backtest_synthetic_lineups.py
    uv run --group build python scripts/backtest_synthetic_lineups.py --seasons 1898
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from collections.abc import Iterable
from pathlib import Path

import duckdb
import polars as pl

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "bc"))

from python_models.synthetic_box_scores import (  # noqa: E402
    build_synthetic_lineup_assignments,
    compute_modal_lineups,
)
from python_models.synthetic_box_scores.game_lineups import (  # noqa: E402
    CANDIDATE_INPUT_COLUMNS,
    GAME_INPUT_COLUMNS,
    LINEUP_INPUT_COLUMNS,
)
from python_models.synthetic_box_scores.transactions import (  # noqa: E402
    fetch_tran_db,
    parse_team_changes,
    transaction_stint_windows,
)

_LOG = logging.getLogger("backtest_synthetic_lineups")

_TRAN_DB_CACHE = REPO_ROOT / ".cache" / "tranDB.zip"


def _build_transaction_windows(
    games: pl.DataFrame,
    candidates: pl.DataFrame,
) -> dict[tuple[int, str, str, int], tuple[int, int]]:
    """Materialize the season → ordered date list and ask the transactions
    module for date-bounded stint windows. Returns an empty dict if any
    upstream step fails so the optimizer falls back to proportional."""
    try:
        zip_path = fetch_tran_db(_TRAN_DB_CACHE)
        team_changes = parse_team_changes(zip_path)
    except Exception as exc:  # noqa: BLE001
        _LOG.warning("transaction fetch/parse failed (%s); falling back", exc)
        return {}

    season_dates_df = (
        games.select(["season", "date"])
        .unique()
        .with_columns(pl.col("date").dt.strftime("%Y-%m-%d").alias("date_key"))
        .sort(["season", "date_key"])
    )
    season_dates: dict[int, list[str]] = {}
    for season, frame in season_dates_df.group_by("season"):
        season_dates[int(season[0])] = frame["date_key"].to_list()

    candidate_stints: list[tuple[int, str, str, int]] = [
        (int(season), str(team_id), str(player_id), int(stint))
        for season, team_id, player_id, stint in candidates.select(
            ["season", "team_id", "player_id", "stint"]
        )
        .unique()
        .iter_rows()
    ]

    return transaction_stint_windows(
        candidate_stints=candidate_stints,
        season_dates=season_dates,
        team_changes=team_changes,
    )

_BACKTEST_SEASONS: tuple[int, ...] = (
    1871,
    1872,
    1874,
    1898,
    1899,
    1900,
    1901,
    1902,
    1903,
    1904,
    1905,
    1906,
    1907,
    1908,
    1909,
    1910,
)


_BUILD_LOCAL_APPEARANCES_SQL = """
CREATE OR REPLACE TEMP TABLE _bt_appearances AS
WITH team_id_crosswalk AS (
    SELECT
        year_id,
        team_id AS databank_team_id,
        team_id_retro AS team_id
    FROM baseballdatabank.teams
),

games_total AS (
    SELECT
        a.player_id AS databank_player_id,
        a.year_id AS season,
        t.team_id AS team_id,
        a.lg_id AS league_id,
        a.g_all AS games_total
    FROM baseballdatabank.appearances AS a
    INNER JOIN team_id_crosswalk AS t
        ON a.year_id = t.year_id AND a.team_id = t.databank_team_id
),

fielding_source AS (
    SELECT
        f.player_id AS databank_player_id,
        f.year_id AS season,
        t.team_id AS team_id,
        f.lg_id AS league_id,
        f.stint,
        CASE f.pos
            WHEN 'P' THEN 1
            WHEN 'C' THEN 2
            WHEN '1B' THEN 3
            WHEN '2B' THEN 4
            WHEN '3B' THEN 5
            WHEN 'SS' THEN 6
        END::UTINYINT AS fielding_position,
        f.g AS games_at_position
    FROM baseballdatabank.fielding AS f
    INNER JOIN team_id_crosswalk AS t
        ON f.year_id = t.year_id AND f.team_id = t.databank_team_id
    WHERE f.pos IN ('P', 'C', '1B', '2B', '3B', 'SS')
),

fielding_outfield AS (
    SELECT
        f.player_id,
        f.year_id,
        t.team_id AS team_id,
        f.lg_id,
        f.stint,
        f.g AS stint_of_g,
        SUM(f.g) OVER (PARTITION BY f.player_id, f.year_id, f.team_id) AS team_of_g
    FROM baseballdatabank.fielding AS f
    INNER JOIN team_id_crosswalk AS t
        ON f.year_id = t.year_id AND f.team_id = t.databank_team_id
    WHERE f.pos = 'OF'
),

outfield_source AS (
    SELECT
        fo.player_id AS databank_player_id,
        fo.year_id AS season,
        fo.team_id,
        fo.lg_id AS league_id,
        fo.stint,
        ROUND(a.g_lf::DOUBLE * fo.stint_of_g / NULLIF(fo.team_of_g, 0))::INTEGER AS games_left_field,
        ROUND(a.g_cf::DOUBLE * fo.stint_of_g / NULLIF(fo.team_of_g, 0))::INTEGER AS games_center_field,
        ROUND(a.g_rf::DOUBLE * fo.stint_of_g / NULLIF(fo.team_of_g, 0))::INTEGER AS games_right_field
    FROM fielding_outfield AS fo
    INNER JOIN (
        SELECT
            a.player_id,
            a.year_id,
            t.team_id AS team_id,
            a.g_lf,
            a.g_cf,
            a.g_rf
        FROM baseballdatabank.appearances AS a
        INNER JOIN team_id_crosswalk AS t
            ON a.year_id = t.year_id AND a.team_id = t.databank_team_id
    ) AS a
        ON fo.player_id = a.player_id
        AND fo.year_id = a.year_id
        AND fo.team_id = a.team_id
),

outfield_unpivoted AS (
    SELECT * FROM outfield_source
    UNPIVOT (
        games_at_position FOR position_label IN (
            games_left_field,
            games_center_field,
            games_right_field
        )
    )
),

outfield_with_position AS (
    SELECT
        databank_player_id,
        season,
        team_id,
        league_id,
        stint,
        CASE position_label
            WHEN 'games_left_field' THEN 7
            WHEN 'games_center_field' THEN 8
            WHEN 'games_right_field' THEN 9
        END::UTINYINT AS fielding_position,
        games_at_position
    FROM outfield_unpivoted
),

position_games AS (
    SELECT * FROM fielding_source
    UNION ALL
    SELECT * FROM outfield_with_position
),

non_pitcher_outs_played AS (
    SELECT
        f.player_id AS databank_player_id,
        f.year_id AS season,
        t.team_id AS team_id,
        f.stint,
        SUM(f.inn_outs)::INTEGER AS outs_played
    FROM baseballdatabank.fielding AS f
    INNER JOIN team_id_crosswalk AS t
        ON f.year_id = t.year_id AND f.team_id = t.databank_team_id
    WHERE f.pos != 'P'
      AND f.inn_outs IS NOT NULL
    GROUP BY 1, 2, 3, 4
)

SELECT
    people.retrosheet_player_id AS player_id,
    position_games.databank_player_id,
    position_games.season::SMALLINT AS season,
    position_games.stint::SMALLINT AS stint,
    position_games.team_id,
    COALESCE(games_total.league_id, position_games.league_id) AS league_id,
    position_games.fielding_position,
    position_games.games_at_position::USMALLINT AS games_at_position,
    games_total.games_total::USMALLINT AS games_total,
    COALESCE(outs.outs_played, 0)::INTEGER AS outs_played
FROM position_games
INNER JOIN main_models.stg_people AS people USING (databank_player_id)
LEFT JOIN games_total USING (databank_player_id, season, team_id)
LEFT JOIN non_pitcher_outs_played AS outs
    USING (databank_player_id, season, team_id, stint)
WHERE people.retrosheet_player_id IS NOT NULL
  AND position_games.games_at_position IS NOT NULL
  AND position_games.games_at_position > 0
  AND games_total.games_total IS NOT NULL
"""


_BUILD_LOCAL_BATTING_SQL = """
CREATE OR REPLACE TEMP TABLE _bt_batting AS
WITH team_id_crosswalk AS (
    SELECT
        year_id,
        team_id AS databank_team_id,
        team_id_retro AS team_id
    FROM baseballdatabank.teams
)

SELECT
    b.player_id AS databank_player_id,
    b.year_id::SMALLINT AS season,
    b.stint::SMALLINT AS stint,
    t.team_id AS team_id,
    b.lg_id AS league_id,
    b.g AS games,
    b.ab AS at_bats,
    b.bb AS walks,
    b.hbp AS hit_by_pitches,
    b.sh AS sacrifice_hits,
    b.sf AS sacrifice_flies,
    (b.ab + COALESCE(b.bb, 0) + COALESCE(b.hbp, 0)
     + COALESCE(b.sf, 0) + COALESCE(b.sh, 0))::INTEGER AS plate_appearances
FROM baseballdatabank.batting AS b
INNER JOIN team_id_crosswalk AS t
    ON b.year_id = t.year_id AND b.team_id = t.databank_team_id
"""


_GAMES_SQL = """
SELECT
    g.game_id::VARCHAR AS game_id,
    g.date,
    g.season,
    g.use_dh,
    g.home_team_id::VARCHAR AS home_team_id,
    g.away_team_id::VARCHAR AS away_team_id,
    gl.home_starting_pitcher_id::VARCHAR AS home_starting_pitcher_id,
    gl.away_starting_pitcher_id::VARCHAR AS away_starting_pitcher_id,
    g.source_type,
    g.home_league,
    g.away_league
FROM main_models.game_start_info AS g
INNER JOIN main_models.stg_gamelog AS gl USING (game_id)
WHERE g.season IN ({seasons})
  AND g.source_type IN ('BoxScore', 'PlayByPlay')
  AND g.use_dh = FALSE
  AND gl.home_starting_pitcher_id IS NOT NULL
  AND gl.away_starting_pitcher_id IS NOT NULL
"""

_CANDIDATES_SQL = """
WITH valid_team_seasons AS (
    SELECT DISTINCT season, away_team_id::VARCHAR AS team_id
    FROM ({games_cte}) AS gg
    UNION
    SELECT DISTINCT season, home_team_id::VARCHAR AS team_id
    FROM ({games_cte}) AS gg
),

appearances AS (
    SELECT
        a.season,
        a.team_id::VARCHAR AS team_id,
        a.player_id::VARCHAR AS player_id,
        a.stint,
        a.fielding_position,
        SUM(a.games_at_position)::INTEGER AS games_at_position,
        MAX(a.games_total)::INTEGER AS games_total,
        MAX(a.outs_played)::INTEGER AS outs_played
    FROM _bt_appearances AS a
    INNER JOIN valid_team_seasons AS valid USING (season, team_id)
    WHERE a.fielding_position BETWEEN 1 AND 9
      AND a.games_at_position > 0
    GROUP BY 1, 2, 3, 4, 5
),

batting AS (
    SELECT
        b.season,
        b.team_id::VARCHAR AS team_id,
        people.retrosheet_player_id::VARCHAR AS player_id,
        b.stint,
        SUM(COALESCE(b.plate_appearances, 0))::INTEGER AS plate_appearances,
        SUM(COALESCE(b.games, 0))::INTEGER AS games_played
    FROM _bt_batting AS b
    INNER JOIN main_models.stg_people AS people USING (databank_player_id)
    INNER JOIN valid_team_seasons AS valid USING (season, team_id)
    WHERE people.retrosheet_player_id IS NOT NULL
    GROUP BY 1, 2, 3, 4
)

SELECT
    a.season,
    a.team_id,
    a.player_id,
    a.stint,
    a.fielding_position,
    a.games_at_position,
    a.games_total,
    a.outs_played,
    COALESCE(b.plate_appearances, 0)::INTEGER AS plate_appearances,
    COALESCE(b.games_played, 0)::INTEGER AS games_played
FROM appearances AS a
LEFT JOIN batting AS b USING (season, team_id, player_id, stint)
"""


_TRUTH_SQL = """
SELECT
    g.season,
    b.game_id,
    b.side,
    CASE WHEN b.side = 'Home' THEN g.home_team_id ELSE g.away_team_id END::VARCHAR AS team_id,
    b.lineup_position,
    f.fielding_position,
    b.batter_id::VARCHAR AS player_id
FROM main_models.stg_box_score_batting_lines AS b
INNER JOIN main_models.stg_box_score_fielding_lines AS f
    ON b.game_id = f.game_id
    AND b.side = f.side
    AND b.batter_id = f.fielder_id
INNER JOIN main_models.game_start_info AS g
    ON b.game_id = g.game_id
WHERE b.nth_player_at_position = 1
  AND f.nth_position_played_by_player = 1
  AND g.season IN ({seasons})
  AND g.use_dh = FALSE
"""


_TEAM_GAMES_SQL = """
SELECT season, team_id::VARCHAR AS team_id, COUNT(*) AS team_games
FROM main_models.team_game_start_info
WHERE season IN ({seasons})
GROUP BY 1, 2
"""


def _seasons_clause(seasons: Iterable[int]) -> str:
    return ", ".join(str(s) for s in sorted(set(seasons)))


def _validate_input(name: str, frame: pl.DataFrame, expected: tuple[str, ...]) -> None:
    missing = set(expected) - set(frame.columns)
    if missing:
        raise RuntimeError(f"{name} missing columns: {sorted(missing)}")


def _compute_modal_lineups_local(
    candidates: pl.DataFrame, batting_for_modal: pl.DataFrame
) -> pl.DataFrame:
    appearances = (
        candidates.group_by(["season", "team_id", "player_id", "fielding_position"])
        .agg(pl.sum("games_at_position").alias("games_at_position"))
        .with_columns(
            pl.col("season").cast(pl.Int16),
            pl.col("games_at_position").cast(pl.UInt32),
            pl.col("fielding_position").cast(pl.UInt8),
        )
    )
    batting = batting_for_modal.with_columns(
        pl.col("season").cast(pl.Int16),
        pl.col("plate_appearances").cast(pl.UInt32),
        pl.col("games_played").cast(pl.UInt32),
    )
    return compute_modal_lineups(appearances, batting)


_POSITION_GROUPS: dict[int, str] = {
    1: "P",
    2: "C",
    3: "IF",
    4: "IF",
    5: "IF",
    6: "IF",
    7: "OF",
    8: "OF",
    9: "OF",
}


def _era_bucket(season: int) -> str:
    if 1871 <= season <= 1880:
        return "1871-1880"
    if 1881 <= season <= 1890:
        return "1881-1890"
    if 1891 <= season <= 1900:
        return "1891-1900"
    return "1901-1910"


def _team_games_bin(games: int) -> str:
    if games < 60:
        return "<60"
    if games <= 100:
        return "60-100"
    if games <= 130:
        return "100-130"
    return "130+"


def _build_comparison(
    synthetic: pl.DataFrame,
    truth: pl.DataFrame,
) -> pl.DataFrame:
    """One row per (game_id, side, lineup_position) with real and synthetic assignments joined."""
    syn = synthetic.select(
        pl.col("game_id"),
        pl.col("side").cast(pl.String),
        pl.col("season").cast(pl.Int16),
        pl.col("team_id"),
        pl.col("lineup_position").cast(pl.UInt8),
        pl.col("fielding_position").cast(pl.UInt8).alias("syn_fielding_position"),
        pl.col("player_id").alias("syn_player_id"),
    )
    tru = truth.select(
        pl.col("game_id"),
        pl.col("side").cast(pl.String),
        pl.col("lineup_position").cast(pl.UInt8),
        pl.col("fielding_position").cast(pl.UInt8).alias("real_fielding_position"),
        pl.col("player_id").alias("real_player_id"),
    )
    merged = syn.join(
        tru,
        on=["game_id", "side", "lineup_position"],
        how="full",
        coalesce=True,
    )
    return merged.with_columns(
        slot_match=pl.col("syn_player_id") == pl.col("real_player_id"),
    )


def _per_player_per_side(synthetic: pl.DataFrame, truth: pl.DataFrame) -> pl.DataFrame:
    """One row per (game_id, side, player_id) with syn_pos and real_pos.

    Either column is null when that side didn't include the player.
    """
    syn = synthetic.select(
        pl.col("game_id"),
        pl.col("side").cast(pl.String),
        pl.col("season").cast(pl.Int16),
        pl.col("team_id"),
        pl.col("player_id"),
        pl.col("fielding_position").cast(pl.UInt8).alias("syn_pos"),
    )
    tru = truth.select(
        pl.col("game_id"),
        pl.col("side").cast(pl.String),
        pl.col("player_id"),
        pl.col("fielding_position").cast(pl.UInt8).alias("real_pos"),
    )
    return syn.join(
        tru,
        on=["game_id", "side", "player_id"],
        how="full",
        coalesce=True,
    )


def _side_errors(per_player: pl.DataFrame) -> pl.DataFrame:
    """Per (game_id, side): wrong_starters and wrong_positions counts."""
    return (
        per_player.with_columns(
            in_syn=pl.col("syn_pos").is_not_null(),
            in_real=pl.col("real_pos").is_not_null(),
            in_both=pl.col("syn_pos").is_not_null() & pl.col("real_pos").is_not_null(),
            pos_mismatch=(
                pl.col("syn_pos").is_not_null()
                & pl.col("real_pos").is_not_null()
                & (pl.col("syn_pos") != pl.col("real_pos"))
            ),
        )
        .group_by(["game_id", "side"])
        .agg(
            pl.col("in_syn").sum().alias("syn_count"),
            pl.col("in_real").sum().alias("real_count"),
            pl.col("in_both").sum().alias("both_count"),
            pl.col("pos_mismatch").sum().alias("wrong_positions"),
        )
        .with_columns(
            wrong_starters=(pl.col("real_count") - pl.col("both_count")).cast(pl.Int32),
        )
        .select(
            "game_id",
            "side",
            "syn_count",
            "real_count",
            "both_count",
            "wrong_starters",
            "wrong_positions",
        )
    )


def _position_match(synthetic: pl.DataFrame, truth: pl.DataFrame) -> pl.DataFrame:
    """Per (game, side, fielding_position): did syn and real assign the same player?"""
    syn = synthetic.select(
        pl.col("game_id"),
        pl.col("side").cast(pl.String),
        pl.col("fielding_position").cast(pl.UInt8),
        pl.col("player_id").alias("syn_player_id"),
    )
    tru = truth.select(
        pl.col("game_id"),
        pl.col("side").cast(pl.String),
        pl.col("fielding_position").cast(pl.UInt8),
        pl.col("player_id").alias("real_player_id"),
    )
    merged = syn.join(
        tru,
        on=["game_id", "side", "fielding_position"],
        how="full",
        coalesce=True,
    )
    return merged.with_columns(
        position_match=pl.col("syn_player_id") == pl.col("real_player_id"),
    )


def _per_player_season(
    per_player: pl.DataFrame, games_full: pl.DataFrame
) -> pl.DataFrame:
    """Per (season, team, player): aggregate the four error counts."""
    games_lookup = games_full.select(
        pl.col("game_id"),
        pl.col("season").cast(pl.Int16).alias("game_season"),
        pl.col("home_team_id"),
        pl.col("away_team_id"),
    )
    enriched = (
        per_player.drop("season", "team_id", strict=False)
        .join(games_lookup, on="game_id", how="left")
        .with_columns(
            season=pl.col("game_season"),
            team_id=pl.when(pl.col("side") == "Home")
            .then(pl.col("home_team_id"))
            .otherwise(pl.col("away_team_id")),
            in_syn=pl.col("syn_pos").is_not_null(),
            in_real=pl.col("real_pos").is_not_null(),
            in_both=pl.col("syn_pos").is_not_null() & pl.col("real_pos").is_not_null(),
            pos_match=(
                pl.col("syn_pos").is_not_null()
                & pl.col("real_pos").is_not_null()
                & (pl.col("syn_pos") == pl.col("real_pos"))
            ),
            pos_mismatch=(
                pl.col("syn_pos").is_not_null()
                & pl.col("real_pos").is_not_null()
                & (pl.col("syn_pos") != pl.col("real_pos"))
            ),
        )
    )
    return (
        enriched.group_by(["season", "team_id", "player_id"])
        .agg(
            pl.col("in_syn").sum().alias("syn_starts"),
            pl.col("in_real").sum().alias("real_starts"),
            pl.col("in_both").sum().alias("correct_starter_games"),
            pl.col("pos_match").sum().alias("correct_starter_correct_pos"),
            pl.col("pos_mismatch").sum().alias("correct_starter_wrong_pos"),
        )
        .with_columns(
            missed=(pl.col("real_starts") - pl.col("correct_starter_games")).cast(
                pl.Int32
            ),
            added=(pl.col("syn_starts") - pl.col("correct_starter_games")).cast(
                pl.Int32
            ),
            miss_rate=pl.when(pl.col("real_starts") > 0)
            .then(
                (pl.col("real_starts") - pl.col("correct_starter_games"))
                / pl.col("real_starts")
            )
            .otherwise(None),
            add_rate=pl.when(pl.col("syn_starts") > 0)
            .then(
                (pl.col("syn_starts") - pl.col("correct_starter_games"))
                / pl.col("syn_starts")
            )
            .otherwise(None),
            wrong_pos_rate=pl.when(pl.col("correct_starter_games") > 0)
            .then(pl.col("correct_starter_wrong_pos") / pl.col("correct_starter_games"))
            .otherwise(None),
        )
    )


def _enrich_comparison(
    comparison: pl.DataFrame,
    games: pl.DataFrame,
    candidates: pl.DataFrame,
    team_games: pl.DataFrame,
) -> pl.DataFrame:
    """Attach season, era, league, position-group, churn, team-games-bin to comparison rows."""
    games_lookup = games.select(
        pl.col("game_id"),
        pl.col("season").cast(pl.Int16).alias("game_season"),
        pl.col("home_team_id"),
        pl.col("away_team_id"),
        pl.col("home_league"),
        pl.col("away_league"),
    )
    enriched = (
        comparison.drop("season", "team_id", strict=False)
        .join(games_lookup, on="game_id", how="left")
        .with_columns(
            season=pl.col("game_season"),
            team_id=pl.when(pl.col("side") == "Home")
            .then(pl.col("home_team_id"))
            .otherwise(pl.col("away_team_id")),
            league=pl.when(pl.col("side") == "Home")
            .then(pl.col("home_league"))
            .otherwise(pl.col("away_league")),
        )
        .drop("game_season")
    )
    enriched = enriched.with_columns(
        era=pl.col("season").map_elements(_era_bucket, return_dtype=pl.String),
    )
    # Position group for the truth fielding position (reflects what the player actually played).
    enriched = enriched.with_columns(
        real_position_group=pl.col("real_fielding_position").map_elements(
            lambda p: _POSITION_GROUPS.get(int(p)) if p is not None else None,
            return_dtype=pl.String,
        ),
    )
    # Roster churn: classify per (season, team_id, real_player_id) using stint count + games_played.
    if not candidates.is_empty():
        per_player_season = candidates.group_by(["season", "player_id"]).agg(
            pl.col("stint").max().alias("max_stint")
        )
        per_player_team = candidates.group_by(["season", "team_id", "player_id"]).agg(
            pl.col("games_total").max().alias("games_total")
        )
        churn = per_player_team.join(
            per_player_season, on=["season", "player_id"], how="left"
        ).join(
            team_games.with_columns(pl.col("season").cast(pl.Int16)),
            on=["season", "team_id"],
            how="left",
        )
        churn = churn.with_columns(
            churn=pl.when(pl.col("max_stint") > 1)
            .then(pl.lit("multi-stint"))
            .when(
                (pl.col("games_total").cast(pl.Float64))
                / pl.col("team_games").cast(pl.Float64)
                >= 0.8
            )
            .then(pl.lit("single-stint full"))
            .otherwise(pl.lit("single-stint partial"))
        ).select(["season", "team_id", "player_id", "churn"])
        churn = churn.rename({"player_id": "real_player_id"})
        enriched = enriched.join(
            churn, on=["season", "team_id", "real_player_id"], how="left"
        )
    else:
        enriched = enriched.with_columns(churn=pl.lit(None).cast(pl.String))
    enriched = enriched.join(
        team_games.with_columns(pl.col("season").cast(pl.Int16)),
        on=["season", "team_id"],
        how="left",
    ).with_columns(
        team_games_bin=pl.col("team_games").map_elements(
            lambda g: _team_games_bin(int(g)) if g is not None else None,
            return_dtype=pl.String,
        )
    )
    return enriched.select(
        "game_id",
        "side",
        "season",
        "team_id",
        "league",
        "era",
        "lineup_position",
        "syn_fielding_position",
        "real_fielding_position",
        "syn_player_id",
        "real_player_id",
        "slot_match",
        "real_position_group",
        "churn",
        "team_games",
        "team_games_bin",
    )


def _format_pct(numerator: int, denominator: int) -> str:
    if denominator == 0:
        return "n/a"
    return f"{numerator / denominator * 100:.2f}%"


def _accuracy_table(
    df: pl.DataFrame,
    group_cols: list[str],
    *,
    match_col: str = "slot_match",
) -> pl.DataFrame:
    return (
        df.group_by(group_cols)
        .agg(
            pl.len().alias("rows"),
            pl.col(match_col).cast(pl.Int32).sum().alias("matches"),
        )
        .with_columns(accuracy_pct=(pl.col("matches") / pl.col("rows") * 100).round(2))
        .sort(group_cols)
    )


def _error_table(
    side_errors: pl.DataFrame,
    group_cols: list[str],
) -> pl.DataFrame:
    """Per group: avg wrong starters and wrong positions per side / per game."""
    return (
        side_errors.group_by(group_cols)
        .agg(
            (pl.len() // 2).alias("games"),
            pl.len().alias("sides"),
            pl.col("wrong_starters").sum().alias("wrong_starters_total"),
            pl.col("wrong_positions").sum().alias("wrong_positions_total"),
        )
        .with_columns(
            wrong_starters_per_game=(
                pl.col("wrong_starters_total") / pl.col("sides") * 2
            ).round(3),
            wrong_positions_per_game=(
                pl.col("wrong_positions_total") / pl.col("sides") * 2
            ).round(3),
        )
        .sort(group_cols)
        .select(
            group_cols
            + [
                "sides",
                "wrong_starters_per_game",
                "wrong_positions_per_game",
            ]
        )
    )


def _frame_md(df: pl.DataFrame) -> str:
    """Render a polars DataFrame as a plain markdown table."""
    if df.is_empty():
        return "_(no rows)_"
    cols = df.columns
    rows = df.iter_rows()
    header = "| " + " | ".join(cols) + " |"
    sep = "| " + " | ".join("---" for _ in cols) + " |"
    body = []
    for row in rows:
        body.append("| " + " | ".join("" if v is None else str(v) for v in row) + " |")
    return "\n".join([header, sep, *body])


def _orphan_team_seasons(comparison_enriched: pl.DataFrame) -> pl.DataFrame:
    """(season, team_id) pairs where truth rows exist but no synthetic rows do.

    Caused by Lahman/Databank vs Retrosheet team_id mismatches in the input
    layer (e.g., WAS vs WSN). Production ``lineup_assignments`` has the same
    gap; we surface it explicitly so the headline isn't silently dragged.
    """
    per_team = (
        comparison_enriched.group_by(["season", "team_id"])
        .agg(
            pl.col("syn_player_id").is_not_null().sum().alias("syn_rows"),
            pl.col("real_player_id").is_not_null().sum().alias("real_rows"),
        )
        .filter((pl.col("syn_rows") == 0) & (pl.col("real_rows") > 0))
        .select(["season", "team_id", "real_rows"])
        .sort(["season", "team_id"])
    )
    return per_team


def _enrich_side_errors(
    side_errors: pl.DataFrame,
    games_full: pl.DataFrame,
    team_games: pl.DataFrame,
) -> pl.DataFrame:
    games_lookup = games_full.select(
        pl.col("game_id"),
        pl.col("season").cast(pl.Int16),
        pl.col("home_team_id"),
        pl.col("away_team_id"),
        pl.col("home_league"),
        pl.col("away_league"),
    )
    enriched = side_errors.join(games_lookup, on="game_id", how="left").with_columns(
        team_id=pl.when(pl.col("side") == "Home")
        .then(pl.col("home_team_id"))
        .otherwise(pl.col("away_team_id")),
        league=pl.when(pl.col("side") == "Home")
        .then(pl.col("home_league"))
        .otherwise(pl.col("away_league")),
        era=pl.col("season").map_elements(_era_bucket, return_dtype=pl.String),
    )
    return enriched.join(
        team_games.with_columns(pl.col("season").cast(pl.Int16)),
        on=["season", "team_id"],
        how="left",
    ).with_columns(
        team_games_bin=pl.col("team_games").map_elements(
            lambda g: _team_games_bin(int(g)) if g is not None else None,
            return_dtype=pl.String,
        )
    )


def _render_report(
    *,
    side_errors_enriched: pl.DataFrame,
    per_player: pl.DataFrame,
    per_player_season: pl.DataFrame,
    position_match: pl.DataFrame,
    comparison_enriched: pl.DataFrame,
    games_count: int,
    sides_count: int,
    seasons_present: list[int],
    runtime_seconds: float,
    excluded_games: int,
) -> str:
    lines: list[str] = []
    lines.append("# Synthetic-lineup backtest, 1871-1910")
    lines.append("")
    lines.append(
        "Backtest of `synthetic_box_score.lineup_assignments` against real Retrosheet box-score lineups."
    )
    lines.append(
        "The optimizer is run on box-score-era games where real lineups exist, using only the gamelog-only feature set (Lahman/Databank season inputs + gamelog starting pitcher + DH flag). Results are compared to the real assignments."
    )
    lines.append("")
    lines.append("## Methodology")
    lines.append("")
    lines.append(
        f"- Seasons in scope: {', '.join(str(s) for s in sorted(set(seasons_present)))}"
    )
    lines.append(
        f"- Games scored: {games_count} ({sides_count} game-sides, {sides_count * 9} starter slots)"
    )
    lines.append(f"- Optimizer runtime: {runtime_seconds:.1f}s")
    if excluded_games:
        lines.append(
            f"- {excluded_games} eligible games were excluded because the gamelog row is missing or has NULL starting pitchers (input-parity caveat)."
        )
    lines.append(
        "- Inputs: `stg_databank_appearances`, `stg_databank_batting`, gamelog starting pitchers, DH flag. No box-score data on the input side."
    )
    lines.append(
        "- Modal lineups are recomputed from the candidate inputs via `compute_modal_lineups`, not read from a persisted table."
    )
    lines.append(
        "- Truth: `stg_box_score_batting_lines.nth_player_at_position = 1` joined to `stg_box_score_fielding_lines.nth_position_played_by_player = 1` on `(game_id, side, batter_id = fielder_id)`."
    )
    lines.append("")
    lines.append("**Definitions.**")
    lines.append("")
    lines.append(
        "- *Wrong starters* per game-side = number of real starters the synthetic lineup didn't include (= 9 − |syn_set ∩ real_set|). Per game = sum across both sides, max 18."
    )
    lines.append(
        "- *Wrong positions* per game-side = number of correctly-included starters at the wrong fielding position. Per game = sum across both sides, max 18."
    )
    lines.append(
        "- *Per-player rates* are computed on (season, team, player) tuples: `miss_rate = missed / real_starts`; `add_rate = added / syn_starts`; `wrong_pos_rate = wrong_pos / correct_starter_games`."
    )
    lines.append(
        "- The starting pitcher is an input to the optimizer; pitcher-position errors are near-trivial except in orphan team-seasons."
    )
    lines.append(
        "- MILP tie-breaking is not strictly deterministic across runs; positional swaps among tied solutions shift wrong-position counts by a few hundredths per game between runs. Wrong-starter counts are stable."
    )
    lines.append("")

    # Coverage caveat.
    orphans = _orphan_team_seasons(comparison_enriched)
    orphan_keys = {(int(r[0]), str(r[1])) for r in orphans.iter_rows()}
    lines.append("## Coverage caveat: missing team-seasons")
    lines.append("")
    if orphans.is_empty():
        lines.append(
            "- All (season, team) pairs present in truth are also present in synthetic. No coverage gap."
        )
    else:
        lines.append(
            "Some (season, team) pairs have truth rows but no synthetic assignments. This happens when the Lahman/Databank team_id differs from the Retrosheet team_id (e.g., WAS vs WSN), so the candidate inputs and the games list don't join. Production `synthetic_box_score.lineup_assignments` has the same gap. These sides count as 9 wrong starters / 0 wrong positions."
        )
        lines.append("")
        lines.append(_frame_md(orphans))
        lines.append("")
        lines.append(
            "Tables below are reported once over all sides (orphans count as 9 wrong starters each), and headline numbers are also given restricted to in-scope sides."
        )
    lines.append("")

    side_in_scope = (
        side_errors_enriched.filter(
            ~pl.struct(["season", "team_id"]).map_elements(
                lambda s: (
                    int(s["season"]) if s["season"] is not None else -1,
                    str(s["team_id"]),
                )
                in orphan_keys,
                return_dtype=pl.Boolean,
            )
        )
        if orphan_keys
        else side_errors_enriched
    )

    def _avg(df: pl.DataFrame, col: str) -> float:
        if df.is_empty():
            return 0.0
        return float(df[col].mean() or 0)

    lines.append("## Headline error counts")
    lines.append("")
    lines.append("Across all sides (orphans included):")
    lines.append("")
    lines.append(
        f"- Wrong starters per game: {_avg(side_errors_enriched, 'wrong_starters') * 2:.3f} (per side: {_avg(side_errors_enriched, 'wrong_starters'):.3f}) out of 9"
    )
    lines.append(
        f"- Wrong positions per game (right starter, wrong defensive position): {_avg(side_errors_enriched, 'wrong_positions') * 2:.3f} (per side: {_avg(side_errors_enriched, 'wrong_positions'):.3f})"
    )
    lines.append("")
    if orphan_keys:
        lines.append("Restricted to in-scope sides:")
        lines.append("")
        lines.append(
            f"- Wrong starters per game: {_avg(side_in_scope, 'wrong_starters') * 2:.3f} (per side: {_avg(side_in_scope, 'wrong_starters'):.3f})"
        )
        lines.append(
            f"- Wrong positions per game: {_avg(side_in_scope, 'wrong_positions') * 2:.3f} (per side: {_avg(side_in_scope, 'wrong_positions'):.3f})"
        )
        lines.append("")
        # Pitcher sanity floor: in-scope, the gamelog starter is wired through, so
        # "missed pitcher" should be ~0 per side.
        in_scope_games = side_in_scope.select(["game_id", "side"]).unique()
        pos_in_scope = position_match.join(
            in_scope_games, on=["game_id", "side"], how="inner"
        )
        pitcher_pos_in_scope = pos_in_scope.filter(pl.col("fielding_position") == 1)
        pitcher_correct = int(
            pitcher_pos_in_scope.filter(pl.col("position_match")).height
        )
        pitcher_total = int(pitcher_pos_in_scope.height)
        pitcher_missed = pitcher_total - pitcher_correct
        lines.append(
            f"Pitcher position sanity floor (in-scope): {pitcher_missed} of {pitcher_total} sides ({_format_pct(pitcher_missed, pitcher_total)}) had the pitcher input fail to reach the optimizer. Should be ~0."
        )
        lines.append("")

    # Era × league.
    lines.append("## Errors per game by era × league")
    lines.append("")
    lines.append(_frame_md(_error_table(side_errors_enriched, ["era", "league"])))
    lines.append("")

    # Truth-position-group: how often is each defensive position miscounted?
    # Use the position_match df (per game-side per fielding_position).
    pos_group = position_match.with_columns(
        position_group=pl.col("fielding_position").map_elements(
            lambda p: _POSITION_GROUPS.get(int(p)) if p is not None else None,
            return_dtype=pl.String,
        ),
        wrong=(~pl.col("position_match").fill_null(False)),
    )
    lines.append("## Wrong-defender rate by truth fielding position")
    lines.append("")
    lines.append(
        "For each defensive position, fraction of game-sides where the synthetic lineup did *not* put the same player at that position as the real lineup. Counts both 'wrong player' and 'right player at a different position' as errors."
    )
    lines.append("")
    lines.append(
        _frame_md(
            pos_group.group_by("position_group")
            .agg(
                pl.len().alias("sides"),
                pl.col("wrong").sum().alias("wrong_sides"),
            )
            .with_columns(
                wrong_per_game=(pl.col("wrong_sides") / pl.col("sides") * 2).round(3)
            )
            .sort("position_group")
        )
    )
    lines.append("")
    lines.append("By individual fielding position:")
    lines.append("")
    lines.append(
        _frame_md(
            position_match.with_columns(
                wrong=~pl.col("position_match").fill_null(False)
            )
            .group_by("fielding_position")
            .agg(
                pl.len().alias("sides"),
                pl.col("wrong").sum().alias("wrong_sides"),
            )
            .with_columns(
                wrong_per_game=(pl.col("wrong_sides") / pl.col("sides") * 2).round(3)
            )
            .sort("fielding_position")
        )
    )
    lines.append("")

    # Roster churn (uses per-(season, team, player) classification on the real player).
    lines.append("## Per-player rates by truth player's roster churn")
    lines.append("")
    lines.append(
        "Classification on each (season, team, real-player) tuple. multi-stint = the player had >1 stints in the league that season; single-stint full = stint=1 and games_total >= 80% of team games; single-stint partial = otherwise. Aggregated over all real-player game-sides for the bucket."
    )
    lines.append("")
    churn_lookup = (
        comparison_enriched.filter(pl.col("churn").is_not_null())
        .select("season", "team_id", "real_player_id", "churn")
        .unique()
        .rename({"real_player_id": "player_id"})
    )
    per_player_with_churn = per_player_season.join(
        churn_lookup, on=["season", "team_id", "player_id"], how="left"
    )
    churn_summary = (
        per_player_with_churn.filter(
            pl.col("churn").is_not_null() & (pl.col("real_starts") > 0)
        )
        .group_by("churn")
        .agg(
            pl.col("real_starts").sum().alias("real_starts_total"),
            pl.col("missed").sum().alias("missed_total"),
            pl.col("correct_starter_wrong_pos").sum().alias("wrong_pos_total"),
        )
        .with_columns(
            miss_rate_pct=(
                pl.col("missed_total") / pl.col("real_starts_total") * 100
            ).round(2),
            wrong_pos_rate_pct=(
                pl.col("wrong_pos_total")
                / (pl.col("real_starts_total") - pl.col("missed_total"))
                * 100
            ).round(2),
        )
        .sort("churn")
    )
    lines.append(_frame_md(churn_summary))
    lines.append("")

    # Team games-played bin.
    lines.append("## Errors per game by team games-played bin")
    lines.append("")
    lines.append(
        _frame_md(
            _error_table(side_errors_enriched, ["team_games_bin"]).sort(
                "team_games_bin"
            )
        )
    )
    lines.append("")

    # Best/worst team-seasons by wrong-starter rate.
    team_season = _error_table(
        side_errors_enriched, ["season", "team_id", "league"]
    ).filter(pl.col("sides") >= 9 * 2)
    lines.append("## Best 10 team-seasons (fewest wrong starters per game)")
    lines.append("")
    lines.append(_frame_md(team_season.sort("wrong_starters_per_game").head(10)))
    lines.append("")
    lines.append("## Worst 10 team-seasons (most wrong starters per game)")
    lines.append("")
    lines.append(
        _frame_md(team_season.sort("wrong_starters_per_game", descending=True).head(10))
    )
    lines.append("")

    # Per-player aggregate summary.
    lines.append("## Per-player error rates")
    lines.append("")
    n_player_seasons = per_player_season.height
    exact = per_player_season.filter(
        (pl.col("syn_starts") == pl.col("real_starts"))
        & (pl.col("correct_starter_wrong_pos") == 0)
    ).height
    starts_match_pos_off = per_player_season.filter(
        (pl.col("syn_starts") == pl.col("real_starts"))
        & (pl.col("correct_starter_wrong_pos") > 0)
    ).height
    over = per_player_season.filter(pl.col("syn_starts") > pl.col("real_starts")).height
    under = per_player_season.filter(
        pl.col("syn_starts") < pl.col("real_starts")
    ).height
    lines.append(f"- (season, team, player) tuples: {n_player_seasons}")
    lines.append(
        f"- Synthetic count == real count AND every shared start at the right position: {exact} ({_format_pct(exact, n_player_seasons)})"
    )
    lines.append(
        f"- Synthetic count == real count, but some position errors: {starts_match_pos_off} ({_format_pct(starts_match_pos_off, n_player_seasons)})"
    )
    lines.append(
        f"- Synthetic over-starts the player: {over} ({_format_pct(over, n_player_seasons)})"
    )
    lines.append(
        f"- Synthetic under-starts the player: {under} ({_format_pct(under, n_player_seasons)})"
    )
    lines.append("")
    lines.append(
        "**Top 10 most-missed real starters** (real_starts ≥ 30, sorted by missed/real_starts):"
    )
    lines.append("")
    most_missed = (
        per_player_season.filter(pl.col("real_starts") >= 30)
        .with_columns(miss_rate_pct=(pl.col("miss_rate") * 100).round(2))
        .sort(["miss_rate", "real_starts"], descending=[True, True])
        .head(10)
        .select(
            "season",
            "team_id",
            "player_id",
            "real_starts",
            "syn_starts",
            "missed",
            "miss_rate_pct",
        )
    )
    lines.append(_frame_md(most_missed))
    lines.append("")
    lines.append(
        "**Top 10 most-wrongly-added players** (syn_starts ≥ 20, sorted by added/syn_starts):"
    )
    lines.append("")
    most_added = (
        per_player_season.filter(pl.col("syn_starts") >= 20)
        .with_columns(add_rate_pct=(pl.col("add_rate") * 100).round(2))
        .sort(["add_rate", "syn_starts"], descending=[True, True])
        .head(10)
        .select(
            "season",
            "team_id",
            "player_id",
            "syn_starts",
            "real_starts",
            "added",
            "add_rate_pct",
        )
    )
    lines.append(_frame_md(most_added))
    lines.append("")
    lines.append(
        "**Top 10 worst position-misplacement** (correct_starter_games ≥ 30, sorted by wrong_pos/correct_starter_games):"
    )
    lines.append("")
    worst_pos = (
        per_player_season.filter(pl.col("correct_starter_games") >= 30)
        .with_columns(wrong_pos_rate_pct=(pl.col("wrong_pos_rate") * 100).round(2))
        .sort(["wrong_pos_rate", "correct_starter_games"], descending=[True, True])
        .head(10)
        .select(
            "season",
            "team_id",
            "player_id",
            "correct_starter_games",
            "correct_starter_wrong_pos",
            "wrong_pos_rate_pct",
        )
    )
    lines.append(_frame_md(worst_pos))
    lines.append("")
    lines.append("## Artifacts")
    lines.append("")
    lines.append(
        "- Per-(game, side, lineup_position) comparison: `logs/synthetic_lineup_backtest/comparison.parquet`"
    )
    lines.append(
        "- Per-(season, team, player) error rates: `logs/synthetic_lineup_backtest/per_player.parquet`"
    )
    lines.append(
        "- Per-(game, side) error counts: `logs/synthetic_lineup_backtest/side_errors.parquet`"
    )
    return "\n".join(lines)


def _parse_seasons_arg(spec: str | None) -> tuple[int, ...]:
    if spec is None:
        return _BACKTEST_SEASONS
    out: set[int] = set()
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            lo, hi = part.split("-", 1)
            for y in range(int(lo), int(hi) + 1):
                out.add(y)
        else:
            out.add(int(part))
    selected = tuple(sorted(out))
    invalid = [s for s in selected if s not in _BACKTEST_SEASONS]
    if invalid:
        raise SystemExit(
            f"--seasons values {invalid} are not in the backtest scope {_BACKTEST_SEASONS}"
        )
    return selected


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument(
        "--seasons",
        default=None,
        help="Comma- or hyphen-separated list of seasons (subset of the backtest scope). Default: all.",
    )
    _ = parser.add_argument(
        "--db",
        default=str(REPO_ROOT / "bc.db"),
    )
    _ = parser.add_argument(
        "--parquet-out",
        default=str(
            REPO_ROOT / "logs" / "synthetic_lineup_backtest" / "comparison.parquet"
        ),
    )
    _ = parser.add_argument(
        "--report-out",
        default=str(REPO_ROOT / "notes" / "synthetic-lineup-backtest-1871-1910.md"),
    )
    _ = parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=str(args.log_level).upper(),
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    )
    seasons = _parse_seasons_arg(args.seasons)
    seasons_clause = _seasons_clause(seasons)
    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"bc.db not found at {db_path}")
    parquet_out = Path(args.parquet_out)
    report_out = Path(args.report_out)
    parquet_out.parent.mkdir(parents=True, exist_ok=True)
    report_out.parent.mkdir(parents=True, exist_ok=True)

    _LOG.info("opening %s read-only", db_path)
    con = duckdb.connect(str(db_path), read_only=True)

    _LOG.info("rebuilding stinted appearances locally (TEMP table)")
    started = time.monotonic()
    _ = con.sql(_BUILD_LOCAL_APPEARANCES_SQL)
    apps_count = con.sql("SELECT COUNT(*) FROM _bt_appearances").fetchone()
    _LOG.info(
        "_bt_appearances: %d rows in %.1fs",
        0 if apps_count is None else int(apps_count[0]),
        time.monotonic() - started,
    )

    _LOG.info("rebuilding batting locally (TEMP table)")
    started = time.monotonic()
    _ = con.sql(_BUILD_LOCAL_BATTING_SQL)
    bat_count = con.sql("SELECT COUNT(*) FROM _bt_batting").fetchone()
    _LOG.info(
        "_bt_batting: %d rows in %.1fs",
        0 if bat_count is None else int(bat_count[0]),
        time.monotonic() - started,
    )

    games_sql = _GAMES_SQL.format(seasons=seasons_clause)
    games_full: pl.DataFrame = con.sql(games_sql).pl()
    eligible_total = con.sql(
        f"""
        SELECT COUNT(*) FROM main_models.game_start_info g
        WHERE g.season IN ({seasons_clause})
          AND g.source_type IN ('BoxScore','PlayByPlay')
          AND g.use_dh = FALSE
        """
    ).fetchone()
    eligible_count = 0 if eligible_total is None else int(eligible_total[0])
    excluded = eligible_count - games_full.shape[0]
    _LOG.info(
        "games eligible=%d, included=%d, excluded(no-gamelog/no-starter)=%d",
        eligible_count,
        games_full.shape[0],
        excluded,
    )

    games = games_full.select(list(GAME_INPUT_COLUMNS))
    candidates_sql = _CANDIDATES_SQL.format(games_cte=games_sql)
    candidates: pl.DataFrame = con.sql(candidates_sql).pl()
    _LOG.info("candidates: %d rows", candidates.shape[0])

    batting_for_modal = con.sql(
        f"""
        SELECT
            b.season,
            b.team_id::VARCHAR AS team_id,
            people.retrosheet_player_id::VARCHAR AS player_id,
            SUM(COALESCE(b.plate_appearances, 0))::INTEGER AS plate_appearances,
            SUM(COALESCE(b.games, 0))::INTEGER AS games_played
        FROM _bt_batting AS b
        INNER JOIN main_models.stg_people AS people USING (databank_player_id)
        WHERE people.retrosheet_player_id IS NOT NULL
          AND b.season IN ({seasons_clause})
        GROUP BY 1, 2, 3
        """
    ).pl()
    lineups = _compute_modal_lineups_local(candidates, batting_for_modal)
    _LOG.info("modal lineups: %d rows", lineups.shape[0])

    _validate_input("games", games, GAME_INPUT_COLUMNS)
    _validate_input("lineups", lineups, LINEUP_INPUT_COLUMNS)
    _validate_input("candidates", candidates, CANDIDATE_INPUT_COLUMNS)

    txn_windows = _build_transaction_windows(games, candidates)
    _LOG.info(
        "transaction-derived stint windows: %d (out of %d candidate stint keys)",
        len(txn_windows),
        candidates.select(["season", "team_id", "player_id", "stint"]).n_unique(),
    )

    started = time.monotonic()
    synthetic = build_synthetic_lineup_assignments(
        games, lineups, candidates, transaction_windows=txn_windows
    )
    runtime = time.monotonic() - started
    _LOG.info(
        "optimizer produced %d synthetic assignment rows in %.1fs",
        synthetic.shape[0],
        runtime,
    )

    truth_sql = _TRUTH_SQL.format(seasons=seasons_clause)
    truth: pl.DataFrame = con.sql(truth_sql).pl()
    truth = truth.filter(pl.col("game_id").is_in(games["game_id"].implode()))
    _LOG.info("truth: %d rows", truth.shape[0])

    team_games = con.sql(_TEAM_GAMES_SQL.format(seasons=seasons_clause)).pl()

    comparison = _build_comparison(synthetic, truth)
    enriched = _enrich_comparison(comparison, games_full, candidates, team_games)
    per_player = _per_player_per_side(synthetic, truth)
    side_errors = _side_errors(per_player)
    side_errors_enriched = _enrich_side_errors(side_errors, games_full, team_games)
    pos_match = _position_match(synthetic, truth)
    per_player_season = _per_player_season(per_player, games_full)

    enriched_sorted = enriched.sort(
        ["season", "team_id", "game_id", "side", "lineup_position"],
        nulls_last=True,
    )
    enriched_sorted.write_parquet(parquet_out)
    _LOG.info("wrote %s (%d rows)", parquet_out, enriched.shape[0])

    side_errors_sorted = side_errors_enriched.sort(
        ["season", "team_id", "game_id", "side"], nulls_last=True
    )
    side_errors_path = parquet_out.with_name("side_errors.parquet")
    side_errors_sorted.write_parquet(side_errors_path)
    _LOG.info("wrote %s (%d rows)", side_errors_path, side_errors_sorted.shape[0])

    per_player_sorted = per_player_season.sort(
        ["season", "team_id", "player_id"], nulls_last=True
    )
    per_player_path = parquet_out.with_name("per_player.parquet")
    per_player_sorted.write_parquet(per_player_path)
    _LOG.info("wrote %s (%d rows)", per_player_path, per_player_sorted.shape[0])

    seasons_present = sorted(
        {int(s) for s in enriched["season"].drop_nulls().to_list()}
    )
    games_count = int(enriched.select(pl.col("game_id").n_unique()).item())
    sides_count = int(enriched.select(pl.struct(["game_id", "side"]).n_unique()).item())
    report_md = _render_report(
        side_errors_enriched=side_errors_enriched,
        per_player=per_player,
        per_player_season=per_player_season,
        position_match=pos_match,
        comparison_enriched=enriched,
        games_count=games_count,
        sides_count=sides_count,
        seasons_present=seasons_present,
        runtime_seconds=runtime,
        excluded_games=excluded,
    )
    report_out.write_text(report_md)
    _LOG.info("wrote %s", report_out)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
