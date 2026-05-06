"""Modal seasonal lineup picker for gamelog-only games.

For each team-season, start with the nine players with the most games
played for that team and assign each to his most common fielding position.
If those players leave fielding positions uncovered, replace the lowest
games players from overrepresented positions with the best eligible player
at each missing position. The final nine players are ranked by plate
appearances per game to assign lineup positions.
"""

from __future__ import annotations

import polars as pl

APPEARANCES_INPUT_COLUMNS: tuple[str, ...] = (
    "season",
    "team_id",
    "player_id",
    "fielding_position",
    "games_at_position",
)

BATTING_INPUT_COLUMNS: tuple[str, ...] = (
    "season",
    "team_id",
    "player_id",
    "plate_appearances",
    "games_played",
)

MODAL_LINEUP_OUTPUT_COLUMNS: tuple[str, ...] = (
    "season",
    "team_id",
    "lineup_position",
    "fielding_position",
    "player_id",
)

_FIELDING_POSITIONS: tuple[int, ...] = (1, 2, 3, 4, 5, 6, 7, 8, 9)


def compute_modal_lineups(
    appearances: pl.DataFrame,
    batting: pl.DataFrame,
) -> pl.DataFrame:
    """Pick one modal lineup per (season, team_id).

    Inputs are long-format. See APPEARANCES_INPUT_COLUMNS and
    BATTING_INPUT_COLUMNS for the expected schemas.
    """
    if not set(APPEARANCES_INPUT_COLUMNS).issubset(appearances.columns):
        missing = set(APPEARANCES_INPUT_COLUMNS) - set(appearances.columns)
        raise ValueError(f"appearances missing columns: {sorted(missing)}")
    if not set(BATTING_INPUT_COLUMNS).issubset(batting.columns):
        missing = set(BATTING_INPUT_COLUMNS) - set(batting.columns)
        raise ValueError(f"batting missing columns: {sorted(missing)}")

    enriched = (
        appearances.filter(
            pl.col("fielding_position").is_in(list(_FIELDING_POSITIONS))
            & (pl.col("games_at_position") > 0)
        )
        .join(
            batting.select(list(BATTING_INPUT_COLUMNS)),
            on=["season", "team_id", "player_id"],
            how="left",
        )
        .with_columns(
            pl.col("plate_appearances").fill_null(0).cast(pl.UInt32),
            pl.col("games_played").fill_null(0).cast(pl.UInt32),
        )
    )

    if enriched.is_empty():
        return _empty_output()

    primary = _pick_lineups(enriched)

    if primary.is_empty():
        return _empty_output()

    with_pa_per_game = primary.with_columns(
        pa_per_game=pl.when(pl.col("games_played") > 0)
        .then(pl.col("plate_appearances").cast(pl.Float64) / pl.col("games_played"))
        .otherwise(0.0)
    )

    ordered = with_pa_per_game.sort(
        ["season", "team_id", "pa_per_game", "plate_appearances", "player_id"],
        descending=[False, False, True, True, False],
    ).with_columns(
        lineup_position=(
            pl.int_range(1, pl.len() + 1).over(["season", "team_id"])
        ).cast(pl.UInt8)
    )

    return ordered.select(
        [
            pl.col("season").cast(pl.Int16),
            pl.col("team_id"),
            pl.col("lineup_position"),
            pl.col("fielding_position").cast(pl.UInt8),
            pl.col("player_id"),
        ]
    )


def _empty_output() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "season": pl.Int16,
            "team_id": pl.String,
            "lineup_position": pl.UInt8,
            "fielding_position": pl.UInt8,
            "player_id": pl.String,
        }
    )


def _pick_lineups(
    enriched: pl.DataFrame,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []

    for team_season in enriched.partition_by(
        ["season", "team_id"], maintain_order=True
    ):
        lineup = _build_team_season_lineup(team_season)
        if lineup is None:
            continue

        rows.extend(lineup)

    if not rows:
        return pl.DataFrame(
            schema={
                "season": pl.Int16,
                "team_id": pl.String,
                "player_id": pl.String,
                "fielding_position": pl.UInt8,
                "plate_appearances": pl.UInt32,
                "games_played": pl.UInt32,
            }
        )

    return pl.DataFrame(rows).select(
        pl.col("season").cast(pl.Int16),
        pl.col("team_id"),
        pl.col("player_id"),
        pl.col("fielding_position").cast(pl.UInt8),
        pl.col("plate_appearances").cast(pl.UInt32),
        pl.col("games_played").cast(pl.UInt32),
    )


def _build_team_season_lineup(
    team_season: pl.DataFrame,
) -> list[dict[str, object]] | None:
    player_rows = team_season.group_by("player_id").agg(
        pl.first("season").alias("season"),
        pl.first("team_id").alias("team_id"),
        pl.sum("games_at_position").alias("total_games"),
        pl.max("plate_appearances").alias("plate_appearances"),
        pl.max("games_played").alias("games_played"),
    )
    modal_positions = (
        team_season.sort(
            ["player_id", "games_at_position", "fielding_position"],
            descending=[False, True, False],
        )
        .group_by("player_id", maintain_order=True)
        .head(1)
        .select("player_id", "fielding_position", "games_at_position")
        .rename({"games_at_position": "games_at_assigned_position"})
    )
    players = (
        player_rows.join(modal_positions, on="player_id", how="inner")
        .sort(
            ["total_games", "plate_appearances", "player_id"],
            descending=[True, True, False],
        )
        .to_dicts()
    )

    if len(players) < len(_FIELDING_POSITIONS):
        return None

    lineup = players[: len(_FIELDING_POSITIONS)]
    candidate_rows = team_season.join(
        player_rows.select(
            "player_id", "total_games", "plate_appearances", "games_played"
        ),
        on="player_id",
        how="inner",
    )

    missing_positions = sorted(
        set(_FIELDING_POSITIONS)
        - {int(player["fielding_position"]) for player in lineup}
    )
    if missing_positions:
        for missing_position in missing_positions:
            position_counts: dict[int, int] = {}
            for player in lineup:
                fielding_position = int(player["fielding_position"])
                position_counts[fielding_position] = (
                    position_counts.get(fielding_position, 0) + 1
                )

            drop_candidates = sorted(
                (
                    player
                    for player in lineup
                    if position_counts[int(player["fielding_position"])] > 1
                ),
                key=lambda player: (
                    int(player["total_games"]),
                    int(player["plate_appearances"]),
                    str(player["player_id"]),
                ),
            )
            if not drop_candidates:
                return None

            replacement_pool = (
                candidate_rows.filter(
                    (pl.col("fielding_position") == missing_position)
                    & (
                        ~pl.col("player_id").is_in(
                            [str(player["player_id"]) for player in lineup]
                        )
                    )
                )
                .sort(
                    [
                        "games_at_position",
                        "total_games",
                        "plate_appearances",
                        "player_id",
                    ],
                    descending=[True, True, True, False],
                )
                .to_dicts()
            )
            if not replacement_pool:
                return None

            drop_player = drop_candidates[0]
            replacement = replacement_pool[0]
            lineup = [
                player
                for player in lineup
                if str(player["player_id"]) != str(drop_player["player_id"])
            ]
            lineup.append(
                {
                    "season": replacement["season"],
                    "team_id": replacement["team_id"],
                    "player_id": replacement["player_id"],
                    "total_games": replacement["total_games"],
                    "plate_appearances": replacement["plate_appearances"],
                    "games_played": replacement["games_played"],
                    "fielding_position": missing_position,
                    "games_at_assigned_position": replacement["games_at_position"],
                }
            )

    if len(lineup) != len(_FIELDING_POSITIONS):
        return None
    if len({str(player["player_id"]) for player in lineup}) != len(_FIELDING_POSITIONS):
        return None
    if {int(player["fielding_position"]) for player in lineup} != set(
        _FIELDING_POSITIONS
    ):
        return None

    return lineup
