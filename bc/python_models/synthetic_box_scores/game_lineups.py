from __future__ import annotations

from collections.abc import Iterable

import polars as pl

GAME_INPUT_COLUMNS: tuple[str, ...] = (
    "game_id",
    "season",
    "home_team_id",
    "away_team_id",
    "home_starting_pitcher_id",
    "away_starting_pitcher_id",
)

LINEUP_INPUT_COLUMNS: tuple[str, ...] = (
    "season",
    "team_id",
    "fielding_position",
    "player_id",
)

CANDIDATE_INPUT_COLUMNS: tuple[str, ...] = (
    "season",
    "team_id",
    "player_id",
    "fielding_position",
    "games_at_position",
    "plate_appearances",
    "games_played",
)

_FIELDING_POSITIONS: tuple[int, ...] = (1, 2, 3, 4, 5, 6, 7, 8, 9)
_SIDE_SPECS: tuple[tuple[str, str, str], ...] = (
    ("Away", "away_team_id", "away_starting_pitcher_id"),
    ("Home", "home_team_id", "home_starting_pitcher_id"),
)


def build_synthetic_batting_core(
    games: pl.DataFrame,
    lineups: pl.DataFrame,
    candidates: pl.DataFrame,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for lineup in _iter_game_side_lineups(games, lineups, candidates, use_dh=True):
        ordered = sorted(
            lineup,
            key=lambda row: (
                -_pa_per_game(row),
                -int(row["plate_appearances"]),
                str(row["player_id"]),
            ),
        )
        for lineup_position, row in enumerate(ordered, start=1):
            rows.append(
                {
                    "game_id": row["game_id"],
                    "batter_id": row["player_id"],
                    "side": row["side"],
                    "lineup_position": lineup_position,
                }
            )

    if not rows:
        return _empty_batting_core()

    return pl.DataFrame(rows).select(
        pl.col("game_id"),
        pl.col("batter_id"),
        pl.col("side"),
        pl.col("lineup_position").cast(pl.UInt8),
    )


def build_synthetic_fielding_core(
    games: pl.DataFrame,
    lineups: pl.DataFrame,
    candidates: pl.DataFrame,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for lineup in _iter_game_side_lineups(games, lineups, candidates, use_dh=False):
        for row in sorted(lineup, key=lambda item: int(item["fielding_position"])):
            rows.append(
                {
                    "game_id": row["game_id"],
                    "fielder_id": row["player_id"],
                    "side": row["side"],
                    "fielding_position": row["fielding_position"],
                }
            )

    if not rows:
        return _empty_fielding_core()

    return pl.DataFrame(rows).select(
        pl.col("game_id"),
        pl.col("fielder_id"),
        pl.col("side"),
        pl.col("fielding_position").cast(pl.UInt8),
    )


def _iter_game_side_lineups(
    games: pl.DataFrame,
    lineups: pl.DataFrame,
    candidates: pl.DataFrame,
    *,
    use_dh: bool,
) -> Iterable[list[dict[str, object]]]:
    _validate_columns(games, GAME_INPUT_COLUMNS)
    _validate_columns(lineups, LINEUP_INPUT_COLUMNS)
    _validate_columns(candidates, CANDIDATE_INPUT_COLUMNS)

    lineup_lookup = _lineup_lookup(lineups, candidates)
    candidate_lookup = _candidate_lookup(candidates)
    metrics_lookup = _metrics_lookup(candidates)

    for game in games.to_dicts():
        substitution_enabled = not bool(game.get("use_dh", False)) if use_dh else True
        for side, team_col, pitcher_col in _SIDE_SPECS:
            team_id = str(game[team_col])
            key = (int(game["season"]), team_id)
            base_lineup = lineup_lookup.get(key)
            if base_lineup is None:
                continue

            lineup = [
                {
                    **row,
                    "game_id": game["game_id"],
                    "side": side,
                }
                for row in base_lineup
            ]
            if substitution_enabled:
                lineup = _insert_starting_pitcher(
                    lineup,
                    candidate_lookup=candidate_lookup,
                    metrics_lookup=metrics_lookup,
                    listed_pitcher_id=game[pitcher_col],
                    season=key[0],
                    team_id=key[1],
                )
                if lineup is None:
                    continue

            yield lineup


def _insert_starting_pitcher(
    lineup: list[dict[str, object]],
    *,
    candidate_lookup: dict[tuple[int, str, int], list[dict[str, object]]],
    metrics_lookup: dict[tuple[int, str, str], dict[str, object]],
    listed_pitcher_id: object,
    season: int,
    team_id: str,
) -> list[dict[str, object]] | None:
    if listed_pitcher_id is None:
        return lineup

    pitcher_id = str(listed_pitcher_id)
    pitcher_index = _index_for_position(lineup, 1)
    if pitcher_index is None:
        return None

    modal_pitcher_id = str(lineup[pitcher_index]["player_id"])
    if pitcher_id == modal_pitcher_id:
        return lineup

    existing_index = _index_for_player(lineup, pitcher_id)
    pitcher_row = _lineup_row_for_player(
        lineup[pitcher_index], pitcher_id, metrics_lookup, season, team_id
    )
    lineup[pitcher_index] = {
        **pitcher_row,
        "game_id": lineup[pitcher_index]["game_id"],
        "side": lineup[pitcher_index]["side"],
    }

    if existing_index is None:
        return lineup

    occupied = {
        str(row["player_id"])
        for index, row in enumerate(lineup)
        if index != existing_index
    }
    vacated_position = int(lineup[existing_index]["fielding_position"])
    replacement = _next_candidate(
        candidate_lookup.get((season, team_id, vacated_position), []),
        occupied,
    )
    if replacement is None:
        return None

    lineup[existing_index] = {
        **replacement,
        "game_id": lineup[existing_index]["game_id"],
        "side": lineup[existing_index]["side"],
        "fielding_position": vacated_position,
    }
    return lineup


def _lineup_lookup(
    lineups: pl.DataFrame,
    candidates: pl.DataFrame,
) -> dict[tuple[int, str], list[dict[str, object]]]:
    metrics = _metrics_lookup(candidates)
    lookup: dict[tuple[int, str], list[dict[str, object]]] = {}
    for row in lineups.to_dicts():
        season = int(row["season"])
        team_id = str(row["team_id"])
        player_id = str(row["player_id"])
        lookup.setdefault((season, team_id), []).append(
            _lineup_row_for_player(row, player_id, metrics, season, team_id)
        )

    return {
        key: sorted(rows, key=lambda row: int(row["fielding_position"]))
        for key, rows in lookup.items()
        if len(rows) == len(_FIELDING_POSITIONS)
    }


def _candidate_lookup(
    candidates: pl.DataFrame,
) -> dict[tuple[int, str, int], list[dict[str, object]]]:
    with_total_games = candidates.with_columns(
        pl.sum("games_at_position")
        .over(["season", "team_id", "player_id"])
        .alias("total_games")
    )
    lookup: dict[tuple[int, str, int], list[dict[str, object]]] = {}
    for row in with_total_games.to_dicts():
        key = (int(row["season"]), str(row["team_id"]), int(row["fielding_position"]))
        lookup.setdefault(key, []).append(
            {
                "player_id": str(row["player_id"]),
                "plate_appearances": int(row["plate_appearances"]),
                "games_played": int(row["games_played"]),
                "games_at_position": int(row["games_at_position"]),
                "total_games": int(row["total_games"]),
            }
        )

    for rows in lookup.values():
        rows.sort(
            key=lambda row: (
                -int(row["games_at_position"]),
                -int(row["total_games"]),
                -int(row["plate_appearances"]),
                str(row["player_id"]),
            )
        )

    return lookup


def _metrics_lookup(
    candidates: pl.DataFrame,
) -> dict[tuple[int, str, str], dict[str, object]]:
    if candidates.is_empty():
        return {}

    metrics = candidates.group_by(["season", "team_id", "player_id"]).agg(
        pl.max("plate_appearances").alias("plate_appearances"),
        pl.max("games_played").alias("games_played"),
    )
    return {
        (int(row["season"]), str(row["team_id"]), str(row["player_id"])): {
            "plate_appearances": int(row["plate_appearances"]),
            "games_played": int(row["games_played"]),
        }
        for row in metrics.to_dicts()
    }


def _lineup_row_for_player(
    row: dict[str, object],
    player_id: str,
    metrics_lookup: dict[tuple[int, str, str], dict[str, object]],
    season: int,
    team_id: str,
) -> dict[str, object]:
    metrics = metrics_lookup.get(
        (season, team_id, player_id),
        {"plate_appearances": 0, "games_played": 0},
    )
    return {
        "season": season,
        "team_id": team_id,
        "player_id": player_id,
        "fielding_position": int(row["fielding_position"]),
        "plate_appearances": int(metrics["plate_appearances"]),
        "games_played": int(metrics["games_played"]),
    }


def _next_candidate(
    candidates: list[dict[str, object]],
    occupied: set[str],
) -> dict[str, object] | None:
    for candidate in candidates:
        if str(candidate["player_id"]) not in occupied:
            return candidate
    return None


def _index_for_position(
    lineup: list[dict[str, object]],
    fielding_position: int,
) -> int | None:
    for index, row in enumerate(lineup):
        if int(row["fielding_position"]) == fielding_position:
            return index
    return None


def _index_for_player(
    lineup: list[dict[str, object]],
    player_id: str,
) -> int | None:
    for index, row in enumerate(lineup):
        if str(row["player_id"]) == player_id:
            return index
    return None


def _pa_per_game(row: dict[str, object]) -> float:
    games_played = int(row["games_played"])
    if games_played == 0:
        return 0.0
    return int(row["plate_appearances"]) / games_played


def _validate_columns(frame: pl.DataFrame, columns: tuple[str, ...]) -> None:
    missing = set(columns) - set(frame.columns)
    if missing:
        raise ValueError(f"missing columns: {sorted(missing)}")


def _empty_batting_core() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "game_id": pl.String,
            "batter_id": pl.String,
            "side": pl.String,
            "lineup_position": pl.UInt8,
        }
    )


def _empty_fielding_core() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "game_id": pl.String,
            "fielder_id": pl.String,
            "side": pl.String,
            "fielding_position": pl.UInt8,
        }
    )
