from __future__ import annotations

import argparse
import hashlib
import logging
import platform
from pathlib import Path
from typing import cast

import duckdb
import polars as pl

from python_models.statistical.backtests.geometry_reliability_data import (
    BASE,
    DATABASE,
    SCHEMA,
    write_json,
)
from python_models.statistical.evidence_binding import file_digest

LOGGER = logging.getLogger(__name__)
RESERVE = BASE / "historical_stress/20260911-reserve-v1/reserved_games.parquet"
RESERVE_FILE_SHA256 = "fa6e83509bec2b412781340c4e66cd940159ada4d95de8e4fe5b3fba2d325a35"
RESERVE_GAME_DIGEST = "5ac641fe78b19e98ec5cb576ebb85e652ff289e445e0f6e54e109c523a614dba"
PREVIOUS_BRIDGE_GAMES = frozenset({"HOU202504190", "KCA202504070", "NYN202508130"})
SEASONS = (2016, 2017, 2018)
SMOKE_GAMES_PER_SEASON = 2
SELECTION_PREFIX = "geometry-statcast-bridge-selection-v1:"
SMOKE_PREFIX = "geometry-statcast-bridge-smoke-v1:"
ARTIFACT_ID = "20260911-statcast-bridge-selection-v1"
QUERY = f"""SELECT
    g.game_id,
    g.game_key,
    g.season,
    CAST(g.park_id AS VARCHAR) AS park_id,
    g.date,
    g.start_time,
    CAST(g.home_team_id AS VARCHAR) AS home_team_id,
    CAST(g.away_team_id AS VARCHAR) AS away_team_id,
    CAST(RIGHT(g.game_id, 1) AS UTINYINT) AS game_number,
    CAST(g.doubleheader_status AS VARCHAR) AS doubleheader_status,
    CAST(g.game_type AS VARCHAR) AS game_type,
    CAST(g.account_type AS VARCHAR) AS account_type,
    g.source_type,
    g.filename,
    'TRAIN' AS primary_fold
FROM main_models.stg_games AS g
INNER JOIN (
    SELECT DISTINCT game_id
    FROM {SCHEMA}.model_input_geometry
    WHERE primary_fold = 'TRAIN'
) AS train USING (game_id)
WHERE g.season IN {SEASONS}
  AND g.game_type = 'RegularSeason'
  AND g.account_type = 'PlayByPlay'
  AND g.source_type = 'PlayByPlay'
  AND g.doubleheader_status = 'SingleGame'
ORDER BY g.season, park_id, g.game_id"""


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def selection_hash(game: str) -> str:
    return sha256_text(SELECTION_PREFIX + game)


def smoke_hash(game: str) -> str:
    return sha256_text(SMOKE_PREFIX + game)


def game_digest(games: list[str]) -> str:
    digest = hashlib.sha256()
    for game in sorted(set(games)):
        encoded = game.encode()
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def assign_selection(
    source: pl.DataFrame,
    *,
    excluded_games: set[str],
    smoke_games_per_season: int = SMOKE_GAMES_PER_SEASON,
) -> pl.DataFrame:
    eligible = (
        source.filter(~pl.col("game_id").is_in(sorted(excluded_games)))
        .with_columns(
            pl.col("game_id")
            .map_elements(selection_hash, return_dtype=pl.String)
            .alias("selection_sha256"),
            pl.col("game_id")
            .map_elements(smoke_hash, return_dtype=pl.String)
            .alias("smoke_sha256"),
        )
        .sort("season", "smoke_sha256", "game_id")
        .with_columns(
            pl.int_range(1, pl.len() + 1)
            .over("season")
            .cast(pl.UInt16)
            .alias("smoke_rank")
        )
    )
    if eligible.is_empty() or eligible["game_id"].n_unique() != eligible.height:
        raise ValueError("selection source is empty or has duplicate games")
    if set(eligible["primary_fold"].to_list()) != {"TRAIN"}:
        raise ValueError("selection source is not restricted to TRAIN")
    return (
        eligible.with_columns(
            pl.lit("fitting").alias("acquisition_fold"),
            (pl.col("smoke_rank") <= smoke_games_per_season).alias("is_smoke"),
        )
        .with_columns(
            pl.when(pl.col("is_smoke"))
            .then(pl.lit("mechanics_fit"))
            .otherwise(pl.lit("full_fit_after_mechanics"))
            .alias("acquisition_stage")
        )
        .sort("season", "park_id", "game_id")
    )


def build_selection(
    output: Path,
    *,
    database: Path = DATABASE,
    reserve: Path = RESERVE,
    reserve_file_sha256: str = RESERVE_FILE_SHA256,
    reserve_game_digest: str = RESERVE_GAME_DIGEST,
) -> None:
    if file_digest(reserve) != reserve_file_sha256:
        raise ValueError("reserve file digest mismatch")
    reserve_games = cast(
        list[str], pl.read_parquet(reserve, columns=["game_id"])["game_id"].to_list()
    )
    if game_digest(reserve_games) != reserve_game_digest:
        raise ValueError("reserve game ID digest mismatch")
    with duckdb.connect(str(database), read_only=True) as connection:
        source = connection.execute(QUERY).pl()
    source_games = cast(list[str], source["game_id"].to_list())
    reserve_overlap = sorted(set(source_games) & set(reserve_games))
    prior_overlap = sorted(set(source_games) & PREVIOUS_BRIDGE_GAMES)
    selected = assign_selection(
        source, excluded_games=set(reserve_games) | PREVIOUS_BRIDGE_GAMES
    )
    if set(selected["game_id"].to_list()) & set(reserve_games):
        raise ValueError("selected games overlap reserve")
    smoke = selected.filter(pl.col("is_smoke"))
    if smoke.group_by("season").len()["len"].to_list() != [
        SMOKE_GAMES_PER_SEASON
    ] * len(SEASONS):
        raise ValueError("unexpected smoke selection count")
    output.mkdir(parents=True, exist_ok=False)
    selected.write_parquet(output / "selected_games.parquet")
    (output / "selection.sql").write_text(QUERY + "\n")
    per_season = {
        str(season): int(count)
        for season, count in selected.group_by("season")
        .len()
        .sort("season")
        .iter_rows()
    }
    write_json(
        output / "report.json",
        {
            "artifact_id": ARTIFACT_ID,
            "selection_prefix": SELECTION_PREFIX,
            "smoke_prefix": SMOKE_PREFIX,
            "seasons": list(SEASONS),
            "source_games_before_exclusions": source.height,
            "reserve_overlap_before_exclusion": len(reserve_overlap),
            "prior_bridge_overlap_before_exclusion": prior_overlap,
            "selected_games": selected.height,
            "selected_games_by_season": per_season,
            "smoke_games": smoke.height,
            "smoke_game_ids": sorted(smoke["game_id"].to_list()),
            "selected_reserve_overlap": 0,
            "selected_game_ids_sha256_length_prefixed_utf8": game_digest(
                cast(list[str], selected["game_id"].to_list())
            ),
            "query_sha256": sha256_text(QUERY),
            "reserve_binding": {
                "file_sha256": reserve_file_sha256,
                "sorted_game_ids_sha256_length_prefixed_utf8": reserve_game_digest,
                "games": len(reserve_games),
            },
            "source_binding": {
                "database": str(database),
                "database_size_bytes": database.stat().st_size,
                "schema": SCHEMA,
            },
            "label_access": {
                "play_or_angle_labels_read": False,
                "recorded_label_exposure": "Every selected game is PRIMARY TRAIN, so existing recorded trajectory labels are already development-exposed.",
            },
            "runtime": {
                "python": platform.python_version(),
                "duckdb": duckdb.__version__,
                "polars": pl.__version__,
            },
        },
    )
    write_json(
        output / "manifest.json",
        {
            "files_sha256": {
                path.name: file_digest(path)
                for path in sorted(output.iterdir())
                if path.name != "manifest.json"
            }
        },
    )
    LOGGER.info(
        "Selected %d games (%d smoke) into %s", selected.height, smoke.height, output
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output", type=Path, default=BASE / "geometry_reliability" / ARTIFACT_ID
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    build_selection(cast(Path, args.output))


if __name__ == "__main__":
    main()
