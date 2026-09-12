from __future__ import annotations

from pathlib import Path
from typing import cast

import duckdb
import polars as pl

from python_models.statistical.backtests.geometry_air_bridge_check import (
    standardize_paired,
)
from python_models.statistical.backtests.geometry_reliability_data import (
    BASE,
    DATABASE,
    SCHEMA,
)
from python_models.statistical.backtests.geometry_statcast_targets import (
    RECORDED_AIR_SUBTYPES,
    recorded_air_subtype,
)

CLASSES = RECORDED_AIR_SUBTYPES
PIPELINES: dict[str, tuple[int, int]] = {
    "A": (1989, 2008),
    "B": (2009, 2019),
    "C": (2020, 2100),
}
REFERENCE_SEASONS: dict[str, tuple[int, ...]] = {
    "B": (2015, 2016, 2017, 2018, 2019),
    "C": (2023, 2025),
}
RESULT_GROUPS = ("hit", "out_in_play", "other")
FIELDER_GROUPS = ("infield", "outfield", "none")
DEPTH_GROUPS = ("shallow", "deep", "default")
CLUE_LEVELS = tuple(
    f"{result}|{fielder}|{depth}"
    for result in RESULT_GROUPS
    for fielder in FIELDER_GROUPS
    for depth in DEPTH_GROUPS
)
COVERAGE_FRAME = (
    BASE
    / "geometry_reliability/20260911-air-development-full-v1/coverage_frame.parquet"
)
ACQUISITION_ROOT = (
    BASE / "geometry_reliability/20260911-statcast-bridge-fitting-2016-2018-v1"
)
RESERVE = BASE / "historical_stress/20260911-reserve-v1/reserved_games.parquet"
CLUE_QUERY = f"""
WITH trajectory AS (
    SELECT game_id, event_key, batted_to_fielder
    FROM {SCHEMA}.model_input_geometry
    WHERE geometry_dimension = 'trajectory'
      AND game_id IN (SELECT game_id FROM clue_games)
), depth AS (
    SELECT game_id, event_key, class AS depth
    FROM {SCHEMA}.model_input_geometry
    WHERE geometry_dimension = 'location_depth'
      AND observed_status = 'observed'
      AND game_id IN (SELECT game_id FROM clue_games)
)
SELECT trajectory.game_id, CAST(trajectory.event_key AS BIGINT) AS event_key,
    CAST(trajectory.batted_to_fielder AS INTEGER) AS batted_to_fielder,
    depth.depth
FROM trajectory LEFT JOIN depth USING (game_id, event_key)
"""
HISTORICAL_QUERY = f"""
WITH air AS (
    SELECT game_id, event_key, season, raw_value AS recorded_air_subtype,
        result_family, CAST(batted_to_fielder AS INTEGER) AS batted_to_fielder
    FROM {SCHEMA}.model_input_geometry
    WHERE geometry_dimension = 'trajectory'
      AND observed_status = 'observed'
      AND raw_value IN ({", ".join(f"'{name}'" for name in RECORDED_AIR_SUBTYPES)})
      AND game_type = 'RegularSeason'
      AND primary_fold = 'TRAIN'
      AND game_id NOT IN (SELECT game_id FROM reserve_games)
      AND season BETWEEN ? AND ?
), depth AS (
    SELECT game_id, event_key, class AS depth
    FROM {SCHEMA}.model_input_geometry
    WHERE geometry_dimension = 'location_depth' AND observed_status = 'observed'
)
SELECT air.season, air.recorded_air_subtype, air.result_family,
    air.batted_to_fielder, depth.depth, COUNT(*) AS n
FROM air LEFT JOIN depth USING (game_id, event_key)
GROUP BY 1, 2, 3, 4, 5
"""


def pipeline_of(season: int) -> str:
    for name, (first, last) in PIPELINES.items():
        if first <= season <= last:
            return name
    raise ValueError(f"season {season} belongs to no declared pipeline")


def clue_label(
    result_family: str | None, batted_to_fielder: int | None, depth: str | None
) -> str:
    result = result_family if result_family in {"hit", "out_in_play"} else "other"
    if batted_to_fielder is None or not 1 <= batted_to_fielder <= 9:
        fielder = "none"
    elif batted_to_fielder <= 6:
        fielder = "infield"
    else:
        fielder = "outfield"
    if depth == "Shallow":
        depth_group = "shallow"
    elif depth in {"Deep", "ExtraDeep"}:
        depth_group = "deep"
    else:
        depth_group = "default"
    return f"{result}|{fielder}|{depth_group}"


def with_clues(frame: pl.DataFrame) -> pl.DataFrame:
    fielder = pl.col("batted_to_fielder")
    return frame.with_columns(
        pl.concat_str(
            pl.when(pl.col("result_family").is_in(["hit", "out_in_play"]))
            .then(pl.col("result_family"))
            .otherwise(pl.lit("other")),
            pl.lit("|"),
            pl.when(fielder.is_between(1, 6))
            .then(pl.lit("infield"))
            .when(fielder.is_between(7, 9))
            .then(pl.lit("outfield"))
            .otherwise(pl.lit("none")),
            pl.lit("|"),
            pl.when(pl.col("depth") == "Shallow")
            .then(pl.lit("shallow"))
            .when(pl.col("depth").is_in(["Deep", "ExtraDeep"]))
            .then(pl.lit("deep"))
            .otherwise(pl.lit("default")),
        ).alias("clue")
    )


def load_clue_columns(games: list[str], *, database: Path) -> pl.DataFrame:
    with duckdb.connect(str(database), read_only=True) as connection:
        connection.register("clue_games", pl.DataFrame({"game_id": games}))
        return connection.execute(CLUE_QUERY).pl()


def development_reference_rows(coverage_frame: Path) -> pl.DataFrame:
    frame = pl.read_parquet(coverage_frame)
    subtypes = [
        recorded_air_subtype(value)
        for value in cast(list[str | None], frame["recorded_class"].to_list())
    ]
    return (
        frame.with_columns(
            pl.Series("recorded_air_subtype", subtypes, dtype=pl.String),
            pl.col("event_key").cast(pl.Int64),
            pl.col("season").cast(pl.Int64),
        )
        .filter(
            pl.col("recorded_air_subtype").is_not_null()
            & (pl.col("target_status") == "air_angle_standardized")
            & pl.col("target_class").is_not_null()
        )
        .select(
            "event_key",
            "game_id",
            "season",
            "recorded_air_subtype",
            "result_family",
            "target_class",
        )
        .with_columns(pl.lit("development_frame").alias("source"))
    )


def acquisition_reference_rows(acquisition_root: Path) -> pl.DataFrame:
    frame = standardize_paired(
        pl.read_parquet(acquisition_root / "paired_events.parquet")
    )
    return (
        frame.filter(pl.col("eligible"))
        .select(
            pl.col("event_key").cast(pl.Int64),
            "game_id",
            pl.col("season").cast(pl.Int64),
            "recorded_air_subtype",
            "result_family",
            "target_class",
        )
        .with_columns(pl.lit("bridge_acquisition").alias("source"))
    )


def load_reference_frame(
    *,
    coverage_frame: Path = COVERAGE_FRAME,
    acquisition_root: Path = ACQUISITION_ROOT,
    database: Path = DATABASE,
) -> pl.DataFrame:
    rows = pl.concat(
        [
            development_reference_rows(coverage_frame),
            acquisition_reference_rows(acquisition_root),
        ],
        how="vertical",
    )
    if rows["event_key"].n_unique() != rows.height:
        raise ValueError("reference rows must have unique event keys")
    seasons = cast(list[int], rows["season"].unique().to_list())
    expected = {season for values in REFERENCE_SEASONS.values() for season in values}
    if set(seasons) != expected:
        raise ValueError(
            f"reference seasons {sorted(seasons)} differ from {sorted(expected)}"
        )
    if not set(rows["target_class"].to_list()) <= set(CLASSES):
        raise ValueError("reference targets must be airborne bands")
    if any(not value for value in rows["result_family"].to_list()):
        raise ValueError("result families must be nonempty")
    clues = load_clue_columns(
        cast(list[str], rows["game_id"].unique().to_list()), database=database
    )
    joined = rows.join(clues, on=["game_id", "event_key"], how="left")
    if joined.height != rows.height:
        raise ValueError("clue join changed the reference row count")
    return with_clues(joined).with_columns(
        pl.Series(
            "pipeline",
            [pipeline_of(int(season)) for season in joined["season"].to_list()],
            dtype=pl.String,
        )
    )


def load_historical_counts(
    seasons: tuple[int, int],
    *,
    database: Path = DATABASE,
    reserve: Path = RESERVE,
) -> pl.DataFrame:
    with duckdb.connect(str(database), read_only=True) as connection:
        connection.register(
            "reserve_games", pl.read_parquet(reserve, columns=["game_id"])
        )
        frame = connection.execute(HISTORICAL_QUERY, list(seasons)).pl()
    return (
        with_clues(frame.with_columns(pl.col("season").cast(pl.Int64)))
        .group_by("season", "recorded_air_subtype", "clue")
        .agg(pl.col("n").cast(pl.Int64).sum())
        .sort("season", "recorded_air_subtype", "clue")
    )
